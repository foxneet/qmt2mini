# -*- coding: utf-8 -*-
"""
qmt2mini 服务端 —— 把大QMT改造成miniQMT的中枢（定位：局域网行情中心 + 交易节点）

架构: 外部Python(shim, 本机或局域网) ⇄ 本服务端 ⇄ 大QMT内置桥策略(主动连入)
协议: TCP 长度前缀(4字节小端) + UTF-8 JSON

安全模型:
  - 默认只监听 127.0.0.1；--lan 才绑定 0.0.0.0（局域网开放必须显式）
  - --token        连接令牌：设置后所有连入方(客户端/桥)必须携带
  - --order-token  下单令牌：设置后只有携带该令牌的客户端能转发下单/撤单
  - --enable-order 下单总开关：不开则任何客户端都无法下单（服务端门控）

运行: py -3.13 qmt2mini_server.py [--port 51880] [--lan] [--token XX] [--order-token YY] [--enable-order]
"""
import argparse
import itertools
import json
import socket
import struct
import threading
import time

PROTO_VER = 1
MAX_FRAME = 64 * 1024 * 1024
BRIDGE_IDLE_TIMEOUT = 60          # 桥超过此秒数无任何消息判失联
ORDER_OPS = {'order_stock', 'order_stock_async', 'cancel_order_stock'}


# ---------- 帧收发 ----------
def recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('对端关闭')
        buf += chunk
    return buf


def recv_msg(sock):
    (ln,) = struct.unpack('<I', recv_exact(sock, 4))
    if ln > MAX_FRAME:
        raise ConnectionError('帧过大: %d' % ln)
    return json.loads(recv_exact(sock, ln).decode('utf-8'))


class Sender:
    """带发送锁的消息端"""
    def __init__(self, sock, peer):
        self.sock = sock
        self.peer = peer
        self.send_lock = threading.Lock()
        self.alive = True
        self.can_order = False     # 客户端鉴权后由服务端赋值

    def send(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        try:
            with self.send_lock:
                self.sock.sendall(struct.pack('<I', len(data)) + data)
        except OSError:
            self.alive = False
            raise

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except OSError:
            pass


def local_ipv4s():
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith('127.'):
                ips.add(ip)
    except OSError:
        pass
    return sorted(ips)


# ---------- 服务端 ----------
class Server:
    def __init__(self, host, port, enable_order, token, order_token):
        self.host = host
        self.port = port
        self.enable_order = enable_order
        self.token = token
        self.order_token = order_token
        self.bridge = None                     # Sender
        self.bridge_lock = threading.Lock()
        self.bridge_last_seen = 0.0
        self.bridge_queue = []                 # 桥短连接模式下暂存请求 (msg, cli, orig_id, deadline)
        self.bridge_queue_lock = threading.Lock()
        self.clients = {}                      # id -> Sender
        self.clients_lock = threading.Lock()
        self.pending = {}                      # server_id -> (client Sender, orig_id, deadline)
        self.pending_lock = threading.Lock()
        self.req_ids = itertools.count(1)
        self.client_ids = itertools.count(1)

    # --- 鉴权 ---
    def _auth_ok(self, hello):
        if self.token and hello.get('token') != self.token:
            return False
        return True

    # --- 客户端消息 ---
    def on_client_msg(self, cli, msg):
        t = msg.get('t')
        if t == 'req':
            self.forward_request(cli, msg)
        elif t == 'ping':
            cli.send({'t': 'pong'})
        else:
            cli.send({'t': 'resp', 'id': msg.get('id', 0), 'ok': False,
                      'err': '未知消息类型: %r' % t})

    def forward_request(self, cli, msg):
        op = msg.get('op', '')
        orig_id = msg.get('id', 0)
        if op in ORDER_OPS:
            if not self.enable_order:
                cli.send({'t': 'resp', 'id': orig_id, 'ok': False,
                          'err': '下单功能未开启：服务端需以 --enable-order 启动（安全门控）'})
                return
            if self.order_token and not cli.can_order:
                cli.send({'t': 'resp', 'id': orig_id, 'ok': False,
                          'err': '下单被拒：该连接未持有下单令牌(--order-token)'})
                return
        with self.bridge_lock:
            bridge = self.bridge
        if bridge is None or not bridge.alive:
            # 桥为每秒短连接: 请求入队, 桥下一连接周期冲账
            deadline = time.time() + float(msg.get('timeout', 60))
            with self.bridge_queue_lock:
                if len(self.bridge_queue) > 200:
                    self.bridge_queue.pop(0)
                self.bridge_queue.append((msg, cli, orig_id, deadline))
            return
        sid = next(self.req_ids)
        deadline = time.time() + float(msg.get('timeout', 60))
        with self.pending_lock:
            self.pending[sid] = (cli, orig_id, deadline)
        try:
            bridge.send({'t': 'req', 'id': sid, 'op': op, 'args': msg.get('args') or {}})
        except OSError:
            with self.pending_lock:
                self.pending.pop(sid, None)
            cli.send({'t': 'resp', 'id': orig_id, 'ok': False, 'err': '桥发送失败'})

    def on_bridge_msg(self, bridge, msg):
        self.bridge_last_seen = time.time()
        t = msg.get('t')
        if t == 'resp':
            sid = msg.get('id')
            with self.pending_lock:
                entry = self.pending.pop(sid, None)
            if entry:
                cli, orig_id, _ = entry
                out = dict(msg)
                out['id'] = orig_id
                if cli.alive:
                    try:
                        cli.send(out)
                    except OSError:
                        pass
        elif t == 'push':
            self.broadcast(msg)
        elif t == 'ping':
            bridge.send({'t': 'pong'})
        # pong 忽略

    def broadcast(self, msg):
        with self.clients_lock:
            targets = [c for c in self.clients.values() if c.alive]
        for cli in targets:
            try:
                cli.send(msg)
            except OSError:
                pass

    # --- 连接生命周期 ---
    def watchdog(self):
        while True:
            time.sleep(5)
            now = time.time()
            # 清理超时的在途请求
            with self.pending_lock:
                expired = [sid for sid, (_, _, dl) in self.pending.items() if now > dl]
                entries = [self.pending.pop(sid) for sid in expired]
            for cli, orig_id, _ in entries:
                if cli.alive:
                    try:
                        cli.send({'t': 'resp', 'id': orig_id, 'ok': False, 'err': '桥响应超时'})
                    except OSError:
                        pass
            # 清理排队中超过期限的请求(桥短连接模式下可能积压)
            with self.bridge_queue_lock:
                expired_q = [x for x in self.bridge_queue if now > x[3]]
                self.bridge_queue = [x for x in self.bridge_queue if now <= x[3]]
            for msg, cli, orig_id, _dl in expired_q:
                if cli.alive:
                    try:
                        cli.send({'t': 'resp', 'id': orig_id, 'ok': False, 'err': '排队等待桥超时'})
                    except OSError:
                        pass

    def set_bridge(self, sender):
        with self.bridge_lock:
            old = self.bridge
            self.bridge = sender
            self.bridge_last_seen = time.time()
        if old is not None and old is not sender:
            old.close()

    @staticmethod
    def log(s):
        print(time.strftime('[%H:%M:%S] ') + s, flush=True)

    def serve_conn(self, sock, addr):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        peer = Sender(sock, addr)
        try:
            hello = recv_msg(sock)
        except (ConnectionError, OSError, ValueError):
            peer.close()
            return
        role = hello.get('role')
        if not self._auth_ok(hello):
            self.log('鉴权失败 来自 %s (token不匹配)' % (addr,))
            try:
                peer.send({'t': 'resp', 'id': 0, 'ok': False, 'err': 'token不匹配'})
            except OSError:
                pass
            peer.close()
            return
        if role == 'bridge':
            now_gap = time.time() - self.bridge_last_seen
            self.bridge_last_seen = time.time()
            if now_gap > 10:                     # 短连接模式: 只在真实离线后重连才打印
                self.log('桥已连入 来自 %s' % (addr,))
            self.set_bridge(peer)
            self.flush_bridge_queue(peer)
            self.bridge_loop(peer)
        elif role == 'client':
            # 下单权限：持 order_token（未设置 order_token 时跟随总开关）
            peer.can_order = (not self.order_token) or (hello.get('order_token') == self.order_token)
            cid = next(self.client_ids)
            with self.clients_lock:
                self.clients[cid] = peer
            self.log('客户端#%d 连入 %s (下单:%s, 当前%d个)'
                     % (cid, addr, '可' if peer.can_order else '否', len(self.clients)))
            try:
                while True:
                    msg = recv_msg(sock)
                    if not peer.alive:
                        break
                    self.on_client_msg(peer, msg)
            except (ConnectionError, OSError, ValueError):
                pass
            with self.clients_lock:
                self.clients.pop(cid, None)
            self.log('客户端#%d 断开 (剩%d个)' % (cid, len(self.clients)))
        peer.close()

    def flush_bridge_queue(self, bridge):
        now = time.time()
        with self.bridge_queue_lock:
            items = self.bridge_queue
            self.bridge_queue = []
        for msg, cli, orig_id, deadline in items:
            if not cli.alive:
                continue
            if now > deadline:
                try:
                    cli.send({'t': 'resp', 'id': orig_id, 'ok': False, 'err': '排队等待桥超时'})
                except OSError:
                    pass
                continue
            sid = next(self.req_ids)
            with self.pending_lock:
                self.pending[sid] = (cli, orig_id, deadline)
            try:
                bridge.send({'t': 'req', 'id': sid, 'op': msg.get('op'),
                             'args': msg.get('args') or {}})
            except OSError:
                with self.pending_lock:
                    self.pending.pop(sid, None)

    def bridge_loop(self, peer):
        try:
            while True:
                msg = recv_msg(peer.sock)
                self.on_bridge_msg(peer, msg)
        except (ConnectionError, OSError, ValueError):
            pass
        with self.bridge_lock:
            if self.bridge is peer:              # 短连接模式: 静默清理, 不广播
                self.bridge = None

    def run(self):
        self.log('qmt2mini 服务端 启动 proto=v%d 绑定=%s:%d 下单=%s'
                 % (PROTO_VER, self.host, self.port,
                    '开启' if self.enable_order else '锁定(--enable-order可开)'))
        if self.host != '127.0.0.1':
            self.log('⚠ 局域网模式：本机地址 %s（其他机器 shim 连接任一即可）' % (local_ipv4s() or '?',))
            if not self.token:
                self.log('⚠ 警告：局域网开放但未设置 --token，同网段任何主机都可连入！')
        threading.Thread(target=self.watchdog, daemon=True).start()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(16)
        self.log('监听 %s:%d 等待桥策略与外部脚本连入...' % (self.host, self.port))
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=self.serve_conn, args=(sock, addr), daemon=True).start()


def main():
    ap = argparse.ArgumentParser(description='qmt2mini 服务端（大QMT→miniQMT改造中枢 / 局域网行情交易节点）')
    ap.add_argument('--port', type=int, default=51880)
    ap.add_argument('--lan', action='store_true',
                    help='绑定0.0.0.0开放局域网（默认仅127.0.0.1；建议配合 --token）')
    ap.add_argument('--token', default='', help='连接令牌（设置后连入方必须携带）')
    ap.add_argument('--order-token', default='', help='下单令牌（设置后仅持有者可下单/撤单）')
    ap.add_argument('--enable-order', action='store_true',
                    help='开启下单/撤单转发（默认锁定，安全门控）')
    args = ap.parse_args()
    host = '0.0.0.0' if args.lan else '127.0.0.1'
    try:
        Server(host, args.port, args.enable_order, args.token, args.order_token).run()
    except KeyboardInterrupt:
        print('\n退出')


if __name__ == '__main__':
    main()
