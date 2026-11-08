# -*- coding: utf-8 -*-
"""qmt2mini 连接层（外部脚本侧，py3.13，可用线程）"""
import itertools
import json
import os
import socket
import struct
import threading

PROTO_VER = 1
MAX_FRAME = 64 * 1024 * 1024

def _auto_token():
    t = os.environ.get('QMT2MINI_TOKEN')
    if t:
        return t
    try:
        p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         '转发器_配置.json')
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f).get('token', '')
    except Exception:
        return ''


DEFAULT_HOST = os.environ.get('QMT2MINI_HOST', '127.0.0.1')
DEFAULT_PORT = int(os.environ.get('QMT2MINI_PORT', '51880'))
DEFAULT_TOKEN = _auto_token()
DEFAULT_ORDER_TOKEN = os.environ.get('QMT2MINI_ORDER_TOKEN', '')


class BridgeError(Exception):
    """桥/服务端返回的业务错误"""


class ConnError(Exception):
    """连接层错误（超时/断开）"""


def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('对端关闭')
        buf += chunk
    return buf


def _recv_msg(sock):
    (ln,) = struct.unpack('<I', _recv_exact(sock, 4))
    if ln > MAX_FRAME:
        raise ConnectionError('帧过大: %d' % ln)
    return json.loads(_recv_exact(sock, ln).decode('utf-8'))


class Conn:
    """与服务端的长连接：同步request + 推送分发"""

    def __init__(self, host=None, port=None, token=None, order_token=None):
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.token = token if token is not None else DEFAULT_TOKEN
        self.order_token = order_token if order_token is not None else DEFAULT_ORDER_TOKEN
        self._sock = None
        self._send_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._futs = {}
        self._ids = itertools.count(1)
        self._push_handlers = []

    def is_connected(self):
        return self._sock is not None

    def add_push_handler(self, fn):
        self._push_handlers.append(fn)

    def connect(self):
        with self._state_lock:
            if self._sock is not None:
                return
            try:
                s = socket.create_connection((self.host, self.port), timeout=5)
            except OSError as e:
                raise ConnError('无法连接 qmt2mini 服务端 %s:%d（%s）——请先运行 qmt2mini_server.py'
                                % (self.host, self.port, e))
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(None)
            self._sock = s
            hello = {'t': 'hello', 'role': 'client', 'proto': PROTO_VER}
            if self.token:
                hello['token'] = self.token
            if self.order_token:
                hello['order_token'] = self.order_token
            try:
                self._send(hello)
            except OSError:
                self._drop()
                raise ConnError('握手发送失败')
            threading.Thread(target=self._recv_loop, daemon=True).start()

    def disconnect(self):
        self._drop()

    def _drop(self):
        with self._state_lock:
            sock, self._sock = self._sock, None
            futs = list(self._futs.values())
            self._futs.clear()
        if sock:
            try:
                sock.close()
            except OSError:
                pass
        for ev, box in futs:
            box.setdefault('ok', False)
            box.setdefault('err', '连接已断开')
            ev.set()

    def _send(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        with self._send_lock:
            self._sock.sendall(struct.pack('<I', len(data)) + data)

    def _recv_loop(self):
        sock = self._sock
        try:
            while True:
                msg = _recv_msg(sock)
                t = msg.get('t')
                if t == 'resp':
                    with self._state_lock:
                        fut = self._futs.pop(msg.get('id'), None)
                    if fut:
                        ev, box = fut
                        box['ok'] = msg.get('ok')
                        box['err'] = msg.get('err')
                        box['data'] = msg.get('data')
                        ev.set()
                elif t == 'push':
                    for fn in list(self._push_handlers):
                        try:
                            fn(msg.get('ch'), msg.get('data'))
                        except Exception:
                            pass
                # ping/pong 忽略
        except (OSError, ValueError):
            self._drop()

    def request(self, op, args=None, timeout=60):
        if not self.is_connected():
            self.connect()
        rid = next(self._ids)
        ev = threading.Event()
        box = {}
        with self._state_lock:
            self._futs[rid] = (ev, box)
        msg = {'t': 'req', 'id': rid, 'op': op, 'args': args or {}, 'timeout': timeout}
        try:
            self._send(msg)
        except OSError:
            self._drop()
            self.connect()
            self._send(msg)
        if not ev.wait(max(timeout, 90)):
            with self._state_lock:
                self._futs.pop(rid, None)
            raise ConnError('请求超时: %s（检查服务端与QMT桥策略状态）' % op)
        if not box.get('ok'):
            raise BridgeError(box.get('err') or '桥返回错误')
        return box.get('data')
