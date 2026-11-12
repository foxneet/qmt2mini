# -*- coding: utf-8 -*-
"""
qmt2mini 转发器 —— 大QMT改造miniQMT 的唯一枢纽中间件（对标通达信TQRelay）

架构:
  大QMT(桥_QMT端.py 阻塞init泵) ⇄TCP 127.0.0.1:51880⇄ 本转发器 ⇄HTTP 0.0.0.0:51881⇄ 局域网客户端
  桥端口同时服务本机 shim 客户端(TCP, xtquant同签名)。

LAN HTTP API:
  POST /api    body {"op":"get_full_tick","args":{...}}   header X-Token
               -> {"ok":true,"data":...} / {"ok":false,"err":"..."}
  GET  /status -> 桥存活验证表示: {bridge:{connected,last_seen_sec}, uptime_sec, clients, requests}
  GET  /audit?lines=100 -> 最近审计日志

安全: token鉴权(X-Token头) / allow_ips白名单(空=全放行,警告) / allow_trade总闸(下单/撤单)
审计: 审计/审计_YYYYMMDD.log 逐请求: 时间|IP|类别|开始|结束|耗时ms|请求字节|响应字节|结果
      另记: 桥连入/断开、客户端连入/断开、die指令

用法: python qmt2mini转发器.py          (GUI)
      python qmt2mini转发器.py --serve  (无界面)
打包: pyinstaller --onefile -n qmt2mini转发器 转发器.py
"""
import argparse
import base64
import json
import os
import secrets
import socket
import struct
import sys
import threading
import time
import zlib

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
CONFIG_PATH = os.path.join(BASE_DIR, '转发器_配置.json')

DEFAULT_CONFIG = {
    'bridge_port': 51880,
    'lan_port': 51881,
    'token': '',
    'allow_ips': [],
    'allow_trade': False,
    'audit_dir': os.path.join(BASE_DIR, '转发器审计'),
    'tq_relay': {'enabled': False, 'url': 'http://127.0.0.1:17710', 'token': ''},
    'l2': {'enabled': False},
    'qmt_log_dir': r'D:\QMT\userdata\log',
    'print_prefix': 'Q2M:',
}

PROTO_VER = 1
MAX_FRAME = 64 * 1024 * 1024
ORDER_OPS = {'order_stock', 'order_stock_async', 'cancel_order_stock'}
L2_OPS = {'get_l2_data'}

# op -> 默认超时秒(HTTP网关用)
OP_TIMEOUT = {
    'get_full_tick': 30, 'get_market_data_ex': 120, 'get_trading_dates': 60,
    'get_instrument_detail': 30, 'get_stock_list_in_sector': 60,
    'query_asset': 30, 'query_positions': 30, 'query_orders': 30, 'query_trades': 30,
    'order_stock': 30, 'cancel_order_stock': 30, 'download_history_data2': 900,
    'trader_connect': 15, 'subscribe_account': 15, 'die': 15, 'ping': 10,
}


def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            for k, v in DEFAULT_CONFIG.items():
                cfg.setdefault(k, v)
            return cfg
        except Exception:
            pass
    cfg = dict(DEFAULT_CONFIG)
    cfg['token'] = secrets.token_hex(16)
    save_config(cfg)
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# ---------------- 审计日志 ----------------
class Audit:
    def __init__(self, audit_dir):
        self.dir = audit_dir
        os.makedirs(self.dir, exist_ok=True)
        self.lock = threading.Lock()

    def _path(self):
        return os.path.join(self.dir, time.strftime('审计_%Y%m%d.log'))

    def write(self, line):
        with self.lock:
            try:
                with open(self._path(), 'a', encoding='utf-8') as f:
                    f.write(time.strftime('%Y-%m-%d %H:%M:%S') + ' | ' + line + '\n')
            except OSError:
                pass


# ---------------- 帧收发 ----------------
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
    def __init__(self, sock, peer):
        self.sock = sock
        self.peer = peer
        self.send_lock = threading.Lock()
        self.alive = True

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


# ---------------- 核心服务 ----------------
class Core:
    def __init__(self, cfg, audit):
        self.cfg = cfg
        self.audit = audit
        self.bridge = None
        self.bridge_lock = threading.Lock()
        self.bridge_last_seen = 0.0
        self.q2m_last_seen = 0.0               # print通道最后帧时间(liveness)
        self.started_at = time.time()
        self.clients = {}
        self.clients_lock = threading.Lock()
        self.waiters = {}                      # sid -> callback(resp_dict)
        self.waiters_lock = threading.Lock()
        self.pending_queue = []                # (op, args, callback, deadline)
        self.queue_lock = threading.Lock()
        self.req_ids = __import__('itertools').count(1)
        self.client_ids = __import__('itertools').count(1)
        self.stats = {'requests': 0, 'errors': 0}
        # QMT原生K线: tick攒线(内存, 当日; 订阅后开始积累)
        self.bar_cur = {}                      # code -> 当前未完成分钟bar
        self.bar_done = {}                     # code -> [完成的分钟bar dict]
        # print通道(FormulaOutput tail)状态
        self.tail_off = 0
        self.tail_file = ''
        self.tail_buf = ''
        self.q2m_frags = {}                    # fid -> {'tot':n, 'parts':{idx:chunk}}
        self.q2m_last_fid = 0
        self.tsnap = {}                        # 交易快照(交易桥print推送)
        self.tsnap_time = 0.0

    # --- 状态(liveness 表示) ---
    def status(self):
        with self.bridge_lock:
            b = self.bridge
        with self.clients_lock:
            ncli = len(self.clients)
        ago = time.time() - self.bridge_last_seen if self.bridge_last_seen else None
        return {
            'bridge': {'connected': b is not None and b.alive,
                       'last_seen_sec': round(ago, 1) if ago is not None else None},
            'uptime_sec': round(time.time() - self.started_at, 1),
            'clients': ncli,
            'requests': self.stats['requests'],
            'errors': self.stats['errors'],
            'allow_trade': self.cfg.get('allow_trade', False),
            'tq_relay': dict(self.cfg.get('tq_relay') or {}),
            'l2': dict(self.cfg.get('l2') or {}),
            'native_bars': {'codes': len(set(self.bar_cur) | set(self.bar_done)),
                            'minutes': sum(len(v) for v in self.bar_done.values())},
            'q2m_print': {'last_seen_sec': round(time.time() - self.q2m_last_seen, 1)
                          if self.q2m_last_seen else None},
            'tsnap_age_sec': round(time.time() - self.tsnap_time, 1)
            if self.tsnap_time else None,
        }

    def _audit_req(self, ip, op, t0, t1, req_bytes, resp_bytes, result):
        self.audit.write('%s | 类别=%s | 开始=%s | 结束=%s | 耗时=%dms | 请求=%dB | 响应=%dB | %s'
                         % (ip, op, time.strftime('%H:%M:%S', time.localtime(t0)),
                            time.strftime('%H:%M:%S', time.localtime(t1)),
                            int((t1 - t0) * 1000), req_bytes, resp_bytes, result))

    # --- 发送到桥 ---
    def _send_to_bridge(self, sid, op, args):
        with self.bridge_lock:
            bridge = self.bridge
        if bridge is None or not bridge.alive:
            return False
        try:
            bridge.send({'t': 'req', 'id': sid, 'op': op, 'args': args or {}})
            return True
        except OSError:
            return False

    def _enqueue(self, op, args, callback, timeout):
        # 排队期限必须覆盖指令节律(桥每CMD_POLL_SECONDS连入一次)
        deadline = time.time() + max(timeout, 75)
        with self.queue_lock:
            if len(self.pending_queue) > 500:
                old = self.pending_queue.pop(0)
                old[2]({'ok': False, 'err': '排队溢出被丢弃'})
            self.pending_queue.append((op, args, callback, deadline))

    def flush_queue(self, bridge):
        now = time.time()
        with self.queue_lock:
            items = self.pending_queue
            self.pending_queue = []
        for op, args, callback, deadline in items:
            if not callback or now > deadline:
                callback({'ok': False, 'err': '排队等待桥超时'})
                continue
            sid = next(self.req_ids)
            with self.waiters_lock:
                self.waiters[sid] = callback
            if not self._send_to_bridge(sid, op, args):
                with self.waiters_lock:
                    self.waiters.pop(sid, None)
                callback({'ok': False, 'err': '桥发送失败'})

    # --- TQRelay 聚合(K线历史走通达信, 双源枢纽) ---
    def _tq_call(self, method, params, timeout):
        import urllib.request
        tq = self.cfg.get('tq_relay') or {}
        url = tq.get('url', 'http://127.0.0.1:17710') + '/tq?token=' + (tq.get('token') or '')
        payload = json.dumps({'id': 1, 'method': method, 'params': params}).encode('utf-8')
        req = urllib.request.Request(url, data=payload, headers={
            'Content-Type': 'application/json; charset=utf-8'}, method='POST')
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8', 'replace'))

    def _tq_kline(self, args, timeout):
        """get_market_data_ex 经通达信TQRelay取K线, 转成 shim 的 columns/index/rows 形态"""
        codes = list(args.get('codes') or [])
        params = {
            'stock_list': codes,
            'period': args.get('period') or '1d',
            'start_time': (args.get('start_time') or '').replace('-', '')[:8],
            'end_time': (args.get('end_time') or '').replace('-', '')[:8],
            'dividend_type': {'none': 'none', 'front': 'front', 'back': 'back'}.get(
                args.get('dividend_type') or 'none', 'none'),
            'fill_data': bool(args.get('fill_data', False)),
        }
        resp = self._tq_call('get_market_data', params, min(timeout, 300))
        if resp.get('error'):
            raise Exception('TQRelay: %s' % resp['error'])
        res = resp.get('result') or {}
        if str(res.get('ErrorId', '0')) not in ('0', 0):
            raise Exception('TQ错误: %s' % res.get('Error'))
        val = res.get('Value') or {}
        keymap = [('Date', 'stime'), ('Open', 'open'), ('High', 'high'), ('Low', 'low'),
                  ('Close', 'close'), ('Volume', 'volume'), ('Amount', 'amount')]
        out = {}
        for code in codes:
            d = val.get(code)
            if not d:
                continue
            series = {shim: (d.get(tq) or []) for tq, shim in keymap}
            n = min((len(v) for v in series.values()), default=0)
            rows = []
            for i in range(n):
                rows.append([series['stime'][i], series['open'][i], series['high'][i],
                             series['low'][i], series['close'][i],
                             series['volume'][i], series['amount'][i] * 10000.0])
            out[code] = {'columns': ['stime', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                         'index': [str(x) for x in series['stime'][:n]], 'rows': rows}
        return out

    def route_op(self, op, args=None):
        if op == 'get_market_data_ex':
            if (self.cfg.get('tq_relay') or {}).get('enabled'):
                return 'tq'                      # 显式开启才走通达信(可选项)
            if (args or {}).get('period') in ('1m', '1d'):
                return 'native'                  # QMT原生: tick攒线(当日)
        return 'bridge'

    def on_bridge_resp(self, msg):
        sid = msg.get('id')
        with self.waiters_lock:
            cb = self.waiters.pop(sid, None)
        if cb:
            try:
                cb({'ok': msg.get('ok'), 'data': msg.get('data'), 'err': msg.get('err')})
            except Exception:
                pass

    # --- 同步请求(HTTP网关走这里) ---
    def request_sync(self, ip, op, args, timeout=None):
        timeout = timeout or OP_TIMEOUT.get(op, 60)
        if op in ORDER_OPS and not self.cfg.get('allow_trade'):
            self.audit.write('%s | 类别=%s | 拒绝: allow_trade未开启' % (ip, op))
            return {'ok': False, 'err': '下单功能未开启: allow_trade=false(配置文件修改后重启)'}
        if op in L2_OPS and not (self.cfg.get('l2') or {}).get('enabled'):
            self.audit.write('%s | 类别=%s | 拒绝: L2未开启' % (ip, op))
            return {'ok': False, 'err': 'L2行情未开启: 配置 l2.enabled=true 后重启'}
        req_json = json.dumps({'op': op, 'args': args}, ensure_ascii=False)
        self.stats['requests'] += 1
        t0 = time.time()
        if op in ('query_asset', 'query_positions', 'query_orders', 'query_trades'):
            ts = self._query_from_tsnap(op)
            if ts is not None:
                resp_json = json.dumps(ts, ensure_ascii=False, default=str)
                self._audit_req(ip, op, t0, t0, len(req_json.encode('utf-8')),
                                len(resp_json.encode('utf-8')), 'TSNAP/OK')
                return ts
        route = self.route_op(op, args)
        if route == 'native':
            box = {'ok': True,
                   'data': self._native_bars(list(args.get('codes') or []),
                                             args.get('period') or '1m')}
            t1 = time.time()
            resp_json = json.dumps(box, ensure_ascii=False, default=str)
            self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                            len(resp_json.encode('utf-8')), 'NATIVE/OK')
            return box
        if route == 'tq':
            try:
                data = self._tq_kline(args, timeout)
                box = {'ok': True, 'data': data}
            except Exception as e:
                box = {'ok': False, 'err': str(e)}
            t1 = time.time()
            resp_json = json.dumps(box, ensure_ascii=False, default=str)
            ok = bool(box.get('ok'))
            if not ok:
                self.stats['errors'] += 1
            self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                            len(resp_json.encode('utf-8')), 'TQ/' + ('OK' if ok else 'ERR'))
            return box
        box = {}
        ev = threading.Event()

        def callback(resp):
            box.update(resp)
            ev.set()

        with self.bridge_lock:
            b = self.bridge
        if b is None or not b.alive:
            self._enqueue(op, args, callback, timeout)
        else:
            sid = next(self.req_ids)
            with self.waiters_lock:
                self.waiters[sid] = callback
            if not self._send_to_bridge(sid, op, args):
                with self.waiters_lock:
                    self.waiters.pop(sid, None)
                ev.set()
        if not ev.wait(timeout + 5):
            with self.waiters_lock:
                self.waiters.pop(sid, None)
            box = {'ok': False, 'err': '桥响应超时(%ds)' % timeout}
        t1 = time.time()
        resp_json = json.dumps(box, ensure_ascii=False, default=str)
        ok = bool(box.get('ok'))
        if not ok:
            self.stats['errors'] += 1
        self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                        len(resp_json.encode('utf-8')), 'OK' if ok else 'ERR')
        return box

    # --- TCP客户端消息 ---
    def on_client_msg(self, cli, msg):
        t = msg.get('t')
        ip = cli.peer[0]
        if t == 'req':
            op = msg.get('op', '')
            orig_id = msg.get('id', 0)
            if op in ORDER_OPS and not self.cfg.get('allow_trade'):
                cli.send({'t': 'resp', 'id': orig_id, 'ok': False,
                          'err': '下单功能未开启: allow_trade=false'})
                return
            if op in L2_OPS and not (self.cfg.get('l2') or {}).get('enabled'):
                cli.send({'t': 'resp', 'id': orig_id, 'ok': False,
                          'err': 'L2行情未开启: 配置 l2.enabled=true 后重启'})
                return
                return
            args = msg.get('args') or {}
            req_json = json.dumps({'op': op, 'args': args}, ensure_ascii=False)
            self.stats['requests'] += 1
            t0 = time.time()
            if op in ('query_asset', 'query_positions', 'query_orders', 'query_trades'):
                ts = self._query_from_tsnap(op)
                if ts is not None:
                    resp_json = json.dumps(ts, ensure_ascii=False, default=str)
                    self._audit_req(ip, op, t0, t0, len(req_json.encode('utf-8')),
                                    len(resp_json.encode('utf-8')), 'TSNAP/OK')
                    if cli.alive:
                        try:
                            cli.send({'t': 'resp', 'id': orig_id, 'ok': ts.get('ok'),
                                      'data': ts.get('data'), 'err': ts.get('err')})
                        except OSError:
                            pass
                    return
            timeout = float(msg.get('timeout', OP_TIMEOUT.get(op, 60)))

            route = self.route_op(op, args)
            if route == 'native':
                data = self._native_bars(list(args.get('codes') or []),
                                         args.get('period') or '1m')
                t1 = time.time()
                resp_json = json.dumps({'ok': True, 'data': data}, ensure_ascii=False,
                                       default=str)
                self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                                len(resp_json.encode('utf-8')), 'NATIVE/OK')
                if cli.alive:
                    try:
                        cli.send({'t': 'resp', 'id': orig_id, 'ok': True, 'data': data})
                    except OSError:
                        pass
                return
            if route == 'tq':
                try:
                    data = self._tq_kline(args, timeout)
                    box = {'ok': True, 'data': data}
                except Exception as e:
                    box = {'ok': False, 'err': str(e)}
                t1 = time.time()
                resp_json = json.dumps(box, ensure_ascii=False, default=str)
                ok = bool(box.get('ok'))
                if not ok:
                    self.stats['errors'] += 1
                self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                                len(resp_json.encode('utf-8')), 'TQ/' + ('OK' if ok else 'ERR'))
                if cli.alive:
                    try:
                        cli.send({'t': 'resp', 'id': orig_id, 'ok': box.get('ok'),
                                  'data': box.get('data'), 'err': box.get('err')})
                    except OSError:
                        pass
                return

            def callback(resp, cli=cli, orig_id=orig_id, op=op, t0=t0, req_json=req_json):
                t1 = time.time()
                resp_json = json.dumps(resp, ensure_ascii=False, default=str)
                ok = bool(resp.get('ok'))
                if not ok:
                    self.stats['errors'] += 1
                self._audit_req(ip, op, t0, t1, len(req_json.encode('utf-8')),
                                len(resp_json.encode('utf-8')), 'OK' if ok else 'ERR')
                if cli.alive:
                    try:
                        cli.send({'t': 'resp', 'id': orig_id, 'ok': resp.get('ok'),
                                  'data': resp.get('data'), 'err': resp.get('err')})
                    except OSError:
                        pass

            with self.bridge_lock:
                b = self.bridge
            if b is None or not b.alive:
                self._enqueue(op, args, callback, timeout)
            else:
                sid = next(self.req_ids)
                with self.waiters_lock:
                    self.waiters[sid] = callback
                if not self._send_to_bridge(sid, op, args):
                    with self.waiters_lock:
                        self.waiters.pop(sid, None)
                    callback({'ok': False, 'err': '桥发送失败'})
        elif t == 'ping':
            cli.send({'t': 'pong'})

    # --- QMT原生K线: tick攒线 ---
    def _acc_bars(self, ticks):
        for code, tk in (ticks or {}).items():
            try:
                lp = float(tk.get('lastPrice') or 0)
                vol = float(tk.get('volume') or 0)
                amt = float(tk.get('amount') or 0)
                tms = int(tk.get('time') or 0)
                if lp <= 0:
                    continue
                bucket = tms // 60000 if tms else int(time.time() // 60)
            except Exception:
                continue
            b = self.bar_cur.get(code)
            if b is None or b['bucket'] != bucket:
                if b is not None:
                    done = self.bar_done.setdefault(code, [])
                    done.append({'stime': b['stime'], 'open': b['o'], 'high': b['h'],
                                 'low': b['l'], 'close': b['c'],
                                 'volume': b['v'], 'amount': b['a']})
                    self.bar_done[code] = done[-240:]
                stime = time.strftime('%Y%m%d%H%M', time.localtime(bucket * 60))
                self.bar_cur[code] = {'bucket': bucket, 'stime': stime,
                                      'o': lp, 'h': lp, 'l': lp, 'c': lp,
                                      'v': max(0.0, vol - (b['last_vol'] if b else 0.0)),
                                      'a': max(0.0, amt - (b['last_amt'] if b else 0.0)),
                                      'last_vol': vol, 'last_amt': amt}
            else:
                b['h'] = max(b['h'], lp)
                b['l'] = min(b['l'], lp) if b['l'] else lp
                b['c'] = lp
                b['v'] = max(0.0, vol - b['last_vol'])
                b['a'] = max(0.0, amt - b['last_amt'])
                b['last_vol'] = vol
                b['last_amt'] = amt

    def _native_bars(self, codes, period):
        cols = ['stime', 'open', 'high', 'low', 'close', 'volume', 'amount']
        out = {}
        want_min = (period == '1m')
        want_day = (period == '1d')
        for code in codes:
            rows = []
            if want_min:
                for b in self.bar_done.get(code, []):
                    rows.append([b['stime'], b['open'], b['high'], b['low'],
                                 b['close'], b['volume'], b['amount']])
            cur = self.bar_cur.get(code)
            if cur and (want_min or want_day):
                rows.append([cur['stime'], cur['o'], cur['h'], cur['l'],
                             cur['c'], cur['v'], cur['a']])
            if want_day and rows:
                day = time.strftime('%Y%m%d')
                o = rows[0][1]
                h = max(r[2] for r in rows)
                l = min(r[3] for r in rows)
                c = rows[-1][4]
                v = sum(r[5] for r in rows)
                a = sum(r[6] for r in rows)
                rows = [[day, o, h, l, c, v, a]]
            if rows:
                out[code] = {'columns': cols, 'index': [str(r[0]) for r in rows], 'rows': rows}
        return out

    def on_bridge_msg(self, bridge, msg):
        self.bridge_last_seen = time.time()
        t = msg.get('t')
        if t == 'resp':
            self.on_bridge_resp(msg)
        elif t == 'push':
            if msg.get('ch') == 'quote':
                try:
                    self._acc_bars((msg.get('data') or {}).get('data') or {})
                except Exception:
                    pass
            self.broadcast(msg)
        elif t == 'ping':
            bridge.send({'t': 'pong'})

    def broadcast(self, msg):
        with self.clients_lock:
            targets = [c for c in self.clients.values() if c.alive]
        for cli in targets:
            try:
                cli.send(msg)
            except OSError:
                pass

    def watchdog(self):
        while True:
            time.sleep(5)
            now = time.time()
            with self.waiters_lock:
                pass
            with self.queue_lock:
                expired = [x for x in self.pending_queue if now > x[3]]
                self.pending_queue = [x for x in self.pending_queue if now <= x[3]]
            for op, _a, callback, _dl in expired:
                callback({'ok': False, 'err': '排队等待桥超时'})

    def set_bridge(self, sender):
        with self.bridge_lock:
            old = self.bridge
            self.bridge = sender
            self.bridge_last_seen = time.time()
        if old is not None and old is not sender:
            old.close()

    @staticmethod
    def log(s):
        line = time.strftime('[%H:%M:%S] ') + s
        try:
            print(line, flush=True)
        except UnicodeEncodeError:
            print(line.encode('gbk', 'replace').decode('gbk'), flush=True)

    # --- TCP 服务(桥+本机客户端) ---
    def serve_conn(self, sock, addr):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        peer = Sender(sock, addr)
        try:
            hello = recv_msg(sock)
        except (ConnectionError, OSError, ValueError):
            peer.close()
            return
        role = hello.get('role')
        if self.cfg.get('token') and hello.get('token') != self.cfg['token']:
            self.log('鉴权失败 来自 %s' % (addr,))
            peer.close()
            return
        if role == 'bridge':
            gap = time.time() - self.bridge_last_seen
            self.bridge_last_seen = time.time()
            if gap > 10:
                self.log('桥已连入 来自 %s' % (addr,))
            self.audit.write('桥连入 %s' % (addr,))
            self.set_bridge(peer)
            self.flush_queue(peer)
            try:
                while True:
                    msg = recv_msg(sock)
                    self.on_bridge_msg(peer, msg)
            except (ConnectionError, OSError, ValueError):
                pass
            with self.bridge_lock:
                if self.bridge is peer:
                    self.bridge = None
            self.audit.write('桥断开 %s' % (addr,))
        elif role == 'client':
            cid = next(self.client_ids)
            with self.clients_lock:
                self.clients[cid] = peer
            self.log('客户端#%d 连入 %s' % (cid, addr))
            self.audit.write('客户端#%d 连入 %s' % (cid, addr))
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
            self.audit.write('客户端#%d 断开' % cid)
        peer.close()

    # --- print通道: FormulaOutput tail(零评分出站的接收端) ---
    def _tail_log_path(self):
        return os.path.join(self.cfg.get('qmt_log_dir') or '',
                            time.strftime('XtClient_FormulaOutput_%Y%m%d.log'))

    def _q2m_handle_line(self, line):
        prefix = self.cfg.get('print_prefix') or 'Q2M:'
        pos = line.rfind(prefix)             # 裸行头或 output= 前缀均匹配
        if pos < 0:
            return
        payload = line[pos + len(prefix):].strip()
        if payload.startswith('Q2MERR:'):
            return
        parts = payload.split(':', 4)
        if len(parts) < 5:
            return
        try:
            fid = int(parts[0]); idx = int(parts[1]); tot = int(parts[2]); ln = int(parts[3])
        except ValueError:
            return
        if tot < 1 or idx >= tot or fid <= 0 or ln < 0 or len(parts[4]) != ln:
            return          # 长度不符=行被引擎日志污染, 丢帧(桥每周期重推, 自愈)
        grp = self.q2m_frags.get(fid)
        if grp is None:
            grp = {'tot': tot, 'parts': {}}
            self.q2m_frags[fid] = grp
        grp['parts'][idx] = parts[4]
        if len(grp['parts']) < tot:
            return
        del self.q2m_frags[fid]
        if len(self.q2m_frags) > 500:
            for k in list(self.q2m_frags)[:200]:
                self.q2m_frags.pop(k, None)
        data = ''.join(grp['parts'][i] for i in range(tot))
        try:
            obj = json.loads(zlib.decompress(base64.b64decode(data)).decode('utf-8'))
        except Exception:
            return          # 解压/解码失败=帧损, 丢弃(自愈)
        self.q2m_last_fid = max(self.q2m_last_fid, fid)
        self.q2m_last_seen = time.time()
        if obj.get('t') == 'push':
            ch = obj.get('ch')
            data = obj.get('data')
            if ch == 'quote':
                try:
                    self._acc_bars((data or {}).get('data') or {})
                except Exception:
                    pass
                self.broadcast({'t': 'push', 'ch': 'quote', 'data': data})
            elif ch == 'tsnap':
                self.tsnap = data or {}
                self.tsnap_time = time.time()
                self.broadcast({'t': 'push', 'ch': 'tsnap', 'data': data})

    def _tail_pump(self):
        prefix = self.cfg.get('print_prefix') or 'Q2M:'
        while True:
            try:
                p = self._tail_log_path()
                if p != self.tail_file:
                    self.tail_file = p
                    self.tail_off = 0
                    self.tail_buf = ''
                if os.path.exists(p):
                    size = os.path.getsize(p)
                    if size < self.tail_off:
                        self.tail_off = 0
                        self.tail_buf = ''
                    if size > self.tail_off:
                        with open(p, 'rb') as f:
                            f.seek(self.tail_off)
                            chunk = f.read(size - self.tail_off)
                        self.tail_off += len(chunk)
                        self.tail_buf += chunk.decode('utf-8', 'replace')
                        if '\n' in self.tail_buf:
                            lines = self.tail_buf.split('\n')
                            self.tail_buf = lines.pop()
                            for line in lines:
                                if prefix in line:
                                    self._q2m_handle_line(line)
            except Exception:
                pass
            time.sleep(1)

    def _tsnap_fresh(self):
        return bool(self.tsnap_time) and (time.time() - self.tsnap_time < 15)

    def _query_from_tsnap(self, op):
        m = {'query_asset': 'account', 'query_positions': 'positions',
             'query_orders': 'orders', 'query_trades': 'trades'}
        sec = m.get(op)
        if not sec or not self._tsnap_fresh():
            return None
        if not self.tsnap or sec not in self.tsnap:
            return {'ok': True, 'data': None if sec == 'account' else []}
        return {'ok': True, 'data': self.tsnap[sec]}

    def run_tcp(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', self.cfg['bridge_port']))
        srv.listen(16)
        self.log('桥/本机端口 127.0.0.1:%d' % self.cfg['bridge_port'])
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=self.serve_conn, args=(sock, addr), daemon=True).start()


# ---------------- HTTP LAN 网关 ----------------
def run_http(core, cfg, audit):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import urlparse, parse_qs

    class Handler(BaseHTTPRequestHandler):
        def _ip(self):
            return self.client_address[0]

        def _authed(self):
            token = cfg.get('token') or ''
            if not token:
                return True
            got = self.headers.get('X-Token') or ''
            if not got:
                q = parse_qs(urlparse(self.path).query)
                got = (q.get('token') or [''])[0]
            return got == token

        def _ip_allowed(self):
            wl = cfg.get('allow_ips') or []
            return (not wl) or (self._ip() in wl)

        def _json(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False, default=str).encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if not self._ip_allowed():
                self._json(403, {'ok': False, 'err': 'IP不在白名单'})
                return
            u = urlparse(self.path)
            if u.path == '/status':
                self._json(200, {'ok': True, 'data': core.status()})
            elif u.path == '/audit':
                q = parse_qs(u.query)
                n = int((q.get('lines') or ['100'])[0])
                p = os.path.join(audit.dir, time.strftime('审计_%Y%m%d.log'))
                lines = []
                try:
                    with open(p, 'r', encoding='utf-8') as f:
                        lines = f.readlines()[-n:]
                except OSError:
                    pass
                self._json(200, {'ok': True, 'data': [l.rstrip('\n') for l in lines]})
            else:
                self._json(404, {'ok': False, 'err': '未知路径, 可用: /status /audit'})

        def do_POST(self):
            if not self._ip_allowed():
                self._json(403, {'ok': False, 'err': 'IP不在白名单'})
                return
            if urlparse(self.path).path != '/api':
                self._json(404, {'ok': False, 'err': '未知路径, POST /api'})
                return
            if not self._authed():
                self._json(401, {'ok': False, 'err': 'token无效(X-Token头)'})
                return
            try:
                ln = int(self.headers.get('Content-Length') or 0)
                body = json.loads(self.rfile.read(min(ln, 32 * 1024 * 1024)).decode('utf-8'))
                op = body.get('op')
                args = body.get('args') or {}
                if not op:
                    raise ValueError('缺少op')
            except Exception as e:
                self._json(400, {'ok': False, 'err': '请求体错误: %s' % e})
                return
            timeout = float(body.get('timeout') or OP_TIMEOUT.get(op, 60))
            box = core.request_sync(self._ip(), op, args, timeout)
            self._json(200, box)

        def log_message(self, fmt, *args):
            pass

    srv = ThreadingHTTPServer(('0.0.0.0', cfg['lan_port']), Handler)
    core.log('LAN HTTP 网关 0.0.0.0:%d (POST /api | GET /status | GET /audit)' % cfg['lan_port'])
    srv.serve_forever()


# ---------------- 入口 ----------------
def main_serve(cfg, audit, core):
    core.log('qmt2mini 转发器 --serve 启动 (审计目录: %s)' % cfg['audit_dir'])
    if not cfg.get('token'):
        core.log('[!] token为空: LAN HTTP 未鉴权!')
    if not (cfg.get('allow_ips') or []):
        core.log('[!] allow_ips为空: 局域网任意主机可连(建议配置白名单)')
    core.log('下单总闸 allow_trade = %s' % cfg.get('allow_trade'))
    threading.Thread(target=core.watchdog, daemon=True).start()
    threading.Thread(target=core.run_tcp, daemon=True).start()
    threading.Thread(target=core._tail_pump, daemon=True).start()
    run_http(core, cfg, audit)


def main_gui(cfg, audit, core):
    import tkinter as tk
    from tkinter import scrolledtext
    win = tk.Tk()
    win.title('qmt2mini 转发器 — 大QMT⇄局域网 行情/交易枢纽')
    win.geometry('680x520')
    tk.Label(win, text='桥状态:').grid(row=0, column=0, sticky='w', padx=8, pady=4)
    lbl_bridge = tk.Label(win, text='...', fg='red')
    lbl_bridge.grid(row=0, column=1, sticky='w')
    tk.Label(win, text='本机端口: %d    LAN端口: %d' %
             (cfg['bridge_port'], cfg['lan_port'])).grid(row=1, column=1, sticky='w')
    tk.Label(win, text='token:').grid(row=2, column=0, sticky='w', padx=8)
    lbl_token = tk.Label(win, text=cfg.get('token') or '(未启用)', fg='blue')
    lbl_token.grid(row=2, column=1, sticky='w')
    var_trade = tk.BooleanVar(value=bool(cfg.get('allow_trade')))

    def on_trade_toggle():
        cfg['allow_trade'] = bool(var_trade.get())
        save_config(cfg)
        audit.write('allow_trade -> %s' % cfg['allow_trade'])
    tk.Checkbutton(win, text='允许下单/撤单(allow_trade 总闸)', variable=var_trade,
                   command=on_trade_toggle).grid(row=3, column=1, sticky='w')
    tk.Label(win, text='审计日志(尾部):').grid(row=4, column=0, sticky='w', padx=8)
    txt = scrolledtext.ScrolledText(win, width=88, height=22)
    txt.grid(row=5, column=0, columnspan=2, padx=8, pady=4)

    def refresh():
        st = core.status()
        if st['bridge']['connected']:
            lbl_bridge.config(text='在线 (最后心跳 %s 秒前)  请求=%d 错误=%d 客户端=%d'
                              % (st['bridge']['last_seen_sec'], st['requests'],
                                 st['errors'], st['clients']), fg='green')
        else:
            lbl_bridge.config(text='离线 (桥策略未运行?)', fg='red')
        try:
            p = os.path.join(audit.dir, time.strftime('审计_%Y%m%d.log'))
            if os.path.exists(p):
                with open(p, 'r', encoding='utf-8') as f:
                    lines = f.readlines()[-200:]
                txt.delete('1.0', 'end')
                txt.insert('end', ''.join(lines))
                txt.see('end')
        except Exception:
            pass
        win.after(2000, refresh)
    win.after(1000, refresh)
    threading.Thread(target=core.watchdog, daemon=True).start()
    threading.Thread(target=core.run_tcp, daemon=True).start()
    threading.Thread(target=core._tail_pump, daemon=True).start()
    threading.Thread(target=run_http, args=(core, cfg, audit), daemon=True).start()
    win.mainloop()


def main():
    ap = argparse.ArgumentParser(description='qmt2mini 转发器 — 大QMT⇄局域网枢纽')
    ap.add_argument('--serve', action='store_true', help='无界面后台模式')
    ap.add_argument('--lan-port', type=int, help='覆盖LAN端口')
    args = ap.parse_args()
    cfg = load_config()
    if args.lan_port:
        cfg['lan_port'] = args.lan_port
    audit = Audit(cfg['audit_dir'])
    core = Core(cfg, audit)
    if args.serve:
        main_serve(cfg, audit, core)
    else:
        main_gui(cfg, audit, core)


if __name__ == '__main__':
    main()
