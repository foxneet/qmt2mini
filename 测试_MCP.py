# -*- coding: utf-8 -*-
"""
测试_MCP.py — qmt2mini_mcp.py 端到端测试（无需真实转发器/QMT）
内置 mock HTTP 转发器(127.0.0.1:51991)，子进程启动 MCP 服务(stdio)，
按 MCP 协议逐项验证：握手/工具清单/行情/K线截断/下单闸/撤单/未知方法/未知工具。
用法: py -3.14 测试_MCP.py   （全部PASS退出码0）
"""
import json
import os
import queue
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
MOCK_PORT = 51991
TOKEN = 'testtoken123'

# 大K线: 1200根 → 文本必然超过 MAX_TEXT(20000) 触发截断
_KROWS = [[20261112, '20261112', 10.0 + i * 0.01, 10.1 + i * 0.01, 9.9 + i * 0.01,
           10.0 + i * 0.01, 10000 + i, 100000.0 + i * 100.0] for i in range(1200)]
_KINDEX = ['202601%02d' % (i % 28 + 1) for i in range(1200)]
_KCOLS = ['time', 'stime', 'open', 'high', 'low', 'close', 'volume', 'amount']


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith('/status'):
            self._send(200, {'ok': True, 'bridge': {'connected': True},
                             'q2m_print': {'last_seen_sec': 1}, 'allow_trade': False})
        else:
            self._send(404, {'ok': False, 'err': 'no route'})

    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        try:
            req = json.loads(self.rfile.read(n).decode('utf-8'))
        except Exception:
            self._send(400, {'ok': False, 'err': 'bad json'})
            return
        if self.headers.get('X-Token') != TOKEN:
            self._send(401, {'ok': False, 'err': 'token不匹配(401)'})
            return
        op = req.get('op')
        if op == 'get_full_tick':
            self._send(200, {'ok': True, 'data': {'600519.SH': {
                'lastPrice': 1453.5, 'volume': 12345, 'askPrice': [1453.6] * 5}}})
        elif op == 'get_market_data_ex':
            self._send(200, {'ok': True, 'data': {'600519.SH': {
                'columns': _KCOLS, 'index': _KINDEX, 'rows': _KROWS}}})
        elif op == 'order_stock':
            self._send(200, {'ok': False, 'err': 'allow_trade=False,下单被总闸拒绝'})
        elif op == 'cancel_order_stock':
            self._send(200, {'ok': True, 'data': {'ok': True}})
        elif op == 'query_asset':
            self._send(200, {'ok': True, 'data': {'cash': 100000.0, 'total_asset': 250000.0}})
        else:
            self._send(200, {'ok': True, 'data': {'echo': op}})


class Driver:
    def __init__(self, proc):
        self.p = proc
        self.q = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            line = line.strip()
            if line:
                self.q.put(line)

    def send(self, obj):
        self.p.stdin.write(json.dumps(obj, ensure_ascii=True) + '\n')
        self.p.stdin.flush()

    def recv(self, timeout=20):
        try:
            return json.loads(self.q.get(timeout=timeout))
        except queue.Empty:
            raise AssertionError('等待MCP响应超时(%ds)' % timeout)


def main():
    srv = ThreadingHTTPServer(('127.0.0.1', MOCK_PORT), MockHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    env = os.environ.copy()
    env.update({'QMT2MINI_HOST': '127.0.0.1', 'QMT2MINI_HTTP_PORT': str(MOCK_PORT),
                'QMT2MINI_TOKEN': TOKEN, 'PYTHONIOENCODING': 'utf-8'})
    p = subprocess.Popen([sys.executable, os.path.join(HERE, 'qmt2mini_mcp.py')],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=env, text=True, encoding='utf-8')
    d = Driver(p)
    results, failed = [], []

    def check(name, cond, detail=''):
        ok = bool(cond)
        results.append(ok)
        print('%s %s%s' % ('PASS' if ok else 'FAIL', name, ('' if ok else ' | ' + str(detail))))
        if not ok:
            failed.append(name)

    try:
        # 1 initialize 握手
        d.send({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
            'protocolVersion': '2025-06-18', 'capabilities': {},
            'clientInfo': {'name': 'test', 'version': '0'}}})
        r = d.recv()
        check('initialize.protocolVersion', r.get('result', {}).get('protocolVersion') == '2025-06-18', r)
        check('initialize.serverInfo', r.get('result', {}).get('serverInfo', {}).get('name') == 'qmt2mini-mcp', r)
        d.send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})

        # 2 ping
        d.send({'jsonrpc': '2.0', 'id': 2, 'method': 'ping'})
        r = d.recv()
        check('ping → {}', r.get('result') == {}, r)

        # 3 tools/list
        d.send({'jsonrpc': '2.0', 'id': 3, 'method': 'tools/list'})
        r = d.recv()
        tools = r.get('result', {}).get('tools', [])
        names = [t['name'] for t in tools]
        expect = {'qmt_status', 'get_full_tick', 'get_kline', 'get_trading_dates',
                  'get_instrument_detail', 'get_sector_stocks', 'query_asset',
                  'query_positions', 'query_orders', 'query_trades',
                  'order_stock', 'cancel_order_stock'}
        check('tools/list 数量=12', len(tools) == 12, names)
        check('tools/list 名称齐全', set(names) == expect, set(names) ^ expect)
        check('tools/list 均带inputSchema', all(t.get('inputSchema', {}).get('type') == 'object' for t in tools))

        # 4 tools/call qmt_status
        d.send({'jsonrpc': '2.0', 'id': 4, 'method': 'tools/call',
                'params': {'name': 'qmt_status', 'arguments': {}}})
        r = d.recv()
        txt = r['result']['content'][0]['text']
        check('qmt_status 存活', r['result'].get('isError') is False and 'last_seen_sec' in txt, txt[:200])

        # 5 tools/call get_full_tick
        d.send({'jsonrpc': '2.0', 'id': 5, 'method': 'tools/call',
                'params': {'name': 'get_full_tick', 'arguments': {'codes': ['600519.SH']}}})
        r = d.recv()
        txt = r['result']['content'][0]['text']
        check('get_full_tick 行情', r['result'].get('isError') is False and '1453.5' in txt, txt[:200])

        # 6 tools/call get_kline 大响应截断
        d.send({'jsonrpc': '2.0', 'id': 6, 'method': 'tools/call',
                'params': {'name': 'get_kline', 'arguments': {'codes': ['600519.SH'], 'count': 1200}}})
        r = d.recv()
        txt = r['result']['content'][0]['text']
        check('get_kline 截断保护', '[已截断' in txt and len(txt) <= 21000, len(txt))

        # 7 tools/call order_stock 受总闸拒绝 → isError
        d.send({'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call',
                'params': {'name': 'order_stock', 'arguments': {
                    'account': '你的资金账号', 'code': '600519.SH', 'order_type': 23,
                    'volume': 100, 'price_type': 11, 'price': 1400.0}}})
        r = d.recv()
        txt = r['result']['content'][0]['text']
        check('order_stock 闸拒=isError', r['result'].get('isError') is True and 'allow_trade' in txt, txt[:200])

        # 8 tools/call cancel_order_stock
        d.send({'jsonrpc': '2.0', 'id': 8, 'method': 'tools/call',
                'params': {'name': 'cancel_order_stock', 'arguments': {'account': '你的资金账号', 'order_id': 12}}})
        r = d.recv()
        check('cancel_order_stock', r['result'].get('isError') is False, r)

        # 9 tools/call query_asset
        d.send({'jsonrpc': '2.0', 'id': 9, 'method': 'tools/call',
                'params': {'name': 'query_asset', 'arguments': {'account': '你的资金账号'}}})
        r = d.recv()
        txt = r['result']['content'][0]['text']
        check('query_asset', 'cash' in txt and r['result'].get('isError') is False, txt[:200])

        # 10 缺参 → 参数错误(isError, 非崩溃)
        d.send({'jsonrpc': '2.0', 'id': 10, 'method': 'tools/call',
                'params': {'name': 'get_full_tick', 'arguments': {}}})
        r = d.recv()
        check('缺参=isError', r['result'].get('isError') is True and 'codes' in r['result']['content'][0]['text'], r)

        # 11 未知方法 → -32601
        d.send({'jsonrpc': '2.0', 'id': 11, 'method': 'bogus/method'})
        r = d.recv()
        check('未知方法→-32601', r.get('error', {}).get('code') == -32601, r)

        # 12 未知工具 → -32602
        d.send({'jsonrpc': '2.0', 'id': 12, 'method': 'tools/call', 'params': {'name': 'nope'}})
        r = d.recv()
        check('未知工具→-32602', r.get('error', {}).get('code') == -32602, r)
    finally:
        try:
            p.kill()
        except Exception:
            pass
        srv.shutdown()
        print('--- stderr ---')
        print(p.stderr.read() if p.poll() is not None else '(进程已终止)')

    print('=== MCP端到端: %d/%d PASS ===' % (sum(results), len(results)))
    sys.exit(0 if not failed else 1)


if __name__ == '__main__':
    main()
