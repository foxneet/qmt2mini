#coding:gbk
r"""
探针I: 最终架构验证
  init: 一次性建立持久连接(此后永不主动connect) + set_account + 订阅保活 + schedule_run
  定时器: 只在已有连接上 send/recv(绝不connect, 绝不close)
  断线: 仅在异常时于定时器内重连(正常运行为0次)
  若10分钟存活 -> 桥按此架构重造, 全案收官
"""
import datetime as dt
import json
import socket
import struct
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880

_st = {'sock': None, 'n': 0, 'frames': 0, 'errs': 0, 'recon': 0}


def _connect():
    s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5.0)
    s.settimeout(0.05)
    hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
    d = json.dumps(hello, ensure_ascii=False).encode('utf-8')
    s.sendall(struct.pack('<I', len(d)) + d)
    _st['sock'] = s


def _tick(C):
    _st['n'] += 1
    s = _st['sock']
    if s is None:
        try:
            _connect()
            _st['recon'] += 1
        except Exception as e:
            if _st['n'] % 10 == 0:
                print('I[重连失败] %s' % repr(e)[:40])
            return
        s = _st['sock']
    try:
        ping = {'t': 'ping'}
        d = json.dumps(ping).encode('utf-8')
        s.sendall(struct.pack('<I', len(d)) + d)
        got = 0
        hard = time.time() + 0.2
        while time.time() < hard:
            try:
                chunk = s.recv(65536)
                if not chunk:
                    raise ConnectionError('closed')
                _st['frames'] += 1
                got += 1
            except socket.timeout:
                break
        if _st['n'] % 10 == 0:
            print('I[存活%d秒] 累计帧=%d 重连=%d' % (_st['n'], _st['frames'], _st['recon']))
    except Exception as e:
        _st['errs'] += 1
        _st['sock'] = None
        try:
            s.close()
        except Exception:
            pass
        if _st['errs'] % 5 == 1:
            print('I[断线] %s' % repr(e)[:50])


def init(C):
    print('I=== 探针I启动: init建持久连接, 定时器零connect ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    _connect()
    print('I[init] 持久连接已建立')
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeI')


def handlebar(C):
    pass
