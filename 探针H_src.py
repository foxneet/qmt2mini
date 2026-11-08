#coding:gbk
r"""
探针H: 文件桥同款连接姿势测生死
  一条持久长连接(连接一次用到底, 与qmt_file_bridge_client完全同族):
    create_connection(timeout=5) -> settimeout(0.05)阻塞短超时
    每秒: sendall(ping) -> recv清收(0.2s硬顶) -> 断线则下拍重连
  无连接churn(不反复新建), 若10分钟存活 -> 桥泵照此重造
"""
import datetime as dt
import json
import socket
import struct
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880

_st = {'sock': None, 'n': 0, 'frames': 0, 'errs': 0}


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
        except Exception as e:
            if _st['n'] % 10 == 0:
                print('H[重连失败] %s' % repr(e)[:40])
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
            print('H[存活%d秒] 累计帧=%d 本拍=%d' % (_st['n'], _st['frames'], got))
    except Exception as e:
        _st['errs'] += 1
        _st['sock'] = None
        try:
            s.close()
        except Exception:
            pass
        if _st['errs'] % 5 == 1:
            print('H[断线重连] %s' % repr(e)[:50])


def init(C):
    print('H=== 探针H启动: 文件桥同款持久连接 + schedule_run ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeH')


def handlebar(C):
    pass
