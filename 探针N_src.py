#coding:gbk
r"""
探针N: 评分衰减测试(最后一问)
  正常模型(不阻塞init, handlebar/timer都活着)
  schedule_run每1秒心跳(无socket), 每60秒做1次 connect+ping+close(计分+1)
  新鲜客户端(评分~0)起步, 阈值≈112-170:
    若无衰减 -> 约112-170分钟(2-3小时)后死
    若有衰减(>=1/分) -> 永活
  跑3小时, 死亡时刻(第N次连接)即答案
"""
import datetime as dt
import json
import socket
import struct
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880

_t = [0]
_c = [0]


def _conn_ping():
    s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5.0)
    try:
        s.settimeout(1.0)
        d = json.dumps({'t': 'ping'}).encode('utf-8')
        s.sendall(struct.pack('<I', len(d)) + d)
        s.recv(1024)
    finally:
        s.close()


def _tick(C):
    _t[0] += 1
    t = _t[0]
    if t % 60 == 0:
        _c[0] += 1
        ok = '?'
        try:
            _conn_ping()
            ok = 'OK'
        except Exception as e:
            ok = repr(e)[:30]
        print('N[第%d次连接 t=%d秒] %s' % (_c[0], t, ok))


def init(C):
    print('N=== 探针N启动: 每60秒1次计分连接, 阈值测量 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeN')


def handlebar(C):
    pass
