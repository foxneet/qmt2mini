#coding:gbk
r"""
探针F: 分阶段定位模型自动停止元凶(一次运行=三次实验)
A相 0-120秒   纯Python轮询(无socket)  -> 若死于此相: 引擎无差别回收常驻模型
B相 120-240秒 每秒连接+关闭(不收发)   -> 若死于此相: 连接行为触发
C相 240秒起   完整收发(hello+recv)    -> 若死于此相: 数据交换触发
心跳每10秒写入 F:\qmt2mini\probe_heart.log, 死亡时间以文件为准
运行后放置12分钟以上再来查看
"""
import datetime as dt
import json
import socket
import struct
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880
LOG_PATH = r'F:\qmt2mini\probe_heart.log'


def _log(msg):
    try:
        f = open(LOG_PATH, 'a')
        try:
            f.write('%s %s\n' % (time.strftime('%H:%M:%S'), msg))
        finally:
            f.close()
    except Exception:
        pass


def _cb(*a):
    pass


_n = [0]


def _tick(C):
    _n[0] += 1
    t = _n[0]
    if t <= 120:
        phase = 'A'
    elif t <= 240:
        phase = 'B'
    else:
        phase = 'C'
    ok = '-'
    if t > 120:
        try:
            s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=1.0)
            try:
                if t > 240:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    s.settimeout(0.05)
                    hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
                    data = json.dumps(hello, ensure_ascii=False).encode('utf-8')
                    s.sendall(struct.pack('<I', len(data)) + data)
                    hard = time.time() + 0.25
                    while time.time() < hard:
                        try:
                            chunk = s.recv(262144)
                        except socket.timeout:
                            break
                        except OSError:
                            break
                        if not chunk:
                            break
            finally:
                try:
                    s.close()
                except Exception:
                    pass
            ok = 'OK'
        except Exception as e:
            ok = repr(e)[:30]
    if t % 10 == 0:
        _log('phase=%s t=%ds socket=%s' % (phase, t, ok))


def init(C):
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', _cb)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeF')
    _log('=== probeF启动 ===')


def handlebar(C):
    pass
