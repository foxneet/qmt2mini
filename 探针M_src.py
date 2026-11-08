#coding:gbk
r"""
探针M: 阻塞init服务架构验证
  init() 内 while True 常驻循环(永不返回):
    每秒: C.get_full_tick 轮询行情 -> print('Q2M:...') 出站
          + 持久socket收发(文件桥同款姿势, init上下文=实测不计分)
  验证: (1)引擎容忍阻塞init多久 (2)阻塞期间引擎API可调否 (3)init上下文socket长期安全否
  注意: 本策略会阻塞QMT策略线程 -> 运行期间本客户端其他策略暂停(专用源客户端可接受)
"""
import datetime as dt
import json
import socket
import struct
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880


def _connect():
    s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5.0)
    s.settimeout(0.3)
    hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
    d = json.dumps(hello, ensure_ascii=False).encode('utf-8')
    s.sendall(struct.pack('<I', len(d)) + d)
    return s


def init(C):
    print('M=== 探针M启动: 进入阻塞init常驻服务循环 ===')
    try:
        C.set_account(account)
    except Exception as e:
        print('M[set_account异常] %s' % repr(e)[:60])
    n = 0
    sock = None
    while True:
        n += 1
        # --- 行情轮询(引擎API在阻塞init内可调否) ---
        try:
            ticks = C.get_full_tick(['000300.SH'])
            lp = (ticks.get('000300.SH') or {}).get('lastPrice')
        except Exception as e:
            lp = None
            if n % 30 == 1:
                print('M[get_full_tick异常] %s' % repr(e)[:50])
        # --- 出站: print遥测(每10秒一条) ---
        if n % 10 == 0:
            print('M[存活%d秒] lastPrice=%s' % (n, lp))
        # --- 入站: 持久socket服务(init上下文) ---
        try:
            if sock is None:
                sock = _connect()
                print('M[socket已建立] %s' % time.strftime('%H:%M:%S'))
            ping = {'t': 'ping'}
            d = json.dumps(ping).encode('utf-8')
            sock.sendall(struct.pack('<I', len(d)) + d)
            hard = time.time() + 0.2
            while time.time() < hard:
                try:
                    chunk = sock.recv(65536)
                    if not chunk:
                        raise ConnectionError('closed')
                    _frames[0] = _frames[0] + 1
                except socket.timeout:
                    break
        except Exception as e:
            try:
                sock.close()
            except Exception:
                pass
            sock = None
            if n % 10 == 1:
                print('M[socket重连] %s' % repr(e)[:50])
        time.sleep(1)


_frames = [0]


def handlebar(C):
    pass
