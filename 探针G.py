#coding:gbk
r"""
探针G(决胜版): 验证"tick上下文的socket是否安全"
  定时器: 全程零socket, 每10秒打印心跳 -> 顺便验证纯常驻能否活过午休(45分钟+)
  订阅回调腿: 每次tick触发 做一次连接+关闭 -> 13:00开盘后激活
  handlebar腿: 每次bar/tick触发 做一次连接+关闭 -> 13:00开盘后激活
  若13:05后两腿持续打点且模型活着 -> 架构定案: 泵由tick驱动, 定时器不碰socket
"""
import datetime as dt
import socket
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880

_sub_count = [0]
_hb_count = [0]
_t_count = [0]


def _conn_close():
    s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=1.0)
    try:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass
    s.close()


def _cb(*a):
    # 订阅回调腿: 每次tick触发 做一次连接+关闭
    _sub_count[0] += 1
    try:
        _conn_close()
        if _sub_count[0] % 20 == 1:
            print('G[订阅腿] 第%d次连接OK %s' % (_sub_count[0], time.strftime('%H:%M:%S')))
    except Exception as e:
        if _sub_count[0] % 20 == 1:
            print('G[订阅腿] 第%d次失败 %s' % (_sub_count[0], repr(e)[:40]))


def _tick(C):
    # 定时器腿: 只打印心跳, 永远不碰socket
    _t_count[0] += 1
    if _t_count[0] % 10 == 0:
        print('G[定时器] 存活%d秒 订阅腿=%d handlebar腿=%d' %
              (_t_count[0], _sub_count[0], _hb_count[0]))


def init(C):
    print('G=== 探针G启动: 定时器零socket, 两腿等13:00开盘激活 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', _cb)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeG')


def handlebar(C):
    # handlebar腿: 每根bar/tick触发 做一次连接+关闭
    _hb_count[0] += 1
    try:
        _conn_close()
        if _hb_count[0] % 20 == 1:
            print('G[handlebar腿] 第%d次连接OK %s' % (_hb_count[0], time.strftime('%H:%M:%S')))
    except Exception as e:
        if _hb_count[0] % 20 == 1:
            print('G[handlebar腿] 第%d次失败 %s' % (_hb_count[0], repr(e)[:40]))
