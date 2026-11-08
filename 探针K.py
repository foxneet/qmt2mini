#coding:gbk
r"""
探针K: 摸清策略内文件IO的真实边界(零socket, 不碰网络评分)
  t=1-6秒 依次测试: 读F盘 / 写F盘 / 读D盘QMT / 写userdata / 读userdata / 列目录
  之后每10秒心跳 -> 顺便验证零socket常驻能否长期存活
  结论全看策略日志(我会自己读FormulaOutput)
"""
import datetime as dt
import time

_n = [0]


def _try(tag, fn):
    try:
        r = fn()
        print('K[%s] OK %s' % (tag, str(r)[:60]))
    except Exception as e:
        print('K[%s] FAIL %s' % (tag, repr(e)[:70]))


def _tick(C):
    _n[0] += 1
    t = _n[0]
    if t == 1:
        def rd_f():
            f = open(r'F:\qmt2mini\探针I.py', 'r')
            try:
                return len(f.read())
            finally:
                f.close()
        _try('读F盘已存在文件', rd_f)
    elif t == 2:
        def wr_f():
            f = open(r'F:\qmt2mini\q2m_probe_write.txt', 'w')
            try:
                f.write('hello')
            finally:
                f.close()
            return 'written'
        _try('写F盘新文件', wr_f)
    elif t == 3:
        def rd_d():
            f = open(r'D:\QMT\userdata\log\XtClient_20261106.log', 'r')
            try:
                return len(f.read())
            finally:
                f.close()
        _try('读QMT日志', rd_d)
    elif t == 4:
        def wr_ud():
            f = open(r'D:\QMT\userdata\q2m_probe_write.txt', 'w')
            try:
                f.write('hello')
            finally:
                f.close()
            return 'written'
        _try('写userdata', wr_ud)
    elif t == 5:
        def rd_ud():
            f = open(r'D:\QMT\userdata\q2m_probe_write.txt', 'r')
            try:
                return f.read()
            finally:
                f.close()
        _try('回读userdata', rd_ud)
    elif t == 6:
        def ls_d():
            import os
            return len(os.listdir(r'D:\QMT\userdata'))
        _try('listdir userdata', ls_d)
    elif t % 10 == 0:
        print('K[心跳] 存活%d秒' % t)


def init(C):
    print('K=== 探针K启动: 零socket, 文件IO边界测试 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeK')


def handlebar(C):
    pass
