#coding:gbk
r"""
探针L: 引擎文件IO白名单测绘(每个路径读+写各试一次)
  候选: python目录 / datadir / export_data / users账号目录 / bin.x64 / 相对路径 / TEMP / export_data子目录
  另打印 os.getcwd() 确认相对路径落点。结果全看策略日志。
"""
import datetime as dt
import os
import time

_CANDIDATES = [
    (r'D:\QMT\python\q2m_test.txt', 'python目录'),
    (r'D:\QMT\datadir\q2m_test.txt', 'datadir'),
    (r'D:\QMT\export_data\q2m_test.txt', 'export_data'),
    (r'D:\QMT\userdata\users\你的资金账号\q2m_test.txt', 'users账号目录'),
    (r'D:\QMT\bin.x64\q2m_test.txt', 'bin.x64'),
    (r'D:\QMT\config\q2m_test.txt', 'config'),
]

_n = [0]


def _try_rw(path, tag):
    # 写
    try:
        f = open(path, 'w')
        try:
            f.write('q2m_' + time.strftime('%H%M%S'))
        finally:
            f.close()
        wr = '写OK'
    except Exception as e:
        wr = '写FAIL ' + repr(e)[:40]
    # 读
    try:
        f = open(path, 'r')
        try:
            content = f.read()
        finally:
            f.close()
        rd = '读OK(%d字节)' % len(content)
    except Exception as e:
        rd = '读FAIL ' + repr(e)[:40]
    print('L[%s] %s | %s' % (tag, wr, rd))


def _tick(C):
    _n[0] += 1
    t = _n[0]
    if t == 1:
        print('L=== getcwd = %s ===' % os.getcwd())
        print('L=== TEMP = %s ===' % os.environ.get('TEMP', '?'))
    elif t == 2:
        _try_rw(_CANDIDATES[0][0], _CANDIDATES[0][1])
    elif t == 3:
        _try_rw(_CANDIDATES[1][0], _CANDIDATES[1][1])
    elif t == 4:
        _try_rw(_CANDIDATES[2][0], _CANDIDATES[2][1])
    elif t == 5:
        _try_rw(_CANDIDATES[3][0], _CANDIDATES[3][1])
    elif t == 6:
        _try_rw(_CANDIDATES[4][0], _CANDIDATES[4][1])
    elif t == 7:
        _try_rw(_CANDIDATES[5][0], _CANDIDATES[5][1])
    elif t == 8:
        _try_rw('q2m_test_rel.txt', '相对路径')
    elif t == 9:
        tmp = os.environ.get('TEMP', r'C:\Temp') + r'\q2m_test.txt'
        _try_rw(tmp, 'TEMP')
    elif t == 10:
        _try_rw(r'D:\QMT\export_data\q2m子目录\q2m_test.txt', 'export_data子目录')
    elif t == 11:
        print('L=== 测绘完毕 ===')


def init(C):
    print('L=== 探针L启动: 文件IO白名单测绘 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeL')


def handlebar(C):
    pass
