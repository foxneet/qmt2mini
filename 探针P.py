#coding:gbk
r"""
探针P: K线数据质量核对
  对 600519 1d 分别取 none/front/back 三种复权, 各打最后2根的全字段
  + get_full_tick 实价对照 -> 定位 1312.97 vs 1450 的差异来源
"""
import datetime as dt
import json
import time

_n = [0]
_done = [0]


def _dump(ContextInfo, tag, divid):
    try:
        raw = ContextInfo.context.get_market_data2(
            [], ['600519.SH'], '1d', '', '', 3, divid, True, False)
    except Exception as e:
        print('P[%s] FAIL %s' % (tag, repr(e)[:110]))
        return
    import pandas as pd
    for s, sdf in ((s, pd.DataFrame(v)) for s, v in (raw or {}).items()):
        if 'stime' in sdf:
            sdf.index = sdf['stime']
        print('P[%s] 列=%s' % (tag, list(sdf.columns)))
        for idx, row in sdf.tail(2).iterrows():
            vals = {c: row[c] for c in sdf.columns if c in (
                'stime', 'open', 'high', 'low', 'close', 'preClose', 'volume', 'settle')}
            print('P[%s] %s: %s' % (tag, idx, vals))


def _tick(C):
    _n[0] += 1
    t = _n[0]
    if t == 1:
        try:
            tk = C.get_full_tick(['600519.SH']).get('600519.SH') or {}
            print('P[tick实价] lastPrice=%s time=%s' % (
                tk.get('lastPrice'), tk.get('timetag', tk.get('time'))))
        except Exception as e:
            print('P[tick] FAIL %s' % repr(e)[:60])
    elif t == 2:
        _dump(C, 'none', 'none')
    elif t == 3:
        _dump(C, 'front', 'front')
    elif t == 4:
        _dump(C, 'back', 'back')
    elif t == 5:
        print('P=== 测绘完毕(对照你图表的复权设置即可定位) ===')


def init(C):
    print('P=== 探针P启动: K线数据质量核对 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=2), 'probeP')


def handlebar(C):
    pass
