#coding:gbk
r"""
探针O2: 原始上下文(ContextInfo.context).get_market_data2 + 就绪重试
  每2秒尝试一次, 拿到K线即PASS打印并停; 60秒窗口内见分晓
"""
import datetime as dt
import time

_n = [0]
_ok = [False]


def _tick(C):
    _n[0] += 1
    if _ok[0]:
        return
    try:
        raw = C.context
    except Exception as e:
        if _n[0] % 5 == 0:
            print('O2[取raw失败] %s' % repr(e)[:70])
        return
    try:
        ori = raw.get_market_data2([], ['600519.SH'], '1d', '', '', 5, 'none', True, False)
    except Exception as e:
        if _n[0] % 5 == 0:
            print('O2[t=%d] get_market_data2异常: %s' % (_n[0], repr(e)[:110]))
        return
    import pandas as pd
    result = {}
    for s in (ori or {}):
        sdf = pd.DataFrame(ori[s])
        if 'stime' in sdf:
            sdf.index = sdf['stime']
        result[s] = sdf
    df = result.get('600519.SH')
    if df is not None and len(df):
        _ok[0] = True
        tail = ''
        for col in ('close', 'stime'):
            if col in df:
                tail += ' %s尾=%s' % (col, df[col].iloc[-1])
        print('O2[K线PASS] t=%d秒 %d行%s' % (_n[0], len(df), tail))
    else:
        if _n[0] % 5 == 0:
            print('O2[t=%d] 转换成功但返回空: %s' % (_n[0], str(ori)[:90]))


def init(C):
    print('O2=== 探针O2启动: 原始上下文K线+就绪重试 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=2), 'probeO2')


def handlebar(C):
    pass
