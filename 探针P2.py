#coding:gbk
r"""
探针P2: 下载增量K线后再取 -> 验证数据由占位填充变为真实
  after_init: download_history_data('600519.SH','1d') 增量下载
  定时器: dump 最后2根(none) + volume核对(>0=真实数据)
"""
import datetime as dt
import time

_n = [0]


def _dump(C):
    try:
        raw = C.context.get_market_data2(
            [], ['600519.SH'], '1d', '', '', 3, 'none', True, False)
    except Exception as e:
        print('P2[dump] FAIL %s' % repr(e)[:110])
        return
    import pandas as pd
    for s, v in (raw or {}).items():
        sdf = pd.DataFrame(v)
        if 'stime' in sdf:
            sdf.index = sdf['stime']
        print('P2[下载后] 列=%s' % list(sdf.columns))
        for idx, row in sdf.tail(2).iterrows():
            vals = {c: row[c] for c in sdf.columns if c in (
                'stime', 'open', 'high', 'low', 'close', 'preClose', 'volume')}
            print('P2[下载后] %s: %s' % (idx, vals))


def _tick(C):
    _n[0] += 1
    if _n[0] == 1:
        _dump(C)
    elif _n[0] == 2:
        print('P2=== 完毕(volume>0且close≈1450=真实数据确认) ===')


def init(C):
    print('P2=== 探针P2启动: 下载后K线质量验证 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=2), 'probeP2')


def after_init(C):
    try:
        download_history_data('600519.SH', '1d', '', '')
        print('P2[after_init] 增量下载完成(全局函数姿势)')
    except NameError:
        try:
            C.context.download_history_data('600519.SH', '1d', '', '')
            print('P2[after_init] 增量下载完成(raw姿势)')
        except Exception as e:
            print('P2[download双姿势均失败] %s' % repr(e)[:90])
    except Exception as e:
        print('P2[download异常] %s' % repr(e)[:80])


def handlebar(C):
    pass
