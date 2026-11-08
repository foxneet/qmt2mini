#coding:gbk
r"""
探针O: (1)运行时取证——socket/open 钩子源码与注入模块排查(只读诊断)
      (2)get_market_data2+pandas 一次性K线验证(官方姿势)
  init内快速执行后正常返回(不打扰探针N的连接计数)
"""
import json
import sys
import time


def _forensics():
    import builtins
    import inspect
    import socket
    print('O[取证] socket.__file__ = %s' % getattr(socket, '__file__', '?'))
    cc = getattr(socket, 'create_connection', None)
    print('O[取证] create_connection = %s' % repr(cc)[:160])
    try:
        src = inspect.getsource(cc)
        for i in range(0, min(len(src), 900), 220):
            print('O[取证] cc源码[%d]: %s' % (i, src[i:i + 220].replace('\n', ' | ')))
    except Exception as e:
        print('O[取证] cc.getsource失败: %s' % repr(e)[:70])
    print('O[取证] builtins.open = %s' % repr(builtins.open)[:160])
    try:
        src = inspect.getsource(builtins.open)
        for i in range(0, min(len(src), 700), 220):
            print('O[取证] open源码[%d]: %s' % (i, src[i:i + 220].replace('\n', ' | ')))
    except Exception as e:
        print('O[取证] open.getsource失败: %s' % repr(e)[:70])
    mods = [m for m in sys.modules
            if any(k in m.lower() for k in ('hook', 'inject', 'policy', 'sandbox', 'guard', 'forbid'))]
    print('O[取证] 可疑注入模块: %s' % mods[:12])
    try:
        so = socket.socket
        print('O[取证] socket.socket类型: %s' % repr(so)[:120])
        print('O[取证] socket.connect: %s' % repr(so.connect)[:160])
    except Exception as e:
        print('O[取证] socket类取证失败: %s' % repr(e)[:60])


def _kline_test(ContextInfo):
    try:
        import pandas as pd
        ori = ContextInfo.get_market_data2(
            [], ['600519.SH'], '1d', '', '', 5, 'none', True, False)
        print('O[K线] get_market_data2返回: 类型=%s keys=%s'
              % (type(ori).__name__, list((ori or {}).keys())[:5]))
        result = {}
        for s in (ori or {}):
            sdf = pd.DataFrame(ori[s])
            if 'stime' in sdf:
                sdf.index = sdf['stime']
            result[s] = sdf
        df = result.get('600519.SH')
        if df is not None and len(df):
            tail = ''
            for col in ('close', 'stime'):
                if col in df:
                    tail += ' %s尾=%s' % (col, df[col].iloc[-1])
            print('O[K线] PASS 组装%d行%s' % (len(df), tail))
        else:
            print('O[K线] 返回空数据(转换通了但无缓存)')
    except Exception as e:
        print('O[K线] FAIL: %s' % repr(e)[:160])


def init(ContextInfo):
    print('O=== 探针O启动: 运行时取证 + K线一次性验证 ===')
    try:
        _forensics()
    except Exception as e:
        print('O[取证]整体失败: %s' % repr(e)[:80])
    try:
        _kline_test(ContextInfo)
    except Exception as e:
        print('O[K线]整体失败: %s' % repr(e)[:80])
    print('O=== 完毕, init正常返回 ===')


def handlebar(C):
    pass
