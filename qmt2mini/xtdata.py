# -*- coding: utf-8 -*-
"""qmt2mini.xtdata —— 与 xtquant.xtdata 同签名的行情门面

底层: qmt2mini_server ⇄ 大QMT桥策略
本模块线程安全; 所有函数可跨机器使用(局域网行情中心客户端)
"""
import itertools
import threading

from . import _conn

_conn_inst = None
_conn_lock = threading.Lock()

_subs = {}                 # seq -> callback
_subs_lock = threading.Lock()
_seq = itertools.count(1)

_push_handlers = []        # 模块级推送处理器（重连不丢）

_RUN_EVENT = threading.Event()

# 与 xtquant 对齐的周期/复权默认值
_PERIODS = ('tick', '1m', '5m', '15m', '30m', '1h', '1d', '1w', '1mon', '1q', '1hy', '1y')


def add_push_handler(fn):
    """注册推送处理器(重连保持有效)"""
    _push_handlers.append(fn)


def _dispatch(ch, data):
    for fn in list(_push_handlers):
        try:
            fn(ch, data)
        except Exception:
            pass


def _on_quote(ch, data):
    if ch != 'quote':
        return
    with _subs_lock:
        cb = _subs.get(data.get('sub_id'))
    if cb:
        try:
            cb(data.get('data'))
        except Exception:
            pass


def _on_download(ch, data):
    if ch != 'download':
        return
    with _subs_lock:
        cb = _subs.get(data.get('sub_id'))
    if cb:
        try:
            cb(data.get('progress'))
        except Exception:
            pass


add_push_handler(_on_quote)
add_push_handler(_on_download)


def _c():
    global _conn_inst
    with _conn_lock:
        if _conn_inst is None:
            c = _conn.Conn()
            c.add_push_handler(_dispatch)
            _conn_inst = c
        return _conn_inst


# ---------- 连接 ----------
def connect(ip='', port=None, remember_if_success=True):
    """连接 qmt2mini 服务端。ip/port 缺省取环境变量 QMT2MINI_HOST/QMT2MINI_PORT。
    与 xtquant 同名兼容：传 ip 时必须同时传 port。"""
    global _conn_inst
    host = ip or _conn.DEFAULT_HOST
    if host in ('localhost',):
        host = '127.0.0.1'
    p = port if port else _conn.DEFAULT_PORT
    with _conn_lock:
        old = _conn_inst
        c = _conn.Conn(host=host, port=p)
        c.add_push_handler(_dispatch)
        c.connect()
        _conn_inst = c
        if old is not None and old is not c:
            old.disconnect()
    return _conn_inst


def disconnect():
    global _conn_inst
    with _conn_lock:
        if _conn_inst is not None:
            _conn_inst.disconnect()
            _conn_inst = None


def get_data_dir():
    try:
        return _c().request('get_data_dir', timeout=10)
    except Exception:
        return ''


# ---------- 行情查询 ----------
def get_full_tick(code_list):
    """实时tick快照 dict{code: {字段...}}，字段同xtquant"""
    return _c().request('get_full_tick', {'codes': list(code_list)}, timeout=30)


def get_market_data_ex(field_list=[], stock_list=[], period='1d',
                       start_time='', end_time='', count=-1,
                       dividend_type='none', fill_data=True):
    """返回 dict{code: DataFrame}，index为时间字符串，columns为字段
    注: 引擎对小count语义怪癖(可能返回空), 故一律请求count=-1取全量本地数据,
        客户端侧按count切片; 有start_time/end_time时按原样传给引擎过滤"""
    ask_count = count if (start_time or end_time) else -1
    raw = _c().request('get_market_data_ex', {
        'fields': list(field_list or []), 'codes': list(stock_list or []),
        'period': period, 'start_time': start_time, 'end_time': end_time,
        'count': ask_count, 'dividend_type': dividend_type, 'fill_data': bool(fill_data),
    }, timeout=120)
    import pandas as pd
    out = {}
    for code, d in (raw or {}).items():
        df = pd.DataFrame(d['rows'], columns=d['columns'], index=d['index'])
        if count > 0 and len(df) > count and not (start_time or end_time):
            df = df.tail(count)
        out[code] = df
    return out


def get_market_data(field_list=[], stock_list=[], period='1d',
                    start_time='', end_time='', count=-1,
                    dividend_type='none', fill_data=True):
    """旧API兼容: dict{field: {code: array}} 且带 'time' 键"""
    ex = get_market_data_ex(field_list or [], stock_list or [], period,
                            start_time, end_time, count, dividend_type, fill_data)
    import numpy as np
    fields = list(field_list) if field_list else (
        list(next(iter(ex.values())).columns) if ex else [])
    out = {'time': {}}
    for f in fields:
        out[f] = {}
    for code, df in ex.items():
        out['time'][code] = np.array([str(t) for t in df.index])
        for f in fields:
            out[f][code] = np.array(df[f].tolist()) if f in df.columns else np.array([])
    return out


def get_trading_dates(market='SH', start_time='', end_time='', count=-1):
    return _c().request('get_trading_dates',
                        {'market': market, 'start_time': start_time,
                         'end_time': end_time, 'count': count}, timeout=60)


def get_instrument_detail(stock_code):
    return _c().request('get_instrument_detail', {'code': stock_code}, timeout=30)


def get_stock_list_in_sector(sector_name):
    return _c().request('get_stock_list_in_sector', {'sector': sector_name}, timeout=60)


def download_history_data2(stock_list, period, start_time='', end_time='', callback=None):
    """批量下载历史数据(桥端逐只串行, 每轮询周期一只); callback收到 {'finished':n,'total':m}"""
    seq = next(_seq)
    with _subs_lock:
        _subs[seq] = callback
    try:
        _c().request('download_history_data2',
                     {'codes': list(stock_list), 'period': period,
                      'start_time': start_time, 'end_time': end_time,
                      'sub_id': seq}, timeout=max(300, len(stock_list) * 60))
    finally:
        with _subs_lock:
            _subs.pop(seq, None)


# ---------- 订阅 ----------
def subscribe_quote(code, period='tick', callback=None, dividend_type='none'):
    """单股订阅, 返回订阅号; 回调收到 dict{code: {字段: 值}}"""
    seq = next(_seq)
    with _subs_lock:
        _subs[seq] = callback
    try:
        _c().request('subscribe_quote', {'code': code, 'period': period,
                                         'dividend_type': dividend_type,
                                         'shim_sub_id': seq}, timeout=30)
    except Exception:
        with _subs_lock:
            _subs.pop(seq, None)
        raise
    return seq


def subscribe_whole_quote(code_list, callback=None):
    """全推订阅(市场代码或个股列表), 返回订阅号; 回调收到 dict{code: tick字典}"""
    seq = next(_seq)
    with _subs_lock:
        _subs[seq] = callback
    try:
        _c().request('subscribe_whole_quote', {'codes': list(code_list),
                                               'shim_sub_id': seq}, timeout=30)
    except Exception:
        with _subs_lock:
            _subs.pop(seq, None)
        raise
    return seq


def unsubscribe_quote(seq):
    with _subs_lock:
        _subs.pop(seq, None)
    try:
        _c().request('unsubscribe_quote', {'sub_id': seq}, timeout=15)
    except Exception:
        pass


def run():
    """阻塞当前线程(与xtdata.run()同义), Ctrl+C可退出"""
    try:
        _RUN_EVENT.wait()
    except KeyboardInterrupt:
        pass
