#coding:gbk
r"""
qmt2mini 交易桥（大QMT端）——会话制交易执行器

与 桥_QMT端.py(阻塞init行情泵) 分工:
  - 本策略: 正常模型(schedule_run驱动), passorder合法上下文 → 负责下单/撤单/交易查询
  - 运行方式: 每 TRADE_POLL_SECONDS 拍一次 短连接交换(连接→执行→关闭),
    评分预算约112-170次连接/客户端会话(实机实测); 演练10秒拍, 生产60秒拍
  - 与行情桥互斥(单线程): 行情走阻塞init桥时本策略静默不连

使用: 全文粘贴为策略, 模型交易选账号实盘运行; 停止=转发器发die或停止策略
"""

import datetime as dt
import json
import socket
import struct
import time

# ================= 配置 =================
SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880
BRIDGE_TOKEN = 'PASTE_TOKEN_HERE'
TRADE_POLL_SECONDS = 10       # 演练10秒拍; 生产建议60秒拍(评分预算约2-3小时/会话)

_INDEX_CODE = {'SH': '000300.SH', 'SZ': '399001.SZ', 'BJ': '899050.BJ'}
_CANCELABLE_STATUS = (49, 50, 51, 52)   # 待报/部撤中状态


class _BridgeState(object):
    def __init__(self):
        self.outbox = []          # 待发推送/应答(每会话冲账)
        self.whole_shim_id = None
        self.whole_codes = []
        self.quote_subs = {}      # shim_id -> code
        self.poll_last = {}       # code -> diff key
        self.order_seq = 0
        self.sysid_map = {}       # order_seq -> 柜台委托号
        self.die = False


g = _BridgeState()
_ACC = ''


# ================= 工具 =================
def _jsonable(v):
    if isinstance(v, bytes):
        try:
            return v.decode('gbk', 'replace')
        except Exception:
            return str(v)
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (int, float, bool, str)) or v is None:
        return v
    return str(v)


def _mobj(obj):
    d = {}
    try:
        for k in dir(obj):
            if k.startswith('m_'):
                d[k[2:]] = _jsonable(getattr(obj, k))
    except Exception:
        pass
    return d


def _frame(obj):
    data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
    return struct.pack('<I', len(data)) + data


def _parse_oid(remark):
    if isinstance(remark, str) and remark.startswith('q2m:'):
        try:
            return int(remark[4:].split(' ')[0])
        except Exception:
            return -1
    return -1


def _dispatch_op(ContextInfo, op, args):
    if op == 'ping':
        return {}
    if op == 'die':
        g.die = True
        return {'bye': 1}
    if op == 'get_data_dir':
        return r'D:\QMT\userdata_mini'

    if op == 'get_full_tick':
        return _jsonable(ContextInfo.get_full_tick(list(args.get('codes') or [])))

    if op == 'get_market_data_ex':
        # 正常运行上下文: 包装器姿势可用(阻塞init才会C层转换失败)
        # 引擎K线库初始为占位填充bar(OHLC平线/volume=0): 检测到即自动增量下载后重取
        codes = list(args.get('codes') or [])
        fields = list(args.get('fields') or [])
        period = args.get('period') or '1d'
        start_time = args.get('start_time') or ''
        end_time = args.get('end_time') or ''
        count = int(args.get('count', -1))
        dividend_type = args.get('dividend_type') or 'none'
        dfs = ContextInfo.get_market_data_ex(
            fields, codes, period, start_time, end_time,
            count, dividend_type)
        need_dl = []
        for code, df in (dfs or {}).items():
            if len(df) and 'volume' in df and float(df['volume'].abs().sum()) == 0:
                need_dl.append(code)
        if need_dl:
            for code in need_dl[:5]:
                try:
                    download_history_data(code, period, start_time, end_time)
                except Exception:
                    pass
            dfs = ContextInfo.get_market_data_ex(
                fields, codes, period, start_time, end_time,
                count, dividend_type)
        out = {}
        for code, df in (dfs or {}).items():
            out[code] = {'columns': [str(x) for x in df.columns],
                         'index': [str(x) for x in df.index],
                         'rows': df.values.tolist()}
        return out

    if op == 'get_trading_dates':
        market = args.get('market') or 'SH'
        code = market if '.' in market else _INDEX_CODE.get(market, '000001.' + market)
        start = args.get('start_time') or ''
        end = args.get('end_time') or ''
        cnt = int(args.get('count', -1) or 0)
        if cnt <= 0:
            cnt = 1 if start else 240
        return _jsonable(ContextInfo.get_trading_dates(code, start, end, cnt, '1d'))

    if op == 'get_instrument_detail':
        return _jsonable(ContextInfo.get_instrument_detail(args.get('code') or ''))

    if op == 'get_stock_list_in_sector':
        return _jsonable(ContextInfo.get_stock_list_in_sector(args.get('sector') or '沪深A股'))

    if op == 'subscribe_whole_quote':
        g.whole_shim_id = args.get('shim_sub_id')
        g.whole_codes = list(args.get('codes') or [])
        return {'sub_id': g.whole_shim_id}

    if op == 'subscribe_quote':
        shim_id = args.get('shim_sub_id')
        g.quote_subs[shim_id] = args.get('code')
        return {'sub_id': shim_id}

    if op == 'unsubscribe_quote':
        g.quote_subs.pop(args.get('sub_id'), None)
        return {}

    if op == 'trader_connect':
        if not _ACC:
            raise Exception('交易桥未绑定资金账号: 请在模型交易界面选择账号后实盘运行')
        return {'account': _ACC}

    if op == 'subscribe_account':
        if _ACC:
            ContextInfo.set_account(_ACC)
        return {'ok': 1}

    if op == 'unsubscribe_account':
        return {'ok': 1}

    if op == 'query_asset':
        lst = get_trade_detail_data(str(args.get('account')),
                                    args.get('account_type') or 'STOCK', 'ACCOUNT')
        return _mobj(lst[0]) if lst else None

    if op == 'query_positions':
        lst = get_trade_detail_data(str(args.get('account')),
                                    args.get('account_type') or 'STOCK', 'POSITION')
        return [_mobj(x) for x in lst]

    if op == 'query_orders':
        lst = get_trade_detail_data(str(args.get('account')),
                                    args.get('account_type') or 'STOCK', 'ORDER')
        out = [_mobj(x) for x in lst]
        if args.get('cancelable_only'):
            out = [x for x in out if x.get('nOrderStatus') in _CANCELABLE_STATUS]
        return out

    if op == 'query_trades':
        lst = get_trade_detail_data(str(_ACC or args.get('account') or ''),
                                    args.get('account_type') or 'STOCK', 'DEAL')
        return [_mobj(x) for x in lst]

    if op == 'order_stock':
        if not _ACC:
            raise Exception('交易桥未绑定资金账号')
        g.order_seq += 1
        seq = g.order_seq
        remark = 'q2m:%d' % seq
        ur = args.get('order_remark') or ''
        if ur:
            remark = (remark + ' ' + ur)[:24]
        passorder(int(args.get('order_type')), 1101, str(args.get('account')),
                  args.get('code'), int(args.get('price_type')),
                  float(args.get('price') or 0), int(args.get('volume')),
                  args.get('strategy_name') or 'q2m', 2, remark, ContextInfo)
        return {'order_id': seq}

    if op == 'cancel_order_stock':
        oid = int(args.get('order_id'))
        acc = str(args.get('account'))
        at = get_account_type_str(args)
        sysid = g.sysid_map.get(oid)
        if not sysid:
            for o in get_trade_detail_data(acc, at, 'ORDER'):
                d = _mobj(o)
                if _parse_oid(d.get('strRemark', '')) == oid and d.get('strOrderSysID'):
                    sysid = d['strOrderSysID']
                    break
        if not sysid:
            raise Exception('未找到委托(order_id=%s), 可能尚未回报' % oid)
        ok = cancel(sysid, acc, at, ContextInfo)
        return {'ok': 1 if ok else 0}

    if op == 'download_history_data2':
        codes = list(args.get('codes') or [])
        g.dl_queue = [(c, args.get('period') or '1d',
                       args.get('start_time') or '', args.get('end_time') or '') for c in codes]
        g.dl_total = len(codes)
        g.dl_done = 0
        g.dl_sub = args.get('shim_sub_id')
        return {'queued': len(codes)}


def _dl_one(item):
    """download_history_data 是全局注入函数(与passorder同族), 非ContextInfo方法"""
    code, period, st, et = item
    download_history_data(code, period, st, et)

    raise Exception('交易桥未实现该操作: %s' % op)


def get_account_type_str(args):
    return args.get('account_type') or 'STOCK'


# ================= 交易主推回调(会话模型线程不被占, 回调可正常触发) =================
def order_callback(*args):
    d = _mobj(args[-1])
    oid = _parse_oid(d.get('strRemark', ''))
    if oid >= 0 and d.get('strOrderSysID'):
        g.sysid_map[oid] = d['strOrderSysID']
    g.outbox.append({'t': 'push', 'ch': 'trade', 'data': {'event': 'stock_order', 'data': d}})


def deal_callback(*args):
    g.outbox.append({'t': 'push', 'ch': 'trade', 'data': {'event': 'stock_trade', 'data': _mobj(args[-1])}})


def position_callback(*args):
    pass


def account_callback(*args):
    pass


def orderError_callback(*args):
    remark = ''
    try:
        remark = args[-2].strategyName
    except Exception:
        pass
    g.outbox.append({'t': 'push', 'ch': 'trade',
                     'data': {'event': 'order_error',
                              'data': {'remark': _jsonable(remark),
                                       'error_msg': _jsonable(args[-1])}}})


# ================= 行情推送辅助(会话制下按拍差分) =================
def _poll_and_push(ContextInfo):
    if g.whole_shim_id is None and not g.quote_subs:
        return
    codes = set(g.whole_codes or [])
    for code in g.quote_subs.values():
        codes.add(code)
    if not codes:
        return
    try:
        ticks = ContextInfo.get_full_tick(sorted(codes))
    except Exception:
        return
    changed = {}
    for code, tk in (ticks or {}).items():
        key = (tk.get('lastPrice'), tk.get('volume'), tk.get('time'))
        if g.poll_last.get(code) != key:
            g.poll_last[code] = key
            changed[code] = tk
    if not changed:
        return
    items = list(changed.items())[:3000]
    payload = _jsonable(dict(items))
    if g.whole_shim_id is not None:
        g.outbox.append({'t': 'push', 'ch': 'quote',
                         'data': {'sub_id': g.whole_shim_id, 'data': payload}})
    for shim_id, code in g.quote_subs.items():
        if code in changed:
            g.outbox.append({'t': 'push', 'ch': 'quote',
                             'data': {'sub_id': shim_id, 'data': {code: payload.get(code)}}})


# ================= 会话泵(每拍: 连接→执行→关闭) =================
def _handle_req(ContextInfo, sock, msg):
    """指令在回调上下文执行: passorder合法(quickTrade=2), 交易缓存实时"""
    try:
        data = _dispatch_op(ContextInfo, msg.get('op'), msg.get('args') or {})
        resp = {'t': 'resp', 'id': msg.get('id'), 'ok': True, 'data': data}
    except Exception as e:
        resp = {'t': 'resp', 'id': msg.get('id'), 'ok': False,
                'err': '%s: %s' % (msg.get('op'), _jsonable(e))}
    sock.sendall(_frame(resp))


def _trade_poll(ContextInfo):
    if g.die:
        return
    # 下载队列: 每拍处理一只(增量下载, 阻塞秒级可接受)
    if g.dl_queue:
        item = g.dl_queue.pop(0)
        try:
            _dl_one(item)
        except Exception:
            pass
        g.dl_done += 1
        if g.dl_sub is not None:
            g.outbox.append({'t': 'push', 'ch': 'download',
                             'data': {'sub_id': g.dl_sub,
                                      'progress': {'finished': g.dl_done, 'total': g.dl_total}}})
    _poll_and_push(ContextInfo)
    try:
        s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=3.0)
    except Exception:
        return
    try:
        s.settimeout(0.05)
        hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
        if BRIDGE_TOKEN:
            hello['token'] = BRIDGE_TOKEN
        s.sendall(_frame(hello))
        while g.outbox:
            s.sendall(_frame(g.outbox.pop(0)))
        buf = b''
        hard = time.time() + 0.4
        while time.time() < hard:
            try:
                chunk = s.recv(262144)
            except socket.timeout:
                break
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            while len(buf) >= 4:
                ln = struct.unpack('<I', buf[:4])[0]
                if len(buf) < 4 + ln:
                    break
                payload = buf[4:4 + ln]
                buf = buf[4 + ln:]
                try:
                    msg = json.loads(payload.decode('utf-8'))
                except Exception:
                    continue
                if msg.get('t') == 'req':
                    _handle_req(ContextInfo, s, msg)
    except OSError:
        pass
    finally:
        try:
            s.close()
        except Exception:
            pass


# ================= 生命周期 =================
def init(C):
    global _ACC
    try:
        _ACC = account
    except NameError:
        _ACC = ''
    if _ACC:
        try:
            C.set_account(_ACC)
        except Exception as e:
            print('q2m交易桥: set_account异常 %s' % _jsonable(e))
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_trade_poll, '20200101000000', -1,
                   dt.timedelta(seconds=TRADE_POLL_SECONDS), 'q2m_trade')
    print('q2m交易桥: 会话制启动 -> %s:%d 每%d秒拍 账号=%s'
          % (SERVER_IP, SERVER_PORT, TRADE_POLL_SECONDS, _ACC))


def after_init(C):
    global _ACC
    if not _ACC:
        try:
            _ACC = account
            if _ACC:
                C.set_account(_ACC)
                print('q2m交易桥: after_init补绑账号 %s' % _ACC)
        except Exception:
            pass


def handlebar(C):
    pass
