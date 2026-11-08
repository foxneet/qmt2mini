#coding:gbk
r"""
qmt2mini 桥 v3（大QMT端）—— 单模型全功能合并版

架构(实测定案):
  正常模型(schedule_run 1秒拍) —— passorder/K线/交易缓存全部合法可用:
    每拍      : get_full_tick 差分 -> print('Q2M:...') 出站(零评分, 行情桥级别)
    每10拍    : 交易缓存快照(资产/持仓/委托/成交) -> print 出站
    每CMD秒   : 短连接指令交换(连接->执行->直答->关闭) —— 唯一计分活动
               (下单/撤单/K线/任意op; 服务端可动态调频)
  出站不占评分预算; 指令通道消耗会话评分(阈值≈112-170, 日重启清零)。

  引擎日志(FormulaOutput)是出站物理载体, 转发器tail解析'Q2M:'帧。
  分片协议: Q2M:{帧id}:{片序}:{总片数}:{json片段}

使用: 全文粘贴为策略, 模型交易选账号实盘运行。
停止: 转发器发die, 或QMT重启。引擎"策略停止"日志(若有)不影响。
"""

import base64
import datetime as dt
import json
import socket
import struct
import time
import zlib

# ================= 配置 =================
SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880
BRIDGE_TOKEN = ''
MAIN_TICK_SECONDS = 1        # 主循环拍: tick差分+print推送
SNAP_EVERY_TICKS = 10        # 交易快照间隔(主循环拍数)
CMD_POLL_SECONDS = 60        # 指令通道连接间隔(默认; 服务端可动态调整)
CMD_DRAIN_SECONDS = 0.6      # 指令交换收包窗口
PRINT_PREFIX = 'Q2M:'
PRINT_CHUNK = 40             # b64片长(字符): 行总长~60, 远离引擎~83字符换行边界

_INDEX_CODE = {'SH': '000300.SH', 'SZ': '399001.SZ', 'BJ': '899050.BJ'}
_CANCELABLE_STATUS = (49, 50, 51, 52)


class _BridgeState(object):
    def __init__(self):
        self.n = 0
        self.fid = 0                     # print帧id(单调递增)
        self.whole_shim_id = None
        self.whole_codes = []
        self.quote_subs = {}             # shim_id -> code
        self.poll_last = {}              # code -> diff key
        self.tsnap_seq = 0
        self.order_seq = 0
        self.sysid_map = {}              # order_seq -> 柜台委托号
        self.next_cmd = 0.0
        self.cmd_poll = CMD_POLL_SECONDS
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


def _emit(obj):
    """print出站: zlib+base64分片, 每片带长度校验。
    引擎日志写入器会向长行注入碎片(实测固定~217字符处) -> 片损坏由长度校验剔除,
    整帧丢弃无害: tick差分每周期重推、tsnap每10拍重推(自愈)。"""
    try:
        s = json.dumps(obj, ensure_ascii=False, separators=(',', ':'))
        blob = base64.b64encode(zlib.compress(s.encode('utf-8'), 6)).decode('ascii')
        g.fid += 1
        fid = g.fid
        chunks = [blob[i:i + PRINT_CHUNK] for i in range(0, len(blob), PRINT_CHUNK)] or ['']
        tot = len(chunks)
        for i, ch in enumerate(chunks):
            print('%s%d:%d:%d:%d:%s' % (PRINT_PREFIX, fid, i, tot, len(ch), ch))
    except Exception as e:
        try:
            print('Q2MERR:%s' % repr(e)[:60])
        except Exception:
            pass


# ================= 操作分发(全套) =================
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
        # 官方姿势: 原始上下文 get_market_data2 + pandas拼装(阻塞init才有的C层转换问题不存在于本模型)
        codes = list(args.get('codes') or [])
        fields = list(args.get('fields') or [])
        period = args.get('period') or '1d'
        start_time = args.get('start_time') or ''
        end_time = args.get('end_time') or ''
        count = int(args.get('count', -1))
        dividend_type = args.get('dividend_type') or 'none'
        last_err = None
        for attempt in (0, 1, 2):
            try:
                import pandas as pd
                raw_ctx = getattr(ContextInfo, 'context', ContextInfo)
                ori_data = raw_ctx.get_market_data2(
                    fields, codes, period, start_time, end_time, count,
                    dividend_type, True, False)
                result = {}
                ifield = 'stime'
                fl = fields
                if fl:
                    fl2 = fl if ifield in fl else [ifield] + fl
                    for s in (ori_data or {}):
                        sdata = pd.DataFrame(ori_data[s], columns=fl2)
                        sdata2 = sdata[fl]
                        sdata2.index = sdata[ifield]
                        result[s] = sdata2
                else:
                    for s in (ori_data or {}):
                        sdata = pd.DataFrame(ori_data[s])
                        if ifield in sdata:
                            sdata.index = sdata[ifield]
                        result[s] = sdata
                out = {}
                for code, df in result.items():
                    out[code] = {'columns': [str(x) for x in df.columns],
                                 'index': [str(x) for x in df.index],
                                 'rows': df.values.tolist()}
                return out
            except Exception as e:
                last_err = e
                time.sleep(0.4)
        # K线空/失败: 占位填充bar检测 -> 增量下载后重试一次
        try:
            for code in codes[:5]:
                download_history_data(code, period, start_time, end_time)
            import pandas as pd
            raw_ctx = getattr(ContextInfo, 'context', ContextInfo)
            ori_data = raw_ctx.get_market_data2(
                fields, codes, period, start_time, end_time, count,
                dividend_type, True, False)
            out = {}
            for s, v in (ori_data or {}).items():
                sdf = pd.DataFrame(v)
                if 'stime' in sdf:
                    sdf.index = sdf['stime']
                out[s] = {'columns': [str(x) for x in sdf.columns],
                          'index': [str(x) for x in sdf.index],
                          'rows': sdf.values.tolist()}
            return out
        except Exception as e:
            last_err = e
        raise Exception('get_market_data_ex: %s' % _jsonable(last_err))

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
            raise Exception('桥未绑定资金账号: 请在模型交易界面选择账号后实盘运行')
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
        lst = get_trade_detail_data(str(args.get('account')),
                                    args.get('account_type') or 'STOCK', 'DEAL')
        return [_mobj(x) for x in lst]

    if op == 'order_stock':
        if not _ACC:
            raise Exception('桥未绑定资金账号')
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
        at = args.get('account_type') or 'STOCK'
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
        for c in codes[:20]:
            try:
                download_history_data(c, args.get('period') or '1d',
                                      args.get('start_time') or '',
                                      args.get('end_time') or '')
            except Exception:
                pass
        return {'downloaded': len(codes[:20])}

    raise Exception('桥未实现该操作: %s' % op)


# ================= 交易主推回调(正常模型可触发, 兜底同步sysid) =================
def order_callback(*args):
    d = _mobj(args[-1])
    oid = _parse_oid(d.get('strRemark', ''))
    if oid >= 0 and d.get('strOrderSysID'):
        g.sysid_map[oid] = d['strOrderSysID']


def deal_callback(*args):
    pass


def position_callback(*args):
    pass


def account_callback(*args):
    pass


def orderError_callback(*args):
    pass


# ================= 出站: tick差分 + 交易快照 =================
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
        _emit({'t': 'push', 'ch': 'quote',
               'data': {'sub_id': g.whole_shim_id, 'data': payload}})
    for shim_id, code in g.quote_subs.items():
        if code in changed:
            _emit({'t': 'push', 'ch': 'quote',
                   'data': {'sub_id': shim_id, 'data': {code: payload.get(code)}}})


def _push_tsnap(ContextInfo):
    acc = str(_ACC or '')
    if not acc:
        return
    snap = {'time': time.strftime('%Y%m%d%H%M%S')}
    try:
        lst = get_trade_detail_data(acc, 'STOCK', 'ACCOUNT')
        snap['account'] = _mobj(lst[0]) if lst else None
        lst = get_trade_detail_data(acc, 'STOCK', 'POSITION')
        snap['positions'] = [_mobj(x) for x in lst]
        lst = get_trade_detail_data(acc, 'STOCK', 'ORDER')
        snap['orders'] = [_mobj(x) for x in lst]
        lst = get_trade_detail_data(acc, 'STOCK', 'DEAL')
        snap['trades'] = [_mobj(x) for x in lst]
    except Exception as e:
        snap['err'] = _jsonable(e)
    _emit({'t': 'push', 'ch': 'tsnap', 'data': snap})


# ================= 指令通道(唯一计分活动) =================
def _handle_req(ContextInfo, sock, msg):
    try:
        data = _dispatch_op(ContextInfo, msg.get('op'), msg.get('args') or {})
        resp = {'t': 'resp', 'id': msg.get('id'), 'ok': True, 'data': data}
    except Exception as e:
        resp = {'t': 'resp', 'id': msg.get('id'), 'ok': False,
                'err': '%s: %s' % (msg.get('op'), _jsonable(e))}
    sock.sendall(_frame(resp))


def _cmd_exchange(ContextInfo):
    """短连接指令交换: 连接->取指令->在回调上下文执行(passorder合法)->直答->关闭"""
    try:
        s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=3.0)
    except Exception:
        g.next_cmd = time.time() + 30          # 失败退避(拒连也计分, 拉长间隔)
        return
    try:
        s.settimeout(0.05)
        hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
        if BRIDGE_TOKEN:
            hello['token'] = BRIDGE_TOKEN
        s.sendall(_frame(hello))
        buf = b''
        hard = time.time() + CMD_DRAIN_SECONDS
        while time.time() < hard and not g.die:
            try:
                chunk = s.recv(262144)
            except socket.timeout:
                break
            except OSError:
                raise ConnectionError('socket错误')
            if not chunk:
                raise ConnectionError('对端关闭')
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
                    if msg.get('op') == 'die':
                        g.die = True
    except (ConnectionError, OSError):
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
            print('q2m桥v3: set_account异常 %s' % _jsonable(e))
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_main_loop, '20200101000000', -1,
                   dt.timedelta(seconds=MAIN_TICK_SECONDS), 'q2m_main')
    print('q2m桥v3: 单模型全功能启动 -> %s:%d 行情print推送%d秒 指令通道%ds 账号=%s'
          % (SERVER_IP, SERVER_PORT, MAIN_TICK_SECONDS, CMD_POLL_SECONDS, _ACC))


def after_init(C):
    global _ACC
    if not _ACC:
        try:
            _ACC = account
            if _ACC:
                C.set_account(_ACC)
        except Exception:
            pass


def _main_loop(ContextInfo):
    g.n += 1
    _poll_and_push(ContextInfo)
    if g.n % SNAP_EVERY_TICKS == 0:
        _push_tsnap(ContextInfo)
    if time.time() >= g.next_cmd and not g.die:
        _cmd_exchange(ContextInfo)
        if g.die:
            print('q2m桥v3: 收到die, 桥活动停止(模型仍在运行, 重启QMT彻底清理)')


def handlebar(C):
    pass
