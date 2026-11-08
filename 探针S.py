#coding:gbk
r"""
探针S: 线程泵架构验证(最终架构决胜)
  主线程(init/handlebar/定时器): 引擎调用(passorder/查询/print) — 拥有交易权
  后台Thread: 唯一socket所在(全双工泵), 与主线程经队列交换数据
  验证: (1)threading可用否 (2)线程socket长期存活否(评分是否豁免线程)
        (3)运行中模型passorder(主线程) (4)print出站
  若10分钟存活+交易通 = 最终架构定案
"""
import datetime as dt
import json
import socket
import struct
import threading
import time

SERVER_IP = '127.0.0.1'
SERVER_PORT = 51880
BRIDGE_TOKEN = 'PASTE_TOKEN_HERE'

_q = {'cmds': [], 'out': [], 'lock': threading.Lock()}
_st = {'thread_alive': False, 'frames': 0, 'errs': 0}


def _thread_main():
    _st['thread_alive'] = True
    sock = None
    n = 0
    while True:
        n += 1
        try:
            if sock is None:
                s = socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5.0)
                s.settimeout(0.05)
                hello = {'t': 'hello', 'role': 'bridge', 'proto': 1, 'token': BRIDGE_TOKEN}
                d = json.dumps(hello, ensure_ascii=False).encode('utf-8')
                s.sendall(struct.pack('<I', len(d)) + d)
                sock = s
                if n % 30 == 1:
                    print('S[线程泵] socket已建立 %s' % time.strftime('%H:%M:%S'))
        except Exception as e:
            sock = None
            if n % 30 == 1:
                print('S[线程泵] 连接失败 %s' % repr(e)[:40])
            time.sleep(1)
            continue
        try:
            # 发: 主线程出站队列
            with _q['lock']:
                out, _q['out'] = _q['out'], []
            for frame in out:
                dd = json.dumps(frame, ensure_ascii=False).encode('utf-8')
                sock.sendall(struct.pack('<I', len(dd)) + dd)
            # 收: 请求入cmds队列(主线程执行引擎调用)
            hard = time.time() + 0.15
            buf = b''
            while time.time() < hard:
                try:
                    chunk = sock.recv(262144)
                except socket.timeout:
                    break
                if not chunk:
                    raise ConnectionError('closed')
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
                        with _q['lock']:
                            _q['cmds'].append(msg)
                        _st['frames'] += 1
        except Exception as e:
            _st['errs'] += 1
            try:
                sock.close()
            except Exception:
                pass
            sock = None
            time.sleep(1)


def _dispatch(C, op, args):
    if op == 'ping':
        return {}
    if op == 'get_full_tick':
        return C.get_full_tick(list(args.get('codes') or []))
    if op == 'get_trading_dates':
        ex = C.get_market_data_ex([], ['000300.SH'], '1d', '', '', 250, 'none', True, False)
        df = (ex or {}).get('000300.SH')
        return [str(x)[:8] for x in df.index] if df is not None else []
    if op == 'get_instrument_detail':
        return C.get_instrument_detail(args.get('code') or '')
    if op == 'trader_connect':
        return {'account': '你的资金账号'}
    if op == 'subscribe_account':
        C.set_account(account)
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
        return [_mobj(x) for x in lst]
    if op == 'query_trades':
        lst = get_trade_detail_data(str(args.get('account')),
                                    args.get('account_type') or 'STOCK', 'DEAL')
        return [_mobj(x) for x in lst]
    if op == 'order_stock':
        seq = int(time.time()) % 100000
        remark = 'q2m:%d' % seq
        ur = args.get('order_remark') or ''
        if ur:
            remark = (remark + ' ' + ur)[:24]
        passorder(int(args.get('order_type')), 1101, str(args.get('account')),
                  args.get('code'), int(args.get('price_type')),
                  float(args.get('price') or 0), int(args.get('volume')),
                  args.get('strategy_name') or 'q2m', 2, remark, C)
        return {'order_id': seq, 'remark': remark}
    if op == 'cancel_order_stock':
        acc = str(args.get('account'))
        at = args.get('account_type') or 'STOCK'
        lst = get_trade_detail_data(acc, at, 'ORDER')
        sysid = None
        for o in lst:
            if getattr(o, 'm_strOrderSysID', '') == str(args.get('sysid', '')):
                sysid = str(args.get('sysid'))
                break
        if not sysid:
            return {'ok': 0, 'err': '柜台未找到该委托号'}
        ok = cancel(sysid, acc, at, C)
        return {'ok': 1 if ok else 0}
    raise Exception('未实现: %s' % op)


def _mobj(obj):
    d = {}
    try:
        for k in dir(obj):
            if k.startswith('m_'):
                v = getattr(obj, k)
                d[k[2:]] = v.decode('gbk', 'replace') if isinstance(v, bytes) else (
                    v if isinstance(v, (int, float, bool, str)) or v is None else str(v))
    except Exception:
        pass
    return d


def _drain_cmds(C):
    with _q['lock']:
        cmds, _q['cmds'] = _q['cmds'], []
    for msg in cmds:
        try:
            data = _dispatch(C, msg.get('op'), msg.get('args') or {})
            resp = {'t': 'resp', 'id': msg.get('id'), 'ok': True, 'data': _j2(data)}
        except Exception as e:
            resp = {'t': 'resp', 'id': msg.get('id'), 'ok': False,
                    'err': '%s: %s' % (msg.get('op'), repr(e)[:80])}
        with _q['lock']:
            _q['out'].append(resp)


def _j2(v):
    if isinstance(v, bytes):
        try:
            return v.decode('gbk', 'replace')
        except Exception:
            return str(v)
    if isinstance(v, dict):
        return {str(k): _j2(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_j2(x) for x in v]
    if isinstance(v, (int, float, bool, str)) or v is None:
        return v
    return str(v)


_n = [0]


def _tick(C):
    _n[0] += 1
    t = _n[0]
    _drain_cmds(C)
    if t % 5 == 0:
        try:
            tk = C.get_full_tick(['000300.SH'])
            d = (tk or {}).get('000300.SH') or {}
            with _q['lock']:
                _q['out'].append({'t': 'push', 'ch': 'quote',
                                  'data': {'sub_id': 0, 'data': {'000300.SH': {
                                      'lp': d.get('lastPrice'), 'time': d.get('time')}}}})
        except Exception:
            pass
        print('S[心跳] t=%d 线程=%s 帧=%d 错=%d' % (
            t, _st['thread_alive'], _st['frames'], _st['errs']))


def init(C):
    print('S=== 探针S启动: 线程泵架构验证 ===')
    try:
        C.set_account(account)
    except Exception as e:
        print('S[set_account]', repr(e)[:50])
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    try:
        th = threading.Thread(target=_thread_main)
        th.daemon = True
        th.start()
        time.sleep(0.3)
        print('S[线程] 已启动 alive=%s' % th.is_alive())
    except Exception as e:
        print('S[线程启动FAIL]', repr(e)[:80])
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeS')
    print('S=== 定时器+主线程就绪, 观察存活与交易 ===')


def handlebar(C):
    _drain_cmds(C)
