# -*- coding: utf-8 -*-
"""qmt2mini 模拟联调：服务端+shim 全协议测试（mock桥替代QMT桥策略）
通过标准: 全部 [PASS]
运行: py -3.13 测试_模拟.py
"""
import json
import struct
import sys
import threading
import time

sys.path.insert(0, r'F:\qmt2mini')

from qmt2mini_server import Server, recv_msg          # noqa: E402
from qmt2mini import xtdata                            # noqa: E402
from qmt2mini import _conn as xc                       # noqa: E402
from qmt2mini.xttrader import XtQuantTrader, XtQuantTraderCallback  # noqa: E402
from qmt2mini.xttype import StockAccount               # noqa: E402
from qmt2mini import xtconstant                        # noqa: E402

RESULTS = []


def check(name, cond, detail=''):
    RESULTS.append((name, bool(cond), detail))
    print('[%s] %s %s' % ('PASS' if cond else 'FAIL', name, detail), flush=True)


def send_msg(sock, obj):
    data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
    sock.sendall(struct.pack('<I', len(data)) + data)


TICK = {'600519.SH': {'time': 1733118954000, 'lastPrice': 1800.5, 'open': 1790.0,
                      'high': 1810.0, 'low': 1785.0, 'lastClose': 1795.0,
                      'amount': 8.6e8, 'volume': 758613, 'askPrice': [1801.0] * 5,
                      'bidPrice': [1800.0] * 5}}
KLINE = {'600519.SH': {'columns': ['time', 'open', 'high', 'low', 'close', 'volume'],
                       'index': ['20261103', '20261104', '20261105'],
                       'rows': [[20261103, 1790.0, 1800.0, 1780.0, 1795.0, 700000],
                                [20261104, 1795.0, 1810.0, 1790.0, 1800.0, 720000],
                                [20261105, 1800.0, 1815.0, 1795.0, 1800.5, 758613]]}}
ASSET = {'strAccountID': '你的资金账号', 'dAvailable': 123456.78, 'dBalance': 223456.78,
         'dFrozenCash': 5000.0, 'dInstrumentValue': 95000.0}
POSITION = {'strAccountID': '你的资金账号', 'strExchangeID': 'SH', 'strInstrumentID': '600519',
            'nVolume': 100, 'nCanUseVolume': 100, 'dOpenPrice': 1750.0,
            'dMarketValue': 180050.0, 'nFrozenVolume': 0, 'nOnRoadVolume': 0,
            'nYesterdayVolume': 100, 'dAvgOpenPrice': 1751.0}
ORDER = {'strAccountID': '你的资金账号', 'strExchangeID': 'SH', 'strInstrumentID': '600519',
         'strOrderSysID': '202611050001', 'strRemark': 'q2m:1', 'nDirection': 48,
         'nVolumeTotalOriginal': 100, 'dLimitPrice': 1799.0, 'nVolumeTraded': 100,
         'dTradedPrice': 1799.0, 'nOrderStatus': 56, 'strInsertTime': '093501'}


class MockBridge:
    """模拟QMT桥策略：应答全部操作 + 主动推送"""

    def __init__(self, port, token=''):
        self.events = threading.Event()
        t = threading.Thread(target=self._run, args=(port, token), daemon=True)
        t.start()

    def _run(self, port, token):
        import socket
        s = socket.create_connection(('127.0.0.1', port), timeout=5)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        hello = {'t': 'hello', 'role': 'bridge', 'proto': 1}
        if token:
            hello['token'] = token
        send_msg(s, hello)
        lock = threading.Lock()

        def push(ch, data):
            with lock:
                send_msg(s, {'t': 'push', 'ch': ch, 'data': data})

        while True:
            try:
                msg = recv_msg(s)
            except Exception:
                return
            if msg.get('t') != 'req':
                continue
            op, args = msg.get('op'), msg.get('args') or {}
            resp = {'t': 'resp', 'id': msg['id'], 'ok': True, 'data': None}
            if op == 'get_full_tick':
                resp['data'] = TICK
            elif op == 'get_market_data_ex':
                resp['data'] = KLINE
            elif op == 'get_trading_dates':
                resp['data'] = ['20261103', '20261104', '20261105']
            elif op == 'get_instrument_detail':
                resp['data'] = {'InstrumentID': '600519', 'InstrumentName': '贵州茅台',
                                'UpStopPrice': 1979.0, 'DownStopPrice': 1619.0}
            elif op == 'subscribe_whole_quote':
                sid = args.get('shim_sub_id', 777)
                resp['data'] = {'sub_id': sid}
                threading.Timer(0.3, push, args=('quote', {'sub_id': sid,
                                                           'data': TICK})).start()
            elif op == 'subscribe_quote':
                sid = args.get('shim_sub_id', 778)
                resp['data'] = {'sub_id': sid}
                threading.Timer(0.3, push, args=('quote', {'sub_id': sid,
                                                           'data': TICK})).start()
            elif op == 'trader_connect':
                resp['data'] = {'account': '你的资金账号'}
            elif op == 'subscribe_account':
                resp['data'] = {'ok': 1}
            elif op == 'query_asset':
                resp['data'] = ASSET
            elif op == 'query_positions':
                resp['data'] = [POSITION]
            elif op == 'query_orders':
                resp['data'] = [ORDER]
            elif op == 'query_trades':
                resp['data'] = []
            elif op == 'order_stock':
                resp['data'] = {'order_id': 1}
                threading.Timer(0.2, push, args=('trade', {'event': 'stock_order',
                                                           'data': ORDER})).start()
            elif op == 'cancel_order_stock':
                resp['data'] = {'ok': 1}
            else:
                resp = {'t': 'resp', 'id': msg['id'], 'ok': False, 'err': 'mock未实现:%s' % op}
            with lock:
                try:
                    send_msg(s, resp)
                except Exception:
                    return


def start_server(port, enable_order, token='', order_token=''):
    srv = Server('127.0.0.1', port, enable_order, token, order_token)
    threading.Thread(target=srv.run, daemon=True).start()
    time.sleep(0.3)
    return srv


def main():
    print('== qmt2mini 模拟联调 ==', flush=True)
    try:
        import pandas  # noqa: F401
        has_pandas = True
    except ImportError:
        has_pandas = False

    PORT = 51899
    start_server(PORT, enable_order=True)
    MockBridge(PORT)

    # --- 行情 ---
    xtdata.connect(port=PORT)
    t = xtdata.get_full_tick(['600519.SH'])
    check('get_full_tick', t['600519.SH']['lastPrice'] == 1800.5)

    if has_pandas:
        ex = xtdata.get_market_data_ex([], ['600519.SH'], period='1d', count=3)
        df = ex['600519.SH']
        check('get_market_data_ex', df.shape == (3, 6) and abs(df['close'].iloc[-1] - 1800.5) < 1e-9,
              'shape=%s' % (df.shape,))
        md = xtdata.get_market_data(['close'], ['600519.SH'], period='1d', count=3)
        check('get_market_data(旧API)', 'close' in md and len(md['close']['600519.SH']) == 3)
    else:
        print('[SKIP] pandas未安装, 跳过DataFrame用例')

    dates = xtdata.get_trading_dates('SH', '20261101', '20261105')
    check('get_trading_dates', dates[-1] == '20261105')
    det = xtdata.get_instrument_detail('600519.SH')
    check('get_instrument_detail', det['InstrumentName'] == '贵州茅台')

    got = []
    sub = xtdata.subscribe_whole_quote(['SH', 'SZ'], lambda d: got.append(d))
    time.sleep(1.0)
    check('subscribe_whole_quote推送', len(got) >= 1 and got[0]['600519.SH']['lastPrice'] == 1800.5)

    got2 = []
    sub2 = xtdata.subscribe_quote('600519.SH', period='tick', callback=lambda d: got2.append(d))
    time.sleep(1.0)
    check('subscribe_quote推送', len(got2) >= 1)
    xtdata.unsubscribe_quote(sub2)

    # --- 交易 ---
    trader = XtQuantTrader(r'D:\QMT\userdata_mini', 888001)
    check('trader.connect', trader.connect() == 0)
    acc = StockAccount('你的资金账号')
    check('trader.subscribe', trader.subscribe(acc) == 0)
    asset = trader.query_stock_asset(acc)
    check('query_stock_asset', asset is not None and asset.cash == 123456.78
          and asset.total_asset == 223456.78, repr(asset))
    poss = trader.query_stock_positions(acc)
    check('query_stock_positions', len(poss) == 1 and poss[0].stock_code == 'SH.600519'
          and poss[0].can_use_volume == 100, repr(poss[0]))
    orders = trader.query_stock_orders(acc)
    check('query_stock_orders', len(orders) == 1 and orders[0].order_id == 1
          and orders[0].order_type == 23 and orders[0].order_status == 56, repr(orders[0]))

    oid = trader.order_stock(acc, '600519.SH', xtconstant.STOCK_BUY, 100,
                             xtconstant.FIX_PRICE, 1799.0, '测试策略', '')
    check('order_stock', oid == 1, 'order_id=%s' % oid)
    time.sleep(0.6)  # 等推送

    cb_events = []
    trader2 = XtQuantTrader(r'D:\QMT\userdata_mini', 888002,
                            callback=RecordingCallback(cb_events))
    trader2.connect()
    trader2.order_stock(acc, '600519.SH', xtconstant.STOCK_BUY, 100,
                        xtconstant.FIX_PRICE, 1799.0)
    time.sleep(0.8)
    check('委托主推回调', any(e[0] == 'order' for e in cb_events),
          str([e[0] for e in cb_events]))
    check('cancel_order_stock', trader.cancel_order_stock(acc, 1) == 0)

    # --- 门控: 未开 --enable-order 的服务端必须拒绝下单 ---
    PORT2 = 51898
    start_server(PORT2, enable_order=False)
    MockBridge(PORT2)
    c2 = xc.Conn(port=PORT2)
    c2.connect()
    try:
        c2.request('order_stock', {'account': '你的资金账号'}, timeout=10)
        check('下单门控(未enable-order拒绝)', False, '竟然成功了')
    except xc.BridgeError as e:
        check('下单门控(未enable-order拒绝)', '未开启' in str(e), str(e)[:40])

    # --- 令牌: token不匹配必须被拒 ---
    PORT3 = 51897
    start_server(PORT3, enable_order=True, token='secret')
    c3 = xc.Conn(port=PORT3)
    c3.connect()
    try:
        c3.request('ping_op', timeout=5)
        check('token鉴权拒绝', False, '竟然成功了')
    except (xc.BridgeError, xc.ConnError) as e:
        check('token鉴权拒绝', True, str(e)[:40])
    c4 = xc.Conn(port=PORT3, token='secret')
    c4.connect()
    ok = False
    detail = ''
    try:
        # 鉴权通过后无桥 → 请求入队等待 → 超时(而不是被拒)
        c4.request('get_full_tick', {'codes': []}, timeout=3)
        ok = True
    except xc.ConnError as e:
        ok = '超时' in str(e)
        detail = str(e)[:40]
    except xc.BridgeError as e:
        detail = str(e)[:40]
    check('token正确可通行(排队至超时)', ok, detail)

    # --- 短连接排队: 无桥时请求入队, 桥连上后冲账 ---
    PORT4 = 51896
    start_server(PORT4, enable_order=True)
    c5 = xc.Conn(port=PORT4)
    c5.connect()
    box = {}

    def _bg():
        try:
            box['r'] = c5.request('get_full_tick', {'codes': ['600519.SH']}, timeout=15)
        except Exception as e:
            box['r'] = ('ERR', str(e))

    th = threading.Thread(target=_bg)
    th.start()
    time.sleep(0.8)              # 请求此时已入队(无桥在线)
    MockBridge(PORT4)            # 桥连上 → 服务端冲账
    th.join(15)
    r = box.get('r')
    check('排队请求冲账', isinstance(r, dict)
          and r.get('600519.SH', {}).get('lastPrice') == 1800.5, str(r)[:80])

    print()
    npass = sum(1 for _, okk, _ in RESULTS if okk)
    print('==== 模拟联调: %d/%d 通过 ====' % (npass, len(RESULTS)))
    if npass != len(RESULTS):
        sys.exit(1)


class RecordingCallback(XtQuantTraderCallback):
    def __init__(self, sink):
        self.sink = sink

    def on_stock_order(self, order):
        self.sink.append(('order', order.order_id, order.order_status))

    def on_stock_trade(self, trade):
        self.sink.append(('trade', trade.traded_id))


if __name__ == '__main__':
    main()
