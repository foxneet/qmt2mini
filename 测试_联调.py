# -*- coding: utf-8 -*-
"""qmt2mini 实机联调：连接真实服务端+QMT桥策略逐项验证
前提: qmt2mini_server.py 已运行, 大QMT里桥_QMT端.py 已实盘运行
用法: py -3.13 测试_联调.py [--host H] [--port 51880] [--code 600519.SH] [--account 你的资金账号] [--order]
"""
import argparse
import sys
import time

sys.path.insert(0, r'F:\qmt2mini')

from qmt2mini import xtdata, xtconstant                # noqa: E402
from qmt2mini.xttrader import XtQuantTrader             # noqa: E402
from qmt2mini.xttype import StockAccount                # noqa: E402

RES = []


def check(name, ok, detail=''):
    RES.append((name, bool(ok)))
    print('[%s] %s %s' % ('PASS' if ok else 'FAIL', name, detail), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=51880)
    ap.add_argument('--code', default='600519.SH')
    ap.add_argument('--account', default='你的资金账号')
    ap.add_argument('--order', action='store_true', help='下单演练(需服务端--enable-order启动)')
    ap.add_argument('--volume', type=int, default=100)
    ap.add_argument('--price', type=float, default=0.0, help='0=最新价')
    a = ap.parse_args()

    xtdata.connect(a.host, a.port)
    print('== 已连接 qmt2mini 服务端 %s:%d ==\n' % (a.host, a.port))

    # ---- 行情 ----
    ticks = xtdata.get_full_tick([a.code])
    lp = (ticks.get(a.code) or {}).get('lastPrice')
    check('get_full_tick', bool(lp), 'lastPrice=%s' % lp)

    ex = xtdata.get_market_data_ex([], [a.code], period='1d', count=5)
    df = ex.get(a.code)
    check('get_market_data_ex(1d)', df is not None and len(df) > 0,
          'rows=%s' % (0 if df is None else len(df)))

    dates = xtdata.get_trading_dates('SH')
    check('get_trading_dates', len(dates) > 0,
          '%d天 最新=%s' % (len(dates), dates[-1] if dates else '-'))

    det = xtdata.get_instrument_detail(a.code)
    check('get_instrument_detail', bool(det),
          str((det or {}).get('InstrumentName')))

    got = []
    seq = xtdata.subscribe_whole_quote(['SH', 'SZ'], lambda d: got.append(d))
    print('    全推订阅采样10秒...', flush=True)
    time.sleep(10)
    xtdata.unsubscribe_quote(seq)
    nstock = sum(len(d) for d in got)
    check('subscribe_whole_quote', len(got) > 0,
          '10秒 %d批 %d只次' % (len(got), nstock))

    # ---- 交易(查询) ----
    trader = XtQuantTrader(r'D:\QMT\userdata_mini', 666001)
    rc = trader.connect()
    check('trader.connect', rc == 0, 'rc=%s' % rc)
    if rc == 0:
        acc = StockAccount(a.account)
        trader.subscribe(acc)
        asset = trader.query_stock_asset(acc)
        check('query_stock_asset', asset is not None, repr(asset))
        poss = trader.query_stock_positions(acc)
        check('query_stock_positions', poss is not None, '%d只' % len(poss or []))
        for p in (poss or [])[:5]:
            print('    ', p)
        orders = trader.query_stock_orders(acc)
        check('query_stock_orders', orders is not None, '%d笔' % len(orders or []))

        # ---- 下单演练(默认关闭) ----
        if a.order:
            price = a.price if a.price > 0 else (lp or 0)
            if not price:
                print('[FAIL] 下单演练: 无可用价格')
            else:
                ans = input('确认在账号%s 模拟%s %s x%d @%.2f ? (输入YES): '
                            % (a.account, '买入' if True else '', a.code, a.volume, price))
                if ans.strip() == 'YES':
                    oid = trader.order_stock(acc, a.code, xtconstant.STOCK_BUY,
                                             a.volume, xtconstant.FIX_PRICE, price,
                                             'q2m演练', '')
                    check('order_stock', oid > 0, 'order_id=%s' % oid)
                    time.sleep(3)
                    orders2 = trader.query_stock_orders(acc)
                    mine = [o for o in (orders2 or []) if o.order_id == oid]
                    check('下单回报查询', bool(mine),
                          repr(mine[0]) if mine else '未查到')
                    if mine and mine[0].order_status in (48, 49, 50, 51, 52):
                        cc = trader.cancel_order_stock(acc, oid)
                        check('cancel_order_stock', cc == 0, 'rc=%s' % cc)
                else:
                    print('[SKIP] 未输入YES, 跳过下单演练')

    print()
    npass = sum(1 for _, okk in RES if okk)
    print('==== 实机联调: %d/%d 通过 ====' % (npass, len(RES)))
    sys.exit(0 if npass == len(RES) else 1)


if __name__ == '__main__':
    main()
