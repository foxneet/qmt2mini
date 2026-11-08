# -*- coding: utf-8 -*-
"""qmt2mini.xttrader —— 与 xtquant.xttrader 同签名的交易门面

order_id 机制: 桥端用投资备注 'q2m:<seq>' 标记本桥发出的委托,
push/查询回来的委托按备注反解 order_id, 与 xtquant 行为对齐。
下单/撤单受服务端 --enable-order 与 --order-token 门控。
"""
import itertools

from . import xtdata as _xd

_seq = itertools.count(1)
_c = _xd._c                     # 与xtdata共享同一条服务端连接
_add_push_handler = _xd.add_push_handler


def _parse_order_id(remark):
    """从投资备注 'q2m:N ...' 反解桥内order_id; 解析失败返回-1"""
    if isinstance(remark, str) and remark.startswith('q2m:'):
        try:
            return int(remark[4:].split(' ')[0])
        except ValueError:
            return -1
    return -1


# ---------- 数据结构（字段与xtquant官方文档一致） ----------
class XtAsset(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.cash = 0.0
        self.frozen_cash = 0.0
        self.market_value = 0.0
        self.total_asset = 0.0
        for k, v in kw.items():
            setattr(self, k, v)

    def __repr__(self):
        return ('XtAsset(account_id=%r, total_asset=%s, cash=%s, frozen_cash=%s, market_value=%s)'
                % (self.account_id, self.total_asset, self.cash, self.frozen_cash, self.market_value))


class XtPosition(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.stock_code = ''
        self.volume = 0
        self.can_use_volume = 0
        self.open_price = 0.0
        self.market_value = 0.0
        self.frozen_volume = 0
        self.on_road_volume = 0
        self.yesterday_volume = 0
        self.avg_price = 0.0
        self.direction = 0
        self.raw = None
        for k, v in kw.items():
            setattr(self, k, v)

    def __repr__(self):
        return ('XtPosition(stock_code=%r, volume=%s, can_use_volume=%s, avg_price=%s, market_value=%s)'
                % (self.stock_code, self.volume, self.can_use_volume, self.avg_price, self.market_value))


class XtOrder(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.stock_code = ''
        self.order_id = -1
        self.order_sysid = ''
        self.order_time = 0
        self.order_type = 0
        self.order_volume = 0
        self.price_type = 0
        self.price = 0.0
        self.traded_volume = 0
        self.traded_price = 0.0
        self.order_status = 0
        self.status_msg = ''
        self.strategy_name = ''
        self.order_remark = ''
        self.direction = 0
        self.offset_flag = 0
        self.raw = None
        for k, v in kw.items():
            setattr(self, k, v)

    def __repr__(self):
        return ('XtOrder(order_id=%s, stock_code=%r, order_type=%s, price=%s, volume=%s, '
                'traded=%s, status=%s, sysid=%r)'
                % (self.order_id, self.stock_code, self.order_type, self.price,
                   self.order_volume, self.traded_volume, self.order_status, self.order_sysid))


class XtTrade(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.stock_code = ''
        self.order_type = 0
        self.traded_id = ''
        self.traded_time = 0
        self.traded_price = 0.0
        self.traded_volume = 0
        self.traded_amount = 0.0
        self.order_id = -1
        self.order_sysid = ''
        self.strategy_name = ''
        self.order_remark = ''
        self.direction = 0
        self.offset_flag = 0
        self.raw = None
        for k, v in kw.items():
            setattr(self, k, v)

    def __repr__(self):
        return ('XtTrade(traded_id=%r, stock_code=%r, price=%s, volume=%s, amount=%s)'
                % (self.traded_id, self.stock_code, self.traded_price, self.traded_volume, self.traded_amount))


class XtOrderError(object):
    def __init__(self, **kw):
        self.order_id = -1
        self.error_id = 0
        self.error_msg = ''
        for k, v in kw.items():
            setattr(self, k, v)


class XtCancelOrderError(object):
    def __init__(self, **kw):
        self.order_id = -1
        self.error_id = 0
        self.error_msg = ''
        for k, v in kw.items():
            setattr(self, k, v)


class XtOrderResponse(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.order_id = -1
        self.strategy_name = ''
        self.order_remark = ''
        for k, v in kw.items():
            setattr(self, k, v)


class XtAccountStatus(object):
    def __init__(self, **kw):
        self.account_type = 2
        self.account_id = ''
        self.status = 0
        for k, v in kw.items():
            setattr(self, k, v)


# ---------- 构建器：桥端序列化的 m_ 字段 dict → xtquant 对象 ----------
def _build_asset(d, account_id='', account_type='STOCK'):
    return XtAsset(account_id=d.get('strAccountID', account_id),
                   cash=d.get('dAvailable', 0.0),
                   frozen_cash=d.get('dFrozenCash', 0.0),
                   market_value=d.get('dInstrumentValue', 0.0),
                   total_asset=d.get('dBalance', 0.0))


def _build_position(d):
    code = '%s.%s' % (d.get('strExchangeID', ''), d.get('strInstrumentID', ''))
    return XtPosition(account_id=d.get('strAccountID', ''),
                      stock_code=code,
                      volume=d.get('nVolume', 0),
                      can_use_volume=d.get('nCanUseVolume', 0),
                      open_price=d.get('dOpenPrice', 0.0),
                      market_value=d.get('dMarketValue', 0.0),
                      frozen_volume=d.get('nFrozenVolume', 0),
                      on_road_volume=d.get('nOnRoadVolume', 0),
                      yesterday_volume=d.get('nYesterdayVolume', 0),
                      avg_price=d.get('dAvgOpenPrice', 0.0),
                      direction=d.get('nDirection', 0),
                      raw=d)


def _build_order(d):
    remark = d.get('strRemark', '') or ''
    direction = d.get('nDirection', 0)
    order_type = 23 if direction == 48 else (24 if direction == 49 else direction)
    return XtOrder(account_id=d.get('strAccountID', ''),
                   stock_code='%s.%s' % (d.get('strExchangeID', ''), d.get('strInstrumentID', '')),
                   order_id=_parse_order_id(remark),
                   order_sysid=d.get('strOrderSysID', ''),
                   order_time=int(d.get('strInsertTime', 0) or 0),
                   order_type=order_type,
                   order_volume=d.get('nVolumeTotalOriginal', 0),
                   price_type=d.get('nOrderPriceType', 0),
                   price=d.get('dLimitPrice', 0.0),
                   traded_volume=d.get('nVolumeTraded', 0),
                   traded_price=d.get('dTradedPrice', 0.0),
                   order_status=d.get('nOrderStatus', 0),
                   status_msg=d.get('strErrorMsg', '') or d.get('strCancelInfo', ''),
                   order_remark=remark,
                   direction=direction,
                   offset_flag=d.get('nOffsetFlag', 0),
                   raw=d)


def _build_trade(d):
    remark = d.get('strRemark', '') or ''
    direction = d.get('nDirection', 0)
    order_type = 23 if direction == 48 else (24 if direction == 49 else direction)
    return XtTrade(account_id=d.get('strAccountID', ''),
                   stock_code='%s.%s' % (d.get('strExchangeID', ''), d.get('strInstrumentID', '')),
                   order_type=order_type,
                   traded_id=d.get('strTradeID', ''),
                   traded_time=d.get('strTradeTime', ''),
                   traded_price=d.get('dPrice', 0.0),
                   traded_volume=d.get('nVolume', 0),
                   traded_amount=d.get('dTradeAmount', 0.0),
                   order_id=_parse_order_id(remark),
                   order_sysid=d.get('strOrderSysID', ''),
                   order_remark=remark,
                   direction=direction,
                   offset_flag=d.get('nOffsetFlag', 0),
                   raw=d)


# ---------- 回调基类 ----------
class XtQuantTraderCallback(object):
    def on_connected(self):
        """连接成功"""

    def on_disconnected(self):
        """连接断开"""

    def on_account_status(self, status):
        """账号状态推送 XtAccountStatus"""

    def on_stock_order(self, order):
        """委托状态推送 XtOrder"""

    def on_stock_trade(self, trade):
        """成交变动推送 XtTrade"""

    def on_stock_position(self, position):
        """持仓变动推送 XtPosition"""

    def on_order_error(self, order_error):
        """下单失败推送 XtOrderError"""

    def on_cancel_error(self, cancel_error):
        """撤单失败推送 XtCancelOrderError"""

    def on_order_stock_async_response(self, response):
        """异步下单回报 XtOrderResponse"""


# ---------- 推送分发 ----------
def _register_trade_callback(trader):
    def handler(ch, data):
        if ch == 'trade':
            ev = data.get('event')
            d = data.get('data') or {}
            cb = trader.callback
            if cb is None:
                return
            try:
                if ev == 'stock_order':
                    cb.on_stock_order(_build_order(d))
                elif ev == 'stock_trade':
                    cb.on_stock_trade(_build_trade(d))
                elif ev == 'stock_position':
                    cb.on_stock_position(_build_position(d))
                elif ev == 'account_status':
                    cb.on_account_status(XtAccountStatus(account_id=d.get('strAccountID', ''),
                                                         status=d.get('m_nStatus', d.get('nStatus', 0))))
                elif ev == 'order_error':
                    cb.on_order_error(XtOrderError(order_id=_parse_order_id(d.get('remark', '')),
                                                   error_id=d.get('error_id', -1),
                                                   error_msg=d.get('error_msg', '')))
                elif ev == 'cancel_error':
                    cb.on_cancel_error(XtCancelOrderError(order_id=_parse_order_id(d.get('remark', '')),
                                                          error_id=d.get('error_id', -1),
                                                          error_msg=d.get('error_msg', '')))
                elif ev == 'async_response':
                    cb.on_order_stock_async_response(XtOrderResponse(
                        account_id=d.get('account_id', ''), order_id=d.get('order_id', -1),
                        order_remark=d.get('order_remark', '')))
            except Exception:
                pass
        elif ch == 'status':
            cb = trader.callback
            if cb is not None:
                try:
                    if data.get('bridge'):
                        cb.on_connected()
                    else:
                        cb.on_disconnected()
                except Exception:
                    pass
    _add_push_handler(handler)


class XtQuantTrader(object):
    """XtQuantTrader(path, session_id, callback=None) —— path仅为兼容保留"""

    def __init__(self, path, session_id, callback=None):
        self.path = path
        self.session_id = session_id
        self.callback = callback
        self.connected = False
        if callback is not None:
            _register_trade_callback(self)

    def register_callback(self, callback):
        self.callback = callback
        _register_trade_callback(self)

    def start(self):
        """兼容接口：连接由服务端统一管理，无需额外动作"""

    def stop(self):
        pass

    def connect(self):
        """连接桥, 成功返回0, 失败返回-1（同xtquant语义）"""
        try:
            _c().request('trader_connect', {'session_id': self.session_id}, timeout=15)
            self.connected = True
            return 0
        except Exception:
            self.connected = False
            return -1

    def subscribe(self, account):
        try:
            _c().request('subscribe_account',
                         {'account': account.account_id, 'account_type': account.account_type},
                         timeout=15)
            return 0
        except Exception:
            return -1

    def unsubscribe(self, account):
        try:
            _c().request('unsubscribe_account',
                         {'account': account.account_id, 'account_type': account.account_type},
                         timeout=15)
            return 0
        except Exception:
            return -1

    def run_forever(self):
        from . import xtdata
        xtdata.run()

    # ---------- 查询 ----------
    def query_stock_asset(self, account):
        d = _c().request('query_asset',
                         {'account': account.account_id, 'account_type': account.account_type},
                         timeout=30)
        return _build_asset(d, account.account_id, account.account_type) if d else None

    def query_stock_positions(self, account):
        d = _c().request('query_positions',
                         {'account': account.account_id, 'account_type': account.account_type},
                         timeout=30)
        return [_build_position(x) for x in (d or [])]

    def query_stock_orders(self, account, cancelable_only=False):
        d = _c().request('query_orders',
                         {'account': account.account_id, 'account_type': account.account_type,
                          'cancelable_only': bool(cancelable_only)},
                         timeout=30)
        return [_build_order(x) for x in (d or [])]

    def query_stock_trades(self, account):
        d = _c().request('query_trades',
                         {'account': account.account_id, 'account_type': account.account_type},
                         timeout=30)
        return [_build_trade(x) for x in (d or [])]

    # ---------- 交易 ----------
    def order_stock(self, account, stock_code, order_type, order_volume,
                    price_type, price, strategy_name='', order_remark=''):
        """同步下单, 返回order_id(本桥内自增); 失败返回-1"""
        try:
            r = _c().request('order_stock', {
                'account': account.account_id, 'account_type': account.account_type,
                'code': stock_code, 'order_type': int(order_type),
                'volume': int(order_volume), 'price_type': int(price_type),
                'price': float(price), 'strategy_name': strategy_name,
                'order_remark': order_remark,
            }, timeout=30)
            return int(r.get('order_id', -1))
        except Exception:
            return -1

    def order_stock_async(self, account, stock_code, order_type, order_volume,
                          price_type, price, strategy_name='', order_remark=''):
        """异步下单, 立即返回order_id, 回报走 on_order_stock_async_response"""
        r = _c().request('order_stock', {
            'account': account.account_id, 'account_type': account.account_type,
            'code': stock_code, 'order_type': int(order_type),
            'volume': int(order_volume), 'price_type': int(price_type),
            'price': float(price), 'strategy_name': strategy_name,
            'order_remark': order_remark, 'async': True,
        }, timeout=30)
        return int(r.get('order_id', -1))

    def cancel_order_stock(self, account, order_id):
        """同步撤单, 成功返回0, 失败返回-1"""
        try:
            _c().request('cancel_order_stock', {
                'account': account.account_id, 'account_type': account.account_type,
                'order_id': int(order_id),
            }, timeout=30)
            return 0
        except Exception:
            return -1

    def cancel_order_stock_async(self, account, order_id):
        try:
            _c().request('cancel_order_stock', {
                'account': account.account_id, 'account_type': account.account_type,
                'order_id': int(order_id), 'async': True,
            }, timeout=30)
            return 0
        except Exception:
            return -1
