# -*- coding: utf-8 -*-
"""qmt2mini.xttype —— 与 xtquant.xttype 同签名"""


class StockAccount(object):
    """定义证券账号类, 用于证券账号的报撤单等

    StockAccount(account_id, account_type='STOCK')
    account_type: 'STOCK'/'CREDIT'/'FUTURE'/'FUTURE_OPTION'/'STOCK_OPTION'/'HUGANGTONG'/'SHENGANGTONG'
    """

    def __init__(self, account_id, account_type='STOCK'):
        self.account_id = account_id
        self.account_type = account_type

    def __repr__(self):
        return 'StockAccount(%r, %r)' % (self.account_id, self.account_type)
