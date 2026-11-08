# -*- coding: utf-8 -*-
"""
qmt2mini —— 把大QMT改造为miniQMT的兼容层
用法与 xtquant 完全同签名:
    from qmt2mini import xtdata
    from qmt2mini.xttrader import XtQuantTrader, XtQuantTraderCallback
    from qmt2mini.xttype import StockAccount
    from qmt2mini import xtconstant
前置: 本机(或局域网)运行 qmt2mini_server.py, 大QMT内运行桥_QMT端.py
"""
__version__ = '1.0.0'
