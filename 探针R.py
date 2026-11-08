#coding:gbk
r"""
探针R: 最终架构验证(运行中模型, 零socket)
  1) KV自环: init里 set_kv('q2m_self','hello') -> 定时器轮询 get_kv 回读
  2) 运行中模型 passorder: 启动即一笔 600519@1400 限价买(排队不成交) -> 轮询柜台状态 -> print
  3) print出站遥测: 每5秒 Q2M:{tick json}
  4) 常驻存活基线
  全部结论看策略日志(FormulaOutput)
"""
import datetime as dt
import json
import time

_n = [0]
_oid = [None]
_placed = [False]


def init(C):
    print('R=== 探针R启动: 运行中模型 零socket ===')
    # 1) KV自环写入
    try:
        set_kv('q2m_self', 'R_selftest_' + time.strftime('%H%M%S'))
        print('R[KV写入OK] q2m_self')
    except Exception as e:
        print('R[KV写入FAIL]', repr(e)[:80])
    # 2) 运行中模型下单(600519@1400, 排队不成交)
    try:
        passorder(23, 1101, '你的资金账号', '600519.SH', 11, 1400.0, 100, 'q2mR', 2,
                  'q2mR:probe', C)
        _placed[0] = True
        print('R[passorder已调用] 600519@1400 x100')
    except Exception as e:
        print('R[passorder异常]', repr(e)[:80])
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeR')


def _tick(C):
    _n[0] += 1
    t = _n[0]
    # KV回读(前5秒每秒读, 验证自环)
    if t <= 5:
        try:
            v = get_kv('q2m_self')
            print('R[KV回读] %r' % (v,))
        except Exception as e:
            print('R[KV读FAIL]', repr(e)[:80])
    # 我的委托状态轮询
    if _placed[0] and t in (3, 6, 10, 15):
        try:
            lst = get_trade_detail_data('你的资金账号', 'STOCK', 'ORDER')
            mine = [o for o in lst if getattr(o, 'm_strRemark', '') == 'q2mR:probe']
            if mine:
                o = mine[0]
                print('R[委托状态] 状态=%d sysid=%s (49待报/50已报=通道通)' % (
                    o.m_nOrderStatus, o.m_strOrderSysID))
            else:
                if t == 15:
                    print('R[委托状态] 15秒仍未见到委托 -> passorder未生效(运行中模型也不行?)')
                    print('R[全量委托] ' + '; '.join(
                        '%s@%s 状态%d' % (getattr(o, 'm_strInstrumentID', '?'),
                                          getattr(o, 'm_dLimitPrice', '?'),
                                          getattr(o, 'm_nOrderStatus', '?'))
                        for o in lst)[:200])
        except Exception as e:
            print('R[查询异常]', repr(e)[:80])
    # print出站遥测
    if t % 5 == 0:
        try:
            tk = C.get_full_tick(['000300.SH'])
            d = (tk or {}).get('000300.SH') or {}
            print('Q2M:%s' % json.dumps({'t': d.get('time'), 'lp': d.get('lastPrice')},
                                        ensure_ascii=False))
        except Exception as e:
            print('Q2M_ERR:%s' % repr(e)[:60])
        print('R[心跳] 存活%d秒' % t)


import json  # noqa: E402  (遥测用, 放尾部导入亦可)


def handlebar(C):
    pass
