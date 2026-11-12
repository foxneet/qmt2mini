# -*- coding: utf-8 -*-
"""
qmt2mini_mcp.py — qmt2mini 的 MCP 服务端（stdio 传输，纯标准库零依赖）

把 qmt2mini转发器（大QMT改造miniQMT的枢纽）的行情/交易能力暴露为 MCP 工具，
供 AI 助手（ZCode / Claude Desktop 等）以 MCP 客户端身份直连使用。

架构位置（不改动桥与转发器，无需重启QMT）：
    AI助手(MCP客户端) --stdio--> qmt2mini_mcp.py --HTTP 51881--> qmt2mini转发器 --桥--> QMT
    · token鉴权 / IP白名单 / allow_trade下单总闸 / 审计 全部沿用转发器服务端
    · 可部署在局域网任意机器（AI助手所在机即可），只要能访问转发器 51881

配置（优先级：命令行 > 环境变量 > 同目录转发器_配置.json）：
    QMT2MINI_HOST       转发器机IP（默认 127.0.0.1）
    QMT2MINI_HTTP_PORT  HTTP端口（默认 51881）
    QMT2MINI_TOKEN      转发器token（32位）
    命令行：--host= --port= --token=

AI客户端注册示例（mcpServers）：
    "qmt2mini": {"command": "py", "args": ["-3.14", r"<路径>\\qmt2mini_mcp.py"],
                 "env": {"QMT2MINI_HOST": "<转发器IP>", "QMT2MINI_TOKEN": "<token>"}}

协议：MCP stdio —— 按行分隔的 JSON-RPC 2.0（UTF-8、无内嵌换行）。
    stdout 只输出协议消息（ensure_ascii 纯ASCII，杜绝编码问题）；日志一律走 stderr。
    工具响应超过 MAX_TEXT 字符自动截断（保护 AI 上下文）。

已知节律（与 qmt2mini-client skill 一致）：
    · 下单延迟 0~60 秒（指令经桥的60秒轮询通道执行）
    · 桥离线时请求在转发器排队（期限>=75秒）——本服务 HTTP 超时设 90 秒覆盖
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

SERVER_INFO = {'name': 'qmt2mini-mcp', 'version': '1.0.0'}
PROTO_LATEST = '2025-06-18'
PROTO_SUPPORTED = ('2024-11-05', '2025-03-26', '2025-06-18')
MAX_TEXT = 20000
HTTP_TIMEOUT = 90.0

CFG = {'host': '127.0.0.1', 'port': 51881, 'token': ''}


class RpcError(Exception):
    """JSON-RPC 协议级错误（区别于工具业务错误 isError=true）"""
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def _log(msg):
    sys.stderr.write('[qmt2mini-mcp %s] %s\n' % (time.strftime('%H:%M:%S'), msg))
    sys.stderr.flush()


# ---------------------------------------------------------------- 配置

def _load_cfg():
    for a in sys.argv[1:]:
        for key, ck in (('--host=', 'host'), ('--port=', 'port'), ('--token=', 'token')):
            if a.startswith(key):
                v = a[len(key):].strip()
                if ck == 'port':
                    CFG['port'] = int(v)
                else:
                    CFG[ck] = v
    if os.environ.get('QMT2MINI_HOST'):
        CFG['host'] = os.environ['QMT2MINI_HOST'].strip()
    if os.environ.get('QMT2MINI_HTTP_PORT'):
        CFG['port'] = int(os.environ['QMT2MINI_HTTP_PORT'].strip())
    if os.environ.get('QMT2MINI_TOKEN'):
        CFG['token'] = os.environ['QMT2MINI_TOKEN'].strip()
    if not CFG['token']:
        # 兜底：同目录转发器_配置.json（本机部署场景）
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), '转发器_配置.json')
        try:
            with open(p, 'r', encoding='utf-8') as f:
                CFG['token'] = (json.load(f).get('token') or '').strip()
            _log('token 来自 %s' % p)
        except Exception:
            pass
    if not CFG['token']:
        _log('[!] 无token（环境变量QMT2MINI_TOKEN / --token= / 同目录转发器_配置.json均未找到），业务工具将收到401')


# ---------------------------------------------------------------- HTTP 上行

def _post_api(op, args):
    """POST 转发器 /api → 返回 {"ok":bool, "data"/"err"}"""
    url = 'http://%s:%d/api' % (CFG['host'], CFG['port'])
    body = json.dumps({'op': op, 'args': args}, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(url, data=body, method='POST', headers={
        'X-Token': CFG['token'], 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            r = json.loads(e.read().decode('utf-8'))
            return {'ok': False, 'err': r.get('err') or ('HTTP %d' % e.code)}
        except Exception:
            return {'ok': False, 'err': 'HTTP %d' % e.code}
    except Exception as e:
        return {'ok': False, 'err': '转发器不可达(%s:%d): %s' % (CFG['host'], CFG['port'], e)}


def _get_status():
    """GET 转发器 /status（无需token）"""
    url = 'http://%s:%d/status' % (CFG['host'], CFG['port'])
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return {'ok': False, 'err': '转发器不可达(%s:%d): %s' % (CFG['host'], CFG['port'], e)}


# ---------------------------------------------------------------- 参数整理

def _int(v, name):
    if v is None or (isinstance(v, str) and not v.strip()):
        raise ValueError('缺少参数 %s(整数)' % name)
    if isinstance(v, bool):
        raise ValueError('参数 %s 须为整数' % name)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            raise ValueError('参数 %s 不是整数: %r' % (name, v))


def _float(v, name):
    if v is None or (isinstance(v, str) and not v.strip()):
        raise ValueError('缺少参数 %s(数值)' % name)
    if isinstance(v, bool):
        raise ValueError('参数 %s 须为数值' % name)
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except ValueError:
        raise ValueError('参数 %s 不是数值: %r' % (name, v))


def _codes(v):
    if v is None:
        raise ValueError('缺少参数 codes(数组)')
    if isinstance(v, str):
        v = [v]
    if not isinstance(v, list) or not v:
        raise ValueError('codes 须为非空数组, 如 ["600519.SH"]')
    return [str(c).strip() for c in v if str(c).strip()]


def _account(v):
    s = str(v or '').strip()
    if not s:
        raise ValueError('缺少参数 account(资金账号)')
    return s


def _p_tick(a):
    return {'codes': _codes(a.get('codes'))}


def _p_kline(a):
    out = {'codes': _codes(a.get('codes'))}
    out['period'] = str(a.get('period') or '1d')
    c = 20 if a.get('count') is None else _int(a.get('count'), 'count')
    out['count'] = max(1, min(800, c))
    for k in ('start_time', 'end_time', 'dividend_type'):
        if a.get(k) not in (None, ''):
            out[k] = str(a[k])
    return out


def _p_dates(a):
    out = {'market': str(a.get('market') or 'SH')}
    for k in ('start_time', 'end_time'):
        if a.get(k):
            out[k] = str(a[k])
    if a.get('count') is not None:
        out['count'] = _int(a.get('count'), 'count')
    return out


def _p_code(a):
    s = str(a.get('code') or '').strip()
    if not s:
        raise ValueError('缺少参数 code(如 "600519.SH")')
    return {'code': s}


def _p_sector(a):
    s = str(a.get('sector') or '').strip()
    if not s:
        raise ValueError('缺少参数 sector(如 "沪深300")')
    return {'sector': s}


def _p_account_type(a):
    out = {'account': _account(a.get('account'))}
    if a.get('account_type'):
        out['account_type'] = str(a['account_type'])
    return out


def _p_orders(a):
    out = _p_account_type(a)
    if a.get('cancelable_only') is not None:
        out['cancelable_only'] = bool(a.get('cancelable_only'))
    return out


def _p_order(a):
    out = {'account': _account(a.get('account'))}
    code = str(a.get('code') or '').strip()
    if not code:
        raise ValueError('缺少参数 code(如 "600519.SH")')
    out['code'] = code
    out['order_type'] = _int(a.get('order_type'), 'order_type(23=买,24=卖)')
    out['volume'] = _int(a.get('volume'), 'volume(股)')
    out['price_type'] = _int(a.get('price_type'), 'price_type(11=限价,5=最新)')
    out['price'] = _float(a.get('price'), 'price')
    if a.get('strategy_name'):
        out['strategy_name'] = str(a['strategy_name'])
    if a.get('order_remark'):
        out['order_remark'] = str(a['order_remark'])
    return out


def _p_cancel(a):
    out = {'account': _account(a.get('account'))}
    oid = a.get('order_id')
    if oid is None or str(oid).strip() == '':
        raise ValueError('缺少参数 order_id')
    s = str(oid).strip()
    out['order_id'] = int(s) if s.isdigit() else s
    return out


# ---------------------------------------------------------------- 工具表

def _s(props, required=None):
    return {'type': 'object', 'properties': props, 'required': required or []}


TOOLS = [
    {'name': 'qmt_status',
     'description': '查询qmt2mini转发器与QMT桥的存活状态(无需token)。健康判据: bridge.connected=true 且 q2m_print.last_seen_sec<=3。也用于排查401/超时前先看链路。',
     'inputSchema': _s({}),
     'status': True},
    {'name': 'get_full_tick',
     'description': '实时tick快照(多代码)。返回{code:{lastPrice,open,high,low,lastClose,volume(累计股),amount(累计元),askPrice[5],bidPrice[5],time(ms),...}}。',
     'inputSchema': _s({'codes': {'type': 'array', 'items': {'type': 'string'},
                                  'description': '代码数组, 如["600519.SH","000300.SH"]'}}, ['codes']),
     'op': 'get_full_tick', 'prepare': _p_tick},
    {'name': 'get_kline',
     'description': 'K线(多代码)。返回{code:{columns,index,rows}}, index为stime("YYYYMMDD"或"YYYYMMDDHHMM")。桥对占位bar(volume=0)自动增量下载重取。count上限800。',
     'inputSchema': _s({'codes': {'type': 'array', 'items': {'type': 'string'}, 'description': '代码数组'},
                        'period': {'type': 'string', 'description': "'1d'/'1m'/'5m'/'15m'/'30m'/'60m', 默认1d"},
                        'count': {'type': 'integer', 'description': '根数, 默认20, 上限800'},
                        'start_time': {'type': 'string', 'description': "'YYYYMMDD'或'YYYYMMDDHHMM', 可省"},
                        'end_time': {'type': 'string', 'description': '可省'},
                        'dividend_type': {'type': 'string', 'description': "'none'/'front'/'back', 默认none"}},
                       ['codes']),
     'op': 'get_market_data_ex', 'prepare': _p_kline},
    {'name': 'get_trading_dates',
     'description': '交易日历。返回["YYYYMMDD",...]。',
     'inputSchema': _s({'market': {'type': 'string', 'description': "'SH'/'SZ', 默认SH"},
                        'start_time': {'type': 'string'}, 'end_time': {'type': 'string'}}),
     'op': 'get_trading_dates', 'prepare': _p_dates},
    {'name': 'get_instrument_detail',
     'description': '合约详情(名称/涨跌停价UpStopPrice,DownStopPrice/交易所/类型等)。',
     'inputSchema': _s({'code': {'type': 'string', 'description': '如"600519.SH"'}}, ['code']),
     'op': 'get_instrument_detail', 'prepare': _p_code},
    {'name': 'get_sector_stocks',
     'description': '板块成分股。返回["code.market",...]。板块如"沪深A股"/"沪深300"/自定义板块。',
     'inputSchema': _s({'sector': {'type': 'string'}}, ['sector']),
     'op': 'get_stock_list_in_sector', 'prepare': _p_sector},
    {'name': 'query_asset',
     'description': '查资金。返回{cash(可用),frozen_cash,market_value,total_asset,...}或null。',
     'inputSchema': _s({'account': {'type': 'string', 'description': '资金账号'},
                        'account_type': {'type': 'string', 'description': "'STOCK'/'CREDIT'/'FUTURE', 默认STOCK"}}, ['account']),
     'op': 'query_asset', 'prepare': _p_account_type},
    {'name': 'query_positions',
     'description': '查持仓。返回[{stock_code,volume,can_use_volume,frozen_volume,avg_price,market_value},...]。',
     'inputSchema': _s({'account': {'type': 'string'}, 'account_type': {'type': 'string'}}, ['account']),
     'op': 'query_positions', 'prepare': _p_account_type},
    {'name': 'query_orders',
     'description': '查委托。返回[{order_id,order_status,order_sysid,price,traded_volume},...]。状态: 49待报/50已报/53部撤/54已撤/55部成/56已成/57废单。',
     'inputSchema': _s({'account': {'type': 'string'}, 'account_type': {'type': 'string'},
                        'cancelable_only': {'type': 'boolean', 'description': '只看可撤, 默认全部'}}, ['account']),
     'op': 'query_orders', 'prepare': _p_orders},
    {'name': 'query_trades',
     'description': '查成交。返回[{traded_id,traded_price,traded_volume,traded_amount,traded_time},...]。',
     'inputSchema': _s({'account': {'type': 'string'}, 'account_type': {'type': 'string'}}, ['account']),
     'op': 'query_trades', 'prepare': _p_account_type},
    {'name': 'order_stock',
     'description': '下单(限价/最新)。注意: 受转发器allow_trade总闸(关=拒); 执行延迟0~60秒(桥60秒指令节律); 返回的order_id为桥内序号, 撤单/查单用它。order_type: 23=买,24=卖; price_type: 11=限价,5=最新价。',
     'inputSchema': _s({'account': {'type': 'string', 'description': '资金账号'},
                        'code': {'type': 'string', 'description': '如"600519.SH"'},
                        'order_type': {'type': 'integer', 'description': '23买/24卖'},
                        'volume': {'type': 'integer', 'description': '股数(100整数倍)'},
                        'price_type': {'type': 'integer', 'description': '11限价/5最新'},
                        'price': {'type': 'number', 'description': '限价价格(price_type=11时必填)'},
                        'strategy_name': {'type': 'string'}, 'order_remark': {'type': 'string'}},
                       ['account', 'code', 'order_type', 'volume', 'price_type', 'price']),
     'op': 'order_stock', 'prepare': _p_order},
    {'name': 'cancel_order_stock',
     'description': '撤单(按order_stock返回的order_id)。同样受allow_trade总闸与0~60秒延迟。',
     'inputSchema': _s({'account': {'type': 'string'}, 'order_id': {'type': 'string', 'description': '下单返回的order_id'}},
                       ['account', 'order_id']),
     'op': 'cancel_order_stock', 'prepare': _p_cancel},
]

TOOL_MAP = {t['name']: t for t in TOOLS}


# ---------------------------------------------------------------- 工具执行

def _dump(data):
    s = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    if len(s) > MAX_TEXT:
        s = s[:MAX_TEXT] + '\n...[已截断: 全文%d字符, 请缩小count/代码范围]' % len(s)
    return s


def _tool_result(text, is_error):
    return {'content': [{'type': 'text', 'text': text}], 'isError': bool(is_error)}


def _tools_call(params):
    params = params or {}
    t = TOOL_MAP.get(params.get('name'))
    if t is None:
        raise RpcError(-32602, '未知工具: %r' % params.get('name'))
    arguments = params.get('arguments') or {}
    try:
        if t.get('status'):
            return _tool_result(_dump(_get_status()), False)
        args = t['prepare'](arguments)
        r = _post_api(t['op'], args)
        if r.get('ok') is True:
            return _tool_result(_dump(r.get('data')), False)
        return _tool_result(_dump({'ok': False, 'err': r.get('err', '未知错误')}), True)
    except ValueError as e:
        return _tool_result('参数错误: %s' % e, True)


# ---------------------------------------------------------------- 协议层

def _handle(msg, send):
    method = msg.get('method')
    mid = msg.get('id')
    try:
        if method == 'initialize':
            pv = (msg.get('params') or {}).get('protocolVersion')
            result = {'protocolVersion': pv if pv in PROTO_SUPPORTED else PROTO_LATEST,
                      'capabilities': {'tools': {'listChanged': False}},
                      'serverInfo': SERVER_INFO}
        elif method == 'ping':
            result = {}
        elif method == 'tools/list':
            result = {'tools': [{'name': t['name'], 'description': t['description'],
                                 'inputSchema': t['inputSchema']} for t in TOOLS]}
        elif method == 'tools/call':
            result = _tools_call(msg.get('params'))
        elif method == 'resources/list':
            result = {'resources': []}
        elif method == 'prompts/list':
            result = {'prompts': []}
        elif method is None:
            return  # 对服务端请求的应答(本服务不发起)——忽略
        else:
            if mid is not None:
                send({'jsonrpc': '2.0', 'id': mid,
                      'error': {'code': -32601, 'message': 'method not found: %s' % method}})
            return
        if mid is not None:
            send({'jsonrpc': '2.0', 'id': mid, 'result': result})
    except RpcError as e:
        if mid is not None:
            send({'jsonrpc': '2.0', 'id': mid,
                  'error': {'code': e.code, 'message': e.message}})
    except Exception as e:
        _log('handle异常: %r' % e)
        if mid is not None:
            send({'jsonrpc': '2.0', 'id': mid,
                  'error': {'code': -32603, 'message': 'internal error: %s' % e}})


def main():
    _load_cfg()
    _log('启动 → http://%s:%d  工具数=%d  py=%s' % (CFG['host'], CFG['port'], len(TOOLS),
                                                   sys.version.split()[0]))
    out_lock = threading.Lock()

    def send(obj):
        line = json.dumps(obj, ensure_ascii=True, separators=(',', ':'))
        with out_lock:
            sys.stdout.write(line + '\n')
            sys.stdout.flush()

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except Exception as e:
            _log('非JSON输入丢弃: %s' % e)
            continue
        for m in (msg if isinstance(msg, list) else [msg]):
            if not isinstance(m, dict):
                continue
            # 每个请求独立线程处理：工具调用(最长90秒)期间 ping/新请求不被阻塞
            threading.Thread(target=_handle, args=(m, send), daemon=True).start()

    _log('stdin关闭, 退出')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
