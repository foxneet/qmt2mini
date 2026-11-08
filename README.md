# qmt2mini — 把大QMT改造为miniQMT（某券商定制版实测方案）

> 某券商QMT（模拟/实盘）在没有 miniQMT 权限的环境下，把**大QMT客户端变成局域网行情中心 + 交易节点**：
> 外部 Python 用 **xtquant 同签名 API** 取实时行情、查资产持仓、下单撤单；转发器对局域网提供 HTTP 服务并逐请求审计。

```
┌─ 大QMT（源）──────────┐   ┌─ qmt2mini转发器（唯一枢纽）──────────────┐   ┌─ 客户端 ─────────┐
│ 桥_QMT端.py 阻塞init泵 │⇄⇄│ 桥端口 127.0.0.1:51880（源接入+本机shim） │   │ shim(xtquant同签名)│
│ 零schedule_run零回调   │   │ LAN端口 0.0.0.0:51881（HTTP+token+审计） │⇄⇄│ 任意语言HTTP POST │
└───────────────────────┘   └──────────────────────────────────────────┘   └───────────────────┘
```

---

## 〇、仓库文件说明

| 文件 | 说明 |
|---|---|
| `桥_QMT端.py` / `交易_QMT端.py` | 粘贴进QMT策略运行的桥，**GBK编码成品**（GitHub网页预览中文注释会乱码，下载raw即正常；`*_src.py` 为UTF-8编辑源码，改完用 `转换GBK.py` 转换） |
| `qmt2mini转发器.py` | 中间件枢纽（GUI 或 `--serve`；`打包转发器.bat` 打包exe） |
| `qmt2mini/` | 客户端SDK（xtquant同签名，客户机无需安装xtquant） |
| `探针F~S*.py` | 引擎行为诊断脚本——「红宝书」16条结论的证据链，复测照抄改一版即可 |
| `测试_模拟.py` / `测试_联调.py` | 离线仿真转发器 / 实机分级联调 |

> **首次部署改动三处**：① `转发器_配置.json` 首次运行自动生成（token自动生成，不入库）；② 需要 token 的文件（`交易_QMT端.py`、`探针Q/S`）把 `BRIDGE_TOKEN = 'PASTE_TOKEN_HERE'` 填入配置里的token；③ 脚本中的本机路径（`D:\QMT`、`F:\qmt2mini`）按实际环境调整。出于风控考虑，文档统一以「某券商」指代实测券商。

---

## 一、快速开始

### 1) 源端（大QMT，一次性部署）
1. 记事本打开 `桥_QMT端.py`（GBK），全选复制
2. QMT → 模型研究 → 新建策略，粘贴保存（命名随意，如 `q2m桥`）
3. **模型交易**界面选资金账号，**实盘模式**运行
4. 日志出现 `q2m桥v2: 阻塞init常驻泵启动` + `已连接转发器` 即成功
   - ⚠ 引擎随后写一行 `策略停止` 是**无效日志**（本构建杀不死阻塞init的策略），循环持续打印=活着
   - ⚠ 桥占用QMT策略线程：**该客户端运行桥期间，其他策略暂停**（源客户端建议专用）

### 2) 中间件（本机）
```
python qmt2mini转发器.py            # GUI 模式
python qmt2mini转发器.py --serve    # 无界面后台
```
- 首次运行自动生成 `转发器_配置.json`（**token** 在里面）
- 打包 exe：双击 `打包转发器.bat`（需 `py -3.13 -m pip install pyinstaller`）

### 3) 客户端
**本机**（xtquant 同签名）：
```python
from qmt2mini import xtdata                      # pip 无需 xtquant
xtdata.connect(port=51880)
print(xtdata.get_full_tick(['600519.SH']))
```
**局域网**（任意语言，HTTP）：
```bash
curl -X POST http://<转发器机IP>:51881/api \
  -H "X-Token: <token>" -H "Content-Type: application/json" \
  -d "{\"op\":\"get_full_tick\",\"args\":{\"codes\":[\"600519.SH\"]}}"
```

---

## 二、客户端 API（qmt2mini shim，与 xtquant 同签名）

| 模块 | 可用接口 |
|---|---|
| `xtdata` | `connect(ip,port)` `get_full_tick` `get_market_data_ex` `get_market_data` `get_trading_dates` `get_instrument_detail` `get_stock_list_in_sector` `subscribe_quote` `subscribe_whole_quote` `unsubscribe_quote` `download_history_data2` `run` |
| `xttrader` | `XtQuantTrader`(connect/subscribe/order_stock/async/cancel/query_asset/positions/orders/trades/run_forever) `XtQuantTraderCallback`(on_stock_order/on_stock_trade/…) |
| `xttype` | `StockAccount(account_id, account_type)` |
| `xtconstant` | 与 xtquant 同值常量（自带，客户机无需装xtquant） |

**HTTP 网关**（LAN）：`POST /api {"op":"...","args":{...}}`；op 即上表同名接口；`GET /status` 桥存活验证；`GET /audit?lines=100` 审计尾部。

---

## 三、审计日志（转发器）

逐请求落盘 `转发器审计/审计_YYYYMMDD.log`（按天滚动）：
```
2026-11-06 14:15:34 | 127.0.0.1 | 类别=ping | 开始=14:15:28 | 结束=14:15:34 | 耗时=6088ms | 请求=26B | 响应=45B | ERR
```
另记事件：桥连入/断开、客户端#N 连入/断开、allow_trade 变更。
配置 `转发器_配置.json`：`token`(首次自动生成) / `allow_ips`(建议填客户端IP，空=全放行并警告) / `allow_trade`(下单总闸,默认false) / 端口 / `audit_dir`。

---

## 四、运维手册

| 操作 | 方法 |
|---|---|
| 看桥是否活着 | `GET /status` 的 `bridge.connected` + `last_seen_sec`；或 GUI 状态灯；或 QMT 日志 `q2m桥:` 持续打印 |
| 停桥 | `POST /api {"op":"die"}`（桥收到后 init 正常返回、模型自然结束）或重启QMT |
| QMT界面出现"策略停止" | 正常现象（引擎无效停止），以 `/status` 与循环打印为准 |
| 桥占用策略线程 | 设计使然——源客户端专用；其他策略在桥运行期间暂停 |
| 转发器重启 | 桥每秒自动重连，无需人工干预 |
| 下单演练节律 | 见「已知限制」——取决于探针N的评分衰减结论（当前默认：每客户端会话预算约112-170次连接） |

**实机验证的能力矩阵（2026-11-06）**：实时tick推送 ✓｜日历/合约/板块 ✓｜资产/持仓/委托/成交查询 ✓｜die遥控 ✓｜K线 ✗（见下）｜下单 ✗（见下）

---

## 五、已知限制与对策

| 限制 | 原因（实测定案） | 对策 |
|---|---|---|
| K线历史取不到 | K线数据子系统绑死策略线程事件循环，阻塞init内 C层返回转换失败（`get_market_data2` 官方姿势/subscribe=False 均试过） | 转发器聚合**通达信TQRelay**补K线（用户已有TQ环境）；或由tick流自攒当日分钟线 |
| 下单/撤单未通 | `passorder` 在阻塞init被引擎**静默忽略**（实测冻结资金0、委托列表空）——交易需引擎认可的运行上下文 | 正常模型+低频TCP指令通道（探针N测评分衰减中：衰减→1次/分钟永续；不衰减→约2-3小时/会话重启一次）；或申请miniQMT权限 |
| 策略内文件IO全禁 | 引擎文件IO白名单为空（探针L：8类路径读写全 `Forbidden fileIO`，仅 listdir 放行） | 本方案已绕开：出站走print、入站走TCP |
| socket被评分回收 | 引擎会话级"socket活动评分"：定时器/handlebar内socket计分（跨模型累积、不衰减、阈值≈112-170），过阈停模型；**init上下文不计分** | 桥泵全部在阻塞init内；其他策略勿在定时器里做socket |

---

## 六、某券商QMT策略环境红宝书（实机踩坑全集，写策略必读）

1. **策略文件必须 GBK**：第一行 `#coding:gbk`；含GBK外字符直接不加载。
2. **旧 `run_time` 定时器有毒**：本构建注册即致模型被停（实测5连发）。**改用新版** `ContextInfo.schedule_run(func, 'yyyymmddHHMMSS', -1, datetime.timedelta(seconds=n), 'name')`——函数对象直传，`-1`=无限重复。
3. **行情/交易回调只传1个参数**：`def cb(data)`（文档写 `(ContextInfo, data)` 是错的）。**所有回调一律 `*args` 取 `args[-1]`**。
4. **单线程世界**：所有策略共用一个线程；阻塞它=暂停全部策略；回调里禁止长阻塞（但阻塞init例外，见5）。
5. **阻塞init = 不可杀领域**：init 内 `while True` 不还线程，引擎的停止机制失效（只写一行无效日志），循环内 get_full_tick/print/socket 全部可用——常驻服务的唯一安全容器。
6. **`passorder` 上下文敏感**：handlebar(实时)/定时器/after_init/交易回调内必须 `quickTrade=2`；**编辑器界面与阻塞init内调用被静默忽略（不产生委托）**。
7. **`get_market_data_ex` 有坑**：直接调命中C层报返回值转换失败。**官方姿势**（照抄 `_PyContextInfo.py:181`）：调 `ContextInfo.get_market_data2(fields, codes, period, start, end, count, dividend_type, True, False)` 后本地 `import pandas` 拼装（ifield='stime'）。
8. **`get_trading_dates` 传满5参**：`(code, start, end, count, '1d')`。
9. **文件IO白名单为空**：open() 读写任何路径（含QMT自家userdata）都 `Forbidden fileIO`；只有 `os.listdir` 放行——需要文件能力就走外部通道。
10. **socket 会评分**：见上表「socket被评分回收」；init 上下文除外。
11. **策略worker是独立进程**（python.exe）：阻塞init不冻结QMT界面。

---

## 七、探针工具箱（诊断/再验证用，均在项目根目录）

| 探针 | 用途 |
|---|---|
| 探针F/G/H/I/J/K/L/M/N | 依次定位：文件IO边界 / socket上下文生死 / 连接姿势对比 / init建连 / 文件桥原版对照 / 文件白名单测绘 / 阻塞init泵 / 评分衰减 |
| 复测建议 | 任何"策略能不能做X"的疑问，抄对应探针改一版即可，10分钟出答案 |

---

## 八、故障排查

| 症状 | 原因/处理 |
|---|---|
| `无法连接 qmt2mini 服务端` | 转发器没跑/端口不对 |
| `排队等待桥超时` | QMT里桥策略没运行（或被stop后没重启）；`/status` 看桥状态 |
| 桥日志狂刷 `转发器不可达` | 转发器没启动；启动后桥每秒自动重连 |
| 客户端401 | X-Token 与 `转发器_配置.json` 不符 |
| 客户端403 | IP不在 `allow_ips` |
| 下单返回 allow_trade 拒绝 | 转发器配置/勾选开启总闸 |
| 桥"策略停止"后日志还在打 | 正常（无效停止），勿慌 |

---

*方案演进：v1 桥接仿真 → v2 直连判死后转桥接 → v3 三层/两层架构（本版）。历史与探针证据链见 `历史版本/` 与 `方案_大QMT改造miniQMT.md`。*
