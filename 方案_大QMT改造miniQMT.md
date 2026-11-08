# 方案：把大QMT改造为miniQMT（qmt2mini）· v3 三层架构版

> 状态：实机联调攻坚中（桥通道存活问题已定位，探针K验证文件通道）
> 日期：2026-11-06
> 实机：某券商证券大QMT @ `D:\QMT`（策略环境py3.12），外部 py -3.13 + xtquant 250807

## 〇、目标架构（用户定版，2026-11-06；同日修订：本机服务并入中间件）

```
┌─ 大QMT（源）─┐        ┌─ 转发中间件.exe（唯一枢纽）────────────────┐        ┌─ 客户端 ────────┐
│ 桥_QMT端.py  │ ⇄TCP⇄  │ qmt2mini转发器.exe                        │ ⇄HTTP⇄ │ 局域网 shim/    │
│ (内置策略)   │ 51880  │  桥端口 127.0.0.1:51880（源接入+本机shim） │ 51881  │ 任意语言        │
└──────────────┘        │  LAN端口 0.0.0.0:51881（转发+审计）        │        └─────────────────┘
                        └───────────────────────────────────────────┘
```

- **两层到底**：大QMT=源（只对127.0.0.1）；`qmt2mini转发器.exe`=唯一中间件（原qmt2mini_server职能全部并入：桥接入、本机shim服务、LAN转发、审计）
- **转发中间件.exe**（对标通达信TQRelay）：HTTP JSON API + token鉴权 + IP白名单 + allow_trade总闸
- **审计日志（用户明确要求）**：逐请求记录 —— 客户端IP、请求类别(op)、开始时间、结束时间、请求/响应数据大小；日志落盘可查
- 交付形态：GUI + `--serve` 无界面双模式（仿文件桥/TQRelay惯例），PyInstaller 打包 exe
- 客户端两种姿势：qmt2mini shim（xtquant兼容，本机连51880/局域网连51881的HTTP模式）或任意语言直接 HTTP POST `{"op":..,"args":..}`

## 一、实机勘察与通道攻坚结论（2026-11-05/06 实测）

| 项 | 结论 |
|---|---|
| 大QMT直连(58600/51314) | 协议同族但行情/交易RPC处理器不对外注册，判死 |
| 内置策略环境 | py3.12（用户确认），GBK文件、禁多线程、回调单参(实测) |
| 旧 `run_time` 定时器 | **本构建有缺陷：注册即致模型被停** → 必须用新版 `schedule_run`（实测探针定位） |
| 策略发起TCP | **被引擎评分管控**：定时器/handlebar内socket活动计分（跨模型累计、不衰减），过阈即停模型（E1/F~112s、E2~168s、持久2-3s、风暴1s、评分打满后秒死；init上下文不计分——文件桥自测37连接全过） |
| 文件桥16/16自测(12:57) | 策略↔本机服务端TCP通道本身健康（init上下文） |
| 结论 | 常驻桥的泵**不能**在策略内做socket；入站/出站改走：出站=print→FormulaOutput日志tail（无评分、无限流），入站=文件open().read()（探针K验证中） |

## 二、组件清单（v3）

| 组件 | 环境 | 职责 | 状态 |
|---|---|---|---|
| `桥_QMT端.py` | 大QMT策略(GBK) | 出站print推送(行情/成交) + 入站读指令文件 + passorder | 骨架完成，待探针K定入站通道 |
| `qmt2mini_server.py` | py -3.13 | 桥接入+本机客户端+日志tail+指令文件投递 | 已运行(127.0.0.1:51880) |
| `qmt2mini/` shim | py -3.13 / 任意机 | xtquant同签名API；新增HTTP传输模式指向转发器 | 19/19模拟联调过 |
| `qmt2mini转发器.py/.exe` | py -3.13→PyInstaller | LAN转发+token+白名单+allow_trade+**审计日志(IP/类别/起止/大小)** | 待建（对标TQRelay源码 Y:\fox\quant-toolbox\TQ转发器_GitHub\） |
| `测试_联调.py` 等 | py -3.13 | 分级验证 | 就绪 |

## 三、里程碑（v3）

- **M1' 桥通道定案**：探针K（文件IO边界）→ 定入站方案 → 重造桥_QMT端（print出站+文件入站，零socket）
- **M2' 实机联调**：行情五项+交易查询四项（账号你的资金账号）
- **M3' 下单演练**：`--enable-order` 门控，模拟账号演练
- **M4' 转发中间件**：qmt2mini转发器.exe（HTTP+token+白名单+审计日志+GUI/serve双模式）
- **M5' 打包交付**：exe+README+历史版本快照

## 四、风险与对策（v3 增补）

| 风险 | 对策 |
|---|---|
| 引擎socket评分机制 | 策略内零socket（架构已改print/文件通道） |
| print→日志tail的吞吐与轮转 | FormulaOutput按天分文件、批量合并打印；服务端tail断点续读 |
| 指令文件入站延迟 | 1秒轮询（schedule_run 1s已验证安全）；可调 |
| 审计日志膨胀 | 按天滚动+保留N天 |
| 大QMT必须开着 | 文档明示+服务端启动自检 |

（v2 勘察细节、v1 桥接仿真原始方案见历史版本快照）

## 一、M0 勘察结论（实机事实）

| # | 事实 | 含义 |
|---|---|---|
| 1 | 大QMT进程监听 **0.0.0.0:58600**（58610系端口族） | 行情/数据服务的本地入口大概率就是它 |
| 2 | `xtquant` 250807 已装 py3.13；`xtdata.connect(ip, port)` **官方支持指定端口**（xtdata.py:115-125） | `xtdata.connect(port=58600)` 一条语句即可指向大QMT，无需改包、无需portproxy |
| 3 | `%USERPROFILE%\.xtquant\` 下7个实例目录均无 `xtdata.cfg` | 裸 `connect()` 只会试 58610 失败——必须显式指定端口，这正是"适配包"要解决的问题 |
| 4 | `D:\QMT\userdata_mini\` 真实存在：datadir 有行情数据（.ldb）、`miniqmtShm*` 共享内存缓存、`down_queue_xtmodel-0`、log 里有 **2026-10-15 的 XtMiniQmt 运行日志** | mini引擎在此机跑过；XtQuantTrader 的连接路径现成 |
| 5 | `bin.x64\` 同时有 `XtMiniQmt.exe`、`XtQuantServer.dll`、python36 与 python312 双环境；**用户确认：该大QMT支持 Python 3.12**（内置策略环境） | 客户端自带 quant 服务组件；后备桥（如触发）可用 3.12 语法（f-string等），但 GBK 编码与粘贴运行方式仍按实机验证为准 |
| 6 | xtquant 包内含 cp36~cp313 全套 pyd | 库本身兼容性无忧 |

## 二、路线修订：直连适配为主，内置桥为后备

### v1 的桥接仿真方案降级为后备
原方案B（大QMT内置策略↔Python服务端桥，仿xtquant API）不再需要做全套——因为勘察发现**不需要仿真协议**：xtquant 官方就支持指定连接端口。

### 新主路径：qmt2mini 直连适配包
```
外部Python (py -3.13, xtquant 250807)
   │  from qmt2mini import ...   ← 封装 xtdata.connect(port=58600)
   │                                与 XtQuantTrader('D:\\QMT\\userdata_mini', session)
   ▼
大QMT (XtItClient.exe 已监听58600, XtQuantServer组件)
```
- 行情：`xtdata.connect(port=58600)` → get_full_tick / get_market_data / subscribe_quote / subscribe_whole_quote / run
- 交易：`XtQuantTrader(r'D:\QMT\userdata_mini', session_id)` → connect / subscribe(账号你的资金账号) / 查询 → 下单

### 后备路径（仅当 M2 交易直连被拒）
交易专用内置桥：qmt-file-bridge 已验证的"大QMT策略↔本机TCP"模式，策略端调 passorder / get_trade_detail_data / 主推回调，外部封成 XtQuantTrader 同签名。行情仍走直连。

## 三、交付物

| 组件 | 环境 | 职责 |
|---|---|---|
| `qmt2mini.py` | py -3.13 | 一键连接助手+环境探测：自动发现QMT安装/监听端口、生成连接配置、诊断输出 |
| `测试_联调.py` | py -3.13 | 分级验证：行情查询→订阅推送→交易查询→下单演练（`--enable-order` 显式开启，账号 你的资金账号） |
| （条件）`桥_交易_QMT端.py` + 服务端 | 大QMT策略(GBK) + py -3.13 | 仅 M2 失败时启用 |
| `README.md` + `历史版本/` 快照与更新日志 | — | 按全局版本留存规则 |

## 四、里程碑

- **M1 行情直连**：xtdata.connect(58600) → 实时tick/K线/订阅推送逐项验证。预期一次通过。
- **M2 交易直连**：XtQuantTrader 连 userdata_mini → connect/subscribe → 查询资产/持仓/委托/成交。**关键未知**：大QMT上下文是否放行交易会话——M2 一测便知。
- **M3 下单演练**：模拟账号 你的资金账号（用户已确认可按实盘流程演练），`--enable-order` 门控，限价单→撤单→查询闭环。
- **M4 后备桥**（条件触发）：交易直连失败才做，复用 qmt-file-bridge 通道模式（GBK、标准库、心跳重连）。
- **M5 打包交付**：一键 bat、README、历史版本快照。

## 五、风险与对策

| 风险 | 对策 |
|---|---|
| 58600 不是完整 XtQuant 数据服务（或握手有终端类型校验） | M1 首测即知；不通则回退 v1 桥接仿真（方案文件v1存档于历史） |
| 交易服务拒绝大QMT上下文的会话 | M4 后备桥；交易永远是最后验证、默认门控 |
| 大QMT必须开着 | 文档明示前提；qmt2mini.py 启动时检测进程与端口并给出明确提示 |
| 0.0.0.0:58600 对局域网开放 | 适配包不改动它；README 提醒仅本机使用的连接地址写 127.0.0.1 |
| 下单误操作 | 模拟账号先行；下单默认锁死，需显式参数+逐次确认 |

## 六、已确认决策

1. 安装路径：`D:\QMT`（某券商）✓
2. 范围：行情+交易全要，逐段交付 ✓
3. 形态：兼容原 miniQMT（xtquant 同签名）✓
4. 下单演练账号：`你的资金账号`（虚拟账号，按实盘流程演练）✓
