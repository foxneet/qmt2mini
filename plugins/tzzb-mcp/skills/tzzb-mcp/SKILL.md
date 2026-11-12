---
name: tzzb-mcp
description: 同花顺投资账本（TZZB）远程 MCP 的授权与排错。当需要连接/授权 tzzb.10jqka.com.cn/mcp、MCP 工具列表缺失或调用报 401 时使用。
---

# TZZB 投资账本 MCP

本插件把同花顺投资账本的远程 MCP 服务挂载到 ZCode：

- 端点：`https://tzzb.10jqka.com.cn/mcp`（HTTP 传输，OAuth 2.1 保护）
- 服务键名：`tzzb`；工具由服务端在授权后通过 `tools/list` 下发，工具清单以实际下发为准

## 首次授权（必须）

1. 安装插件后，tzzb 出现在 ZCode 设置 → MCP 的「Plugin MCP 服务器」分组下。
2. 未授权时任何调用返回 401 —— 这是正常状态，不是故障。
3. 在该条目上点「打开授权」，浏览器跳转同花顺授权页，登录同花顺账号并点「授权」。
4. 授权回跳完成后，新建任务/新会话，tzzb 的工具即出现在工具列表。

## 排错

- 401 Unauthorized：服务在线但本地无 token → 走上面的授权步骤。
- 工具列表为空：授权状态在会话建立时读取，新建任务再试。
- 确认服务在线：`curl -i -X POST https://tzzb.10jqka.com.cn/mcp` 返回 401 即代表服务正常（等授权），问题在本地授权状态。
- 重复条目：若用户级 `~/.zcode/cli/config.json` 也手动配了同一服务（键名 TZZB），工具会出现两份，建议二选一保留。
