# v1.5.8 工具传递诊断

这是临时的本地诊断功能，v1.5.8 默认开启。设置 → 数据可以关闭；页面同时显示实际日志路径。通常在 `%APPDATA%/Atoapi/`，使用独立配置目录时随配置目录变化。

## 能定位到哪一层

按相同 request_id 对比四个 stage。跨请求比较工具名称时，还要考虑合法的动态工具加载及 tool_choice 限制。

新版 Codex 可能把工具目录放在 `input` 的 `additional_tools` 项中，而不是顶层 `tools`。因此，判断工具是否存在必须同时查看：

- `tools` / `tools_present`：传统顶层工具目录。
- `additional_tools` / `additional_tools_present`：新版 Codex 的嵌套工具目录。
- `additional_tools_complete`：嵌套目录是否在诊断上限内完整观察。

`tools=[]` 只表示顶层目录为空，不能单独证明 Codex 没有提供工具。

| 观察 | 可以得出的结论 | 不能据此断定 |
| --- | --- | --- |
| `tools` 和 `additional_tools` 都缺少 | 代理收到的这次请求没有观察到工具目录 | 客户端为什么没有提供、手机是否断连 |
| 顶层 `tools` 为空但 `additional_tools_present=true` | 工具通过新版 Codex 嵌套目录提供 | 不能把顶层 `tools=[]` 当成工具丢失 |
| ingress 有、完整 egress 没有 | Atoapi 请求组装或转换需要排查 | 仅凭名称数量就认定丢失；跨协议名称可被扁平化 |
| ingress 有调用结果、完整 egress 缺结果或 call_id 改变 | Atoapi 请求转换或历史处理需要排查 | 上游执行器故障 |
| upstream_response 有调用、downstream_response 缺少 | Atoapi 响应转换、转发或客户端断连需要排查 | 客户端一定收到了下游已入队数据 |
| 两边调用和结果都在，上游报告缺少 output | 错误在上游边界返回，需要结合上游解析及请求关联继续调查 | 无内容日志无法证明结果正文满足上游格式 |
| 两边都有调用，下次完整 ingress 未包含结果 | 客户端或工具执行生命周期需要排查，也可能是 continuation 外置历史 | 本地执行器一定未运行 |

`history_links` 只记录协议级调用 / 结果标识；`pairing` 是观察，不是执行器结论。`previous_response_id`、`history_complete=false`、`tools_complete=false`、`discovered_tools_complete=false`、`observer_aborted=true`、`dropped_events_before>0` 或记录超限都意味着证据有边界，不能当作确定的丢失证据。长会话只观察最近最多 4096 个协议项；工具元数据最多 1024 项，响应检查最多 8 MiB。

`downstream_response` 表示已接受到代理下游队列，并不等于 Codex 客户端已接收或执行。缺失自定义工具结果的错误会归类为 `missing_custom_tool_output`，保留对应 call_id；不保存原始错误内容。

## 隐私、开销和移除

不保存完整请求和工具内容。正常标识可读，异常标识以稳定 SHA-256 表示。不新增网络请求、重试、探测或 Token；仍有有限的本地元数据处理和异步落盘开销。写入饱和或失败不阻塞模型请求，而会累计丢弃标记。

关闭开关会停止新请求的记录；关闭前已在途的请求可能完成最后的边界记录。原有文件保留，方便用户自行备份或删除。滚动保留当前和 previous 两个文件，各约 4 MiB。

后续源码删除入口：`src-tauri/src/proxy/tool_diagnostics.rs`，以及标记 `TOOL_DIAGNOSTICS` 的钩子、AppState writer、tool_diagnostics_enabled 配置 / 公共配置 / 保存入参 / 前端开关及对应测试。移除该独立诊断模块不应改动路由、请求重试或工具执行逻辑。
