# Atoapi 更新记录

## v0.0.34 - Responses 可避免缺口前台小预热

### 修复

- 根据 v0.0.33 实时日志，修复 Responses 同一前缀下连续出现 `2560`、`3584`、`9216` 级可避免缺口的问题。
- 新增高水位前台小预热闸门：当前缀已连续多次出现可避免缺口、且真实缓存命中率仍处于高位时，先用 `max_output_tokens=1` 对同一 full body 补热一次，再发送真实请求。

### 约束

- 只作用于 Responses 通道。
- 只有开启后台预热开关时才启用。
- 低命中、低水位、首次缺口、小于 2k 的缺口不会触发，避免乱补。
- Chat / Anthropic 不受影响。

### 验证

- `cargo test -- --nocapture` 通过：115 个测试全绿。
- `npm.cmd run build` 通过。
- 新增回归覆盖 v0.0.33 实测形态：`173065 input / 169472 cached / 3584 可避免缺口 / streak 18` 会触发前台补热；低命中或首次缺口不会触发。

## v0.0.33 - Responses 技能修复与高命中保护

### 修复

- 修复 Responses 通道技能偶发不触发的问题：`previous_response_id` 会话 fallback 现在必须匹配同一套 `instructions` / `tools` 稳定作用域，避免把有技能的新请求复用到没有技能的旧会话链路。
- 对 Responses 的 `system` / `developer` / `instructions` 等价形态做缓存身份规范化，减少同一技能规则因为摆放位置不同而拆成多条 provider 前缀线。

### 优化

- 保持 v0.0.19 的高命中底线思路：不改变 Chat / Anthropic，不改变 Responses 实际语义，只修复技能链路和缓存身份。
- Responses 大可避免缺口增加高水位后台恢复：只有已经有大量缓存命中时才补热，避免低命中样本乱补导致越补越乱。

### 验证

- `cargo test -- --nocapture` 通过：114 个测试全绿。
- `npm.cmd run build` 通过。
- 新增回归覆盖：同一技能规则在 `instructions` 与 `developer input` 两种形态下 scope 一致；不同技能 scope 不允许 session fallback。

## v0.0.32 - Responses 中等新尾巴后台预热

### 修复

- 根据 v0.0.31 实时日志，修复 Responses 中等纯新尾巴没有触发后台预热的问题。
- `2k-8k` 新尾巴需要已有 `95%` 命中才进入后台预热，覆盖 `2560`、`3072`、`4096`、`6656` 这类稳定前缀下反复出现的缺口。
- `8k-16k` 新尾巴需要已有 `90%` 命中，或已命中 `12.8 万` 以上稳定前缀才补热，覆盖 v0.0.31 实测的 `15872` 缺口但避免低命中乱补。

### 说明

- 本次采用“只补不拖”：不额外拉长当前请求等待，只用后台 `max_output_tokens=1` 预热下一轮。
- 只调整 Responses 后台预热触发条件，不改变 Chat / Anthropic 策略。
- 可避免缺口和新尾巴继续分开统计；本次修的是 `avoidable=0` 的纯新尾巴。

### 验证

- 新增 v0.0.31 实测缺口回归：`6656` 与 `15872` 纯新尾巴会触发 Responses 后台预热。

## v0.0.31 - Responses 运行态持久化与后台预热诊断

### 优化

- Responses 会话续接状态和 provider 前缀水位会保存到本地 `runtime-state.json`，软件重启后 30 分钟内可恢复，减少重启导致的大新尾巴和重复冷启动。
- 后台预热新增诊断统计，`/admin/metrics` 会显示各通道预热触发次数、成功次数、触发时的新尾巴 token 和可避免缺口 token。
- 成功学习前缀水位、更新 Responses 会话、清理失效会话时都会同步保存运行态，避免只保存一半导致下次启动判断漂移。

### 说明

- 本次只增强底层状态恢复和诊断统计，不改变 Responses 请求语义，不调整 Chat / Anthropic 路由策略。
- 运行态只保留 30 分钟内的数据，过期会自动丢弃，避免旧上游缓存水位误导新的真实请求。
- v0.0.31 继续以 v0.0.19 / v0.0.28 的 Responses 稳定线为底线，v0.0.30 的稳定字段顺序优化保留。

### 验证

- `cargo test -- --nocapture` 通过，112 个测试全绿。

## v0.0.30 - Responses 稳定字段顺序优化

### 优化

- Responses 请求序列化顺序调整：`tools`、`tool_choice`、`parallel_tool_calls`、格式/采样参数会排在动态 `input` 前面。
- `previous_response_id`、`include`、`stream`、`metadata` 等动态字段仍排在 `input` 后面，避免提前打断稳定前缀。
- 目标是让 agent 工具 schema 这类大且稳定的内容更容易进入 provider 前缀缓存。

### 说明

- 本次只改 Responses 请求字段顺序，不改变请求内容和语义。
- Chat / Anthropic 不受影响。

## v0.0.29 - Responses 大新尾巴与可避免倒退保护

### 优化

- 在 v0.0.19 Responses 底线上增加有限保护，不回到 v0.0.23 / v0.0.24 的激进策略。
- 根据 5 分钟日志样本，`64k+` 大新尾巴会触发 Responses 保守等待和后台补热，减少下一轮继续大尾巴。
- 同前缀 cached_tokens 倒退造成的中等可避免缺口会触发恢复补热，降低可避免缺口连续出现概率。
- `3k` 左右的小纯新尾巴仍保持 v0.0.19 行为，不额外补热。

### 说明

- 本次只改 Responses 通道。
- Chat / Anthropic 原优化保留，不受影响。
- 保留 v0.0.20 的 Responses 技能触发修复。

## v0.0.28 - Responses 恢复 v0.0.19 稳定线并保留技能修复

### 调整

- 按要求将 Responses 通道底层恢复到 v0.0.19 稳定线。
- 撤回 v0.0.23 / v0.0.24 / v0.0.28 对 Responses 增加的专属同前缀冷却和后台补热策略。
- 保留 v0.0.20 的技能触发修复：`system` / `developer` 仍会提升到 `instructions`，避免 Responses 通道把技能规则当成普通用户内容。

### 说明

- 软件 UI 保持当前样式，请求记录通道标签、完整缺口拆分和长文本换行显示仍保留。
- Chat、Anthropic、Agent 注入、上游配置和统计 UI 不回退。
- 这版的目标是以 v0.0.19 的 Responses 缓存行为为底线，只叠加技能修复。

## v0.0.27 - 请求记录长文本完整显示

### 修复

- 修复请求记录右侧缺口文本固定宽度导致显示不完整的问题。
- 长缺口说明现在会在当前记录内自动换行，并保留鼠标悬停完整内容提示。
- 请求记录仍保留通道标签，方便同时查看 `Responses`、`Chat`、`Anthropic` 和完整缺口拆分。

### 说明

- 本次只改 UI 展示，不改变缓存命中、预热、冷却、路由和上游请求参数。

## v0.0.26 - 请求记录通道标签

### 新增

- 请求记录每一条新增通道标签，直接显示这条请求走的是 `Responses`、`Chat` 还是 `Anthropic`。
- 如果入口通道和上游通道不同，会显示类似 `Responses -> Chat` 的转换关系。

### 说明

- 本次只改 UI 展示，不改变缓存命中、预热、冷却、路由和上游请求参数。

## v0.0.25 - 请求记录缺口展示修复

### 修复

- 修复请求记录里同时存在“可避免缺口”和“新尾巴”时，只显示可避免缺口的问题。
- 例如 `66560 / 72353` 这一类记录，现在会显示总缺口，并同时标出“可避免 512 / 新尾巴 5120”，避免误以为总缺口只有 512。

### 说明

- 本次只改 UI 展示文案，不改变 Responses、Chat、Anthropic 的底层缓存策略。
- 真实 token 命中率仍按 `cache_read_tokens / input_tokens` 显示。

## v0.0.24 - Responses 超大新尾巴冷却修复

### 修复

- 根据 v0.0.23 最新日志，修复 14k-16k 级新尾巴后，下一轮可能变成可避免大缺口的问题。
- Responses 对超大新尾巴增加更保守冷却：16k 级新尾巴等待 24 秒，8k 级新尾巴等待 16 秒，4k 级新尾巴等待 10 秒。
- 512/1k/2k 级新尾巴仍保持轻等待。

### 说明

- UI 里缺口桶是累计统计，旧缺口不会自动下降；判断修复是否有效要看最新请求和近 5 分钟窗口。
- 本次只改 Responses 冷却策略，不改 Chat / Anthropic。

### 验证

- `cargo test responses_prefix_settle -- --nocapture` 通过，3 个 Responses 冷却测试全绿。

## v0.0.23 - Responses 可避免大缺口冷却修复

### 修复

- 根据 v0.0.22 实测日志，修复 Responses 同前缀在出现 2k-5k 可避免缺口后，下一条仍可能掉出 24k 级可避免大缺口的问题。
- Responses 现在有专属同前缀冷却策略：2k 级可避免缺口从约 12 秒提升到 20 秒，5k 级可避免缺口从约 18 秒提升到 28 秒。
- 纯新尾巴仍保持轻等待，避免所有请求都被无差别拖慢。

### 说明

- 日志显示 `provider_prefix_key` 和 `provider_prefix_fingerprint` 全程稳定，问题不是技能修复导致 key 持续抖动。
- 技能修复会让旧版本的上游前缀缓存首次换 key，可能冷一次；但持续大缺口主要是 Responses 同前缀冷却不足。
- 本次只改 Responses 冷却策略，不改 Chat / Anthropic。

### 验证

- `cargo test responses_prefix_settle -- --nocapture` 通过。

## v0.0.22 - Anthropic token-aware 缓存断点与保守预热

### 优化

- Anthropic 消息断点从按位置选择升级为 token-aware 选择。
- 短消息、低价值历史不再浪费 `cache_control` breakpoint。
- 长历史会按 Claude 20-block lookback 选择最新可覆盖的稳定窗口，尽量把断点放在更有价值的位置。
- 后台预热开关现在也支持 Anthropic，但只在已有部分缓存命中、缺口可补时触发，并限制 `max_tokens=1`。

### 说明

- 本次只改 Anthropic 通道。
- Chat 和 Responses 的缓存策略、冷却、续接、缺口统计保持不变。
- Anthropic 冷启动不预热，避免为完全未命中的请求重复消耗输入 token。

### 验证

- `cargo test anthropic -- --nocapture` 通过，7 个 Anthropic 相关测试全绿。
- `cargo test background_prewarm -- --nocapture` 通过。

## v0.0.21 - Anthropic 前缀缓存增强

### 优化

- Anthropic 通道现在会按最多 4 个 `cache_control` breakpoint 控制缓存点，避免超过 Claude 限额。
- 保留原来的优先级：`tools -> system -> messages`。
- 长历史消息会在“最近稳定消息”之外，额外补一个更早的稳定消息断点，降低超过 20 个 block 后旧缓存断开的概率。
- 最后一条新问题不会被打 `cache_control`，避免把动态尾巴误当稳定前缀。

### 说明

- 本次只改 Anthropic 通道的 `cache_control` 布点策略。
- Chat 和 Responses 的缓存冷却、续接、预热、缺口统计保持不变。

### 验证

- `cargo test anthropic -- --nocapture` 通过，5 个 Anthropic 相关测试全绿。
- `cargo test -- --nocapture` 通过，108 个测试全绿。

## v0.0.20 - Responses 技能触发修复

### 修复

- 修复 Responses 通道里 `developer` 技能规则被降级成普通 `user` 内容的问题。
- Chat 转 Responses 时，`system` 和 `developer` 现在都会合并进入 `instructions`。
- 原生 Responses 的 `input/messages` 中如果包含 `system/developer`，也会提升到 `instructions`，并从普通输入里移除。

### 说明

- 这次只修协议转换里的指令优先级，不改 Responses 的缓存冷却、会话续接、后台预热和缺口统计。
- 现象原因：Chat 通道保留了 `developer` 高优先级规则；Responses 通道之前把它当成普通用户文本，模型就不稳定触发技能，常常需要你显式发指令。

### 验证

- `cargo test developer -- --nocapture` 通过，新增 3 个 Responses 技能触发回归测试。
- `cargo test -- --nocapture` 通过，106 个测试全绿。

## v0.0.15 - 上游 prompt_cache_retention 开关

### 新增

- 每个上游配置新增“发送 prompt_cache_retention”开关。
- 开关默认开启，Responses 请求会发送 `prompt_cache_retention=24h`。
- 鼠标悬停会显示用途说明。
- 如果检测到上游不支持该参数，会弹窗提醒关闭当前上游的开关。
- 请求记录主百分比改为真实 token 命中率，“满桶”只作为桶状态显示。

### 说明

- 该参数用于请求支持的上游更久保留前缀缓存。
- 部分第三方中转不支持，可能返回 `Unsupported parameter`。
- 遇到不兼容上游时，关闭该上游的开关即可。

### 验证

- `cargo fmt` 通过
- `cargo test` 通过，99 个测试全绿
- `npm run build` 通过
- `npm run tauri:build` 通过

## v0.0.14 - Responses 可避免缺口收窄

### 修复

- 根据实时日志里的 `3072 总缺口 / 2560 新尾巴 / 512 可避免缺口` 做针对性收窄。
- 后台预热现在会参考可避免缺口，不再因为总缺口超过 2048 就完全跳过。
- 512 可避免缺口后的同前缀等待从 1.2 秒提高到 1.5 秒，连续出现时继续自适应升档。

### 说明

- 只影响 Responses 通道前缀缓存补热与等待策略。
- 不修改 Agent 注入、上游选择、UI 布局。
- 继续只使用真实 provider usage 统计命中，不伪造缓存数据。

### 验证

- `cargo fmt` 通过
- `cargo test` 通过，97 个测试全绿
- `npm run build` 通过
- `npm run tauri:build` 通过

## v0.0.13 - Agent 已启用并绑定状态条自动显示

### 修复

- 重新打开软件后，已开启且已绑定上游的 Agent 会自动显示顶部绿色状态条。
- 不再需要手动点击上游卡片，才能看到“已启用并绑定”的提示。

### 说明

- 本版只调整 UI 状态展示。
- 未改动缓存底层、路由逻辑、Agent 注入写配置逻辑。
- 继续保留 v0.0.12、v0.0.11、v0.0.10、v0.0.9 的关键修复。

### 验证

- `npm run build` 通过
- `cargo test` 通过，97 个测试全绿
- `npm run tauri:build` 通过
