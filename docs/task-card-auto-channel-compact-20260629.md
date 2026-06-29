# 任务卡：自动通道、压缩兼容与多 Key 管理

## 目标

- 上游配置默认使用“自动识别通道”，普通用户不用手动猜 Chat / Responses / Anthropic。
- 保留高级手动通道：Responses、Chat、Anthropic，用于特殊上游或排障。
- 压缩格式分两条线：默认快速模式；需要严格非 SSE Responses JSON 校验的 Agent 才开启“非 SSE 压缩校验兼容”。
- 多 Key 管理做成上游级能力：可开启/关闭、批量添加、测活、单 key 启停、别名、排序、负载均衡策略。
- 不让旧 UI、乱码文案和后端无关残留继续污染当前版本。

## 硬边界

- 不主动热补。
- 不新增同步请求。
- 不恢复普通 main session-delta。
- 不改工具输出内容。
- 不把非 SSE 兼容格式全局套到所有 Agent。
- 不动 Chat / Anthropic 已有高命中转发逻辑。
- 本轮先改 UI/逻辑，不打包。

## 已完成

- [x] 配置层新增上游通道模式：auto / manual。
- [x] 配置层新增压缩兼容模式：默认 cc-switch fast，按上游单独开启 non-sse validation。
- [x] 上游配置 UI 改成“自动识别（推荐）/ 手动通道”。
- [x] prompt_cache_retention 与大请求体 gzip 改成紧凑开关，说明只在 hover/focus 时显示。
- [x] 新增“非 SSE 压缩校验兼容”上游级开关，默认关闭。
- [x] 新增多 Key 管理 UI：批量添加、策略选择、单 key 测活、全部测活、启停、别名、排序。
- [x] 后端新增 key pool 保存、加密、公开预览、删除清理、轮询/优先级/最少使用/随机/顺序策略。
- [x] 后端新增 key 测活命令，失败 key 可自动标记不可用。
- [x] 清理前端/后端编码损坏、BOM、旧 UI 残留。
- [x] 保持普通 Responses 快速模式默认不补非 SSE 校验字段；仅开关开启时补齐。

## 验证

- [x] `npm run build` 通过。
- [x] `cargo fmt --manifest-path G:\Atoapi\src-tauri\Cargo.toml` 通过。
- [x] `cargo test --manifest-path G:\Atoapi\src-tauri\Cargo.toml` 通过：227 passed。
- [x] `git diff --check` 通过。
- [x] 扫描旧 UI/乱码/BOM 关键字未发现残留。

## 后续

- 等用户测试 UI 后再决定是否打包。
- 下一轮若继续命中率优化，先读 v0.1.50 及新日志，再和基线版本对比，不把未验证历史策略直接照搬。