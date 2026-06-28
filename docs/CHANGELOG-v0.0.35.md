# v0.0.35 更新说明

## 优化

- Responses 前台补热扩展到 `128 / 256 / 512 / 1024 / 2048` 级可避免缺口。
- 小缺口必须连续出现，并且真实 provider token 命中率仍处于高位时才触发，避免低命中样本被乱补。
- 前台补热会使用已经续接后的 `previous_response_id` delta 请求体；如果没有续接 id 且完整请求体过大，会跳过本次前台补热，防止补热自己触发上游 `413 Payload Too Large`。

## 诊断

- 上游 `413` 现在单独归类为 `upstream_payload_too_large`，方便区分真实上下文过大、上游限制和普通 provider 错误。
- 当前真实日志里看到的 `413 Payload Too Large` 本质是上游拒绝过大的请求体；如果发生在主请求上，多半是上下文、附件、历史太大，或 Responses 会话续接没有生效，不是本地代理伪造的错误。

## 约束

- 本次只调整 Responses 小缺口前台补热和 413 诊断保护。
- 不改变 Chat / Anthropic 缓存策略。
- 不改变 UI 布局。

## 验证

- `cargo test responses_foreground_prewarm -- --nocapture` 通过。
- `cargo test upstream_errors_are_scoped_by_cause -- --nocapture` 通过。
