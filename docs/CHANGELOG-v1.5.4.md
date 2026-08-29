# Atoapi v1.5.4

## 修复

- 为 Codex 的每 Provider 自动压缩阈值同时写入
  `model_auto_compact_token_limit_scope = "body_after_prefix"`。
  压缩后的上下文前缀不会再次占满阈值，只有压缩后新增的内容达到阈值时才会再次压缩。
- 保存、禁用或移除 Atoapi 注入时，完整保留用户原有的自动压缩阈值及其统计范围。

## 兼容性

- 未经明确验证的第三方 Provider 仍不会自动启用
  `previous_response_id` 原生续接、探测或重试。
- 保留 v1.5.3 的超大 FullReplay 本地保护，避免将无法解析的请求体继续上传到上游。
