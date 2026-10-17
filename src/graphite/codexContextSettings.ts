import type { CodexContextSettings } from "../lib/api";

export function parseCodexContextSettings(
  contextWindow: unknown,
  autoCompactLimit: unknown
): CodexContextSettings {
  const parse = (value: unknown, label: string): number | null => {
    const text = String(value ?? "").trim();
    if (!text) return null;
    const parsed = Number(text);
    if (!/^[0-9]+$/.test(text) || !Number.isSafeInteger(parsed) || parsed < 1 || parsed > 4_294_967_295) {
      throw new Error(`${label}必须为 1 至 4294967295 的整数，留空使用 Codex 默认值`);
    }
    return parsed;
  };
  const window = parse(contextWindow, "上下文窗口");
  const limit = parse(autoCompactLimit, "自动压缩上限");
  if (window != null && limit != null && limit > window) {
    throw new Error("自动压缩上限不能大于上下文窗口");
  }
  return { model_context_window: window, model_auto_compact_token_limit: limit };
}
