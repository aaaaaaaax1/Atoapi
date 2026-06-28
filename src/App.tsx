import {
  Activity,
  Bot,
  BrainCircuit,
  Check,
  ChevronDown,
  Copy,
  DatabaseZap,
  Eye,
  EyeOff,
  Gauge,
  KeyRound,
  Link2,
  Loader2,
  Play,
  Plus,
  RefreshCw,
  Save,
  Settings2,
  ShieldCheck,
  Square,
  TerminalSquare,
  Trash2,
  Workflow,
  X,
  Zap
} from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  AgentInjectionConfig,
  AgentInjectionResult,
  AppConfig,
  Channel,
  command,
  FetchModelsInput,
  GeneralConfigInput,
  MetricsSnapshot,
  model,
  ModelConfig,
  ProviderConfig,
  ProviderInput,
  ProxyStatus
} from "./lib/api";

type ViewId = "agent" | "gateway" | "cache";

interface ProviderDraft {
  id?: string;
  name: string;
  base_url: string;
  models_url: string;
  is_full_url: boolean;
  custom_user_agent: string;
  api_key: string;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  models: ModelConfig[];
  enabled: boolean;
}

const channelOptions: Array<{ value: Channel; label: string; endpoint: string }> = [
  { value: "anthropic", label: "Anthropic", endpoint: "/v1/messages" },
  { value: "chat", label: "OpenAI Chat", endpoint: "/v1/chat/completions" },
  { value: "responses", label: "OpenAI Responses", endpoint: "/v1/responses" }
];

const utilityViews: Array<{ id: ViewId; label: string; icon: ReactNode }> = [
  { id: "gateway", label: "本地代理设置", icon: <Settings2 size={16} /> },
  { id: "cache", label: "缓存统计", icon: <Gauge size={16} /> }
];

const requestPageSize = 20;
const maxRequestPages = 10;
const appVersion = "v0.1.49";
const appVersionNotes = [
  "v0.1.49: 主命中线恢复到 v0.1.41 稳定基线，压缩线保留 v0.1.45+ 的成功回退和冷却",
  "v0.1.49: 当前版本只恢复正确组合基线，不夹带未验证的 v0.1.48 动态尾巴实验策略",
  "v0.1.49: 后续命中优化必须基于当前真实日志验证，历史正优化只做参考，不直接照搬",
  "v0.1.48: 以 v0.1.46/v0.1.41 的稳定命中线为主，撤回 v0.1.47 的 8k+ 工具尾巴泛化满 5s 负优化",
  "v0.1.48: 保留 v0.1.45 压缩 Chat 兼容失败冷却，压缩线和 Responses 主命中线分开统计、分开判断",
  "v0.1.48: 对照 v0.1.27/v0.1.28/v0.1.29，只保留冷读隔离、成熟大工具短追等不增加额外请求的正向规则",
  "v0.1.46: 压缩线保留 v0.1.45 的 Chat 兼容失败回退和 15 分钟冷却，优先保证压缩成功",
  "v0.1.46: 普通 Responses 主流式命中控制回到 v0.1.41 线，撤掉 v0.1.42 的中等尾巴后小工具 5 秒追桶保护",
  "v0.1.46: 保留 512/1024/1536/2048 小桶保护、+5s 上限、零热补、零额外主请求和禁用普通 main session-delta",
  "v0.1.45: 同一上游/模型的 Chat 兼容压缩遇到 429/500/502/503/504/524 后，写入 15 分钟短冷却",
  "v0.1.45: 冷却期内压缩直接走 Responses 原生聚合，避免每次都先浪费一次 Chat 兼容失败",
  "v0.1.45: 同步压缩/总结诊断标记为 compact，不再显示为本地响应缓存 miss，减少命中率面板误导",
  "v0.1.44: Chat 兼容压缩遇到 429/500/502/503/504/524 时，自动回退到 Responses 原生聚合，优先保证压缩成功",
  "v0.1.44: 记录 Chat 兼容失败的上游错误摘要，避免只看到压缩失败却不知道是节点限频还是代理问题",
  "v0.1.44: 只作用于压缩 sync 兜底，不影响普通流式主请求，不恢复热补和 main session-delta",
  "v0.1.43: 彻底禁用 Responses 压缩/总结的 Chat fast-json 路径，mixed 大工具压缩也不会再触发 524 快路",
  "v0.1.43: 压缩兼容统一走 Chat stream 聚合，优先保证压缩成功，再通过底层耗时诊断继续优化速度",
  "v0.1.43: 保留 v0.1.42 的中等尾巴后小工具输出追桶保护，继续遵守 +5s、不热补、不新增同步请求",
  "v0.1.42: 压缩/总结 message-only 请求改走 Chat stream 聚合，禁止回到 v0.1.40 的 Chat fast-json 524 负路径",
  "v0.1.42: 6144/4096 等中等尾巴后，如果下一轮只是小工具输出，会给最多 5 秒追桶保护，减少二次 512/1024 掉桶",
  "v0.1.42: 继续坚持 +5s 上限、不热补、不新增同步请求、不恢复普通 main session-delta、不修改工具输出",
  "v0.1.41: 撤回 v0.1.40 的 message-only 压缩 Chat fast-json 负优化，恢复 v0.1.38 慢但成功的 Responses 兼容路径",
  "v0.1.41: 保留 sync compact 统计隔离，但不把压缩失败/压缩冷启动写进主对话命中和 TTFT p95",
  "v0.1.41: 坚持 +5s 上限，不新增 +8s、不热补、不恢复普通 main session-delta、不改工具输出",
  "v0.1.40: 压缩/总结 sync compact 隔离主对话 usage、TTFT p95 和 gap bucket，压缩慢不再拉低主命中面板",
  "v0.1.40: 非流式 Responses 压缩路由阈值下探到 12KB / 8KB message，减少漏进 responses-sync-main 慢路径",
  "v0.1.40: 同一稳定会话里反复出现 512/1024/1536 小桶尾巴时，保留最多 5 秒窄保护，不新增热补请求",
  "v0.1.39: 非流式 Responses 压缩/总结形态直接走 Chat fast-json，覆盖 33KB / 6 input / 26k message 的慢压缩场景",
  "v0.1.39: 压缩/总结兼容路径不写 provider usage 和 prefix 水线，避免压缩冷启动拉低主对话累计命中",
  "v0.1.39: 继续保留 v0.1.38 的 stale 小尾巴 5 秒保护；大工具真实新尾巴不靠热补或额外请求硬刷",
  "v0.1.38: 修复 v0.1.37 小尾巴保护未生效的问题；settle window 过期后仍可触发最多 5 秒的小尾巴守护",
  "v0.1.38: 压缩 Chat 兼容快路按转换后请求体判断，128KB 内直接走非流式快路，覆盖 95KB 旧对话压缩场景",
  "v0.1.38: 继续限制在 512/1024/1536/2048 小桶尾巴，不热补、不新增常规同步请求、不恢复普通 main session-delta",
  "v0.1.37: 压缩兼容路径如果转换后的 Chat 请求体很小，会优先走非流式快路，减少旧对话压缩 stream 聚合等待",
  "v0.1.37: Responses 稳定会话里反复出现 512/1024/1536/2048 小工具尾巴滞后时，当前请求给满最多 5 秒短保护",
  "v0.1.37: 继续不主动热补、不新增常规同步请求、不恢复普通 main session-delta；目标是在 +5s 内压小新尾巴",
  "v0.1.36: 合并压缩兼容修复与 v0.1.29 的 +5s Responses 命中控制主基线，不再把两条线拆到下个版本",
  "v0.1.36: 保留 v0.1.27 的同前缀 cache_read=0 冷读隔离，以及 v0.1.28 的大工具输出最多 5 秒短保护",
  "v0.1.36: Responses 前台等待统一收口到最多 5 秒；不主动热补、不新增同步请求、不恢复普通 main session-delta",
  "v0.1.36: 中等 mixed 非流式压缩请求可走 Chat 兼容回退，避免旧对话压缩被 Responses 非流式校验卡住",
  "v0.1.35: 压缩兼容路径增加 upstream_headers_ms / upstream_first_chunk_ms / aggregate_done_ms 底层诊断，用来分清慢在上游响应头、首个 SSE 分片，还是本地聚合",
  "v0.1.35: 旧对话压缩在安全小体量场景优先走 Chat 非流式快路；大旧对话仍走 Chat stream 聚合兜底，避免 524 长等待",
  "v0.1.35: Response 命中线增加错误隔离，SSE error / failed / incomplete 和 200 错误 JSON 不再学习 prefix 水线、不写本地缓存",
  "v0.1.35: 命中率基线继续对照 v0.1.27 / v0.1.28 / v0.1.29；不新增热补、不新增同步请求、不恢复普通 main session-delta",
  "v0.1.34: 老会话 Responses 压缩走 Chat 兼容时，上游改用 stream=true，本地聚合后仍返回非流式 Responses JSON，规避 524 长等待超时",
  "v0.1.34: 压缩兼容请求会移除工具字段，避免压缩总结误触发工具调用；只影响旧对话压缩兼容路径",
  "v0.1.34: Chat SSE 聚合新增 error/半截断流防护，且不会把 chatcmpl ID 写入 Responses 会话状态；不增加额外请求、不热补",
  "v0.1.33: 旧对话 Responses 压缩仍被上游校验拒绝时，按旧对话特征单次改走 Chat 兼容再转回 Responses JSON",
  "v0.1.33: 只覆盖 900KB+、mixed/大量工具输出的非流式压缩请求；新对话和正常流式不受影响",
  "v0.1.33: 不增加额外请求、不主动热补、不恢复普通 session-delta，目标是修旧对话压缩失败",
  "v0.1.32: 修复 Responses 非流式压缩/compact 主请求大包未 gzip，导致 1MB+ 原样发送后上游校验失败的问题",
  "v0.1.32: sync-main 仍保持单次上游请求，不为了 gzip fallback 额外补发；不增加真实调用成本",
  "v0.1.32: sync-main 遇到 400/429/5xx 会按同前缀短冷却，避免压缩失败时连续打爆上游",
  "v0.1.31: 修复 exact prefix 已经 warm 后 cache_read=0/极低命中被误学成水线的问题",
  "v0.1.31: 20 万 token 级前缀断裂不再污染普通 new_tail，保留旧 warm 水线等待下一轮恢复",
  "v0.1.31: 纠偏 v0.1.30 负优化，不改请求语义、不新增同步请求、不恢复主动热补",
  "v0.1.30: 修复 alias/fingerprint warm 后巨大动态冷读把整段 20 万 token 误记成 new_tail 的问题",
  "v0.1.30: cold_read=0 且已有 warm alias 时保留旧 warm 水线，避免下一轮被 cache_read=0 状态污染",
  "v0.1.30: 普通跨上游 alias 仍不参与可避免计算，只隔离巨大 mixed/tool_output 动态冷读，不改请求、不增加调用",
  "v0.1.29: 在 v0.1.28 基础上补早段新会话大工具尾巴追桶，覆盖 8k-16k 输入阶段出现的 5k-12k 新尾巴",
  "v0.1.29: 大工具尾巴阈值下探到 tool_output >= 18000 或最大单块 >= 10000，仍只给最多 5 秒短保护",
  "v0.1.29: 继续保持零额外请求、不主动热补、不恢复普通 main session-delta，不改工具输出和语义",
  "v0.1.28: 针对大工具输出后的真实新尾巴，增加最多 5 秒的短追桶保护，减少 12k-17k 级新尾巴",
  "v0.1.28: 只对当前 tail_tool_output_chars >= 20000 或最大单块 >= 12000 的 Responses 工具尾巴生效，普通 512/1024 小尾巴不动",
  "v0.1.28: 保持零额外请求、不主动热补、不恢复普通 main session-delta，目标是在不明显拉长首字的情况下压新尾巴",
  "v0.1.27: 修复同前缀 warm 后 cache_read=0 大冷读被误算成可避免缺口的问题",
  "v0.1.27: 同一会话/同一 prefix 的冷读只标记冷启动和不稳定，不覆盖已有 warm 水线",
  "v0.1.27: 继续保持零额外请求、不主动热补、不恢复普通 main session-delta，重点防止累计命中被错误水线拖垮",
  "v0.1.26: 隔离大工具输出尾巴，避免 8 万字符以上的 mixed/tool_output 动态内容被误学成可避免缺口",
  "v0.1.26: 修正大工具输出污染水线的问题，不新增同步请求、不主动热补、不恢复普通 main session-delta",
  "v0.1.26: 保留 v0.1.25 的流式总耗时 fast path；本版重点是让可避免缺口统计更可信，防止错误等待策略继续放大",
  "v0.1.25: 流式总耗时优化，SSE usage / response_id 改为边转发边增量提取，结束时不再全量扫描整段 SSE",
  "v0.1.25: 保留完整输出和本地缓存回放能力，不裁剪工具输出、不改变上游请求、不增加额外请求",
  "v0.1.25: 长输出的总耗时仍主要由上游持续生成决定，本版降低的是代理自身收尾解析开销",
  "v0.1.24: 大请求体 gzip 如果被上游拒绝并回退，会给同上游同通道写入短冷却，避免后续大请求反复多一次失败调用",
  "v0.1.24: 保持 gzip 为上游级可选能力，不默认强开；支持的上游可降低传输体积，不支持的上游会自动绕开一段时间",
  "v0.1.24: 明确不恢复普通主路 previous_response_id + delta，避免影响 Responses 技能和会话语义",
  "v0.1.24: 当前日志显示可避免缺口为 0，低命中主要来自巨大工具输出新尾巴，不靠加等待处理",
  "v0.1.23: 修复小尾巴学习水线，512/1024/1536/2048 不再直接推进完整 sent bucket，减少重复可避免缺口",
  "v0.1.23: 保留 v0.1.13 的 3072/4096 高命中中等尾巴学习，不把有效正优化一起撤掉",
  "v0.1.23: 小 512 级缺口会按 provider 尾部粒度归类，避免 UI 和统计里把正常尾粒度误报成可避免缺口",
  "v0.1.23: 继续保持本地 prefix 守护最多 +5 秒，不主动热补、不新增同步请求、不恢复普通 main session-delta",
  "v0.1.22: Responses 本地 prefix 守护统一收口到最多 +5 秒，避免再出现旧版 8/10 秒本地等待",
  "v0.1.22: 加强 cold_unstable 最近热前缀保护和 tool_tail_burst 水线保留，减少冷读污染稳定水线",
  "v0.1.22: 上游 429/502/503 错误不再给 prefix 写入普通冷却，失败请求不污染后续缓存判断",
  "v0.1.22: 上游配置新增大请求体 gzip 可选开关，默认关闭；底层记录 sent_body_bytes、gzip_attempted、gzip_fallback_used",
  "v0.1.22: 继续不主动热补、不新增同步请求、不恢复普通 main session-delta、不改工具输出内容",
  "v0.1.21: 增加上游响应头等待分类，区分 retry、大请求体和普通上游慢",
  "v0.1.21: 增加 request_body_bytes 分桶统计，600KB 以上标记 high，1MB 以上标记 critical",
  "v0.1.21: 增加 cold_unstable 底层诊断和 10 秒内窄保护，针对同一前缀刚热又冷的情况",
  "v0.1.21: 不新增同步请求、不主动热补、不恢复普通 main session-delta、不靠几十秒等待堆命中率",
  "v0.1.20: 新增底层 TTFT 拆分诊断，区分本地准备、prefix 等待、上游响应头、SSE 首块和 retry 等待",
  "v0.1.20: 每条主请求记录 request_body_bytes / upstream_attempts / upstream_retry_wait_ms，方便排查后台首字和代理首字不一致",
  "v0.1.20: 只加日志诊断，不改缓存策略、不新增请求、不恢复热补、不动 UI 请求记录",
  "v0.1.19: 保留 v0.1.13/v0.1.18 成本线，新增 Responses 会话锚点底层诊断，排查多会话冷启动串线",
  "v0.1.19: 会话兄弟水线只作为等待参考，不参与可避免缺口记账，避免隔离过头造成重复冷启动",
  "v0.1.19: 大工具输出冷读标记为 tool_tail_burst，不再误写 family 水线，也不把真实工具尾巴伪装成可避免缺口",
  "v0.1.19: 继续不主动热补、不新增同步请求、不恢复普通 main session-delta、不改工具输出内容",
  "v0.1.18: 在 v0.1.17 基础上清理主路径里的前台补热壳，避免名字误导后续判断",
  "v0.1.18: 保留 v0.1.13 的 TTFT/新尾巴稳定线和 v0.1.15 早段会话锚点隔离",
  "v0.1.18: 收回 v0.1.16 的 tail-lag 可继承水线逻辑；tail-lag 只保留底层诊断",
  "v0.1.18: Responses 非流式 sync-main 遇到 502/5xx 不再内部三连重试，减少错误时的重复上游调用",
  "v0.1.18: 继续不新增热补、不新增同步请求、不恢复普通 main session-delta，守住零额外请求成本线",
  "v0.1.15: v0.1.13 升级为新的累计命中对照基线，冷启动计入历史累计命中，不再只看近 5 分钟",
  "v0.1.15: Responses 本地水线按会话锚点隔离，避免多开对话、切换对话后共用同一条 prefix 状态",
  "v0.1.15: 上游 prompt_cache_key 继续保持全局稳定以保留 provider 缓存，本地 fingerprint 才区分会话",
  "v0.1.15: 不新增热补、不新增同步请求、不恢复普通 main session-delta，目标是减少同上游多会话冷启动串线",
  "v0.1.15: 自动校准旧高水线，避免版本切换或 provider 回退后把真实新尾巴误标成大可避免缺口",
  "v0.1.15: 8 秒等待后仍弱命中的 3k+ 缺口会降低内部水线，减少反复假可避免和错误等待",
  "v0.1.15: 不新增热补、不新增同步请求、不拉长 TTFT 上限，继续优先守真实 99.5 命中",
  "v0.1.14: Responses 过期小可避免缺口增加 6-8 秒风险短保护，减少 512 后突然扩大成 3k-6k 缺口",
  "v0.1.14: 不新增热补、不新增同步请求、不恢复普通 main session-delta，继续守住零额外请求成本",
  "v0.1.13: 高命中 3072/4096 新尾巴会学习为下一轮水线，减少同类桶缺口反复出现",
  "v0.1.13: 大工具低命中尾巴不会被误学为水线，避免把真实新增内容伪装成可避免缺口",
  "v0.1.13: 修正跨上游别名导致的可避免缺口误判，切换上游后的首条冷启动不再污染当前上游统计",
  "v0.1.13: 不新增热补、不新增同步请求、不恢复普通 main session-delta，继续守住零额外请求成本",
  "v0.1.12: 以 v0.1.0 正常转发体验为基线，把 Responses 512/1024/1536 可避免缺口改成轻量短保护",
  "v0.1.12: 小缺口不再默认吃满 10 秒；大缺口、冷启动和不稳定大工具尾巴仍保留强保护",
  "v0.1.12: 不恢复热补、不新增同步请求、不启用普通 main session-delta，继续守住零额外请求成本",
  "v0.1.11: 正常转发优先，流式响应不再持有同前缀锁到整条 SSE 输出结束",
  "v0.1.11: 降低代理本地排队放大的首字和总耗时，不增加热补、不新增同步请求",
  "v0.1.11: 保留请求准备阶段的前缀串行保护，避免破坏缓存命中和工具语义",
  "v0.1.10: 新增底层 upstream_ttft_ms 诊断，等于 ttft_ms - prefix_guard_wait_ms",
  "v0.1.10: 日志可区分首字慢来自本地短保护等待，还是上游实际首字慢",
  "v0.1.10: 不改变缓存策略、不增加热补、不新增同步请求，保持 v0.1.9 行为基线",
  "v0.1.9: 对照 v89/v90/v0.1.8，修复高上下文连续可避免缺口只等 6 秒仍压不住的问题",
  "v0.1.9: Responses 已证明可避免缺口在 64k+ 或混合工具尾巴场景下，每轮至少给 10 秒短保护",
  "v0.1.9: 仍保持零热补、零额外同步请求、不恢复普通 main session-delta，只修正可避免水位",
  "v0.1.8: 对照 v89/v90/v1.0，修复 v0.1.6 现场 settle_window_elapsed 后可避免缺口继续漏出的回归",
  "v0.1.8: Responses 4k-16k 小上下文的 1024/2048+ 可避免缺口提升为 6-10 秒短保护，仍保持 10 秒成本上限",
  "v0.1.8: 非流式 Responses 同步请求遇到同前缀上游 429/503 后进入短冷却，减少一直同步失败和重复打上游",
  "v0.1.7: 根据 v0.1.6 日志，将 4k-16k 小上下文也纳入 Responses stale recovery 短保护",
  "v0.1.7: 512/1024 可避免缺口保护提升到 6 秒，2048+ 中型缺口提升到 10 秒",
  "v0.1.7: 继续保持零热补、零新增同步请求，只调整底层等待边界",
  "v0.1.6: 修复长时间空闲后巨大混合工具尾巴把旧缓存水位误算成可避免缺口的问题",
  "v0.1.6: 对长期空闲 + 巨大工具尾巴增加 10 秒探测保护，不新增上游请求",
  "v0.1.6: 大历史重排/恢复类尾巴不再污染可避免缺口统计，后续按新水位重新学习",
  "v0.1.5: 针对 v0.1.4 日志里的巨型工具输出后 11k 可避免缺口，提升大工具尾巴短保护到 8-10 秒",
  "v0.1.5: 仍然不恢复热补、不新增同步请求，只在已有可避免缺口且当前工具尾巴很大时保护",
  "v0.1.5: 目标是守住 v0.1.0/v0.1.4 已恢复的冷启动修复，同时压低大工具输出后的可避免缺口",
  "v0.1.4: 修复长上下文同前缀 settle window 过期后直接裸发导致的可避免冷启动",
  "v0.1.4: 只增加 Responses 底层冗余短保护，不恢复热补，不新增同步上游请求",
  "v0.1.4: 保留 v0.1.0 首字基线和压缩/非流式 Responses 兼容修复",
  "v0.1.3: 回到 v0.1.0 缓存基线，只保留压缩/非流式 Responses 跳过 prefix guard 的修复",
  "v0.1.3: 撤掉 v0.1.1 的 next-round 可避免缺口短追，避免大新尾巴场景下负优化",
  "v0.1.3: 目标是复现 v0.1.0 稳定命中表现，同时修好压缩不额外等待",
  "v0.1.2: 修复普通 /v1/responses 非流式压缩请求被当作 main 对话进入 prefix guard，压缩不再额外等待 4.5-10 秒",
  "v0.1.2: 非流式 Responses 主请求日志标记为 responses-sync-main，便于和流式 main、compact endpoint 区分",
  "v0.1.2: 不影响正常流式 Responses 缓存守护；v0.1.1 的 10s 内下一轮可避免缺口保护继续保留",
  "v0.1.1: Responses 可避免缺口新增 10s 内自适应短保护，连续 512/1024/1536/2048 等缺口下一轮优先短等追桶",
  "v0.1.1: 不新增后台热补、不新增同步请求、不恢复普通 main session-delta，继续守住零额外请求和首字最多约 +10s",
  "v0.1.1: 底层日志固定写入 prefix_guard_wait_ms，方便区分本地等待和上游自身慢",
  "v0.1.0: ? 11:22 ? v0.0.99 ?????????? Responses ??????? 10 ??????",
  "v0.1.0: ???? 75 ?????????????????????????",
  "v0.1.0: ????????????????? main session-delta?????????/??/??/??/????",
  "v0.0.98: 小尾巴水位修复，完整主请求出现 512-2048 token 小缺口后，将已发送 bucket 纳入下一轮可避免保护，减少 512/1024/1536 反复新尾巴",
  "v0.0.98: 新增底层 prefix_lag 诊断字段，记录追桶分类、input delta、cache delta、previous gap，便于下一轮直接判断是真实新增还是追桶滞后",
  "v0.0.98: 继续遵守零额外请求、不主动热补、不新增同步、不恢复普通 main session-delta；大工具真实新增尾巴不伪装成满桶",
  "v0.0.97: 撤回 v0.0.96 动态尾巴参与 provider prefix fingerprint 的负优化，恢复 v0.0.89/v0.0.90 风格稳定水位控制 key",
  "v0.0.97: 修复 v0.0.96 每条请求 fingerprint 都变化导致连续 no_prefix_state、前缀保护接不上、水位无法复用的问题",
  "v0.0.97: 后续目标是在 v0.0.89/v0.0.90 优秀底线上找剩余缺口并提高命中率，不再用动态尾巴拆分控制水位",
  "v0.0.96: 修复 provider prefix fingerprint 只看前 64k 导致同头不同尾上下文共用水位的问题，改为 len + head64k + tail16k",
  "v0.0.96: 针对 v0.0.95 3M 样本里 7caa 指纹 1.536 万真可避免缺口，避免 5 万/20 万上下文串线误判",
  "v0.0.96: 不新增热补、不新增同步、不改请求语义；目标是减少真可避免缺口并继续对照 v0.0.89/v0.0.90",
  "v0.0.95: 真正可避免缺口不管大小都继续强保护；针对 v0.0.94 暴露的 2048/2560/3072/8192 伪可避免工具输出白等加成本上限",
  "v0.0.95: 保留可避免统计和 v0.0.89 可避免优先；只有 cache_instability_score>=2 且当前是 4k+ 工具输出时才限制已证明 weak 的白等",
  "v0.0.95: 目标是减少 weak_long_wait 和 TTFT 浪费，不新增热补、不新增同步、不改工具输出语义",
  "v0.0.94: 针对 v0.0.93 现场 16.1 万可避免冷读，新增底层 prefix_guard_skip_reason 诊断，区分 no_prefix_state / wait_zero / settle_window_elapsed",
  "v0.0.94: 新增大可避免冷读回归测试，确认 17 万级已知高水位冷读会走 responses_avoidable_gap 强保护",
  "v0.0.94: 不新增热补、不新增同步、不扩大盲目等待；继续按 v0.0.89 / v0.0.90 / 当前版本三组对照判断有效性",
  "v0.0.93: 撤回 v0.0.92 紧凑工具尾巴恢复保护；实测该 guard 未触发，命中率和 TTFT 未优于 v0.0.89/v0.0.90 对照",
  "v0.0.93: 底层回到 v0.0.89/v0.0.91 缓存线，继续保留可避免优先、零额外请求、不热补、不新增同步请求",
  "v0.0.93: 后续优化必须按 v0.0.89 / v0.0.90 / 当前版本三组日志对照，先证明有效再进入底线",
  "v0.0.92: 负优化记录：针对 3072/3584/4608 的 compact guard 在现场样本未触发，不作为后续底线",
  "v0.0.91: 撤回 v0.0.90 连续小新尾巴 60/75 秒升档，回到 v0.0.89 等待强度，避免 TTFT p95 被拖高",
  "v0.0.91: 保留 v0.0.89 可避免优先修复；v0.0.90 结论记为局部有效但整体负优化，不作为底线",
  "v0.0.91: 下一步不再靠盲目加秒数压 512/1024，而是区分真实工具输出新尾巴和可追桶滞后",
  "v0.0.90: 针对 v0.0.89 日志里的连续 512/1536 新尾巴，新增 Responses 小新尾巴连续追桶升档",
  "v0.0.90: 512/1024/1536/2048 连续出现时等待从 45 秒逐级升到 60/75 秒，单次偶发不放大",
  "v0.0.90: 继续零额外请求、不主动热补、不新增同步、不改请求语义，目标是压新尾巴同时守住 v0.0.89 命中率",
  "v0.0.89: 在 v0.0.88 收回线基础上继续优化；有 exact 可避免缺口证据时，不再被当前 8k+ 工具尾巴 cap 截短等待",
  "v0.0.89: 工具尾巴 cap 只用于纯新尾巴/无可避免证据场景，保留 v0.85 可避免保护、v0.67 工具追桶、v0.75 压缩兼容",
  "v0.0.89: 继续遵守零额外请求、不主动热补、不恢复 normal main session-delta；目标是先压可避免，再压真实新尾巴",
  "v0.0.88: 缓存底层回到 v0.85 可避免保护主线，保留 exact/fingerprint 可避免缺口记账和强保护",
  "v0.0.88: 回灌 v0.67 大工具输出追桶正优化；当前请求 8k+ 工具尾巴会限制等待时长，避免 TTFT 被拖爆",
  "v0.0.88: 保留 v0.75 Responses 非 SSE 压缩兼容修复；不新增热补、不新增同步请求、不恢复 normal main session-delta",
  "v0.0.86: Responses 可避免缺口前置过滤；当前请求带 1024+ 工具输出时不再误进可避免长等",
  "v0.0.86: 缺口统计同步修正，明显工具尾巴优先归为新尾巴，避免 UI 和后续策略被伪可避免带偏",
  "v0.0.86: 继续零额外请求、不主动热补、不改工具内容，只减少无效 60-120 秒等待",
  "v0.0.85: 撤销 v0.0.84 missing-state 相关前缀长等待负优化；没有 exact 前缀证据时不再空等 75 秒",
  "v0.0.85: Responses 当前请求带大工具输出时只做短追桶上限，避免新尾巴误触发 75/90 秒长等待",
  "v0.0.85: 保留零额外请求底线，不恢复主动热补、不新增同步请求、不恢复 normal/main session-delta",
  "v0.0.84: 针对 v0.0.83 真实日志中 8.6 万/8.9 万可避免冷读，新增 Responses 冷读不稳定隔离，不额外发请求",
  "v0.0.84: prefix 运行态重启恢复收紧到 8 分钟，避免陈旧水位把正常冷启动误判成可避免命中",
  "v0.0.84: 暖回 99% 后不立刻解除保护，冷读/大缺口需要连续小缺口或满桶逐步降级",
  "v0.0.83: 撤回 v0.0.82 的纯新尾巴短等待负优化，恢复 v0.0.81 风格的 Responses 强保护，优先守住可避免缺口为 0",
  "v0.0.83: 针对 30k 上下文、小工具输出 128 字符、512 新尾巴演变成 2 万级可避免缺口的真实日志补回归保护",
  "v0.0.83: 保留 prefix_guard_wait_effect 底层诊断，继续零额外请求、不主动热补、不恢复 normal/main session-delta",
  "v0.0.82: 负优化记录：纯新尾巴小桶等待降档后，真实日志出现 23552 token 可避免缺口，已在 v0.0.83 撤回",
  "v0.0.81: Responses 可避免缺口改为不限大小动态保护，512 到 9 万以上都会进入同前缀等待闸门",
  "v0.0.81: 针对 512 可整除的新尾巴桶统一做前置保护，覆盖 512/1024/1536/2048/7168/9728 等反复掉桶形态",
  "v0.0.81: 当前请求带入工具输出或短消息尾巴时提前保护，继续零额外上游请求、不主动热补、不恢复 normal/main session-delta",
  "v0.0.80: 加强 Responses exact 同前缀可避免缺口保护，冷读大可避免提升到 60/90 秒保护",
  "v0.0.80: 针对 1024/1536 小可避免缺口提升保护强度，目标减少反复掉桶",
  "v0.0.80: 继续零额外上游请求、不主动热补、不恢复 normal/main session-delta",
  "v0.0.79: 撤销 v0.0.78 prompt_cache_key family waterline，避免不同动态工具输出被串成同一水位导致命中率判断偏低",
  "v0.0.79: 回到 v0.0.77 exact/fingerprint+alias 底线，只保留 prefix_guard_wait_source 后端诊断",
  "v0.0.79: 继续守住零额外上游请求、不主动热补、不恢复 normal/main session-delta",
  "v0.0.78: 新增 Responses/Chat 稳定 prompt_cache_key 家族水位兜底，动态工具输出换 fingerprint 时仍可参考同一稳定前缀历史",
  "v0.0.78: 后端 /admin/metrics 新增 prefix_guard_wait_source，用于区分 exact/family/missing-state 等待来源，UI 请求记录不显示",
  "v0.0.78: 继续守住零额外上游请求、不主动热补、不恢复 normal/main session-delta 的成本底线",
  "v0.0.77: 修复 Responses 冷读但已有可避免缺口时等待逻辑过早返回的问题，让 responses_avoidable_gap 强保护生效",
  "v0.0.77: 继续守住零额外上游请求、不主动热补、不恢复 normal/main session-delta 的成本底线",
  "v0.0.77: 清理 conservative main session-delta 残留测试，只保留 413 Payload Too Large 自救 delta 路径",
  "v0.0.76: 回到 v0.0.69 的 Responses 成本优先缓存线，保留 v0.0.75 压缩兼容修复，不恢复热补、不新增同步请求",
  "v0.0.76: 新增同模型/通道/稳定前缀 fingerprint 的底层水位别名，减少路由或上游标识细微变化后掉回 350ms 缺状态保护",
  "v0.0.76: 针对 1024/1536/2048 这类重复新尾巴，优先让已有同前缀串行等待策略重新生效，UI 请求记录不增加底层诊断噪声",
  "v0.0.75: 修复官方 Responses JSON 里 error:null 被误判为错误响应，导致 ZCode 压缩字段补齐被跳过的问题",
  "v0.0.75: Responses 输出对象缺 id 时统一补齐，content 项缺 annotations 时统一补 []，覆盖 ZCode 严格校验路径",
  "v0.0.75: 仍不新增上游请求、不恢复热补、不改 Chat/Anthropic，也不改当前缓存命中策略",
  "v0.0.74: Responses 非 SSE 模式按客户端 stream=false/未声明为准，不再因为上游 content-type=text/event-stream 就透传流式",
  "v0.0.74: 如果上游把 JSON 误标为 SSE，会强制返回 application/json 并补齐 ZCode 要求的 output.message.id 和 annotations=[]",
  "v0.0.74: 如果上游真的返回 SSE，但客户端要非流式，会聚合 response.completed/delta 为标准 Responses JSON，不新增任何上游请求",
  "v0.0.73: 修复 Responses 压缩同步 JSON 被上游误标 content-type 后绕过字段补齐的问题",
  "v0.0.73: Responses 返回不再依赖 content-type 判断，只要 body 是 response JSON 就补齐 output.message.id 和 annotations=[]",
  "v0.0.73: 真正 SSE 解析不了 JSON 会原样返回，Chat/Anthropic 通道不受影响，不新增上游请求",
  "v0.0.72: 修复旧本地 exact cache replay 绕过 Responses JSON 补齐的问题，旧缓存命中也能兼容 ZCode 压缩校验",
  "v0.0.72: 不清空缓存、不重打上游，只在缓存返回客户端前补齐 output.message.id 和 output_text.annotations=[]",
  "v0.0.72: 保持 v0.0.52 成本底线：不恢复热补、不新增伴随同步请求、不改变主流式缓存策略",
  "v0.0.71: 修复 ZCode 压缩时 Responses 非流式 JSON 形状校验失败，自动补齐 output.message.id",
  "v0.0.71: Responses 同步 JSON 自动补齐 output_text.annotations=[]，兼容严格校验客户端，不改变模型文本和 usage",
  "v0.0.71: 只调整返回给客户端的 JSON 外壳，不改流式主请求、不恢复热补、不新增同步请求、不改变缓存命中策略",
  "v0.0.70: 新增 Responses compact 兼容入口 /v1/responses/{id}/compact 和 /v1/responses/compact，支持 agent 发起非 SSE 压缩请求",
  "v0.0.70: 上游不支持官方 compact endpoint 时，会自动 fallback 到普通 /v1/responses 的 stream=false 同步 JSON 模式",
  "v0.0.70: compact 记录单独标记为 compact/compact-fallback，不进入本地响应缓存 miss，也不改变主流式请求缓存策略",
  "v0.0.69: Responses 当前请求如果本身追加了 1024+ 工具输出尾巴，会对无收益的长等待做保守封顶，避免为了真实新内容白等几十秒",
  "v0.0.69: 可避免缺口仍然保留强保护，不因为工具输出封顶而放松；目标是守住 v0.0.52/v0.0.68 成本线和命中底线",
  "v0.0.69: 不恢复主动热补、不新增同步请求、不改工具输出内容；新增逻辑只影响底层同前缀等待时长",
  "v0.0.68: Responses 噪声工具输出追桶保护从 8k+ 下探到 1024 级别，针对 512/1024/1536 这类小桶尾巴更早给上游缓存追平时间",
  "v0.0.68: 新增底层等待诊断 prefix_guard_wait_ms 和 prefix_guard_wait_reason，只写入 /admin/metrics，不显示在 UI 请求记录",
  "v0.0.68: 继续保留成本优先底线：不恢复主动热补、不新增同步请求、不改工具输出内容",
  "v0.0.67: 增加工具输出噪声底层诊断，记录行数、重复行、时间戳、路径、URL、hash 和 JSON-like 长度，只写入 /admin/metrics",
  "v0.0.67: 当 Responses 尾巴来自大工具输出时，不额外发同步请求，只通过下一轮同前缀串行闸门多等 6/12/18 秒，给上游前缀缓存追平时间",
  "v0.0.67: 保留 v0.0.52 成本优先底线和 v0.0.66 行为：无主动热补、无每条流式伴随同步、UI 请求记录不显示底层诊断",
  "v0.0.66: 新增 Responses 尾巴来源底层诊断，只写入 /admin/metrics，不在 UI 请求记录显示",
  "v0.0.66: 记录 tail_input_items、tail_message_chars、tail_tool_call_chars、tail_tool_output_chars、tail_largest_tool_output_chars 和 tail_source，方便判断新尾巴是否来自工具输出",
  "v0.0.66: 不改变请求体、不新增上游请求、不恢复热补、不改变缓存策略，继续保留 v0.0.65 的成本优先基线",
  "v0.0.65: Responses 发送体把 include/stream/store/service_tier/truncation 前移到 input 前，扩大稳定前缀",
  "v0.0.65: 不改语义、不删工具输出、不恢复热补，继续保留 0.0.64 的普通 session-delta 禁用策略",
  "v0.0.64: 普通 Responses 主请求不再主动使用 session-delta，避免先 400 再 full fallback 的负收益",
  "v0.0.64: 仅保留 413 Payload Too Large 后的 session-delta 自救，不新增同步请求、不恢复热补",
  "v0.0.64: 对 Responses 512/1024/2048 小尾巴增加等待保护，让短上下文倍数尾巴更接近满桶",
  "v0.0.64: 目标是降低可避免缺口和短上下文倍数尾巴，让实际 token 命中率继续往 99% 以上靠",
  "v0.0.63: Responses session delta 首次失败冷却从 10 分钟延长到 2 小时，避免短时间后重新试错造成大面积冷读",
  "v0.0.63: 冷却过期后保留失败次数，下一次失败直接升级到 24 小时，防止同一上游/模型反复重置试错",
  "v0.0.63: 不恢复热补、不新增同步请求，只修复 v0.0.62 暴露的 10 分钟重试负优化",
  "v0.0.62: 将 Responses session delta 冷却升级为自适应：首次 10 分钟，第二次 2 小时，第三次本轮长冷却",
  "v0.0.62: 目标是继续压低不兼容上游的 delta 试错次数，保护 v0.0.61 已恢复的实际 token 命中率",
  "v0.0.62: 不改热补、不改桶策略、不新增同步请求，只延长已证实负优化的 session-delta 隔离",
  "v0.0.61: 根据 v0.0.60 日志，隔离不稳定支持 previous_response_id 的上游，避免每条请求先 delta 失败再 full 回退",
  "v0.0.61: 增加 provider/model 级 Responses session delta 冷却，冷却期间直接走完整前缀缓存路径，目标是提高实际命中率并降低 retry 成本",
  "v0.0.61: 新增 response_session_cooldown_active 和 response_session_rejected_status 诊断，方便验证冷却是否生效",
  "v0.0.60: 增加 Responses session 复用失败诊断：候选数、跳过原因、scope 命中数、append-only 是否匹配和 delta item 数",
  "v0.0.60: 不恢复主动热补，不新增同步补热请求；继续守住 v0.0.52 / v0.0.58 的成本优先底线",
  "v0.0.60: 目标是定位 v0.0.59 实测 delta=0 的根因，下一版根据真实 skip_reason 精准修复",
  "v0.0.59: 修复 v0.0.58 的负优化：Responses 流式主请求在严格同 scope、append-only 时允许 previous_response_id + delta",
  "v0.0.59: 不恢复主动热补，不新增同步补热请求；仍保持 cc-switch 风格的一条真实流式请求优先",
  "v0.0.59: 目标是降低 send_body_bytes、完整历史发送、413 风险和新尾巴/可避免缺口，不靠额外请求刷命中率",
  "v0.0.58: 按用户明确规则移除主动热补；代理不再为了缓存命中额外发前台/后台/桶补热同步请求",
  "v0.0.58: 历史配置里即使打开过后台预热，启动/保存时也会强制关闭，避免一条主请求变成两条上游消耗",
  "v0.0.58: 保留不额外发请求的优化：prompt_cache_key、cache_control、请求体规范化、Responses session/delta 续接和 413 自救",
  "v0.0.57: 对齐 v0.0.52 成本优先，缺少 prefix state 时不再主请求前发大包前台同步初始化",
  "v0.0.57: 修复 v0.0.56 日志暴露的问题：10 万级上下文前台补热 cache_read=0 时会放大新尾巴和真实成本",
  "v0.0.57: 保留请求记录透明化；继续显示流式/同步/补热同步/本地，但不新增补热触发条件",
  "v0.0.56: 只改请求记录透明度，底层继续以 v0.0.52 成本优先为基调，不恢复每条流式伴随同步",
  "v0.0.56: 请求记录现在会显示主流式、主同步、本地缓存回放、前台/后台补热同步，方便和中转后台逐条对数",
  "v0.0.56: 新增流式/同步/补热同步/本地标签；补热记录只作为上游观察入列，不计入本地 cache miss",
  "v0.0.55: 以 v0.0.52 成本优先为基调，禁止 Responses / Chat / Anthropic 因纯新尾巴或满桶保活额外同步非流式",
  "v0.0.55: 只保留冷启动初始化和明确可避免回退恢复；正常流式调用不会按调用次数旁边再同步一次",
  "v0.0.55: 保留 v0.0.54 的零额外请求正优化：Chat / Anthropic 实际发送体稳定化",
  "v0.0.54: 回灌历史正优化，但不恢复高成本补热：Chat / Anthropic 的实际发送体现在会做语义安全规范化",
  "v0.0.54: Chat 稳定 tools、tool_choice、response_format 和工具参数 JSON 顺序；Anthropic 稳定 input_schema、tool_use input 与对象 key 顺序",
  "v0.0.54: 不增加额外上游请求，不恢复伴随非流式补热，不改变 Responses 的成本优先底线",
  "v0.0.53: 回灌 cc-switch 的低成本正优化：客户端提供合法稳定 prompt_cache_key 时保留，不再一律覆盖",
  "v0.0.53: 超长、空值或风险 prompt_cache_key 仍会改写为 Atoapi 稳定 key，避免再次触发 string_above_max_length",
  "v0.0.52: 参考 cc-switch 的真实请求优先路线，移除普通新尾巴/小缺口的伴随非流式补热，不再每条流式旁边同步一次",
  "v0.0.52: 后台补热只保留冷启动初始化和明显缓存回退恢复；正常连续对话依靠真实流式请求自然续热",
  "v0.0.52: 缓存统计增加主请求、补热请求、补热 token 展示，解释为什么中转后台请求数会比软件请求记录更多",
  "v0.0.51: 默认端口同步为 18883，并加入 Responses 小桶估算兜底；估算漏掉小尾巴但同前缀已有小缺口证据时，会先轻补 512",
  "v0.0.50: Responses 前台预测保护下探到 512 小桶，1024/1536/2048 这类 512 整数倍新尾巴会在主请求前按刚好桶数补热",
  "v0.0.49：提升 512 小桶优先级，512 整数倍缺口优先按小桶精细补热",
  "可避免缺口优先于新尾巴；1536 按 3 个 512，9728 按 19 个 512，不再把这类缺口当成正常波动",
  "新增 Responses 前台预测保护：主请求前发现 4096-16384 的高命中新尾巴，会先尝试小桶补热",
  "v0.0.48：针对 v0.0.47 实测出现的 Responses 可避免缺口，把满桶/近满桶保活下探到 15k+",
  "15k、52k、100k 这类真实项目上下文都能更早做轻量保活，目标是减少第一条可避免缺口冒出来的概率",
  "继续坚持 512 小桶精细补热原则，不做大桶硬塞，不改 Chat / Anthropic，不降低低命中保护门槛",
  "v0.0.47：针对 v0.0.46 没有 502/cooldown 但仍稳定 1536 纯新尾巴的问题，新增 Responses 512-2048 高命中新尾巴二段后台补热",
  "当同前缀高命中、纯新尾巴在 512-2048 且输入量 32k+ 时，后台补热成功后会等待约 1.4 秒再轻量补一次，目标是把固定 1536 尾巴提前垫进 provider 前缀缓存",
  "本版不继续加主请求等待，不改 Chat / Anthropic，不改大可避免缺口，不启用普通流式 previous_response_id 复用",
  "v0.0.46：针对 v0.0.45 日志里补热请求偶发 502 后触发 prefix_error_cooldown，导致 512-2048 小尾巴保护被跳过的问题做隔离",
  "前台/后台补热遇到 500/502/503 不再给当前前缀写入 45 秒冷却；429 仍保留冷却，主请求错误仍按原规则处理",
  "目标是让上游偶发 502 不再关闭小尾巴补热保护；不改 512-2048 沉淀等待、不改 Chat / Anthropic、不改 session 复用",
  "v0.0.45：针对 v0.0.44 实测里同一 Responses 前缀稳定出现 512-2048 纯新尾巴，增加前台补热后的自适应沉淀等待",
  "补热后不再固定等待 500ms：512 级约 950ms，1024 级约 1200ms，1536/2048 级约 1400ms，4096+ 级约 1800ms；只在高命中补热成功后生效",
  "本版不改冷读回退、不碰 Chat / Anthropic、不启用普通流式 previous_response_id 复用，目标是压小稳定 512-2048 小新尾巴",
  "v0.0.44：对比基线改为 v0.0.42，修复同一 Responses 前缀在冷读回退后把历史高水位当成 cache_read_zero 跳过补热的问题",
  "当同前缀已有历史水位但本轮上游只返回极低缓存时，会按历史水位识别为可避免缺口并先补热，目标是减少 7 万级/8 万级可避免缺口",
  "64k+ 已满桶且空闲较久的 Responses 前缀加入轻量保活，用来压下一轮 512/1024 新尾巴；不启用普通流式 previous_response_id 复用",
  "v0.0.43：针对 v0.0.42 日志里高水位满桶后回退出的 1536 可避免缺口，新增 Responses 高水位满桶保活预热",
  "当 90k+ 上下文已经满桶且间隔一段时间后，真实请求前会轻量刷新一次前缀，目标是减少 provider 缓存回退造成的可避免缺口",
  "保留 v0.0.42 的小上下文 512 尾巴优化和 v0.0.41 的 512+ 新尾巴后台抓取",
  "v0.0.42：针对 v0.0.41 日志里的 512 新尾巴，把小上下文 Responses 前缀初始化和补热门槛从 8k 下探到 4k",
  "Responses 512 桶小尾巴增加更明确的缓存生效等待，目标是减少下一轮继续出现 512，而不是只在 UI 上改显示",
  "保留 v0.0.41 的 512+ 新尾巴统一后台抓取；不改变普通流式 previous_response_id 策略",
  "v0.0.41：根据 v0.0.40 实测日志，Responses 对 512 以上新尾巴统一后台抓取，减少 1024/3072/3584 这类尾巴漏补",
  "前台小缺口保护从 2048 扩展到 4096：上一轮出现 512+ 小尾巴且真实命中仍高时，下一轮更积极预热",
  "保留 v0.0.40 的 missing_prefix_state 初始化、冷启动恢复和上游错误冷却；不改 UI 布局，不重启普通 previous_response_id 复用",
  "v0.0.40：基线改为 v0.0.39，对 Responses 增加 missing_prefix_state 初始化预热、冷启动后恢复补热、大新尾巴补热和上游错误冷却",
  "缺少 prefix state 时不再只记录跳过：体量足够且不触发 payload guard 时，会先用 max_output_tokens=1 做一次轻量初始化",
  "冷启动 cache_read=0 后允许下一轮后台恢复补热；8k 以上高命中新尾巴也纳入只补不拖保护，目标是降低 512 和大新尾巴反复出现",
  "上游 429/500/502/503 会给当前前缀短冷却，避免前台/后台预热在上游不稳时继续放大错误",
  "v0.0.39：把 Responses 可避免缺口补热范围扩大到 0-90000，重点压掉可避免缺口而不是只看总缺口",
  "高水位下的小新尾巴 512-4096 也纳入后台补热，目标是降低反复出现的 512/1536/2560/3584 尾巴",
  "保留低命中保护：冷启动、低缓存、超大真实新尾巴不激进补热",
  "本版回归中撤回了过紧的 95.5% 小尾巴阈值，避免误关掉 6656 这类旧版已验证有效的补热路径",
  "v0.0.38：针对 v0.37 日志里高命中但仍出现 512/2048 可避免缺口的问题，放宽高水位小缺口前台补热",
  "90k+ cached tokens 且真实命中率约 99% 时，512/1024/2048 可避免缺口不再等多轮 streak，下一轮更快补热",
  "低命中、真实新尾巴、冷启动仍保持保守，不降低 ratio_low 保护，避免为了追满桶乱补",
  "写入固定发布流程：每次改版都必须列优化清单，并按 v0.35 对比有效优化、无效或未证明优化、负优化",
  "v0.0.37：修复部分第三方上游返回 previous_response_not_found 导致 agent 直接失败的问题",
  "普通流式 Responses 主请求不再主动复用旧 previous_response_id，避免第三方上游不认旧 resp_* 时打断对话",
  "保留 413 Payload Too Large 自救：只有完整请求过大时才尝试 previous_response_id + delta input，失败后会自动退回完整请求",
  "失效 session 清理更准确：会按实际 previous_response_id 一起摘除旧链路，避免 fallback 反复捡到同一个坏 session",
  "本版保留 v0.0.36 的真实发送体诊断、SSE 结束诊断、后台补热统计和小缺口 skip reason，不改变 UI 布局",
  "v0.0.36：继续强化 upstream_send_body/delta-first，后台补热、流式请求记录和 413 自救都优先看真实发送体",
  "新增底层诊断字段：original_body_bytes、send_body_bytes、send_body_is_delta、payload_too_large_rescue_*，用于判断是否真的走了 previous_response_id delta",
  "Responses 小上下文也纳入前台小缺口保护：8k 以上、128-2048 级缺口在高命中且有连续/恢复信号时会前台补热",
  "新增首次 512 后下一轮保护和 foreground_prewarm_skip 原因记录，能看出是 input_below_8k、ratio_low、streak_low 还是缺少 prefix state",
  "Responses session fallback 增强为多候选排序：同 scope 下优先选择匹配更长、更新的会话，减少完整历史发送",
  "新增 SSE 结束事件底层诊断：记录 completed/[DONE]/EOF/stream_error 和 chunk 数，帮助排查 agent 转圈",
  "v0.0.35：Responses 小缺口补热扩展到 128 / 256 / 512 / 1024 / 2048 级可避免缺口，必须连续出现且真实命中率仍处高位才触发",
  "Responses 前台补热优先使用 previous_response_id 续接后的 delta 请求体；没有续接 id 且完整请求体过大时会跳过补热，避免补热自己制造 Payload Too Large",
  "上游 413 现在归类为 upstream_payload_too_large，方便区分真实上下文过大和普通上游错误",
  "v0.0.34：针对真实日志里连续出现的 Responses 高命中可避免缺口，新增高水位前台小预热闸门",
  "当前缀已连续多次出现 2k-16k 可避免缺口且缓存命中率仍在高位时，会先用 max_output_tokens=1 补热一次再发真实请求，目标是减少 2560、3584、9216 这类反复空洞",
  "该闸门只作用于 Responses，且必须开启后台预热开关；Chat / Anthropic 不受影响",
  "v0.0.33：修复 Responses 技能偶发不触发：会话 fallback 现在必须匹配同一套技能指令 / tools 作用域，避免复用到没有技能的旧链路",
  "Responses 缓存身份会规范化 system/developer/instructions 的等价形态，减少同一技能规则被拆成多条前缀线",
  "Responses 大可避免缺口增加高水位后台恢复：只在已有大量缓存命中时补热，不影响 Chat / Anthropic",
  "v0.0.32：根据 v0.0.31 实时日志，Responses 的 2k-16k 纯新尾巴纳入后台预热，减少 2560、4096、6656、15872 这类中等缺口反复出现",
  "中等新尾巴采用高命中门槛：2k-8k 需要已有 95% 命中；8k-16k 需要 90% 命中，或已命中 12.8 万以上稳定前缀才补",
  "中等新尾巴采用只补不拖策略：不额外拉长当前请求等待，只用后台 max_output_tokens=1 预热下一轮",
  "新增回归用例覆盖 v0.0.31 实测的 6656 和 15872 新尾巴，避免后续策略再次漏掉",
  "v0.0.31：Responses 会话续接和前缀水位会保存到本地运行态，重启软件后 30 分钟内可恢复，减少重新冷启动造成的大新尾巴",
  "后台预热新增诊断指标：/admin/metrics 可看到预热触发次数、成功次数、触发的新尾巴和可避免缺口 token",
  "本次只增强底层状态恢复和诊断统计，不改变 Chat / Anthropic 路由策略，也不调整 UI 布局",
  "Responses 请求字段顺序优化：稳定 tools / tool_choice / 参数会放在动态 input 前面，提升前缀缓存可复用面积",
  "previous_response_id / stream / metadata 等动态字段仍放在 input 后面，避免提前打断稳定前缀",
  "Responses 在 v0.0.19 底线上增加保护：大新尾巴和 cached 倒退造成的可避免缺口会保守等待/补热",
  "3k 左右的小纯新尾巴仍保持 v0.0.19 行为，不额外补热，避免策略过度激进",
  "保留 v0.0.20 的 Responses 技能触发修复：system/developer 仍会提升到 instructions",
  "软件 UI 保持当前样式：请求记录通道标签、完整缺口拆分和换行显示仍保留",
  "请求记录显示空间修复：长缺口文本会完整换行显示，不再被右侧固定宽度截断",
  "请求记录每条仍保留通道标签，方便同时查看通道、命中率和完整缺口拆分",
  "请求记录新增通道标签：每条记录会显示 Responses / Chat / Anthropic，通道转换会显示入口到上游的走向",
  "本次只改请求记录 UI 展示，不改变任何缓存命中和路由策略",
  "请求记录缺口展示修复：可避免缺口和新尾巴会同时显示，不再只露出 512 这类局部数字",
  "本次只改 UI 文案展示，不改变 Responses / Chat / Anthropic 的底层缓存策略",
  "本次不改变 Chat / Anthropic 策略",
  "Anthropic 断点升级为 token-aware：短消息不再浪费 cache_control，优先选择最有价值的稳定前缀位置",
  "Anthropic 长历史断点按 20-block lookback 自适应选择，尽量覆盖最新可缓存窗口",
  "后台预热开关现在也支持 Anthropic：仅在已有部分命中且缺口可补时用 max_tokens=1 保守预热",
  "Chat / Responses 缓存策略保持不变",
  "Anthropic 通道增强前缀缓存：最多使用 4 个 cache_control breakpoint，不超过 Claude 限额",
  "Anthropic 长历史会自动补一个更早的稳定消息断点，降低超过 20 个 block 后旧缓存断开的概率",
  "Anthropic 不给最后一条新问题打 cache_control，避免把动态尾巴误当稳定前缀",
  "Chat / Responses 缓存策略保持不变",
  "修复 Responses 通道技能触发：system/developer 规则会提升到 instructions，不再被当成普通 user 内容",
  "原生 Responses input/messages 里的 developer 规则同样会提升到 instructions，避免必须手动发指令才触发技能",
  "本次不改 Responses 缓存冷却、续接、预热和缺口统计策略",
  "后台预热开关现在也覆盖 Chat 通道：大新尾巴会保守补热，Responses 原逻辑不变",
  "Chat 通道对新尾巴短缺后采用更保守的同前缀冷却，减少同前缀突然 0 命中的可避免冷桶",
  "Responses 通道冷却和续接逻辑保持不变",
  "prompt_cache_retention 开关只在 OpenAI Chat / OpenAI Responses 上游配置中显示",
  "Chat 通道新增稳定前缀 key：system/developer/tools 保持同一路由，动态尾巴不再打散上游缓存",
  "Chat 通道启用同前缀串行冷却，降低并发请求造成的冷桶和可避免缺口",
  "Chat 指纹仍保留完整请求差异，统计能看出真实尾巴变化",
  "请求记录主百分比改为真实 token 命中率，满桶只作为桶状态显示",
  "上游配置新增 prompt_cache_retention 开关，默认开启，遇到不兼容错误会提醒关闭",
  "收窄 Responses 续接里的 512 可避免缺口",
  "后台预热按可避免缺口放行，覆盖总缺口较大但可补部分很小的场景",
  "Agent 已启用并绑定状态条会在重新打开后自动显示",
  "Agent 上游卡片明确显示已启用并绑定",
  "已选择但未启用的 Agent 会单独提示，避免和启用状态混淆",
  "软件启动时自动恢复所有已启用 Agent 注入",
  "已启用 Agent 会按上次保存的上游和模型自动写入配置",
  "启动后 UI 默认选中已启用 Agent，右侧直接显示绑定上游",
  "修复新尾巴 512 未站稳时误算为可避免缺口",
  "前缀水位只按真实 cache_read_tokens 推进",
  "保留 v0.0.9 的第三方上游兼容和 Agent 独立上游逻辑",
  "第三方上游可在上游配置里单独控制 prompt_cache_retention",
  "保留 prompt_cache_key 以维持前缀缓存",
  "Agent 开启时未选上游会默认绑定当前上游",
  "每个 Agent 独立显示并保存自己的上游",
  "版本更新气泡点击外部自动关闭",
  "保留 v0.0.8 缺口统计和小缺口预热"
];

const emptyDraft: ProviderDraft = {
  name: "",
  base_url: "",
  models_url: "",
  is_full_url: false,
  custom_user_agent: "",
  api_key: "",
  channel: "anthropic",
  prompt_cache_retention_enabled: true,
  request_body_gzip_enabled: false,
  models: [],
  enabled: true
};

export default function App() {
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [status, setStatus] = useState<ProxyStatus | null>(null);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [activeView, setActiveView] = useState<ViewId>("agent");
  const [selectedAgentId, setSelectedAgentId] = useState("");
  const [selectedProviderId, setSelectedProviderId] = useState("new");
  const [draft, setDraft] = useState<ProviderDraft>(emptyDraft);
  const [providerEditorOpen, setProviderEditorOpen] = useState(false);
  const [modelCandidates, setModelCandidates] = useState<ModelConfig[]>([]);
  const [selectedFetchedModelId, setSelectedFetchedModelId] = useState("");
  const [apiKeyVisible, setApiKeyVisible] = useState(false);
  const [loadingModels, setLoadingModels] = useState(false);
  const [savingProvider, setSavingProvider] = useState(false);
  const [savingGateway, setSavingGateway] = useState(false);
  const [savingCachePolicy, setSavingCachePolicy] = useState(false);
  const [injectingId, setInjectingId] = useState("");
  const [cacheProviderFilter, setCacheProviderFilter] = useState("all");
  const [includeColdStarts, setIncludeColdStarts] = useState(true);
  const [versionOpen, setVersionOpen] = useState(false);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [dismissedRetentionWarningKey, setDismissedRetentionWarningKey] = useState("");

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => {
      void refreshMetrics();
    }, 2500);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!versionOpen) return;
    const closeVersionPopover = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest(".version-wrap")) return;
      setVersionOpen(false);
    };
    window.addEventListener("mousedown", closeVersionPopover);
    return () => window.removeEventListener("mousedown", closeVersionPopover);
  }, [versionOpen]);

  useEffect(() => {
    if (!config) return;
    if (selectedProviderId === "new") {
      setDraft((current) => (current.id ? emptyDraft : current));
      return;
    }
    const provider = config.providers.find((item) => item.id === selectedProviderId);
    if (!provider) return;
    setDraft((current) => (current.id === provider.id ? current : providerToDraft(provider)));
  }, [config, selectedProviderId]);

  useEffect(() => {
    const items = config?.agent_injections ?? [];
    if (!items.length) return;
    if (selectedAgentId && !items.some((item) => item.id === selectedAgentId)) {
      setSelectedAgentId("");
    }
  }, [config, selectedAgentId]);

  const baseUrl = useMemo(() => {
    if (!config) return "http://127.0.0.1:18883";
    return `http://${config.host}:${config.port}`;
  }, [config]);

  const activeProvider = useMemo(
    () => {
      const selectedAgent = config?.agent_injections.find((item) => item.id === selectedAgentId);
      const agentProvider = selectedAgent?.provider_id
        ? config?.providers.find((item) => item.id === selectedAgent.provider_id)
        : null;
      return agentProvider ?? config?.providers.find((item) => item.id === config.active_provider_id) ?? null;
    },
    [config, selectedAgentId]
  );

  async function refreshAll() {
    setError("");
    const [nextConfig, nextStatus, nextMetrics] = await Promise.all([
      command<AppConfig>("reload_config"),
      command<ProxyStatus>("get_proxy_status"),
      command<MetricsSnapshot>("get_metrics")
    ]);
    setConfig(nextConfig);
    setStatus(nextStatus);
    setMetrics(nextMetrics);
    if (selectedProviderId === "new" && nextConfig.providers.length > 0 && !draftHasInput(draft)) {
      setSelectedProviderId(nextConfig.active_provider_id ?? nextConfig.providers[0].id);
    }
    if (!selectedAgentId) {
      const preferredAgent =
        nextConfig.agent_injections.find((item) => item.enabled) ??
        nextConfig.agent_injections[0];
      if (preferredAgent) {
        setSelectedAgentId(preferredAgent.id);
        setActiveView("agent");
      }
    }
  }

  async function refreshMetrics() {
    try {
      setMetrics(await command<MetricsSnapshot>("get_metrics"));
      setStatus(await command<ProxyStatus>("get_proxy_status"));
    } catch {
      // Keep the last known state in the UI.
    }
  }

  function resetModelPicker() {
    setModelCandidates([]);
    setSelectedFetchedModelId("");
  }

  function createProvider() {
    setSelectedProviderId("new");
    setDraft(emptyDraft);
    setApiKeyVisible(false);
    resetModelPicker();
    setProviderEditorOpen(true);
    setNotice("");
    setError("");
  }

  function editProvider(provider: ProviderConfig) {
    setSelectedProviderId(provider.id);
    setDraft(providerToDraft(provider));
    setApiKeyVisible(false);
    resetModelPicker();
    setProviderEditorOpen(true);
    setNotice("");
    setError("");
  }

  async function selectProvider(provider: ProviderConfig) {
    setSelectedProviderId(provider.id);
    setDraft(providerToDraft(provider));
    setApiKeyVisible(false);
    resetModelPicker();
    setError("");
    try {
      const nextConfig = await command<AppConfig>("select_provider", {
        providerId: provider.id,
        provider_id: provider.id
      });
      setConfig(nextConfig);
      setNotice(`已选择 ${provider.name}`);
    } catch (err) {
      setError(String(err));
    }
  }

  async function fetchModels() {
    if (!draft.base_url.trim()) {
      setError("请先填写 Base URL");
      return;
    }
    setLoadingModels(true);
    setError("");
    setNotice("");
    try {
      const input: FetchModelsInput = {
        provider_id: draft.id,
        name: draft.name,
        base_url: draft.base_url.trim(),
        models_url: draft.models_url.trim() || undefined,
        is_full_url: draft.is_full_url,
        custom_user_agent: draft.custom_user_agent.trim() || undefined,
        channel: draft.channel,
        api_key: draft.api_key || undefined
      };
      const models = await command<ModelConfig[]>("fetch_provider_models", { input });
      setModelCandidates(models);
      setSelectedFetchedModelId(models[0]?.id ?? "");
      setNotice(models.length ? `已获取 ${models.length} 个模型，请从下拉栏选择加入` : "没有获取到模型");
    } catch (err) {
      setError(String(err));
    } finally {
      setLoadingModels(false);
    }
  }

  function addFetchedModel() {
    const selected = modelCandidates.find((item) => item.id === selectedFetchedModelId);
    if (!selected) return;
    setDraft((current) => {
      const exists = current.models.some((item) => item.id === selected.id);
      return {
        ...current,
        models: exists
          ? current.models.map((item) => (item.id === selected.id ? selected : item))
          : [...current.models, selected]
      };
    });
    setNotice(`已加入模型 ${selected.id}`);
  }

  function addManualModel() {
    setDraft((current) => ({
      ...current,
      models: [...current.models, model(nextManualModelId(current.models))]
    }));
  }

  function updateModel(index: number, patch: Partial<ModelConfig>) {
    setDraft((current) => ({
      ...current,
      models: current.models.map((item, itemIndex) =>
        itemIndex === index ? { ...item, ...patch } : item
      )
    }));
  }

  function removeModel(index: number) {
    setDraft((current) => ({
      ...current,
      models: current.models.filter((_, itemIndex) => itemIndex !== index)
    }));
  }

  async function saveProvider() {
    if (!draft.name.trim() || !draft.base_url.trim()) {
      setError("名称和 Base URL 不能为空");
      return;
    }
    setSavingProvider(true);
    setError("");
    setNotice("");
    try {
      const input: ProviderInput = {
        id: draft.id,
        name: draft.name.trim(),
        base_url: draft.base_url.trim(),
        models_url: draft.models_url.trim() || undefined,
        is_full_url: draft.is_full_url,
        custom_user_agent: draft.custom_user_agent.trim() || undefined,
        channel: draft.channel,
        prompt_cache_retention_enabled: draft.prompt_cache_retention_enabled,
        request_body_gzip_enabled: draft.request_body_gzip_enabled,
        api_key: draft.api_key || undefined,
        enabled: draft.enabled
      };
      const previousModelIds = new Set(
        config?.providers.find((item) => item.id === draft.id)?.models.map((item) => item.id) ?? []
      );
      const modelsToSave = normalizeModels(draft.models);
      let nextConfig = await command<AppConfig>("add_or_update_provider", { input });
      const provider =
        nextConfig.providers.find((item) => item.id === draft.id) ??
        nextConfig.providers.find((item) => item.name === input.name && item.base_url === input.base_url);

      if (provider) {
        for (const item of modelsToSave) {
          nextConfig = await command<AppConfig>("add_or_update_model", {
            input: { provider_id: provider.id, model: item }
          });
        }
        for (const modelId of previousModelIds) {
          if (!modelsToSave.some((item) => item.id === modelId)) {
            nextConfig = await command<AppConfig>("delete_model", {
              providerId: provider.id,
              provider_id: provider.id,
              modelId,
              model_id: modelId
            });
          }
        }
        setSelectedProviderId(provider.id);
        setDraft({ ...draft, id: provider.id, models: modelsToSave });
        const selectedAgent = nextConfig.agent_injections.find((item) => item.id === selectedAgentId);
        if (activeView === "agent" && selectedAgent) {
          const selectedModel =
            modelsToSave.find((item) => item.enabled)?.id ??
            modelsToSave[0]?.id ??
            provider.models.find((item) => item.enabled)?.id ??
            provider.models[0]?.id;
          await command<AgentInjectionResult[]>("update_agent_injection_route", {
            input: {
              id: selectedAgent.id,
              provider_id: provider.id,
              model_id: selectedModel ?? null
            }
          });
          if (!selectedAgent.enabled) {
            await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
              input: { id: selectedAgent.id, enabled: true }
            });
          }
          nextConfig = await command<AppConfig>("get_config");
        }
      }
      setConfig(nextConfig);
      setProviderEditorOpen(false);
      setActiveView("agent");
      setNotice("上游配置已保存并已绑定到当前 Agent");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingProvider(false);
    }
  }

  async function removeProvider(provider: ProviderConfig) {
    if (!window.confirm(`删除上游 ${provider.name}？`)) return;
    const nextConfig = await command<AppConfig>("delete_provider", {
      providerId: provider.id,
      provider_id: provider.id
    });
    setConfig(nextConfig);
    if (selectedProviderId === provider.id) {
      setSelectedProviderId("new");
      setDraft(emptyDraft);
    }
  }

  async function toggleProxy() {
    const next = await command<ProxyStatus>(status?.running ? "stop_proxy" : "start_proxy");
    setStatus(next);
  }

  async function toggleApiKeyVisibility(visible: boolean) {
    if (visible && draft.id && !draft.api_key) {
      try {
        const revealed = await command<string | null>("reveal_provider_api_key", {
          providerId: draft.id,
          provider_id: draft.id
        });
        if (revealed) {
          setDraft((current) =>
            current.id === draft.id ? { ...current, ["api_key"]: revealed } : current
          );
        }
      } catch (err) {
        setError(String(err));
      }
    }
    setApiKeyVisible(visible);
  }

  async function saveGatewayConfig() {
    if (!config) return;
    const port = Number(config.port);
    if (!config.host.trim() || !Number.isInteger(port) || port <= 0 || port > 65535) {
      setError("Host 或端口不合法");
      return;
    }
    setSavingGateway(true);
    setError("");
    setNotice("");
    try {
      const savedConfig = await command<AppConfig>("get_config");
      const networkChanged =
        savedConfig.host !== config.host.trim() || Number(savedConfig.port) !== port;
      const wasRunning = Boolean(status?.running);
      if (wasRunning && networkChanged) {
        setStatus(await command<ProxyStatus>("stop_proxy"));
      }
      const input: GeneralConfigInput = {
        host: config.host.trim(),
        port,
        local_key: config.local_key,
        default_channel: config.default_channel,
        workspace_fingerprint: config.workspace_fingerprint.trim() || "default-workspace",
        cache: config.cache
      };
      const nextConfig = await command<AppConfig>("save_config", { input });
      setConfig(nextConfig);
      if (wasRunning && networkChanged) {
        setStatus(await command<ProxyStatus>("start_proxy"));
      }
      setNotice(networkChanged ? "本地代理已保存并重启" : "本地代理配置已保存");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingGateway(false);
    }
  }

  async function toggleAgentInjection(item: AgentInjectionConfig) {
    if (!item.enabled && !item.provider_id) {
      const defaultProvider =
        providers.find((provider) => provider.id === config?.active_provider_id) ??
        activeProvider ??
        providers[0];
      if (!defaultProvider) {
        setSelectedAgentId(item.id);
        setActiveView("agent");
        setNotice("");
        setError("请先添加一个上游，然后再开启这个 Agent。");
        return;
      }
      await activateAgentProvider(item, defaultProvider);
      return;
    }
    setInjectingId(item.id);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
        input: { id: item.id, enabled: !item.enabled }
      });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 已更新`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function applyAgentInjection(item: AgentInjectionConfig) {
    if (!item.provider_id) {
      setSelectedAgentId(item.id);
      setActiveView("agent");
      setNotice("");
      setError("请先为这个 Agent 选择一个上游。");
      return;
    }
    setInjectingId(item.id);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("apply_agent_injection", { id: item.id });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 已注入`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function applyEnabledInjections() {
    setInjectingId("all");
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("apply_enabled_agent_injections");
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results.length ? `已刷新 ${results.length} 个 Agent 配置` : "没有已启用的注入配置");
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function updateAgentInjectionRoute(
    item: AgentInjectionConfig,
    providerId: string,
    modelId?: string
  ) {
    setInjectingId(`${item.id}:route`);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("update_agent_injection_route", {
        input: {
          id: item.id,
          provider_id: providerId || null,
          model_id: modelId || null
        }
      });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 路由已更新`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function activateAgentProvider(
    item: AgentInjectionConfig,
    provider: ProviderConfig,
    modelId?: string
  ) {
    const selectedModel =
      modelId ||
      provider.models.find((item) => item.enabled)?.id ||
      provider.models[0]?.id;
    setInjectingId(`${item.id}:route`);
    setError("");
    setNotice("");
    try {
      await command<AgentInjectionResult[]>("update_agent_injection_route", {
        input: {
          id: item.id,
          provider_id: provider.id,
          model_id: selectedModel ?? null
        }
      });
      let latestConfig = await command<AppConfig>("get_config");
      const latestItem = latestConfig.agent_injections.find((candidate) => candidate.id === item.id);
      if (!latestItem?.enabled) {
        await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
          input: { id: item.id, enabled: true }
        });
        latestConfig = await command<AppConfig>("get_config");
      }
      setConfig(latestConfig);
      setNotice(`${item.label} 已启用并绑定 ${provider.name}`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function saveCachePolicy(nextCache = config?.cache) {
    if (!nextCache) return;
    setSavingCachePolicy(true);
    setError("");
    setNotice("");
    try {
      const nextConfig = await command<AppConfig>("save_cache_policy", { input: nextCache });
      setConfig(nextConfig);
      setNotice("缓存策略已保存");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingCachePolicy(false);
    }
  }

  function updateConfig(patch: Partial<AppConfig>) {
    setConfig((current) => (current ? { ...current, ...patch } : current));
  }

  function updateCache(patch: Partial<AppConfig["cache"]>) {
    setConfig((current) =>
      current ? { ...current, cache: { ...current.cache, ...patch } } : current
    );
  }

  const providers = config?.providers ?? [];
  const injections = config?.agent_injections ?? [];
  const selectedAgent = injections.find((item) => item.id === selectedAgentId) ?? null;
  const selectedAgentProvider =
    selectedAgent?.provider_id
      ? providers.find((provider) => provider.id === selectedAgent.provider_id) ?? null
      : null;
  const promptCacheRetentionWarning = useMemo(() => {
    const recentError = metrics?.recent_errors?.find((item) =>
      /prompt_cache_retention/i.test(item.message) &&
      /(unsupported|invalid|unknown|not support|not_supported)/i.test(item.message)
    );
    if (!recentError) return null;
    const provider =
      activeProvider?.prompt_cache_retention_enabled
        ? activeProvider
        : selectedAgentProvider?.prompt_cache_retention_enabled
          ? selectedAgentProvider
          : providers.find((item) => item.prompt_cache_retention_enabled) ?? activeProvider ?? selectedAgentProvider;
    return {
      key: `${recentError.at}:${recentError.message}`,
      message: recentError.message,
      provider
    };
  }, [activeProvider, metrics, providers, selectedAgentProvider]);
  const showPromptCacheRetentionWarning =
    Boolean(promptCacheRetentionWarning) &&
    promptCacheRetentionWarning?.key !== dismissedRetentionWarningKey;
  const agentBindingNotice =
    activeView === "agent" && selectedAgent?.enabled && selectedAgentProvider
      ? `${selectedAgent.label} 已启用并绑定 ${selectedAgentProvider.name}`
      : "";
  const feedbackMessage = error || notice || agentBindingNotice;
  const summaryUsage = useMemo(() => {
    if (!metrics) return null;
    if (activeView === "cache") {
      return cacheProviderFilter === "all"
        ? metrics.usage
        : metrics.usage.by_provider.find((item) => item.key === cacheProviderFilter) ?? metrics.usage;
    }
    const activeProviderUsage = activeProvider
      ? metrics.usage.by_provider.find((item) => item.key === activeProvider.name)
      : null;
    if (activeProviderUsage && activeProviderUsage.total_tokens > 0) return activeProviderUsage;
    return metrics.usage;
  }, [activeProvider, activeView, cacheProviderFilter, metrics]);
  const summaryColdAdjusted = coldAdjustedUsage(summaryUsage, includeColdStarts);
  const currentProviderInputTokens = summaryColdAdjusted.inputTokens;
  const currentProviderTotalTokens = summaryColdAdjusted.totalTokens;
  const currentProviderCacheReadTokens = summaryUsage?.cache_read_tokens ?? 0;
  const historyCacheRatio =
    currentProviderInputTokens > 0 ? currentProviderCacheReadTokens / currentProviderInputTokens : 0;

  return (
    <main className="app-shell">
      <aside className="side-rail">
        <div className="brand-lockup">
          <div className="brand-mark">
            <Zap size={21} />
          </div>
          <div>
            <h1>Atoapi</h1>
            <p>本地代理加速器</p>
            <div className="version-wrap">
              <button
                className="version-badge"
                type="button"
                onClick={() => setVersionOpen((open) => !open)}
                aria-expanded={versionOpen}
                aria-label="查看版本更新"
              >
                {appVersion}
              </button>
              {versionOpen ? (
                <div className="version-popover" role="status">
                  <strong>更新内容</strong>
                  {appVersionNotes.map((item) => (
                    <span key={item}>{item}</span>
                  ))}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="proxy-card">
          <div>
            <span className={status?.running ? "status-pill online" : "status-pill"}>
              {status?.running ? "运行中" : "已停止"}
            </span>
            <code>{baseUrl}</code>
          </div>
          <div className="proxy-actions">
            <button className="icon-button" onClick={toggleProxy} title={status?.running ? "停止代理" : "启动代理"}>
              {status?.running ? <Square size={16} /> : <Play size={16} />}
            </button>
            <button className="icon-button" onClick={() => void navigator.clipboard.writeText(baseUrl)} title="复制地址">
              <Copy size={16} />
            </button>
          </div>
        </div>

        <div className="side-section-head">
          <span>Agent 注入</span>
          <button className="tiny-button" onClick={() => void applyEnabledInjections()} disabled={injectingId === "all"}>
            {injectingId === "all" ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}
            刷新
          </button>
        </div>

        <nav className="provider-list agent-side-list">
          {injections.map((item) => (
            <AgentSideTab
              key={item.id}
              item={item}
              provider={providers.find((provider) => provider.id === item.provider_id) ?? null}
              selected={selectedAgent?.id === item.id && activeView === "agent"}
              injectingId={injectingId}
              onSelect={() => {
                setSelectedAgentId(item.id);
                setActiveView("agent");
              }}
              onToggle={() => void toggleAgentInjection(item)}
            />
          ))}
          {!injections.length && <div className="empty-mini">还没有 Agent 注入配置。</div>}
        </nav>

        <div className="side-utility-nav">
          {utilityViews.map((view) => (
            <button
              key={view.id}
              className={activeView === view.id ? "utility-tab active" : "utility-tab"}
              onClick={() => setActiveView(view.id)}
            >
              {view.icon}
              {view.label}
            </button>
          ))}
        </div>
      </aside>

      <section className="main-panel">
        <header className="topbar">
          <div>
            <p className="overline">Control desk</p>
            <h2>{activeView === "agent" ? selectedAgent?.label ?? "Agent 注入" : activeView === "gateway" ? "本地代理设置" : "缓存统计"}</h2>
          </div>
          <div className="summary-strip">
            <Summary tone="red" label="累计真实 token" value={formatCompactTokens(currentProviderTotalTokens)} />
            <Summary tone="red" label="累计上游命中" value={formatCompactTokens(currentProviderCacheReadTokens)} />
            <Summary tone="red" label="历史前缀命中率" value={percent(historyCacheRatio)} />
          </div>
        </header>

        {feedbackMessage && (
          <div className={error ? "notice error" : "notice success"}>
            {error ? <ShieldCheck size={16} /> : <Check size={16} />}
            <span>{feedbackMessage}</span>
          </div>
        )}

        <section className="workspace">
          {activeView === "gateway" && (
            <GatewayPanel
              config={config}
              status={status}
              baseUrl={baseUrl}
              savingGateway={savingGateway}
              onConfigChange={updateConfig}
              onSave={() => void saveGatewayConfig()}
              onToggleProxy={() => void toggleProxy()}
            />
          )}

          {activeView === "agent" && selectedAgent && (
            <AgentWorkspace
              item={selectedAgent}
              providers={providers}
              injectingId={injectingId}
              onToggle={toggleAgentInjection}
              onProviderSelect={(provider, modelId) => void activateAgentProvider(selectedAgent, provider, modelId)}
              onModelSelect={(provider, modelId) => void activateAgentProvider(selectedAgent, provider, modelId)}
              onCreateProvider={createProvider}
              onEditProvider={editProvider}
              onDeleteProvider={(provider) => void removeProvider(provider)}
            />
          )}

          {activeView === "agent" && !selectedAgent && (
            <AgentEmptySelection providers={providers} onCreateProvider={createProvider} />
          )}

          {activeView === "cache" && (
            <CachePanel
              config={config}
              metrics={metrics}
              selectedProvider={cacheProviderFilter}
              savingCachePolicy={savingCachePolicy}
              includeColdStarts={includeColdStarts}
              onSelectedProviderChange={setCacheProviderFilter}
              onIncludeColdStartsChange={setIncludeColdStarts}
              onSmartCacheChange={(nextCache) => void saveCachePolicy(nextCache)}
              onRefresh={() => void refreshMetrics()}
            />
          )}
        </section>
      </section>
      {providerEditorOpen && (
        <ProviderEditorModal
          draft={draft}
          config={config}
          selectedProviderId={selectedProviderId}
          apiKeyVisible={apiKeyVisible}
          loadingModels={loadingModels}
          savingProvider={savingProvider}
          modelCandidates={modelCandidates}
          selectedFetchedModelId={selectedFetchedModelId}
          onDraftChange={setDraft}
          onApiKeyVisibleChange={(visible) => void toggleApiKeyVisibility(visible)}
          onFetchModels={() => void fetchModels()}
          onSelectedFetchedModelChange={setSelectedFetchedModelId}
          onAddFetchedModel={addFetchedModel}
          onAddManualModel={addManualModel}
          onUpdateModel={updateModel}
          onRemoveModel={removeModel}
          onSave={() => void saveProvider()}
          onDelete={() => {
            const provider = config?.providers.find((item) => item.id === draft.id);
            if (provider) void removeProvider(provider);
          }}
          onClose={() => setProviderEditorOpen(false)}
        />
      )}
      {showPromptCacheRetentionWarning && promptCacheRetentionWarning && (
        <div className="modal-backdrop warning-backdrop" role="presentation">
          <section className="warning-modal" role="dialog" aria-modal="true" aria-label="prompt_cache_retention 不兼容提醒">
            <div>
              <h3>当前上游可能不支持 prompt_cache_retention</h3>
              <p>
                上游返回了和 prompt_cache_retention 相关的错误。这个参数用于请求更长时间保留前缀缓存，
                但部分第三方中转不支持，会导致请求失败。
              </p>
              <code>{promptCacheRetentionWarning.message}</code>
            </div>
            <div className="warning-actions">
              <button
                className="soft-button"
                onClick={() => setDismissedRetentionWarningKey(promptCacheRetentionWarning.key)}
              >
                知道了
              </button>
              <button
                className="primary-button"
                onClick={() => {
                  setDismissedRetentionWarningKey(promptCacheRetentionWarning.key);
                  if (promptCacheRetentionWarning.provider) {
                    editProvider(promptCacheRetentionWarning.provider);
                  }
                }}
              >
                去关闭这个开关
              </button>
            </div>
          </section>
        </div>
      )}
    </main>
  );
}

function AgentSideTab({
  item,
  provider,
  selected,
  injectingId,
  onSelect,
  onToggle
}: {
  item: AgentInjectionConfig;
  provider: ProviderConfig | null;
  selected: boolean;
  injectingId: string;
  onSelect: () => void;
  onToggle: () => void;
}) {
  const modelLabel = item.model_id || provider?.models.find((model) => model.enabled)?.id || provider?.models[0]?.id;
  const busy = injectingId === item.id || injectingId === `${item.id}:route`;

  return (
    <div
      className={selected ? "agent-side-tab active" : "agent-side-tab"}
      role="button"
      tabIndex={0}
      onClick={onSelect}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect();
        }
      }}
    >
      <div className="agent-icon">{agentIcon(item.kind)}</div>
      <div className="agent-side-copy">
        <b>{item.label}</b>
        <small>{provider ? `${provider.name}${modelLabel ? ` / ${modelLabel}` : ""}` : "未选择上游"}</small>
      </div>
      <button
        className={item.enabled ? "mini-toggle on" : "mini-toggle"}
        disabled={busy}
        onClick={(event) => {
          event.stopPropagation();
          onToggle();
        }}
        title={item.enabled ? "关闭这个 Agent 注入" : "开启这个 Agent 注入"}
      >
        <span />
      </button>
    </div>
  );
}

function AgentEmptySelection({
  providers,
  onCreateProvider
}: {
  providers: ProviderConfig[];
  onCreateProvider: () => void;
}) {
  return (
    <section className="agent-workspace">
      <div className="empty-state agent-empty">
        <Workflow size={26} />
        <span>
          请选择左侧某个 Agent 后再配置上游。已启用的 Agent 会继续使用上次保存的上游和模型，不会在打开软件时被自动改选。
        </span>
        {!providers.length && (
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        )}
      </div>
    </section>
  );
}

function AgentWorkspace({
  item,
  providers,
  injectingId,
  onToggle,
  onProviderSelect,
  onModelSelect,
  onCreateProvider,
  onEditProvider,
  onDeleteProvider
}: {
  item: AgentInjectionConfig;
  providers: ProviderConfig[];
  injectingId: string;
  onToggle: (item: AgentInjectionConfig) => void;
  onProviderSelect: (provider: ProviderConfig, modelId?: string) => void;
  onModelSelect: (provider: ProviderConfig, modelId: string) => void;
  onCreateProvider: () => void;
  onEditProvider: (provider: ProviderConfig) => void;
  onDeleteProvider: (provider: ProviderConfig) => void;
}) {
  const selectedProvider = providers.find((provider) => provider.id === item.provider_id) ?? null;
  const selectedModel =
    selectedProvider?.models.find((model) => model.id === item.model_id) ??
    selectedProvider?.models.find((model) => model.enabled) ??
    selectedProvider?.models[0] ??
    null;
  const routeBusy = injectingId === `${item.id}:route`;
  const itemBusy = injectingId === item.id;

  return (
    <section className="agent-workspace">
      <div className="agent-hero">
        <div className="agent-hero-main">
          <div className="agent-icon large">{agentIcon(item.kind)}</div>
          <div>
            <h3>{item.label}</h3>
            <p>
              {selectedProvider
                ? item.enabled
                  ? `已启用并绑定 ${selectedProvider.name}${selectedModel ? ` / ${selectedModel.id}` : ""}`
                  : `已选择 ${selectedProvider.name}${selectedModel ? ` / ${selectedModel.id}` : ""}，但这个 Agent 未启用`
                : "为这个 Agent 选择一个中转上游。每个 Agent 的上游和模型互不影响。"}
            </p>
            {item.target_path && <code>{item.target_path}</code>}
          </div>
        </div>
        <div className="agent-hero-actions">
          <button
            className={item.enabled ? "agent-switch on" : "agent-switch"}
            onClick={() => onToggle(item)}
            disabled={itemBusy || routeBusy}
          >
            <span />
            {item.enabled ? "已启用" : "未启用"}
          </button>
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        </div>
      </div>

      {item.last_status && <div className="agent-last-status">{item.last_status}</div>}

      <div className="agent-provider-head">
        <div>
          <h3>选择这个 Agent 使用的上游</h3>
          <p>点中某个上游后，这个 Agent 会立即启用、绑定并同步配置；其他 Agent 不会被改动。</p>
        </div>
        {routeBusy && (
          <span className="route-saving">
            <Loader2 className="spin" size={15} />
            保存中
          </span>
        )}
      </div>

      {providers.length ? (
        <div className="agent-provider-grid">
          {providers.map((provider) => {
            const isSelected = item.provider_id === provider.id;
            const providerModel =
              provider.models.find((model) => model.id === (isSelected ? item.model_id : undefined)) ??
              provider.models.find((model) => model.enabled) ??
              provider.models[0] ??
              null;

            return (
              <div
                className={isSelected ? "agent-provider-card active" : "agent-provider-card"}
                key={provider.id}
                role="button"
                tabIndex={0}
                onClick={() => onProviderSelect(provider, providerModel?.id)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onProviderSelect(provider, providerModel?.id);
                  }
                }}
              >
                <div className="provider-card-top">
                  <span className="provider-glyph">{provider.name.slice(0, 1).toUpperCase()}</span>
                  <div>
                    <h4>{provider.name}</h4>
                    <p>{channelLabel(provider.channel)} / {provider.models.length} 个模型</p>
                  </div>
                  {isSelected ? (
                    <span className={item.enabled ? "selected-badge" : "selected-badge pending"}>
                      <Check size={14} />
                      {item.enabled ? "已启用并绑定" : "已选择未启用"}
                    </span>
                  ) : (
                    <span className={provider.enabled ? "state-dot" : "state-dot muted"} />
                  )}
                </div>

                <code>{provider.base_url}</code>

                <div className="provider-card-model" onClick={(event) => event.stopPropagation()}>
                  <Field label="这个 Agent 使用的模型">
                    <SelectShell disabled={!provider.models.length || routeBusy}>
                      <select
                        value={providerModel?.id ?? ""}
                        disabled={!provider.models.length || routeBusy}
                        onChange={(event) => onModelSelect(provider, event.target.value)}
                      >
                        {!provider.models.length && <option value="">请先添加模型</option>}
                        {provider.models.map((model) => (
                          <option key={model.id} value={model.id}>
                            {model.id}
                          </option>
                        ))}
                      </select>
                    </SelectShell>
                  </Field>
                </div>

                <div className="provider-card-actions" onClick={(event) => event.stopPropagation()}>
                  <button className="soft-button" onClick={() => onEditProvider(provider)}>
                    <Settings2 size={15} />
                    编辑
                  </button>
                  <button className="danger-button" onClick={() => onDeleteProvider(provider)}>
                    <Trash2 size={15} />
                    删除
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="empty-state agent-empty">
          <DatabaseZap size={24} />
          <span>还没有上游。先新增一个中转，然后这个 Agent 会自动绑定到它。</span>
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        </div>
      )}
    </section>
  );
}

function ProviderEditorModal({
  draft,
  config,
  selectedProviderId,
  apiKeyVisible,
  loadingModels,
  savingProvider,
  modelCandidates,
  selectedFetchedModelId,
  onDraftChange,
  onApiKeyVisibleChange,
  onFetchModels,
  onSelectedFetchedModelChange,
  onAddFetchedModel,
  onAddManualModel,
  onUpdateModel,
  onRemoveModel,
  onSave,
  onDelete,
  onClose
}: {
  draft: ProviderDraft;
  config: AppConfig | null;
  selectedProviderId: string;
  apiKeyVisible: boolean;
  loadingModels: boolean;
  savingProvider: boolean;
  modelCandidates: ModelConfig[];
  selectedFetchedModelId: string;
  onDraftChange: (draft: ProviderDraft) => void;
  onApiKeyVisibleChange: (visible: boolean) => void;
  onFetchModels: () => void;
  onSelectedFetchedModelChange: (id: string) => void;
  onAddFetchedModel: () => void;
  onAddManualModel: () => void;
  onUpdateModel: (index: number, patch: Partial<ModelConfig>) => void;
  onRemoveModel: (index: number) => void;
  onSave: () => void;
  onDelete: () => void;
  onClose: () => void;
}) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="provider-modal"
        role="dialog"
        aria-modal="true"
        aria-label="上游配置"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="modal-head">
          <div>
            <h3>{selectedProviderId === "new" ? "新增中转上游" : "编辑中转上游"}</h3>
            <p>保存后会回到当前 Agent 页面，并把它绑定到这个上游。</p>
          </div>
          <button className="icon-button" onClick={onClose} title="关闭">
            <X size={17} />
          </button>
        </div>
        <div className="modal-body">
          <ProviderPanel
            draft={draft}
            config={config}
            selectedProviderId={selectedProviderId}
            apiKeyVisible={apiKeyVisible}
            loadingModels={loadingModels}
            savingProvider={savingProvider}
            modelCandidates={modelCandidates}
            selectedFetchedModelId={selectedFetchedModelId}
            onDraftChange={onDraftChange}
            onApiKeyVisibleChange={onApiKeyVisibleChange}
            onFetchModels={onFetchModels}
            onSelectedFetchedModelChange={onSelectedFetchedModelChange}
            onAddFetchedModel={onAddFetchedModel}
            onAddManualModel={onAddManualModel}
            onUpdateModel={onUpdateModel}
            onRemoveModel={onRemoveModel}
            onSave={onSave}
            onDelete={onDelete}
          />
        </div>
      </section>
    </div>
  );
}

function ProviderPanel({
  draft,
  config,
  selectedProviderId,
  apiKeyVisible,
  loadingModels,
  savingProvider,
  modelCandidates,
  selectedFetchedModelId,
  onDraftChange,
  onApiKeyVisibleChange,
  onFetchModels,
  onSelectedFetchedModelChange,
  onAddFetchedModel,
  onAddManualModel,
  onUpdateModel,
  onRemoveModel,
  onSave,
  onDelete
}: {
  draft: ProviderDraft;
  config: AppConfig | null;
  selectedProviderId: string;
  apiKeyVisible: boolean;
  loadingModels: boolean;
  savingProvider: boolean;
  modelCandidates: ModelConfig[];
  selectedFetchedModelId: string;
  onDraftChange: (draft: ProviderDraft) => void;
  onApiKeyVisibleChange: (visible: boolean) => void;
  onFetchModels: () => void;
  onSelectedFetchedModelChange: (id: string) => void;
  onAddFetchedModel: () => void;
  onAddManualModel: () => void;
  onUpdateModel: (index: number, patch: Partial<ModelConfig>) => void;
  onRemoveModel: (index: number) => void;
  onSave: () => void;
  onDelete: () => void;
}) {
  const isActive = Boolean(draft.id && config?.active_provider_id === draft.id);
  const supportsPromptCacheRetention = draft.channel === "chat" || draft.channel === "responses";

  return (
    <div className="panel-grid provider-grid">
      <section className="surface">
        <div className="panel-head">
          <div>
            <h3>{selectedProviderId === "new" ? "新增上游" : "上游配置"}</h3>
            <p>填写真实上游地址，本地代理只负责转发和缓存。</p>
          </div>
          <div className="chip-row">
            {isActive && <span className="active-chip"><Check size={14} /> 当前上游</span>}
          </div>
        </div>

        <div className="form-grid">
          <Field label="名称">
            <input
              value={draft.name}
              onChange={(event) => onDraftChange({ ...draft, name: event.target.value })}
              placeholder="上游名称"
            />
          </Field>
          <Field label="接口格式">
            <SelectShell>
              <select
                value={draft.channel}
                onChange={(event) => onDraftChange({ ...draft, channel: event.target.value as Channel })}
              >
                {channelOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label} {option.endpoint}
                  </option>
                ))}
              </select>
            </SelectShell>
          </Field>
          <Field label="Base URL" wide>
            <div className="input-with-icon">
              <Link2 size={17} />
              <input
                value={draft.base_url}
                onChange={(event) => onDraftChange({ ...draft, base_url: event.target.value })}
                placeholder="上游 Base URL"
              />
            </div>
          </Field>
          <Field label="API Key" wide>
            <div className="input-with-icon">
              <KeyRound size={17} />
              <input
                type={apiKeyVisible ? "text" : "password"}
                value={draft.api_key}
                onChange={(event) => onDraftChange({ ...draft, api_key: event.target.value })}
                placeholder={draft.id ? "留空则保留已保存密钥" : "输入上游 API Key"}
              />
              <button className="inline-icon" onClick={() => onApiKeyVisibleChange(!apiKeyVisible)} type="button">
                {apiKeyVisible ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
          </Field>
        </div>

        {supportsPromptCacheRetention && (
          <div
            className={draft.prompt_cache_retention_enabled ? "provider-option-control active" : "provider-option-control"}
            title="开启后会给 OpenAI Chat / Responses 请求发送 prompt_cache_retention=24h，可能让支持的上游更久保留前缀缓存；不支持的第三方中转可能返回 400，遇到报错时请关闭。Anthropic 通道不使用这个参数。"
          >
            <div>
              <h4>发送 prompt_cache_retention</h4>
              <p>默认开启；仅用于 OpenAI Chat / Responses，请求更长的前缀缓存保留。不支持的上游报错时关闭它。</p>
            </div>
            <button
              className={draft.prompt_cache_retention_enabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
              type="button"
              title="开启后发送 prompt_cache_retention=24h；不支持的上游可能报 Unsupported parameter。"
              onClick={() =>
                onDraftChange({
                  ...draft,
                  prompt_cache_retention_enabled: !draft.prompt_cache_retention_enabled
                })
              }
            >
              <span />
              <b>{draft.prompt_cache_retention_enabled ? "已开启" : "已关闭"}</b>
            </button>
          </div>
        )}

        <div
          className={draft.request_body_gzip_enabled ? "provider-option-control active" : "provider-option-control"}
          title="实验性功能：开启后仅对超过 600KB 的上游请求体尝试 gzip 发送；如果上游不支持会自动回退为普通 JSON。默认关闭。"
        >
          <div>
            <h4>大请求体 gzip 压缩</h4>
            <p>默认关闭；用于降低大包上传和网关等待风险，不改变请求语义。</p>
          </div>
          <button
            className={draft.request_body_gzip_enabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
            type="button"
            title="仅压缩 600KB 以上请求体；失败时自动回退。"
            onClick={() =>
              onDraftChange({
                ...draft,
                request_body_gzip_enabled: !draft.request_body_gzip_enabled
              })
            }
          >
            <span />
            <b>{draft.request_body_gzip_enabled ? "已开启" : "已关闭"}</b>
          </button>
        </div>

        <div className="action-row">
          <button className="primary-button" onClick={onSave} disabled={savingProvider}>
            {savingProvider ? <Loader2 className="spin" size={16} /> : <Save size={16} />}
            保存并绑定当前 Agent
          </button>
          {draft.id && (
            <button className="danger-button" onClick={onDelete}>
              <Trash2 size={16} />
              删除上游
            </button>
          )}
        </div>
      </section>

      <section className="surface model-surface">
        <div className="panel-head compact">
          <div>
            <h3>模型</h3>
            <p>{draft.models.length ? `${draft.models.length} 个模型已在列表中` : "先获取模型，或手动添加模型 ID。"}</p>
          </div>
          <button className="soft-button" onClick={onAddManualModel}>
            <Plus size={16} />
            手动添加
          </button>
        </div>

        <div className="model-picker">
          <button className="accent-button" onClick={onFetchModels} disabled={loadingModels || !draft.base_url}>
            {loadingModels ? <Loader2 className="spin" size={16} /> : <DatabaseZap size={16} />}
            获取模型
          </button>
          <SelectShell disabled={!modelCandidates.length}>
            <select
              value={selectedFetchedModelId}
              disabled={!modelCandidates.length}
              onChange={(event) => onSelectedFetchedModelChange(event.target.value)}
            >
              {!modelCandidates.length && <option value="">等待获取模型列表</option>}
              {modelCandidates.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.id}
                </option>
              ))}
            </select>
          </SelectShell>
          <button className="soft-button" onClick={onAddFetchedModel} disabled={!selectedFetchedModelId}>
            <Plus size={16} />
            加入
          </button>
        </div>

        <div className="model-table">
          {draft.models.length === 0 ? (
            <div className="empty-state">
              <DatabaseZap size={22} />
              <span>模型列表为空。Agent 里填写的 Model ID 必须在这里存在。</span>
            </div>
          ) : (
            draft.models.map((item, index) => (
              <div className="model-row" key={`model-row-${index}`}>
                <input
                  value={item.id}
                  onChange={(event) =>
                    onUpdateModel(index, {
                      id: event.target.value,
                      display_name: event.target.value
                    })
                  }
                  placeholder="模型 ID"
                />
                <input
                  className="context-field"
                  value={formatContextInput(item.context_window)}
                  onChange={(event) => onUpdateModel(index, { context_window: parseContext(event.target.value) })}
                  placeholder="上下文"
                />
                <button
                  className={item.enabled ? "mini-toggle on" : "mini-toggle"}
                  onClick={() => onUpdateModel(index, { enabled: !item.enabled })}
                  title={item.enabled ? "停用模型" : "启用模型"}
                >
                  <span />
                </button>
                <button className="icon-button danger" onClick={() => onRemoveModel(index)} title="删除模型">
                  <Trash2 size={15} />
                </button>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function GatewayPanel({
  config,
  status,
  baseUrl,
  savingGateway,
  onConfigChange,
  onSave,
  onToggleProxy
}: {
  config: AppConfig | null;
  status: ProxyStatus | null;
  baseUrl: string;
  savingGateway: boolean;
  onConfigChange: (patch: Partial<AppConfig>) => void;
  onSave: () => void;
  onToggleProxy: () => void;
}) {
  return (
    <div className="panel-grid gateway-grid">
      <section className="surface">
        <div className="panel-head">
          <div>
            <h3>本地入口</h3>
            <p>Agent 只需要连这里，上游和缓存都由本地代理处理。</p>
          </div>
          <button className="primary-button" onClick={onToggleProxy}>
            {status?.running ? <Square size={16} /> : <Play size={16} />}
            {status?.running ? "停止" : "启动"}
          </button>
        </div>

        <div className="connection-card">
          <span>Base URL</span>
          <code>{baseUrl}</code>
          <button className="icon-button" onClick={() => void navigator.clipboard.writeText(baseUrl)} title="复制 Base URL">
            <Copy size={16} />
          </button>
        </div>

        <div className="form-grid">
          <Field label="Host">
            <input
              value={config?.host ?? ""}
              onChange={(event) => onConfigChange({ host: event.target.value })}
            />
          </Field>
          <Field label="Port">
            <input
              value={String(config?.port ?? "")}
              onChange={(event) => onConfigChange({ port: Number(event.target.value) || 0 })}
            />
          </Field>
          <Field label="Local Key" wide>
            <div className="input-with-icon">
              <KeyRound size={17} />
              <input
                value={config?.local_key ?? ""}
                onChange={(event) => onConfigChange({ local_key: event.target.value })}
              />
              <button className="inline-icon" onClick={() => void navigator.clipboard.writeText(config?.local_key ?? "")}>
                <Copy size={16} />
              </button>
            </div>
          </Field>
        </div>

        <button className="primary-button full" onClick={onSave} disabled={savingGateway}>
          {savingGateway ? <Loader2 className="spin" size={16} /> : <Save size={16} />}
          保存本地代理配置
        </button>
      </section>

      <section className="surface setup-surface">
        <h3>Agent 填写方式</h3>
        <div className="code-tile">
          <span>Anthropic / Claude</span>
          <code>ANTHROPIC_BASE_URL={baseUrl}</code>
        </div>
        <div className="code-tile">
          <span>OpenAI Compatible</span>
          <code>OPENAI_BASE_URL={baseUrl}/v1</code>
        </div>
        <div className="code-tile">
          <span>API Key</span>
          <code>{config?.local_key ?? ""}</code>
        </div>
      </section>
    </div>
  );
}

function AgentsPanel({
  injections,
  providers,
  activeProviderId,
  injectingId,
  onToggle,
  onApply,
  onRouteChange,
  onApplyAll
}: {
  injections: AgentInjectionConfig[];
  providers: ProviderConfig[];
  activeProviderId?: string | null;
  injectingId: string;
  onToggle: (item: AgentInjectionConfig) => void;
  onApply: (item: AgentInjectionConfig) => void;
  onRouteChange: (item: AgentInjectionConfig, providerId: string, modelId?: string) => void;
  onApplyAll: () => void;
}) {
  const fallbackProvider = providers.find((provider) => provider.id === activeProviderId) ?? providers[0];

  return (
    <section className="surface">
      <div className="panel-head">
        <div>
          <h3>Agent 注入</h3>
          <p>每个 Agent 独立绑定上游和模型，可以同时启用。</p>
        </div>
        <button className="primary-button" onClick={onApplyAll} disabled={injectingId === "all"}>
          {injectingId === "all" ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />}
          刷新已启用
        </button>
      </div>

      <div className="agent-grid">
        {injections.map((item) => {
          const selectedProvider =
            providers.find((provider) => provider.id === item.provider_id) ?? fallbackProvider;
          const models = selectedProvider?.models ?? [];
          const selectedModel =
            models.find((model) => model.id === item.model_id) ??
            models.find((model) => model.enabled) ??
            models[0];
          const routeBusy = injectingId === `${item.id}:route`;
          const itemBusy = injectingId === item.id;

          return (
            <div className={item.enabled ? "agent-card active" : "agent-card"} key={item.id}>
              <div className="agent-card-main">
                <div className="agent-icon">{agentIcon(item.kind)}</div>
                <div>
                  <h4>{item.label}</h4>
                  <p>{item.last_status ?? "还没有同步配置"}</p>
                  {item.target_path && <code>{item.target_path}</code>}
                </div>
              </div>

              <div className="agent-route-grid">
                <Field label="上游">
                  <SelectShell disabled={!providers.length || routeBusy}>
                    <select
                      value={selectedProvider?.id ?? ""}
                      disabled={!providers.length || routeBusy}
                      onChange={(event) => {
                        const provider = providers.find((item) => item.id === event.target.value);
                        const model =
                          provider?.models.find((item) => item.enabled)?.id ??
                          provider?.models[0]?.id;
                        onRouteChange(item, event.target.value, model);
                      }}
                    >
                      {!providers.length && <option value="">暂无上游</option>}
                      {providers.map((provider) => (
                        <option key={provider.id} value={provider.id}>
                          {provider.name}
                        </option>
                      ))}
                    </select>
                  </SelectShell>
                </Field>
                <Field label="模型">
                  <SelectShell disabled={!selectedProvider || !models.length || routeBusy}>
                    <select
                      value={selectedModel?.id ?? ""}
                      disabled={!selectedProvider || !models.length || routeBusy}
                      onChange={(event) => onRouteChange(item, selectedProvider?.id ?? "", event.target.value)}
                    >
                      {!models.length && <option value="">暂无模型</option>}
                      {models.map((model) => (
                        <option key={model.id} value={model.id}>
                          {model.id}
                        </option>
                      ))}
                    </select>
                  </SelectShell>
                </Field>
              </div>

              <div className="agent-actions">
                <button
                  className={item.enabled ? "mini-toggle on" : "mini-toggle"}
                  onClick={() => onToggle(item)}
                  title={item.enabled ? "关闭自动注入" : "开启自动注入"}
                >
                  <span />
                </button>
                <button className="soft-button" onClick={() => onApply(item)} disabled={itemBusy || !selectedProvider}>
                  {itemBusy ? <Loader2 className="spin" size={16} /> : <Workflow size={16} />}
                  注入
                </button>
                {routeBusy && <Loader2 className="spin route-spinner" size={16} />}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function CachePanel({
  config,
  metrics,
  selectedProvider,
  savingCachePolicy,
  includeColdStarts,
  onSelectedProviderChange,
  onIncludeColdStartsChange,
  onSmartCacheChange,
  onRefresh
}: {
  config: AppConfig | null;
  metrics: MetricsSnapshot | null;
  selectedProvider: string;
  savingCachePolicy: boolean;
  includeColdStarts: boolean;
  onSelectedProviderChange: (provider: string) => void;
  onIncludeColdStartsChange: (include: boolean) => void;
  onSmartCacheChange: (nextCache: AppConfig["cache"]) => void;
  onRefresh: () => void;
}) {
  const [requestPage, setRequestPage] = useState(1);
  const usage = selectedProvider === "all"
    ? metrics?.usage
    : metrics?.usage.by_provider.find((item) => item.key === selectedProvider);
  const traffic = selectedProvider === "all"
    ? null
    : metrics?.provider_stats.find((item) => item.provider === selectedProvider) ?? null;
  const providerOptions = Array.from(
    new Set([
      ...(metrics?.usage.by_provider.map((item) => item.key) ?? []),
      ...(metrics?.provider_stats.map((item) => item.provider) ?? [])
    ])
  ).filter(Boolean);
  const adjustedUsage = coldAdjustedUsage(usage, includeColdStarts);
  const inputTokens = adjustedUsage.inputTokens;
  const outputTokens = adjustedUsage.outputTokens;
  const cacheReadTokens = usage?.cache_read_tokens ?? 0;
  const cacheCreationTokens = usage?.cache_creation_tokens ?? 0;
  const totalTokens = adjustedUsage.totalTokens;
  const recentUsage = selectedProvider === "all" ? metrics?.recent_usage : traffic?.recent_usage;
  const adjustedRecentUsage = coldAdjustedUsage(recentUsage, includeColdStarts);
  const recentInputTokens = adjustedRecentUsage.inputTokens;
  const recentCacheReadTokens = recentUsage?.cache_read_tokens ?? 0;
  const recentTotalTokens = adjustedRecentUsage.totalTokens;
  const recentCacheRatio = recentInputTokens > 0 ? recentCacheReadTokens / recentInputTokens : 0;
  const coldStartRequests = selectedProvider === "all"
    ? metrics?.usage.cold_start_requests ?? 0
    : usage?.cold_start_requests ?? traffic?.cold_start_requests ?? 0;
  const coldStartScopeLabel = selectedProvider === "all" ? "全部上游冷启动" : "当前上游冷启动";
  const recentColdStartRequests = recentUsage?.cold_start_requests ?? 0;
  const totalRequests = selectedProvider === "all"
    ? metrics?.total_requests ?? 0
    : traffic?.total_requests ?? selectedUsageRequests(usage) ?? 0;
  const cacheRatio = inputTokens > 0 ? cacheReadTokens / inputTokens : 0;
  const activeCacheRatio = recentInputTokens > 0 ? recentCacheRatio : cacheRatio;
  const shownRequests = (metrics?.recent_requests ?? []).filter((request) =>
    selectedProvider === "all" || request.provider === selectedProvider
  );
  const pageableRequests = shownRequests.slice(0, requestPageSize * maxRequestPages);
  const requestPageCount = Math.max(1, Math.ceil(pageableRequests.length / requestPageSize));
  const safeRequestPage = Math.min(requestPage, requestPageCount);
  const requestStart = pageableRequests.length ? (safeRequestPage - 1) * requestPageSize : 0;
  const requestEnd = Math.min(requestStart + requestPageSize, pageableRequests.length);
  const pageRequests = pageableRequests.slice(requestStart, requestEnd);
  const estimatedCost = estimateCost(totalTokens, cacheReadTokens);
  const smartCacheEnabled = Boolean(
    config?.cache.enabled &&
      config.cache.exact_enabled &&
      config.cache.semantic_enabled &&
      config.cache.prewarm_enabled &&
      config.cache.mode === "prefix-prewarm"
  );

  function toggleSmartCache() {
    if (!config) return;
    onSmartCacheChange({
      ...config.cache,
      mode: "prefix-prewarm",
      enabled: !smartCacheEnabled,
      exact_enabled: !smartCacheEnabled,
      semantic_enabled: !smartCacheEnabled,
      semantic_threshold: 0.985,
      prewarm_enabled: !smartCacheEnabled
    });
  }

  useEffect(() => {
    setRequestPage(1);
  }, [selectedProvider]);

  useEffect(() => {
    if (requestPage > requestPageCount) {
      setRequestPage(requestPageCount);
    }
  }, [requestPage, requestPageCount]);

  return (
    <div className="panel-grid cache-grid">
      <section className="surface">
        <div className="panel-head">
          <div>
            <h3>缓存策略</h3>
            <p>简单模式：保留主请求内的安全缓存优化，不再额外补发同步热补请求。</p>
          </div>
        </div>

        <div className={smartCacheEnabled ? "smart-cache-card active" : "smart-cache-card"}>
          <div className="smart-cache-icon">
            <Zap size={20} />
          </div>
          <div>
            <h4>智能最大命中</h4>
            <p>启用上游前缀缓存、稳定 cache key、请求体规范化和安全会话续接；不主动多发热补请求。</p>
          </div>
          <button
            className={smartCacheEnabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
            onClick={toggleSmartCache}
            disabled={savingCachePolicy || !config}
            aria-pressed={smartCacheEnabled}
          >
            <span />
            <b>{savingCachePolicy ? "保存中" : smartCacheEnabled ? "已开启" : "已关闭"}</b>
          </button>
        </div>

        <div className="cold-start-control">
          <div>
            <h4>主动热补</h4>
            <p>已移除。软件不会为了提高缓存命中率额外发送前台、后台或桶补热同步请求。</p>
          </div>
          <div className="cold-start-meta">
            <span>成本优先</span>
            <b>已关闭</b>
            <small>主请求自然续热</small>
          </div>
        </div>

        <div className="cold-start-control">
          <div>
            <h4>冷启动统计口径</h4>
            <p>默认计入冷启动，按原始上游累计计算；临时关闭只用于排查 warm 命中。</p>
          </div>
          <div className="cold-start-meta">
            <span>{coldStartScopeLabel}</span>
            <b>{formatNumber(coldStartRequests)} 次</b>
            <small>近 5 分钟 {formatNumber(recentColdStartRequests)} 次</small>
          </div>
          <button
            className={includeColdStarts ? "smart-cache-toggle on" : "smart-cache-toggle"}
            onClick={() => onIncludeColdStartsChange(!includeColdStarts)}
            aria-pressed={includeColdStarts}
          >
            <span />
            <b>{includeColdStarts ? "计入" : "排除"}</b>
          </button>
        </div>
      </section>

      <section className="surface usage-surface">
        <div className="panel-head compact">
          <div>
            <h3>使用统计</h3>
            <p>查看全部或单个中转的 tokens、成本和缓存命中。</p>
          </div>
        </div>

        <div className="usage-filter">
          <button
            className={selectedProvider === "all" ? "filter-tab active" : "filter-tab"}
            onClick={() => onSelectedProviderChange("all")}
          >
            全部
          </button>
          {providerOptions.map((provider) => (
            <button
              className={selectedProvider === provider ? "filter-tab active" : "filter-tab"}
              key={provider}
              onClick={() => onSelectedProviderChange(provider)}
            >
              {provider}
            </button>
          ))}
          <span className="refresh-hint">
            <RefreshCw size={15} />
            自动刷新
          </span>
          <button className="soft-button refresh-button" onClick={onRefresh}>
            <RefreshCw size={15} />
            刷新
          </button>
        </div>

        <div className="usage-hero">
          <div>
            <div className="usage-title">
              <Zap size={18} />
              <span>真实消耗 Tokens</span>
            </div>
            <strong>{formatNumber(totalTokens)}</strong>
            <small>≈ {formatCompactTokens(totalTokens)} tokens</small>
          </div>
          <div className="usage-side">
            <span>总请求数</span>
            <b>{formatNumber(totalRequests)}</b>
            <span>近 5 分钟</span>
            <b>{formatCompactTokens(recentTotalTokens)}</b>
            <span>估算成本</span>
            <b className="cost">{estimatedCost}</b>
          </div>
        </div>

        <div className="usage-breakdown">
          <MetricTile label="近 5 分钟真实 token" value={formatCompactTokens(recentTotalTokens)} />
          <MetricTile label="近 5 分钟命中" value={formatCompactTokens(recentCacheReadTokens)} />
          <MetricTile label="近 5 分钟命中率" value={percent(recentInputTokens > 0 ? recentCacheRatio : 0)} />
          <MetricTile label="Output" value={formatCompactTokens(outputTokens)} />
          <MetricTile label="累计真实 token" value={formatCompactTokens(totalTokens)} />
          <MetricTile label="累计上游命中" value={formatCompactTokens(cacheReadTokens)} />
          <MetricTile label="主请求" value={formatNumber(totalRequests)} />
        </div>

        <div className="prewarm-cost-note">
          <span>主动热补已移除：正常情况下软件不会为了缓存命中额外补发同步请求。</span>
          <b>中转后台请求数应主要来自真实主请求、失败重试或客户端自己发出的探针。</b>
        </div>

        <div className="hit-meter">
          <div>
            <span>上游前缀缓存命中率（近 5 分钟）</span>
            <b>{percent(activeCacheRatio)}</b>
          </div>
          <div className="meter-track">
            <span style={{ width: `${Math.min(100, Math.max(0, activeCacheRatio * 100))}%` }} />
          </div>
        </div>

        <div className="prefix-cache-strip">
          <Summary label="累计前缀命中率" value={percent(cacheRatio)} />
          <Summary label="创建缓存 token" value={cacheCreationTokens ? formatCompactTokens(cacheCreationTokens) : "0"} />
          <Summary label="累计上游命中" value={formatCompactTokens(cacheReadTokens)} />
        </div>

        <div className="request-log-panel">
          <div className="request-log-head">
            <h4>请求记录</h4>
            <span>
              {pageableRequests.length
                ? `显示第 ${requestStart + 1} 条 - 第 ${requestEnd} 条，共 ${pageableRequests.length} 条`
                : "暂无记录"}
            </span>
          </div>
          <div className="request-feed">
            {pageRequests.map((request) => {
              const cacheDisplay = providerBucketDisplay(
                request.input_tokens ?? 0,
                request.cache_read_tokens ?? 0,
                request.provider_cache_token_ratio ?? 0,
                request.cache_shortfall_tokens ?? 0,
                request.cache_new_tail_gap_tokens ?? 0,
                request.cache_avoidable_gap_tokens ?? 0
              );
              const inputTokens = request.input_tokens ?? 0;
              const outputTokens = request.output_tokens ?? 0;
              const cacheReadTokens = request.cache_read_tokens ?? 0;
              const metricTitle = [
                `首字 ${formatDurationMs(request.ttft_ms)}`,
                `用时 ${formatDurationMs(request.total_ms)}`,
                `输入 ${formatNumber(inputTokens)}`,
                `输出 ${formatNumber(outputTokens)}`,
                `缓存命中 ${formatNumber(cacheReadTokens)}`
              ].join(" · ");
              return (
                <div className="request-row" key={request.id}>
                  <Activity size={14} />
                  <time>{formatRequestTime(request.at)}</time>
                  <span className="request-provider-text">{request.provider} · {request.model}</span>
                  <span className={`request-call-badge ${requestCallKindClass(request.upstream_call_kind)}`}>
                    {requestCallKindLabel(request.upstream_call_kind, request.upstream_call_source)}
                  </span>
                  <span className="request-channel-badge">
                    {requestChannelLabel(request.client_channel, request.upstream_channel)}
                  </span>
                  <span className="request-metrics" title={metricTitle}>
                    <strong>首字 {formatDurationMs(request.ttft_ms)} · 用时 {formatDurationMs(request.total_ms)}</strong>
                    <em>输入 {formatCompactTokens(inputTokens)} · 输出 {formatCompactTokens(outputTokens)} · 命中 {formatCompactTokens(cacheReadTokens)}</em>
                  </span>
                  <b>{request.cache_status}{cacheDisplay.primary ? ` · ${cacheDisplay.primary}` : ""}</b>
                  {cacheDisplay.secondary ? <small title={cacheDisplay.secondary}>{cacheDisplay.secondary}</small> : null}
                </div>
              );
            })}
            {!pageableRequests.length && <div className="empty-mini">等待第一条代理请求。</div>}
          </div>
          {pageableRequests.length > requestPageSize && (
            <div className="request-pager">
              <span>总页数：{requestPageCount}</span>
              <button
                className="icon-button"
                onClick={() => setRequestPage((page) => Math.max(1, page - 1))}
                disabled={safeRequestPage === 1}
                title="上一页"
              >
                <ChevronDown className="pager-prev-icon" size={16} />
              </button>
              {visiblePages(safeRequestPage, requestPageCount).map((page, index) =>
                page === "ellipsis" ? (
                  <span className="pager-ellipsis" key={`ellipsis-${index}`}>...</span>
                ) : (
                  <button
                    className={safeRequestPage === page ? "pager-button active" : "pager-button"}
                    key={page}
                    onClick={() => setRequestPage(page)}
                  >
                    {page}
                  </button>
                )
              )}
              <button
                className="icon-button"
                onClick={() => setRequestPage((page) => Math.min(requestPageCount, page + 1))}
                disabled={safeRequestPage === requestPageCount}
                title="下一页"
              >
                <ChevronDown className="pager-next-icon" size={16} />
              </button>
              <span>每页 20 条</span>
            </div>
          )}
        </div>

        <div className="provider-stats">
          <h4>上游流量统计</h4>
          {(metrics?.provider_stats ?? []).map((item) => (
            <div className="provider-stat-row" key={item.provider}>
              <div>
                <b>{item.provider}</b>
                <span>
                  本地请求 {item.total_requests} · 转发上游 {item.upstream_requests} · 本地未命中 {item.cache_misses} · 绕过 {item.bypassed}
                </span>
              </div>
              <div>
                <b>{percent(item.recent_usage.cache_token_ratio)}</b>
                <span>上游前缀命中 · 完整复用 {percent(item.cache_hit_rate)} · TTFT {item.ttft_p95_ms}ms</span>
              </div>
            </div>
          ))}
          {!metrics?.provider_stats?.length && <div className="empty-mini">还没有上游流量统计。</div>}
        </div>
      </section>
    </div>
  );
}

function ProviderTab({ provider, selected, onSelect }: { provider: ProviderConfig; selected: boolean; onSelect: () => void }) {
  return (
    <button className={selected ? "provider-tab active" : "provider-tab"} onClick={onSelect}>
      <span className="provider-glyph">{provider.name.slice(0, 1).toUpperCase()}</span>
      <span>
        <b>{provider.name}</b>
        <small>{channelLabel(provider.channel)} · {provider.models.length} 模型</small>
      </span>
      {selected ? <Check size={16} /> : <span className={provider.enabled ? "state-dot" : "state-dot muted"} />}
    </button>
  );
}

function Field({ label, wide, children }: { label: string; wide?: boolean; children: ReactNode }) {
  return (
    <label className={wide ? "field wide" : "field"}>
      <span>{label}</span>
      {children}
    </label>
  );
}

function SelectShell({ children, disabled }: { children: ReactNode; disabled?: boolean }) {
  return (
    <div className={disabled ? "select-shell disabled" : "select-shell"}>
      {children}
      <ChevronDown size={16} />
    </div>
  );
}

function Summary({ label, value, tone }: { label: string; value: string; tone?: "red" }) {
  return (
    <div className={tone === "red" ? "summary-card red" : "summary-card"}>
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

function agentIcon(kind: AgentInjectionConfig["kind"]) {
  if (kind === "claude-code") return <TerminalSquare size={18} />;
  if (kind === "codex") return <BrainCircuit size={18} />;
  if (kind === "claude-desktop") return <Bot size={18} />;
  return <Workflow size={18} />;
}

function providerToDraft(provider: ProviderConfig): ProviderDraft {
  return {
    id: provider.id,
    name: provider.name,
    base_url: provider.base_url,
    models_url: provider.models_url ?? "",
    is_full_url: provider.is_full_url,
    custom_user_agent: provider.custom_user_agent ?? "",
    api_key: "",
    channel: provider.channel,
    prompt_cache_retention_enabled: provider.prompt_cache_retention_enabled,
    request_body_gzip_enabled: provider.request_body_gzip_enabled,
    models: provider.models,
    enabled: provider.enabled
  };
}

function normalizeModels(models: ModelConfig[]) {
  const byId = new Map<string, ModelConfig>();
  for (const item of models) {
    const id = item.id.trim();
    if (!id) continue;
    byId.set(id, {
      ...item,
      id,
      display_name: (item.display_name || id).trim() || id
    });
  }
  return [...byId.values()];
}

function nextManualModelId(models: ModelConfig[]) {
  let index = models.length + 1;
  let id = "new-model";
  while (models.some((item) => item.id === id)) {
    id = `new-model-${index}`;
    index += 1;
  }
  return id;
}

function draftHasInput(draft: ProviderDraft) {
  return Boolean(
    draft.id ||
      draft.name.trim() ||
      draft.base_url.trim() ||
      draft.models_url.trim() ||
      draft.custom_user_agent.trim() ||
      draft.api_key.trim() ||
      draft.models.length
  );
}

function percent(value?: number) {
  return `${((value ?? 0) * 100).toFixed(1)}%`;
}

function providerBucketDisplay(
  inputTokens: number,
  cachedTokens: number,
  rawRatio: number,
  shortfallTokens: number,
  newTailGapTokens = 0,
  avoidableGapTokens = 0
) {
  if (!inputTokens) return { primary: "", secondary: "" };
  const tokenSummary = `${formatCompactTokens(cachedTokens)} / ${formatCompactTokens(inputTokens)}`;
  if (!cachedTokens) {
    return {
      primary: "冷启动",
      secondary: `${tokenSummary}${shortfallTokens ? ` · 缺口 ${formatCompactTokens(shortfallTokens)}` : ""}`
    };
  }
  const bucketMax = Math.floor(inputTokens / 512) * 512;
  const bucketGap = Math.max(bucketMax - cachedTokens, 0);
  const realRatio = inputTokens > 0 ? cachedTokens / inputTokens : rawRatio;
  const primary = percent(realRatio);
  if (bucketMax > 0 && bucketGap === 0) {
    return {
      primary,
      secondary: `${tokenSummary} · 满桶`
    };
  }
  return {
    primary,
    secondary: providerGapLabel(
      tokenSummary,
      shortfallTokens || bucketGap,
      newTailGapTokens,
      avoidableGapTokens
    )
  };
}

function providerGapLabel(
  tokenSummary: string,
  totalGapTokens: number,
  newTailGapTokens: number,
  avoidableGapTokens: number
) {
  if (avoidableGapTokens > 0 && newTailGapTokens > 0) {
    return `${tokenSummary} · 总缺口 ${formatCompactTokens(totalGapTokens)}（可避免 ${formatCompactTokens(
      avoidableGapTokens
    )} / 新尾巴 ${formatCompactTokens(newTailGapTokens)}）`;
  }
  if (avoidableGapTokens > 0) {
    return `${tokenSummary} · 可避免缺口 ${formatCompactTokens(avoidableGapTokens)}`;
  }
  if (newTailGapTokens > 0) {
    return `${tokenSummary} · 新尾巴 ${formatCompactTokens(newTailGapTokens)}`;
  }
  return `${tokenSummary} · 缺口 ${formatCompactTokens(totalGapTokens)}`;
}

function channelLabel(channel: Channel) {
  return channelOptions.find((option) => option.value === channel)?.label ?? channel;
}

function requestChannelLabel(clientChannel?: string | null, upstreamChannel?: string | null) {
  const client = compactChannelLabel(clientChannel);
  const upstream = compactChannelLabel(upstreamChannel);
  if (client && upstream && client !== upstream) {
    return `${client} -> ${upstream}`;
  }
  return upstream || client || "Unknown";
}

function requestCallKindLabel(kind?: string | null, source?: string | null) {
  if (kind === "stream") return "流式";
  if (kind === "sync") return "同步";
  if (kind === "prewarm-sync") {
    if (source === "foreground_prewarm") return "前台补热";
    if (source === "background_bucket_prewarm") return "桶补热";
    return "补热同步";
  }
  if (kind === "cache") return "本地";
  return "同步";
}

function requestCallKindClass(kind?: string | null) {
  if (kind === "stream") return "stream";
  if (kind === "prewarm-sync") return "prewarm";
  if (kind === "cache") return "cache";
  return "sync";
}

function compactChannelLabel(channel?: string | null) {
  if (channel === "responses") return "Responses";
  if (channel === "chat") return "Chat";
  if (channel === "anthropic") return "Anthropic";
  return channel || "";
}

function formatContextInput(value?: number | null) {
  if (!value) return "";
  if (value >= 10000) return `${Math.round(value / 10000)}万`;
  return String(value);
}

function parseContext(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed.endsWith("万")) {
    const number = Number(trimmed.slice(0, -1));
    return Number.isFinite(number) ? Math.round(number * 10000) : null;
  }
  const number = Number(trimmed.replace(/,/g, ""));
  return Number.isFinite(number) ? number : null;
}

function formatNumber(value: number) {
  return Math.round(value).toLocaleString("zh-CN");
}

function formatCompactTokens(value: number) {
  if (!value) return "0";
  if (value >= 100_000_000) return `${trimNumber(value / 100_000_000)} 亿`;
  if (value >= 10_000) return `${trimNumber(value / 10_000)} 万`;
  return formatNumber(value);
}

function formatRequestTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--:--";
  return date.toLocaleTimeString("zh-CN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function formatDurationMs(value?: number | null) {
  const ms = Math.max(0, Math.round(value ?? 0));
  if (ms >= 60_000) return `${trimNumber(ms / 60_000)}m`;
  if (ms >= 10_000) return `${Math.round(ms / 1000)}s`;
  if (ms >= 1000) return `${trimNumber(ms / 1000)}s`;
  return `${ms}ms`;
}

function visiblePages(current: number, total: number): Array<number | "ellipsis"> {
  if (total <= 7) {
    return Array.from({ length: total }, (_, index) => index + 1);
  }
  const pages = new Set<number>([1, 2, total - 1, total, current - 1, current, current + 1]);
  const normalized = [...pages]
    .filter((page) => page >= 1 && page <= total)
    .sort((left, right) => left - right);
  const result: Array<number | "ellipsis"> = [];
  for (const page of normalized) {
    const previous = result[result.length - 1];
    if (typeof previous === "number" && page - previous > 1) {
      result.push("ellipsis");
    }
    result.push(page);
  }
  return result;
}

function trimNumber(value: number) {
  return value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2).replace(/\.0+$/, "");
}

function estimateCost(totalTokens: number, cacheReadTokens: number) {
  const billableTokens = Math.max(totalTokens - cacheReadTokens * 0.75, 0);
  return `$${(billableTokens / 1_000_000 * 1.2).toFixed(4)}`;
}

function selectedUsageRequests(usage?: MetricsSnapshot["usage"] | MetricsSnapshot["usage"]["by_provider"][number]) {
  return usage && "requests" in usage ? usage.requests : 0;
}

type ColdAdjustableUsage = {
  input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
} | null | undefined;

function coldAdjustedUsage(usage: ColdAdjustableUsage, includeColdStarts: boolean) {
  const input = usage?.input_tokens ?? 0;
  const output = usage?.output_tokens ?? 0;
  if (includeColdStarts) {
    return {
      inputTokens: input,
      outputTokens: output,
      totalTokens: usage?.total_tokens ?? input + output
    };
  }
  const coldInput = usage?.cold_start_input_tokens ?? 0;
  const coldOutput = usage?.cold_start_output_tokens ?? 0;
  const inputTokens = Math.max(0, input - coldInput);
  const outputTokens = Math.max(0, output - coldOutput);
  return {
    inputTokens,
    outputTokens,
    totalTokens: inputTokens + outputTokens
  };
}
