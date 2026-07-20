export type RequestRecordStateTone =
  | "error"
  | "disconnect"
  | "compact"
  | "cold"
  | "hit"
  | "bypass"
  | "complete";

export interface RequestRecordStateInput {
  status: number;
  cacheStatus: string;
  upstreamCallSource?: string | null;
  downstreamDisconnected?: boolean | null;
  downstreamDisconnectStage?: string | null;
  shadowAffinityLane?: string | null;
  prefixLagClassification?: string | null;
  inputTokens: number;
  cacheReadTokens: number;
  /**
   * Authoritative per-request marker from the backend. Older records may not
   * have it yet, in which case the display uses the conservative legacy
   * classification below; aggregate filtering must use this marker only.
   */
  coldStart?: boolean | null;
}

export interface RequestRecordState {
  label: string;
  tone: RequestRecordStateTone;
}

export interface RequestTransportDisplayInput {
  upstreamCallKind?: string | null;
  cacheStatus?: string | null;
}

export interface RequestTransportDisplay {
  label: "流式" | "同步" | "缓存";
  tone: "stream" | "sync" | "cache";
}

/**
 * The rightmost record column answers a different question than transport or
 * cache hit rate: did this request finish normally, and did a notable runtime
 * condition apply? Keep a normal successful call terse while preserving the
 * few states that are operationally useful to spot in a dense request list.
 */
export interface RequestRecordStatusDisplay {
  label: string;
  detail: string | null;
  tone: RequestRecordStateTone;
}

/**
 * The record column describes transport first. Cache bypass/hit is a separate
 * concern and must not replace the visible stream/sync mode for normal calls.
 */
export function requestTransportDisplay(
  input: RequestTransportDisplayInput
): RequestTransportDisplay {
  const kind = clean(input.upstreamCallKind);
  const cacheStatus = clean(input.cacheStatus);
  if (kind === "cache" || cacheStatus === "exact" || cacheStatus === "semantic") {
    return { label: "缓存", tone: "cache" };
  }
  if (kind === "sync" || kind === "prewarm-sync") {
    return { label: "同步", tone: "sync" };
  }
  return { label: "流式", tone: "stream" };
}

export function requestRecordStatusDisplay(
  input: RequestRecordStateInput
): RequestRecordStatusDisplay {
  const state = requestRecordState(input);
  if (state?.tone === "error") {
    return {
      label: input.status >= 400 ? `Error ${input.status}` : "Error",
      detail: null,
      tone: "error"
    };
  }
  if (state?.tone === "disconnect") {
    return { label: state.label, detail: null, tone: "disconnect" };
  }
  if (state?.tone === "compact" || state?.tone === "cold") {
    return { label: "OK", detail: state.label, tone: state.tone };
  }
  return { label: "OK", detail: null, tone: "complete" };
}

export function requestRecordState(input: RequestRecordStateInput): RequestRecordState | null {
  if (input.status >= 400 || input.cacheStatus === "error") {
    return {
      label: input.status ? `error ${input.status}` : "error",
      tone: "error"
    };
  }
  if (input.cacheStatus === "compact" && isConfirmedCompactionSource(input.upstreamCallSource)) {
    return { label: "实际压缩", tone: "compact" };
  }
  if (
    input.downstreamDisconnected &&
    clean(input.downstreamDisconnectStage) !== "after_terminal"
  ) {
    return { label: "下游已断开", tone: "disconnect" };
  }
  if (input.coldStart !== false && requestIsColdStart(input)) {
    const afterCompaction =
      clean(input.shadowAffinityLane) === "compacted_anchor" &&
      clean(input.prefixLagClassification).startsWith("first_prefix");
    return { label: afterCompaction ? "压缩后冷启动" : "冷启动", tone: "cold" };
  }
  switch (clean(input.cacheStatus)) {
    case "exact":
    case "semantic":
      return { label: "命中", tone: "hit" };
    case "bypass":
      return { label: "直通", tone: "bypass" };
    case "miss":
      return { label: "未命中", tone: "complete" };
    default:
      return { label: "完成", tone: "complete" };
  }
}

/** Aggregate cold-start filtering must never infer from a heuristic. */
export function requestRecordIsBackendColdStart(input: Pick<RequestRecordStateInput, "coldStart">): boolean {
  return input.coldStart === true;
}

export interface RequestAffinityDisplayInput {
  arm?: string | null;
  decision?: string | null;
}

export interface RequestAffinityDisplay {
  primaryLabel: string | null;
  detailLabel: string | null;
  applied: boolean;
}

export function requestAffinityDisplay(
  input: RequestAffinityDisplayInput
): RequestAffinityDisplay {
  const arm = clean(input.arm);
  const decision = clean(input.decision);
  const applied = [
    "candidate_applied",
    "stateless_candidate_applied",
    "validation_candidate_applied"
  ].includes(decision);
  if (applied) {
    return { primaryLabel: "candidate", detailLabel: "candidate 已应用", applied: true };
  }
  if (arm === "baseline") {
    return { primaryLabel: null, detailLabel: "baseline shadow", applied: false };
  }
  if (arm === "candidate") {
    return { primaryLabel: null, detailLabel: "candidate shadow 未应用", applied: false };
  }
  return { primaryLabel: null, detailLabel: null, applied: false };
}

function isConfirmedCompactionSource(source?: string | null) {
  const normalized = clean(source);
  return normalized === "responses-compaction-v2" ||
    normalized === "compact" ||
    normalized.startsWith("compact-");
}

function requestIsColdStart(input: RequestRecordStateInput) {
  if (input.coldStart === true) return true;
  if (input.coldStart === false) return false;
  if (input.inputTokens < 1024) return false;
  // Backward-compatible display only: persisted records from before the
  // explicit marker still need their cold-start/compaction status shown.
  if (input.cacheReadTokens === 0) return true;
  const classification = clean(input.prefixLagClassification);
  if (classification === "cold_start") {
    return true;
  }
  if (!classification.startsWith("first_prefix")) return false;
  return input.cacheReadTokens / input.inputTokens < 0.9;
}

function clean(value?: string | null) {
  return value?.trim().toLowerCase() ?? "";
}
