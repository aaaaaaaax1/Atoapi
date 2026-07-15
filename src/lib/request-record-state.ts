export type RequestRecordStateTone = "error" | "disconnect" | "compact" | "cold";

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
}

export interface RequestRecordState {
  label: string;
  tone: RequestRecordStateTone;
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
  if (requestIsColdStart(input)) {
    const afterCompaction =
      clean(input.shadowAffinityLane) === "compacted_anchor" &&
      clean(input.prefixLagClassification).startsWith("first_prefix");
    return { label: afterCompaction ? "压缩后冷启动" : "冷启动", tone: "cold" };
  }
  return null;
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
  if (input.inputTokens < 1024) return false;
  if (input.cacheReadTokens === 0) return true;

  const classification = clean(input.prefixLagClassification);
  if (classification === "cold_start" || classification === "cold_read_after_warm") {
    return true;
  }
  if (!classification.startsWith("first_prefix")) return false;

  return input.cacheReadTokens / input.inputTokens < 0.9;
}

function clean(value?: string | null) {
  return value?.trim().toLowerCase() ?? "";
}
