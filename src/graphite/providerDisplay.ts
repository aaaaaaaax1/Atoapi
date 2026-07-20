import type { AgentInjectionConfig, AgentInjectionKind, ProviderConfig } from "../lib/api";
import { providerBelongsToAgent } from "./providerScope";

type DisplayProvider = Pick<ProviderConfig, "id" | "name">;
type DisplayAgent = Pick<AgentInjectionConfig, "id" | "label" | "kind">;

export type RequestAgentBadgeTone =
  | "codex"
  | "claude"
  | "gemini"
  | "opencode"
  | "openclaw"
  | "hermes"
  | "proxy"
  | "generic";

export interface RequestAgentBadge {
  label: string;
  tone: RequestAgentBadgeTone;
}

/**
 * Agent-private provider clones carry a generated " / Agent" suffix in their
 * persisted name so that configuration ownership remains unambiguous. It is
 * metadata, not part of the user-facing upstream name, so remove it only when
 * the provider ID proves that this is the named Agent's private clone.
 */
export function providerDisplayName(
  provider: DisplayProvider,
  owner?: Pick<DisplayAgent, "id" | "label"> | null
): string {
  const name = provider.name.trim();
  if (!owner || !providerBelongsToAgent(provider.id, owner.id)) return name;

  const suffixes = [owner.label, owner.id]
    .map((value) => value.trim())
    .filter(Boolean)
    .sort((left, right) => right.length - left.length);
  for (const suffix of suffixes) {
    const matched = name.match(new RegExp(`\\s*/\\s*${escapeRegExp(suffix)}\\s*(?:\\((\\d+)\\))?\\s*$`, "i"));
    if (!matched) continue;
    const base = name.slice(0, matched.index).trimEnd();
    return matched[1] ? `${base} (${matched[1]})` : base;
  }
  return name;
}

export function requestAgentBadge(
  agentId?: string | null,
  agentLabel?: string | null,
  agents: readonly DisplayAgent[] = []
): RequestAgentBadge {
  const configured = agents.find((agent) => agent.id === agentId) ??
    agents.find((agent) => agent.label === agentLabel);
  const label = configured?.label || agentLabel?.trim() || agentId?.trim() || "Agent";
  const kind = configured?.kind ?? knownAgentKind(agentId);
  return { label, tone: badgeTone(kind) };
}

function knownAgentKind(agentId?: string | null): AgentInjectionKind | null {
  const id = agentId?.trim().toLowerCase();
  switch (id) {
    case "claude-code":
    case "claude-desktop":
      return "claude-code";
    case "codex":
      return "codex";
    case "gemini":
      return "gemini";
    case "open-code":
      return "open-code";
    case "openclaw":
    case "open-claw":
      return "open-claw";
    case "hermes":
    case "proxy-mode":
      return id;
    default:
      return null;
  }
}

function badgeTone(kind: AgentInjectionKind | null): RequestAgentBadgeTone {
  switch (kind) {
    case "codex": return "codex";
    case "claude-code":
    case "claude-desktop": return "claude";
    case "gemini": return "gemini";
    case "open-code": return "opencode";
    case "open-claw": return "openclaw";
    case "hermes": return "hermes";
    case "proxy-mode": return "proxy";
    default: return "generic";
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
