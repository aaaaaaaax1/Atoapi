import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { command } from "./lib/api";
import type {
  AgentInjectionConfig,
  AgentProviderTrafficStats,
  AppConfig,
  CacheValidationStatus,
  Channel,
  MetricsSnapshot,
  ReleaseChampionQueryInput,
  ReleaseChampionSnapshot,
  MetricsTrendInput,
  MetricsTrendSnapshot,
  ProxyStatus,
  ProviderConfig
} from "./lib/api";
import { providersForGraphiteAgent } from "./graphite/providerScope";
import {
  recordsForAgent,
  scopesForSuccessfulAgentRequests,
  trafficForAgentScope,
  limitVisibleRequestRecords
} from "./graphite/requestScope";
import {
  requestRecordIsBackendColdStart,
  requestRecordState,
  requestRecordStatusDisplay,
  requestTransportDisplay
} from "./lib/request-record-state";
import { providerDisplayName, requestAgentBadge } from "./graphite/providerDisplay";
import {
  GRAPHITE_BRIDGE_CHANNEL,
  type GraphiteBridgeResponse,
  type GraphiteMessage,
  type GraphitePrototypeHostProps
} from "./graphite/frameProtocol";
import { createGraphitePrototypeDocument } from "./graphite/frameDocument";

// The table exposes ten 20-row pages. This does not govern lifetime metrics.
const MAX_VISIBLE_REQUESTS = 200;

const bridgeSource = String.raw`
(() => {
  const CHANNEL = "atoapi.graphite.bridge.v1";
  const host = { state: null };
  let renderSuspended = false;
  let deferredMetricsDelta = null;
  const $bridge = (selector, root = document) => root.querySelector(selector);
  const clone = (value) => JSON.parse(JSON.stringify(value ?? []));
  const replace = (target, source) => target.splice(0, target.length, ...clone(source));
  const SAVED_SECRET_MASK = "••••••••••••";
  function setSecretInputState(input, hasSavedSecret, emptyPlaceholder, resetValue = false) {
    if (!input) return;
    if (resetValue) input.value = "";
    input.type = "password";
    const masked = Boolean(hasSavedSecret) && !input.value;
    input.classList.toggle("has-saved-secret", masked);
    if (masked) {
      input.setAttribute("data-saved-secret", "true");
      input.placeholder = SAVED_SECRET_MASK;
    } else {
      input.removeAttribute("data-saved-secret");
      input.placeholder = emptyPlaceholder;
    }
  }
  function syncKeyPoolSecretInputs() {
    keyPool.forEach((key) => {
      const input = document.getElementById("keySecret-" + key.id);
      setSecretInputState(input, key.hasSavedSecret === true, "输入 Key");
    });
  }
  const prototypeRenderKeyPool = renderKeyPool;
  renderKeyPool = function renderKeyPoolWithSavedSecretMasks() {
    prototypeRenderKeyPool();
    syncKeyPoolSecretInputs();
  };
  const pendingActions = new Map();
  const asynchronousActions = new Set(["refresh", "toggle-agent", "bind-provider", "save-provider", "delete-provider", "reorder-providers", "fetch-models", "fetch-health-probe-models", "run-health-probe", "probe-provider-balance", "test-provider", "test-provider-key", "test-provider-key-pool", "diagnose-network-paths", "probe-cache-capabilities", "set-cache-validation", "save-cache-enabled", "save-settings", "restart-main-proxy", "clear-cache"]);
  const send = (action, payload = {}) => {
    const requestId = "graphite-" + Date.now() + "-" + Math.random().toString(16).slice(2);
    const source = document.activeElement instanceof HTMLButtonElement ? document.activeElement : null;
    if (source && asynchronousActions.has(action)) {
      pendingActions.set(requestId, source);
      source.classList.add("is-loading");
      source.setAttribute("aria-busy", "true");
      source.disabled = true;
    }
    window.parent.postMessage({ channel: CHANNEL, kind: "action", action, payload, requestId }, "*");
    return requestId;
  };
  const settle = (requestId) => {
    const source = pendingActions.get(requestId);
    if (!source) return;
    pendingActions.delete(requestId);
    source.classList.remove("is-loading");
    source.removeAttribute("aria-busy");
    source.disabled = false;
  };
  const currentAgent = () => agents.find((agent) => agent.id === selectedAgentId) || agents[0];
  const escape = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" })[character]);
  const isOpen = (id) => document.getElementById(id)?.classList.contains("open") === true;
  const byFieldLabel = (scope, label, selector = "input, select, textarea") => Array.from((scope || document).querySelectorAll("label.field")).find((node) => node.querySelector("span")?.textContent?.trim() === label)?.querySelector(selector);
  const setSwitch = (label, checked, disabled) => {
    const node = $bridge('[aria-label="' + label + '"]');
    if (!node) return;
    node.setAttribute("aria-checked", String(Boolean(checked)));
    if (typeof disabled === "boolean") node.disabled = disabled;
    if (label === "使用系统代理") syncSystemProxySelection(Boolean(checked));
  };
  function currentSystemProxySelection() {
    return $bridge('[aria-label="使用系统代理"]')?.getAttribute("aria-checked") === "true";
  }
  function syncSystemProxySelection(checked) {
    const toggle = $bridge('[aria-label="使用系统代理"]');
    const description = toggle?.closest(".option-row")?.querySelector("small");
    if (description) description.textContent = "当前选择：" + (checked ? "系统代理" : "直连");
  }
  function clearConnectionPathTest() {
    const result = $bridge("#providerConnectionTestResult");
    if (!result) return;
    result.hidden = true;
    result.querySelector("b")?.replaceChildren(document.createTextNode("尚未测试"));
    result.querySelector("small")?.replaceChildren(document.createTextNode("测试会获取模型，并仅给出直连或系统代理建议。"));
  }
  const strategyToUi = (value) => ({ "round-robin": "轮询", priority: "优先级", "least-used": "最低使用", random: "随机", sequential: "顺序" }[value] || "顺序");
  const strategyFromUi = (value) => ({ "轮询": "round-robin", "优先级": "priority", "最低使用": "least-used", "随机": "random", "顺序": "sequential" }[value] || "sequential");
  const selectedProviderId = () => editingProviderId || currentAgent()?.provider || "";
  let keyPoolHealthSyncRequestId = "";
  let keyPoolHealthSyncTimer = 0;
  function stopProviderKeyPoolHealthSync() {
    if (!keyPoolHealthSyncTimer) return;
    window.clearInterval(keyPoolHealthSyncTimer);
    keyPoolHealthSyncTimer = 0;
  }
  function syncOpenProviderKeyPoolHealth() {
    if (renderSuspended || !isOpen("providerOverlay") || document.visibilityState === "hidden") {
      stopProviderKeyPoolHealthSync();
      return;
    }
    if (keyPoolHealthSyncRequestId) return;
    const providerId = selectedProviderId();
    if (!providerId) return;
    keyPoolHealthSyncRequestId = send("sync-provider-key-pool-health", { providerId });
  }
  function startProviderKeyPoolHealthSync() {
    if (
      keyPoolHealthSyncTimer ||
      renderSuspended ||
      !isOpen("providerOverlay") ||
      document.visibilityState === "hidden" ||
      !selectedProviderId()
    ) return;
    syncOpenProviderKeyPoolHealth();
    keyPoolHealthSyncTimer = window.setInterval(syncOpenProviderKeyPoolHealth, 500);
  }
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      stopProviderKeyPoolHealthSync();
      cancelReleaseChampionRefresh();
    } else if (isOpen("providerOverlay")) {
      startProviderKeyPoolHealthSync();
    }
  });
  window.addEventListener("pagehide", () => {
    stopProviderKeyPoolHealthSync();
    cancelReleaseChampionRefresh();
  }, { once: true });
  const REQUESTS_PER_PAGE = 20;
  const REQUEST_PAGE_LIMIT = 10;
  const DEFAULT_KEY_PRIORITY = 5;
  let fetchedModelIds = [];
  let requestScopeId = "";
  let requestPage = 1;
  let lastTrendContextKey = "";
  let latestReleaseChampion = null;
  let latestReleaseChampionError = "";
  let latestReleaseChampionContextKey = "";
  let lastReleaseChampionContextKey = "";
  let releaseChampionSequence = 0;
  const RELEASE_CHAMPION_AUTO_REFRESH_MS = 5_000;
  let releaseChampionRefreshTimer = 0;
  let releaseChampionRequestInFlight = false;
  let releaseChampionPendingRefresh = false;
  let releaseChampionPendingImmediate = false;
  let draggingProviderId = "";
  let keyPointerDrag = null;
  let healthProbeProviderId = "";
  let healthProbeKeyIds = [];
  let healthProbeTarget = "current";
  let healthProbeModels = [];
  let healthProbePrompt = "hi";

  const trendController = () => window.__atoapiTrend;

  function exactHistoricalScope(scope) {
    const candidate = scope?.historicalScope;
    if (!candidate) return {};
    const fields = [
      "provider_realm_id",
      "model",
      "client_channel",
      "upstream_channel",
      "upstream_call_kind"
    ];
    if (!fields.every((field) => String(candidate[field] || "").trim())) return {};
    return Object.fromEntries(fields.map((field) => [field, String(candidate[field]).trim()]));
  }

  function exactHistoricalTrendScope(scope) {
    const exact = exactHistoricalScope(scope);
    const stablePrefix = String(scope?.historicalScope?.stable_prefix_cohort_id || "").trim();
    return stablePrefix && Object.keys(exact).length
      ? { ...exact, stable_prefix_cohort_id: stablePrefix }
      : exact;
  }

  function historicalRouteScopeKey(scope) {
    const exact = exactHistoricalScope(scope);
    return [
      exact.provider_realm_id || "aggregate",
      exact.model || "",
      exact.client_channel || "",
      exact.upstream_channel || "",
      exact.upstream_call_kind || ""
    ].join("/");
  }

  function historicalScopeKey(scope) {
    const exact = exactHistoricalTrendScope(scope);
    return [
      historicalRouteScopeKey(scope),
      exact.stable_prefix_cohort_id || ""
    ].join("/");
  }

  function syncTrendController(loadWhenChanged = false) {
    const controller = trendController();
    if (!controller) return;
    const metricState = host.state?.metrics || {};
    const scope = activeRequestScope(metricState);
    const agent = currentAgent();
    const contextKey = [
      agent?.sourceId || agent?.id || "",
      scope?.id || "all",
      historicalRouteScopeKey(scope),
      metricState.includeColdStarts !== false ? "cold-in" : "cold-out",
      metricState.includeCompactions !== false ? "compact-in" : "compact-out"
    ].join("|");
    controller.setContextKey(contextKey);
    controller.syncScopes(metricState.scopes || [], scope?.id || "all");
    const overviewVisible = !$bridge("#metricsView")?.hidden && !$bridge("#overviewPanel")?.hidden;
    if (loadWhenChanged && overviewVisible && contextKey !== lastTrendContextKey) {
      controller.request("context");
    }
    lastTrendContextKey = contextKey;
    if (!overviewVisible || !releaseChampionAutoRefreshAllowed()) return;
    const championContextKey = releaseChampionContext(metricState, scope, agent);
    const needsImmediateChampionLoad = !hasReleaseChampionResult(championContextKey);
    if (needsImmediateChampionLoad) {
      requestReleaseChampion(true);
    } else if (loadWhenChanged) {
      scheduleReleaseChampionRefresh();
    }
  }

  function releaseChampionContext(metricState, scope, agent) {
    return [
      agent?.sourceId || agent?.id || "",
      scope?.providerId || "all",
      historicalRouteScopeKey(scope),
      metricState.includeColdStarts !== false ? "cold-in" : "cold-out",
      metricState.includeCompactions !== false ? "compact-in" : "compact-out"
    ].join("|");
  }

  function hasReleaseChampionResult(contextKey) {
    return contextKey === latestReleaseChampionContextKey &&
      Boolean(latestReleaseChampion || latestReleaseChampionError);
  }

  function releaseChampionAutoRefreshAllowed() {
    return !renderSuspended &&
      document.visibilityState === "visible" &&
      !$bridge("#metricsView")?.hidden &&
      !$bridge("#overviewPanel")?.hidden;
  }

  function cancelReleaseChampionRefresh() {
    if (!releaseChampionRefreshTimer) return;
    window.clearTimeout(releaseChampionRefreshTimer);
    releaseChampionRefreshTimer = 0;
  }

  function requestReleaseChampion(force = false) {
    const metricState = host.state?.metrics || {};
    const scope = activeRequestScope(metricState);
    const agent = currentAgent();
    const contextKey = releaseChampionContext(metricState, scope, agent);
    if (!force && hasReleaseChampionResult(contextKey)) return;
    if (releaseChampionRequestInFlight) {
      // A changed scope must be read as soon as the older request settles.
      // Repeated metrics deltas for the same scope deliberately coalesce.
      if (contextKey !== lastReleaseChampionContextKey) {
        releaseChampionPendingRefresh = true;
        releaseChampionPendingImmediate = true;
      }
      return;
    }
    cancelReleaseChampionRefresh();
    lastReleaseChampionContextKey = contextKey;
    const sequence = ++releaseChampionSequence;
    releaseChampionRequestInFlight = true;
    send("load-release-champion", {
      sequence,
      contextKey,
      input: {
        agent_id: agent?.sourceId || agent?.id || "",
        provider_id: scope?.providerId || null,
        include_cold_starts: metricState.includeColdStarts !== false,
        include_compactions: metricState.includeCompactions !== false,
        ...exactHistoricalTrendScope(scope)
      }
    });
  }

  function scheduleReleaseChampionRefresh() {
    const metricState = host.state?.metrics || {};
    const scope = activeRequestScope(metricState);
    const agent = currentAgent();
    const contextKey = releaseChampionContext(metricState, scope, agent);
    if (!hasReleaseChampionResult(contextKey)) {
      requestReleaseChampion(true);
      return;
    }
    if (releaseChampionRequestInFlight) {
      releaseChampionPendingRefresh = true;
      return;
    }
    if (releaseChampionRefreshTimer || !releaseChampionAutoRefreshAllowed()) return;
    releaseChampionRefreshTimer = window.setTimeout(() => {
      releaseChampionRefreshTimer = 0;
      if (!releaseChampionAutoRefreshAllowed()) return;
      requestReleaseChampion(true);
    }, RELEASE_CHAMPION_AUTO_REFRESH_MS);
  }

  trendController()?.setExternalLoader((query) => {
    const metricState = host.state?.metrics || {};
    const scope = activeRequestScope(metricState);
    const agent = currentAgent();
    send("load-metrics-trend", {
      sequence: query.sequence,
      rangeKey: query.rangeKey,
      input: {
        start_utc: query.startUtc,
        end_utc: query.endUtc,
        agent_id: agent?.sourceId || agent?.id || "",
        provider_id: scope?.providerId || null,
        include_cold_starts: metricState.includeColdStarts !== false,
        include_compactions: metricState.includeCompactions !== false,
        ...exactHistoricalTrendScope(scope)
      }
    });
  });

  trendController()?.setScopeHandler(() => {
    requestPage = 1;
    renderRequests();
    applyMetrics(host.state);
    syncTrendController(false);
  });

  function setKeyPoolEnabled(enabled) {
    const toggle = $bridge("#providerKeyPoolEnabled");
    const panel = $bridge("#providerKeys");
    if (toggle) toggle.setAttribute("aria-checked", String(Boolean(enabled)));
    if (panel) panel.classList.toggle("key-pool-disabled", !enabled);
  }

  function renderModelCandidates() {
    const panel = $bridge("#providerModelCandidatePanel");
    const select = $bridge("#providerModelCandidateSelect");
    const add = $bridge("#addSelectedModelMappingButton");
    if (!panel || !select || !add) return;
    const previous = select.value;
    const candidates = [...new Set(fetchedModelIds.map((value) => String(value || "").trim()).filter(Boolean))];
    panel.hidden = candidates.length === 0;
    select.innerHTML = '<option value="">选择模型后添加映射</option>' + candidates
      .map((model) => '<option value="' + escape(model) + '">' + escape(model) + '</option>')
      .join("");
    if (candidates.includes(previous)) select.value = previous;
    add.disabled = !select.value;
    select.onchange = () => { add.disabled = !select.value; };
    add.onclick = (event) => {
      event.preventDefault();
      const model = select.value.trim();
      if (!model) return;
      mappings.push({ request: model, actual: model, context: "", reasoning: "", supportedReasoningEfforts: [] });
      renderMappings();
      select.value = "";
      add.disabled = true;
      hydrateIcons();
    };
  }

  function channelLabel(channel) {
    if (channel === "responses") return "Responses";
    if (channel === "chat") return "Chat";
    if (channel === "anthropic") return "Anthropic";
    return "Auto";
  }

  function applyEditorData(providerId) {
    const isNewProvider = !providerId;
    const detail = isNewProvider ? null : host.state?.providerDetails?.[providerId] || null;
    const provider = isNewProvider ? null : providers.find((item) => item.id === providerId);
    const name = $bridge("#providerNameInput");
    const url = $bridge("#providerUrlInput");
    const key = $bridge("#providerApiKeyInput");
    const modelsUrl = $bridge("#providerModelsUrlInput");
    const userAgent = $bridge("#providerCustomUserAgentInput");
    const channel = $bridge("#providerChannelInput");
    const keyStrategy = $bridge("#providerKeys .form-grid select");
    const failureThreshold = $bridge("#providerKeys .form-grid input");
    editingProviderId = isNewProvider ? null : providerId;
    fetchedModelIds = [];
    if (name) name.value = detail?.name || provider?.name || "新上游";
    if (url) url.value = detail?.base_url || provider?.url || "";
    setSecretInputState(key, Boolean(detail?.has_api_key), "输入上游 API Key", true);
    if (modelsUrl) modelsUrl.value = detail?.models_url || "";
    if (userAgent) userAgent.value = detail?.custom_user_agent || "";
    if (channel) channel.value = detail?.channel_mode === "auto" ? "auto" : (detail?.channel || "responses");
    replace(mappings, (detail?.models || []).map((model) => ({
      request: model.request_model_id || model.id,
      actual: model.id,
      context: model.context_window ? String(model.context_window) : "",
      reasoning: model.reasoning_effort_override_enabled ? (model.reasoning_effort || "") : "",
      supportedReasoningEfforts: model.supported_reasoning_efforts || []
    })));
    replace(keyPool, (detail?.keys || []).map((item, index) => ({
      id: item.id || "key-" + index,
      name: item.alias || "Key " + (index + 1),
      secret: "",
      hasSavedSecret: item.has_saved_secret === true,
      enabled: item.enabled !== false,
      priority: item.priority || 0,
      status: item.status || "未读取"
    })));
    if (keyStrategy) {
      if (!Array.from(keyStrategy.options).some((option) => option.value === "随机")) {
        const option = document.createElement("option");
        option.value = "随机";
        option.textContent = "随机";
        keyStrategy.append(option);
      }
      const strategyLabel = strategyToUi(detail?.key_pool?.strategy);
      if (!Array.from(keyStrategy.options).some((option) => option.value === strategyLabel)) {
        const option = document.createElement("option");
        option.value = strategyLabel;
        option.textContent = strategyLabel;
        keyStrategy.append(option);
      }
      keyStrategy.value = strategyLabel;
    }
    normalizeKeyPriorities();
    if (failureThreshold) failureThreshold.value = String(detail?.key_pool?.failure_threshold ?? 3);
    setSwitch("使用系统代理", detail?.use_system_proxy ?? true);
    clearConnectionPathTest();
    setSwitch("prompt cache retention", detail?.prompt_cache_retention_enabled ?? true);
    setSwitch("大请求体 gzip", detail?.request_body_gzip_enabled ?? true);
    setSwitch("非 SSE compact 兼容", detail?.non_sse_compact_compat_enabled || false);
    const autoCompactLimit = $bridge("#providerAutoCompactTokenLimit");
    if (autoCompactLimit) {
      autoCompactLimit.value = detail?.auto_compact_token_limit
        ? String(detail.auto_compact_token_limit)
        : "";
    }
    setKeyPoolEnabled(detail?.key_pool?.enabled === true);
    applyNetworkDiagnostic(detail?.network_diagnostic);
    renderModelCandidates();
    renderMappings();
    renderKeyPool();
    hydrateIcons();
  }

  function normalizeKeyPriorities() {
    const strategy = strategyFromUi($bridge("#providerKeys .form-grid select")?.value || "顺序");
    keyPool.forEach((key) => {
      const priority = Number(key.priority);
      if (strategy === "sequential" || !Number.isFinite(priority) || priority < 0) {
        key.priority = DEFAULT_KEY_PRIORITY;
      }
    });
  }

  function renderEditedKeyPool() {
    renderKeyPool();
    hydrateIcons();
  }

  function toggleKeyEnabled(keyId) {
    const key = keyPool.find((item) => item.id === keyId);
    if (!key) return;
    key.enabled = key.enabled === false;
    key.enabledDirty = true;
    renderEditedKeyPool();
  }

  function mergeKeyPoolHealth(keyPoolHealth) {
    const providerId = String(keyPoolHealth?.providerId || "");
    if (!providerId || providerId !== selectedProviderId()) return;
    const persistedKeys = Array.isArray(keyPoolHealth?.keys) ? keyPoolHealth.keys : [];
    const persistedById = new Map(persistedKeys.map((key) => [key?.id, key]));
    let changed = false;
    keyPool.forEach((key) => {
      const persisted = persistedById.get(key.id);
      if (!persisted) return;
      const status = String(persisted.status || key.status || "unknown");
      if (key.status !== status) {
        key.status = status;
        changed = true;
      }
      // A locally toggled Key is still a draft edit. Keep that intent until Save.
      if (!key.enabledDirty && typeof persisted.enabled === "boolean" && key.enabled !== persisted.enabled) {
        key.enabled = persisted.enabled;
        changed = true;
      }
    });
    if (changed) renderEditedKeyPool();
  }

  function addBlankKey() {
    keyPool.push({
      id: "key-" + Date.now() + "-" + Math.random().toString(16).slice(2),
      name: "新 Key " + (keyPool.length + 1),
      secret: "",
      enabled: true,
      priority: DEFAULT_KEY_PRIORITY,
      status: "待填写",
      isNew: true
    });
    normalizeKeyPriorities();
    renderEditedKeyPool();
    const added = keyPool[keyPool.length - 1];
    const input = added ? document.getElementById("keySecret-" + added.id) : null;
    input?.focus();
  }

  function addBulkKeysFromBridge() {
    const input = $bridge("#bulkKeyInput");
    if (!input) return;
    const candidates = [...new Set(input.value.split(/[\s,;，；]+/u).map((value) => value.trim()).filter(Boolean))];
    if (!candidates.length) {
      showToast("请输入至少一个 Key");
      return;
    }
    const existing = new Set(keyPool.map((key) => key.secret).filter(Boolean));
    const additions = candidates.filter((secret) => !existing.has(secret));
    if (!additions.length) {
      showToast("没有可添加的新 Key");
      return;
    }
    additions.forEach((secret, index) => {
      keyPool.push({
        id: "key-" + Date.now() + "-" + index + "-" + Math.random().toString(16).slice(2),
        name: "Key " + (keyPool.length + 1),
        secret,
        enabled: true,
        priority: DEFAULT_KEY_PRIORITY,
        status: "未测试",
        isNew: true
      });
    });
    input.value = "";
    normalizeKeyPriorities();
    renderEditedKeyPool();
    showToast("已添加 " + additions.length + " 个 Key");
  }

  function removeKeyFromBridge(keyId) {
    const index = keyPool.findIndex((key) => key.id === keyId);
    if (index < 0) return;
    keyPool.splice(index, 1);
    normalizeKeyPriorities();
    renderEditedKeyPool();
  }

  function moveKeyFromBridge(keyId, direction) {
    const index = keyPool.findIndex((key) => key.id === keyId);
    const target = index + direction;
    if (index < 0 || target < 0 || target >= keyPool.length) return;
    [keyPool[index], keyPool[target]] = [keyPool[target], keyPool[index]];
    normalizeKeyPriorities();
    renderEditedKeyPool();
  }

  function reorderKeyFromBridge(draggedId, targetId) {
    const fromIndex = keyPool.findIndex((key) => key.id === draggedId);
    const targetIndex = keyPool.findIndex((key) => key.id === targetId);
    if (fromIndex < 0 || targetIndex < 0 || fromIndex === targetIndex) return;
    const [dragged] = keyPool.splice(fromIndex, 1);
    keyPool.splice(targetIndex, 0, dragged);
    normalizeKeyPriorities();
    renderEditedKeyPool();
  }

  function clearProviderDragState() {
    draggingProviderId = "";
    document.querySelectorAll(".provider-row.is-dragging, .provider-row.is-drag-over").forEach((row) => {
      row.classList.remove("is-dragging", "is-drag-over");
    });
  }

  function reorderProvidersFromBridge(draggedId, targetId) {
    const fromIndex = providers.findIndex((provider) => provider.id === draggedId);
    const targetIndex = providers.findIndex((provider) => provider.id === targetId);
    if (fromIndex < 0 || targetIndex < 0 || fromIndex === targetIndex) return;
    const providerIds = providers.map((provider) => provider.id);
    const [dragged] = providerIds.splice(fromIndex, 1);
    providerIds.splice(targetIndex, 0, dragged);
    const agent = currentAgent();
    send("reorder-providers", {
      agentId: agent?.sourceId || agent?.id || "",
      providerIds
    });
  }

  function serializeEditor() {
    const selected = currentAgent();
    const detail = host.state?.providerDetails?.[editingProviderId] || {};
    const modeValue = $bridge("#providerChannelInput")?.value || "auto";
    const channel = modeValue === "auto" ? (detail.channel || "responses") : modeValue;
    const switchState = (label, fallback) => {
      const node = $bridge('[aria-label="' + label + '"]');
      return node ? node.getAttribute("aria-checked") === "true" : fallback;
    };
    const mappingRows = Array.from(document.querySelectorAll("#mappingList .mapping-row"));
    const formModels = mappingRows.map((row) => {
      const inputs = row.querySelectorAll("input");
      const reasoning = row.querySelector("select")?.value || null;
      return {
        id: inputs[1]?.value.trim() || "",
        request_model_id: inputs[0]?.value.trim() || null,
        context_window: Number(inputs[2]?.value) || null,
        reasoning_effort: reasoning || null
      };
    }).filter((mapping) => mapping.id);
    const strategy = strategyFromUi($bridge("#providerKeys .form-grid select")?.value || "顺序");
    const failureThreshold = Number($bridge("#providerKeys .form-grid input")?.value) || 3;
    const autoCompactValue = $bridge("#providerAutoCompactTokenLimit")?.value.trim() || "";
    const autoCompactTokenLimit = autoCompactValue
      ? Math.trunc(Number(autoCompactValue))
      : null;
    if (autoCompactValue && (!Number.isFinite(autoCompactTokenLimit) || autoCompactTokenLimit <= 0)) {
      throw new Error("自动压缩阈值必须是大于 0 的 token 整数");
    }
    const orderedKeys = keyPool.filter((key) => !key.isNew || String(key.secret || "").trim());
    const keys = orderedKeys.map((key) => {
      const priority = Number(key.priority);
      return {
        id: key.id,
        alias: key.name,
        key: key.secret || undefined,
        enabled: key.enabled !== false,
        // List order is authoritative for the default sequential strategy.
        // Preserve a manually chosen priority without rewriting it on reorder.
        priority: Number.isFinite(priority) && priority >= 0
          ? Math.trunc(priority)
          : DEFAULT_KEY_PRIORITY
      };
    });
    return {
      agentId: selected?.sourceId || selected?.id || "",
      id: editingProviderId || null,
      name: $bridge("#providerNameInput")?.value.trim() || "未命名上游",
      base_url: $bridge("#providerUrlInput")?.value.trim() || "",
      models_url: $bridge("#providerModelsUrlInput")?.value.trim() || "",
      api_key: $bridge("#providerApiKeyInput")?.value || "",
      custom_user_agent: $bridge("#providerCustomUserAgentInput")?.value.trim() || "",
      channel,
      channel_mode: modeValue === "auto" ? "auto" : "manual",
      use_system_proxy: switchState("使用系统代理", detail.use_system_proxy ?? true),
      prompt_cache_retention_enabled: switchState("prompt cache retention", detail.prompt_cache_retention_enabled ?? true),
      request_body_gzip_enabled: switchState("大请求体 gzip", detail.request_body_gzip_enabled ?? true),
      non_sse_compact_compat_enabled: switchState("非 SSE compact 兼容", detail.non_sse_compact_compat_enabled || false),
      auto_compact_token_limit: autoCompactTokenLimit,
      models: formModels,
      keys,
      key_pool: {
        enabled: switchState("启用多 Key 池", false),
        strategy,
        failure_threshold: failureThreshold,
        recovery_minutes: Number(detail?.key_pool?.recovery_minutes) || 10
      }
    };
  }

  function applyNetworkDiagnostic(diagnostic) {
    const description = $bridge("#providerGeneral .form-section:nth-of-type(2) .form-section-head p");
    if (!description) return;
    if (!diagnostic?.paths?.length) {
      description.textContent = "点击测试会校验连通并获取模型；结果只给出直连或系统代理建议，不会改开关。";
      return;
    }
    description.textContent = diagnostic.paths.map((path) => {
      const label = path.path === "direct" ? "直连" : path.path === "system-proxy" ? "系统代理" : path.path === "explicit-proxy" ? "显式代理" : path.path;
      const protocol = path.http_version ? " · " + path.http_version : "";
      return label + " " + (path.ok ? path.elapsed_ms + "ms" + protocol : "失败");
    }).join(" · ");
  }

  function applyConnectionPathTest(result) {
    const resultBand = $bridge("#providerConnectionTestResult");
    if (!resultBand) return;
    if (result?.paths?.length) applyNetworkDiagnostic({ paths: result.paths });
    const recommendedSystemProxy = result?.recommended_use_system_proxy === true;
    const recommendedLabel = recommendedSystemProxy ? "系统代理" : "直连";
    const currentLabel = currentSystemProxySelection() ? "系统代理" : "直连";
    const selected = result?.paths?.find((path) => recommendedSystemProxy
      ? path.path === "system-proxy"
      : path.path === "direct");
    const title = resultBand.querySelector("b");
    const detail = resultBand.querySelector("small");
    if (title) title.textContent = result?.ok ? "测试通过 · 推荐" + recommendedLabel : "测试未通过";
    if (detail) {
      if (result?.ok) {
        const timing = selected ? " · " + selected.elapsed_ms + "ms" + (selected.http_version ? " · " + selected.http_version : "") : "";
        const modelCount = Number(result.models_count || 0);
        detail.textContent = "已获取 " + modelCount + " 个模型；当前仍为" + currentLabel + "，开关不会被测试自动修改，请手动决定是否切换。" + timing;
      } else {
        detail.textContent = result?.message || "直连和系统代理均未返回有效模型列表。";
      }
    }
    resultBand.hidden = false;
  }

  function healthProbeModeLabel(mode) {
    return ({
      // minimal_cost is a legacy wire value; never expose its implementation
      // policy in the UI. New probes always default to Responses streaming.
      minimal_cost: "Responses API（流式）",
      responses_streaming: "Responses API（流式）",
      chat_streaming: "Chat Completions（流式）",
      chat_json: "Chat Completions（JSON）",
      responses_json: "Responses API（JSON）",
      anthropic_streaming: "Anthropic Messages（流式）",
      anthropic_json: "Anthropic Messages（JSON）"
    }[mode] || "Responses API（流式）");
  }

  function healthProbeKeyLabel(providerId, keyId) {
    if (!keyId) return "连接信息 Key";
    const key = host.state?.providerDetails?.[providerId]?.keys?.find((item) => item.id === keyId);
    const masked = key?.preview || ("Key · …" + String(keyId).slice(-4));
    return key?.alias ? key.alias + " · " + masked : masked;
  }

  function healthProbeSeedModels(providerId) {
    const detail = host.state?.providerDetails?.[providerId] || {};
    return (detail.models || []).map((item) => String(item.request_model_id || item.id || "").trim()).filter(Boolean);
  }

  function renderHealthProbeModels() {
    const select = $bridge("#healthProbeModelInput");
    if (!select) return;
    const previous = select.value;
    const candidates = [...new Set(healthProbeModels.map((item) => String(item || "").trim()).filter(Boolean))];
    select.innerHTML = candidates.length
      ? candidates.map((model) => '<option value="' + escape(model) + '">' + escape(model) + '</option>').join("")
      : '<option value="">正在获取模型…</option>';
    if (candidates.includes(previous)) select.value = previous;
    else if (candidates.length) select.value = candidates[0];
  }

  function resetHealthProbeResult() {
    const result = $bridge("#healthProbeResult");
    const summary = $bridge("#healthProbeSummary");
    if (result) result.hidden = true;
    if (summary) summary.textContent = "准备就绪：每个启用 Key 只发送一次独立测活请求。";
    const list = $bridge("#healthProbeResultList");
    if (list) list.replaceChildren();
  }

  function openHealthProbe(providerId, target = "current", keyIds = []) {
    const provider = providers.find((item) => item.id === providerId);
    if (!provider) {
      showToast("未找到待测活上游");
      return;
    }
    healthProbeProviderId = providerId;
    healthProbeTarget = target === "all_enabled" || target === "selected" ? target : "current";
    healthProbeKeyIds = Array.isArray(keyIds) ? keyIds.filter(Boolean) : [];
    healthProbeModels = healthProbeSeedModels(providerId);
    const title = $bridge("#healthProbeTitle");
    const subtitle = $bridge("#healthProbeSubtitle");
    const mode = $bridge("#healthProbeModeInput");
    const prompt = $bridge("#healthProbePromptInput");
    if (title) title.textContent = healthProbeTarget === "current" ? "当前 Key 测活" : (healthProbeTarget === "all_enabled" ? "全部 Key 测活" : "指定 Key 测活");
    if (subtitle) subtitle.textContent = provider.name + " · " + (healthProbeTarget === "current" ? "当前可用 Key" : (healthProbeTarget === "all_enabled" ? "全部启用 Key" : "指定 Key"));
    if (mode) {
      mode.value = "responses_streaming";
    }
    if (prompt) prompt.value = healthProbePrompt || "hi";
    renderHealthProbeModels();
    resetHealthProbeResult();
    openOverlay("healthProbeOverlay");
    hydrateIcons();
    send("fetch-health-probe-models", { providerId });
  }

  function runHealthProbe() {
    const model = $bridge("#healthProbeModelInput")?.value.trim() || "";
    const selectedMode = $bridge("#healthProbeModeInput")?.value || "responses_streaming";
    // Accept old DOM/config values without exposing the old implementation
    // label or changing the new default wire shape.
    const mode = selectedMode === "minimal_cost" ? "responses_streaming" : selectedMode;
    const prompt = $bridge("#healthProbePromptInput")?.value ?? "hi";
    healthProbePrompt = prompt || "hi";
    if (!healthProbeProviderId) {
      showToast("请先选择上游");
      return;
    }
    if (!model) {
      showToast("请等待模型获取完成或选择模型");
      return;
    }
    const summary = $bridge("#healthProbeSummary");
    if (summary) summary.textContent = "正在测活：请求不会进入正常 Codex 转发链路。";
    send("run-health-probe", {
      providerId: healthProbeProviderId,
      keyIds: healthProbeKeyIds,
      target: healthProbeTarget,
      modelId: model,
      mode,
      prompt
    });
  }

  function applyHealthProbeModels(models) {
    if (!Array.isArray(models)) return;
    healthProbeModels = models.map((item) => String(item?.id || "").trim()).filter(Boolean);
    renderHealthProbeModels();
  }

  function applyHealthProbeResult(result) {
    if (!result || String(result.provider_id || "") !== healthProbeProviderId) return;
    const results = Array.isArray(result.results) ? result.results : [];
    const passed = results.filter((item) => item?.ok === true).length;
    const summary = $bridge("#healthProbeSummary");
    const resultPanel = $bridge("#healthProbeResult");
    const list = $bridge("#healthProbeResultList");
    if (summary) summary.textContent = "测活完成：" + passed + "/" + results.length + " 通过 · 总耗时 " + Number(result.elapsed_ms || 0) + "ms";
    if (list) {
      list.innerHTML = results.map((item) => {
        const ok = item?.ok === true;
        const key = healthProbeKeyLabel(healthProbeProviderId, item?.key_id);
        const status = item?.status ? "HTTP " + item.status : "无 HTTP 状态";
        const transport = item?.http_version ? " · " + item.http_version : "";
        const elapsed = Number(item?.elapsed_ms || 0);
        const firstResponse = Number.isFinite(Number(item?.first_response_ms))
          ? Number(item.first_response_ms)
          : null;
        const timing = "轻量测活 " + elapsed + "ms · 首个上游事件 " + (firstResponse == null ? "未返回" : firstResponse + "ms");
        const detail = status + transport + " · " + String(item?.message || "");
        const preview = item?.response_preview ? '<small class="health-probe-preview">' + escape(item.response_preview) + '</small>' : "";
        return '<article class="health-probe-row ' + (ok ? "is-pass" : "is-fail") + '">'
          + '<span class="health-probe-icon">' + icon(ok ? "circle-check" : "circle-x", 16) + '</span>'
          + '<div><b>' + escape(key) + " · " + (ok ? "通过" : "不通过") + '</b><small>'
          + '<span class="health-probe-timing">' + escape(timing) + '</span><span>' + escape(detail)
          + '</span>'
          + '</small>' + preview + '</div></article>';
      }).join("") || '<div class="health-probe-empty">没有可测活的启用 Key。</div>';
    }
    if (resultPanel) resultPanel.hidden = false;
    hydrateIcons();
  }

  function balanceProbeLabel(result) {
    if (!result) return "余额未探测";
    if (result.ok && result.balance) return "余额 " + balanceProbeDisplayValue(result.balance);
    if (result.supported === false) return "余额不可查";
    return "余额探测失败";
  }

  function balanceProbeDisplayValue(value) {
    const text = String(value ?? "").trim();
    if (!text || text === "无限额度") return text;
    const normalized = text.replace(/,/g, "");
    return /^[+-]?\d+(?:\.\d+)?$/.test(normalized)
      ? Number(normalized).toFixed(2)
      : text;
  }

  function balanceProbeTone(result) {
    if (!result?.ok || !result.balance) return "is-unknown";
    if (String(result.balance) === "无限额度") return "is-unlimited";
    const numeric = Number(String(result.balance).replace(/,/g, "").match(/[+-]?\d+(?:\.\d+)?/)?.[0]);
    if (Number.isFinite(numeric) && numeric <= 0) return "is-depleted";
    return "is-healthy";
  }

  function keyPoolCountBadge(provider) {
    if (provider?.keyPoolEnabled !== true) return "";
    const count = Number.isFinite(Number(provider?.keyPoolCount))
      ? Math.max(0, Math.floor(Number(provider.keyPoolCount)))
      : 0;
    const label = count + " 个 Key";
    return '<span class="key-pool-count-chip" title="已启用多 Key 池：共 ' + escape(label) + '">' + icon("key-round", 12) + '<span>' + escape(label) + '</span></span>';
  }

  function applyBalanceProbe(result) {
    if (!result?.provider_id) return;
    const provider = providers.find((item) => item.id === result.provider_id);
    if (!provider) return;
    provider.balance = result;
    renderProviders();
    hydrateIcons();
  }

  function activeRequestScope(metric) {
    const scopes = Array.isArray(metric?.scopes) ? metric.scopes : [];
    if (!scopes.length) return {
      id: "all",
      label: "全部",
      successfulRequests: 0,
      errors: 0,
      compactionRequests: 0,
      coldStartRequests: 0,
      cacheRate: "—"
    };
    if (!requestScopeId || !scopes.some((scope) => scope.id === requestScopeId)) {
      const preferred = currentAgent()?.provider ? "provider:" + currentAgent().provider : "all";
      requestScopeId = scopes.some((scope) => scope.id === preferred) ? preferred : "all";
      requestPage = 1;
    }
    return scopes.find((scope) => scope.id === requestScopeId) || scopes[0];
  }

  function applyMetrics(nextState) {
    const metricState = nextState?.metrics || {};
    const metric = activeRequestScope(metricState);
    const hasErrors = typeof metric.errors === "number" && metric.errors > 0;
    const cells = Array.from(document.querySelectorAll(".metric-strip .metric-cell b"));
    const labels = Array.from(document.querySelectorAll(".metric-strip .metric-cell span"));
    if (cells.length >= 4) {
      cells[0].textContent = metric.cacheRate || "—";
      cells[1].textContent = metric.inputTokens || "0";
      cells[2].textContent = metric.cachedTokens || "0";
      cells[3].textContent = hasErrors ? metric.successfulRequests + " / " + metric.errors : String(metric.successfulRequests ?? 0);
    }
    if (labels[0]) labels[0].textContent = metric.label === "全部" ? "历史命中率" : metric.label + " 命中率";
    if (labels[1]) labels[1].textContent = "累计输入";
    if (labels[2]) labels[2].textContent = "累计命中";
    if (labels[3]) labels[3].textContent = hasErrors ? "成功 / error" : "成功请求";
    const includeSpecialRequests = metricState.includeColdStarts !== false &&
      metricState.includeCompactions !== false;
    const successDetails = $bridge("#successMetricDetails");
    if (successDetails) {
      successDetails.hidden = !includeSpecialRequests;
      successDetails.textContent = includeSpecialRequests
        ? "压缩 " + (metric.compactionRequests ?? "—") + " · 冷启动 " + (metric.coldStartRequests ?? "—")
        : "";
    }
    const rate = metric.cacheRate || "—";
    const overview = $bridge("#overviewPanel");
    if (overview) {
      const strong = overview.querySelector(".hit-rate-line strong");
      if (strong) strong.textContent = rate;
      const summary = overview.querySelector(".hit-rate-line span");
      if (summary) summary.innerHTML = escape(metric.label || "全部") + " · " + escape(metric.successfulRequests ?? 0) + " 条成功记录" + (hasErrors ? "<br />" + escape(metric.errors) + " error" : "");
      const progress = overview.querySelector("progress");
      const numericRate = Number.parseFloat(String(rate).replace("%", ""));
      if (progress && Number.isFinite(numericRate)) { progress.value = numericRate; progress.setAttribute("aria-label", "缓存命中率 " + rate); }
      const stats = Array.from(overview.querySelectorAll(".micro-stat b"));
      if (stats.length >= 3) {
        stats[0].textContent = metric.cacheShortfall || "0";
        stats[1].textContent = metric.cacheAvoidable || "0";
        stats[2].textContent = metric.cacheNewTail || "0";
      }
      setSwitch("智能缓存", metricState.cacheEnabled !== false);
      setSwitch("计入冷启动和压缩", includeSpecialRequests);
      setSwitch("提示详细错误", metricState.showDetailedErrors === true);
    }
    applyReleaseChampion(metricState);
    const dock = $bridge("#cacheButton");
    if (dock) {
      const values = dock.querySelectorAll(".utility-stat");
      if (values[0]) values[0].innerHTML = "命中率 <strong>" + escape(rate) + "</strong>";
      if (values[1]) values[1].textContent = hasErrors ? metric.successfulRequests + " 成功 · " + metric.errors + " error" : metric.successfulRequests + " 成功";
    }
    const body = $bridge("#providersPanel tbody");
    if (body && Array.isArray(metricState.providerRows)) {
      body.innerHTML = metricState.providerRows.map((row) => "<tr><td>" + escape(row.provider) + "</td><td>" + escape(row.successful) + "</td><td>" + escape(row.input) + "</td><td>" + escape(row.cached) + "</td><td>" + escape(row.ttft) + "</td><td>" + escape(row.ratio) + "</td></tr>").join("") || "<tr><td colspan=\"6\">暂无上游统计</td></tr>";
    }
  }

  function applyReleaseChampion(metricState) {
    const banner = $bridge("#releaseChampionSummary");
    if (!banner) return;
    const scope = activeRequestScope(metricState);
    const agent = currentAgent();
    const contextKey = releaseChampionContext(metricState, scope, agent);
    const title = banner.querySelector("b");
    const details = banner.querySelector("small");
    const hasLoadedCurrentContext = contextKey === latestReleaseChampionContextKey;
    const snapshot = hasLoadedCurrentContext ? latestReleaseChampion : null;
    const currentError = hasLoadedCurrentContext ? latestReleaseChampionError : "";
    banner.className = "release-champion-summary is-pending";
    if (!snapshot) {
      if (title) title.textContent = currentError || "读取可比版本 cohort…";
      if (details) details.textContent = "同 Provider · Key realm · 模型 · 请求族；历史旧桶不会冒充冠军";
      return;
    }
    const status = String(snapshot.status || "awaiting_current_cohort");
    const delta = Number(snapshot.delta_cache_hit_rate);
    const positivePp = Number.isFinite(delta) ? (delta * 100).toFixed(3) + "pp" : "";
    const current = snapshot.current || null;
    const champion = snapshot.champion || null;
    const statuses = {
      improving: ["is-improving", "正优化 · 超过冠军 +" + positivePp],
      regressed: ["is-regressed", "负优化 · 低于冠军 " + Math.abs(delta * 100).toFixed(3) + "pp"],
      tied: ["is-tied", "持平 · 与冠军一致"],
      insufficient_current_samples: ["is-pending", "样本积累中 · 暂不判定"],
      no_comparable_champion: ["is-pending", "不可比 · 尚无同 cohort 冠军"],
      awaiting_current_cohort: ["is-pending", "等待当前 cohort"],
      legacy_history_unattributed: ["is-pending", "历史未归属版本 · 不冒充冠军"],
      incomplete_filter: ["is-pending", "筛选统计不完整 · 暂不判定"]
    };
    const display = statuses[status] || statuses.awaiting_current_cohort;
    banner.className = "release-champion-summary " + display[0];
    if (title) title.textContent = display[1];
    if (details) {
      if (current && champion) {
        const currentRate = (Number(current.values?.cache_hit_rate || 0) * 100).toFixed(3) + "%";
        const championRate = (Number(champion.values?.cache_hit_rate || 0) * 100).toFixed(3) + "%";
        details.textContent = "当前 v" + current.app_version + " " + currentRate + " · 冠军 v" + champion.app_version + " " + championRate + " · 同 Provider / Key realm / 模型 / 请求族";
      } else {
        details.textContent = String(snapshot.reason || "等待可验证的同 cohort 数据");
      }
    }
  }

  function applySettings(nextState) {
    if (isOpen("settingsOverlay")) return;
    const settings = nextState?.settings;
    if (!settings) return;
    const proxy = $bridge("#settingsProxy");
    const app = $bridge("#settingsApp");
    const appVersion = $bridge("#settingsAppVersion");
    const host = byFieldLabel(proxy, "监听地址");
    const port = byFieldLabel(proxy, "端口");
    const localKey = $bridge("#settingsLocalKeyInput");
    const upstreamProxyUrl = $bridge("#settingsUpstreamProxyUrlInput");
    const defaultChannel = byFieldLabel(app, "默认通道", "select");
    if (host) host.value = settings.host || "127.0.0.1";
    if (port) port.value = settings.port || "18883";
    setSecretInputState(localKey, Boolean(settings.hasLocalKey), "输入本地 Key", true);
    if (upstreamProxyUrl) upstreamProxyUrl.value = settings.upstreamProxyUrl || "";
    if (defaultChannel) {
      const expected = ({ responses: "Responses", chat: "Chat", anthropic: "Anthropic" }[settings.defaultChannel] || "Auto");
      const option = Array.from(defaultChannel.options).find((item) => item.textContent?.trim() === expected);
      if (option) defaultChannel.value = option.value;
    }
    const refreshPolicy = $bridge("#settingsRefreshPolicy");
    if (refreshPolicy) refreshPolicy.value = settings.refreshPolicy || "visible-1s";
    if (appVersion) appVersion.value = settings.appVersion || "—";
    setSwitch("开机自动启动代理", Boolean(settings.proxyAutoStart));
    const data = $bridge("#settingsData");
    if (data) {
      const fingerprint = Array.from(data.querySelectorAll(".detail-row code"))[0];
      if (fingerprint) fingerprint.textContent = settings.workspaceFingerprint || "—";
      const status = data.querySelector(".status-band");
      if (status) {
        const title = status.querySelector("b");
        const detail = status.querySelector("small");
        if (title) title.textContent = "本地状态";
        if (detail) detail.textContent = "缓存统计与运行时状态已加载";
      }
      const values = Array.from(data.querySelectorAll(".detail-row b"));
      if (values[0]) values[0].textContent = settings.updatedAt ? new Date(settings.updatedAt).toLocaleString("zh-CN") : "—";
      if (values[1]) values[1].textContent = settings.persistEncrypted ? "加密 · 已启用" : "未加密";
    }
  }

  function applyProxyStatus(nextState) {
    const status = nextState?.proxyStatus;
    const running = status?.running === true;
    const configuredAddress = nextState?.settings
      ? (() => {
          const host = String(nextState.settings.host || "127.0.0.1");
          const displayHost = host.includes(":") && !host.startsWith("[") ? "[" + host + "]" : host;
          return displayHost + ":" + String(nextState.settings.port || "18883");
        })()
      : "";
    const address = status?.address || configuredAddress || "—";
    const runtime = $bridge(".runtime-state");
    if (runtime) {
      const title = runtime.querySelector("b");
      const detail = runtime.querySelector("small");
      if (title) title.textContent = running ? "代理运行中" : "代理未运行";
      if (detail) detail.textContent = address;
    }
    const settingsStatus = $bridge("#settingsProxy .status-band");
    if (settingsStatus) {
      const title = settingsStatus.querySelector("b");
      const detail = settingsStatus.querySelector("small");
      if (title) title.textContent = running ? "本地代理运行中" : "本地代理未运行";
      if (detail) detail.textContent = running
        ? "当前监听 " + address
        : "保存地址后可手动启动本地代理";
    }
    const brandVersion = $bridge("#brandButton small");
    if (brandVersion) brandVersion.textContent = "Graphite · " + (nextState?.appVersion || "Atoapi");
    const versionPopover = $bridge("#versionPopover");
    if (versionPopover) {
      const title = versionPopover.querySelector("h3");
      const detail = versionPopover.querySelector("p");
      if (title) title.textContent = "Atoapi " + (nextState?.appVersion || "");
      if (detail) detail.textContent = "Graphite Control Desk · 已接入真实 Agent、上游、模型映射、Key 池、缓存与请求诊断。";
    }
  }

  function reasoningEffortRank(value) {
    return ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"].indexOf(String(value || "").trim().toLowerCase());
  }

  function syncPersistedReasoningFallback(previousState, nextState) {
    const providerId = editingProviderId || selectedProviderId();
    if (!providerId) return;
    const previousModels = previousState?.providerDetails?.[providerId]?.models || [];
    const nextModels = nextState?.providerDetails?.[providerId]?.models || [];
    if (!previousModels.length || !nextModels.length) return;
    const previousById = new Map(previousModels.map((model) => [model.id, model]));
    const nextById = new Map(nextModels.map((model) => [model.id, model]));
    let changed = false;
    mappings.forEach((mapping) => {
      const previous = previousById.get(mapping.actual);
      const next = nextById.get(mapping.actual);
      if (!previous?.reasoning_effort_override_enabled || !next?.reasoning_effort_override_enabled) return;
      const from = String(previous.reasoning_effort || "").trim();
      const to = String(next.reasoning_effort || "").trim();
      if (!from || !to || reasoningEffortRank(to) < 0 || reasoningEffortRank(to) >= reasoningEffortRank(from)) return;
      if (String(mapping.reasoning || "").trim() !== from) return;
      mapping.reasoning = to;
      changed = true;
    });
    if (changed) {
      renderMappings();
      hydrateIcons();
    }
  }

  function applyState(nextState) {
    const previousState = host.state;
    host.state = nextState || {};
    replace(agents, nextState?.agents || []);
    replace(providers, nextState?.providers || []);
    replace(requests, nextState?.requests || []);
    const requestedAgent = nextState?.selectedAgentId;
    if (requestedAgent && agents.some((agent) => agent.id === requestedAgent)) selectedAgentId = requestedAgent;
    const scopes = nextState?.metrics?.scopes || [];
    const preferredScopeId = currentAgent()?.provider ? "provider:" + currentAgent().provider : "all";
    if ((!requestScopeId || previousState?.selectedAgentId !== selectedAgentId) && scopes.some((scope) => scope.id === preferredScopeId)) {
      requestScopeId = preferredScopeId;
      requestPage = 1;
    }
    const editorOpen = isOpen("providerOverlay");
    const selected = currentAgent();
    if (!editorOpen && selected) applyEditorData(selected.provider);
    if (editorOpen) {
      syncPersistedReasoningFallback(previousState, nextState);
      renderAgents(); renderContext(); renderProviders(); renderRequests(); hydrateIcons();
    } else {
      renderAll();
    }
    applyMetrics(nextState);
    applySettings(nextState);
    applyProxyStatus(nextState);
    syncTrendController(true);
  }

  function applyMetricsDelta(metrics, nextRequests) {
    if (renderSuspended) {
      deferredMetricsDelta = {
        metrics: metrics || deferredMetricsDelta?.metrics,
        ...(Array.isArray(nextRequests)
          ? { requests: nextRequests }
          : (Array.isArray(deferredMetricsDelta?.requests) ? { requests: deferredMetricsDelta.requests } : {}))
      };
      return;
    }
    if (!host.state) return;
    const nextState = { ...host.state, metrics: metrics || host.state.metrics };
    if (Array.isArray(nextRequests)) {
      replace(requests, nextRequests);
      nextState.requests = nextRequests;
      renderRequests();
    }
    host.state = nextState;
    applyMetrics(nextState);
    syncTrendController(true);
  }

  function applyRenderSuspended(suspended) {
    const next = suspended === true;
    if (renderSuspended === next) return;
    renderSuspended = next;
    document.documentElement.classList.toggle("performance-paused", next);
    if (next) {
      stopProviderKeyPoolHealthSync();
      cancelReleaseChampionRefresh();
      return;
    }
    if (isOpen("providerOverlay")) startProviderKeyPoolHealthSync();
    const deferred = deferredMetricsDelta;
    deferredMetricsDelta = null;
    if (deferred) applyMetricsDelta(deferred.metrics, deferred.requests);
  }

  window.addEventListener("message", (event) => {
    const message = event.data || {};
    if (event.source !== window.parent || message.channel !== CHANNEL) return;
    if (message.kind === "state") applyState(message.state);
    if (message.kind === "metrics-delta") applyMetricsDelta(message.metrics, message.requests);
    if (message.kind === "render-suspension") applyRenderSuspended(message.suspended);
    if (message.kind === "ack") {
      settle(message.requestId);
      if (message.requestId === keyPoolHealthSyncRequestId) keyPoolHealthSyncRequestId = "";
      if (message.closeOverlay) closeOverlay(message.closeOverlay);
      if (message.payload?.models) {
        fetchedModelIds = message.payload.models
          .map((model) => String(model?.id || model || "").trim())
          .filter(Boolean);
        const list = document.getElementById("providerModelCandidates");
        if (list) list.innerHTML = fetchedModelIds.map((model) => "<option value=\"" + escape(model) + "\"></option>").join("");
        renderModelCandidates();
      }
      if (message.payload?.secret) {
        const input = message.payload.targetId ? document.getElementById(message.payload.targetId) : null;
        if (input) {
          input.value = message.payload.secret;
          input.type = "text";
          input.classList.remove("has-saved-secret");
          input.removeAttribute("data-saved-secret");
        }
      }
      if (message.payload?.keyPoolHealth) mergeKeyPoolHealth(message.payload.keyPoolHealth);
      if (message.payload?.networkDiagnostic) applyNetworkDiagnostic(message.payload.networkDiagnostic);
      if (message.payload?.connectionTest) applyConnectionPathTest(message.payload.connectionTest);
      if (message.payload?.healthProbeModels) applyHealthProbeModels(message.payload.healthProbeModels);
      if (message.payload?.healthProbe) applyHealthProbeResult(message.payload.healthProbe);
      if (message.payload?.balanceProbe) applyBalanceProbe(message.payload.balanceProbe);
      if (message.payload?.metricsTrend) {
        const trend = message.payload.metricsTrend;
        if (trend.error) trendController()?.setError(trend.error, trend.sequence, trend.rangeKey);
        else trendController()?.setData(trend.data, trend.sequence, trend.rangeKey);
      }
      if (message.payload?.releaseChampion) {
        const comparison = message.payload.releaseChampion;
        releaseChampionRequestInFlight = false;
        if (comparison.sequence === releaseChampionSequence && comparison.contextKey === lastReleaseChampionContextKey) {
          latestReleaseChampion = comparison.data || null;
          latestReleaseChampionError = comparison.error ? String(comparison.error) : "";
          latestReleaseChampionContextKey = comparison.contextKey;
          applyMetrics(host.state);
        }
        if (releaseChampionPendingRefresh) {
          const refreshImmediately = releaseChampionPendingImmediate;
          releaseChampionPendingRefresh = false;
          releaseChampionPendingImmediate = false;
          if (refreshImmediately && releaseChampionAutoRefreshAllowed()) requestReleaseChampion(true);
          else scheduleReleaseChampionRefresh();
        }
      }
      if (message.notice) showToast(message.notice);
      if (message.error) showToast(message.error);
    }
  });

  document.addEventListener("keydown", (event) => {
    const input = event.target instanceof Element ? event.target.closest("#bulkKeyInput") : null;
    if (!input || event.key !== "Enter" || event.shiftKey) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    addBulkKeysFromBridge();
  }, true);

  function clearKeyPointerDrag() {
    keyPointerDrag = null;
    document.querySelectorAll(".key-editor-row.dragging, .key-editor-row.drop-target").forEach((row) => {
      row.classList.remove("dragging", "drop-target");
    });
  }

  document.addEventListener("pointerdown", (event) => {
    const handle = event.target instanceof Element ? event.target.closest("[data-key-drag]") : null;
    const keyId = handle?.getAttribute("data-key-drag") || "";
    if (!keyId || event.button !== 0) return;
    keyPointerDrag = { pointerId: event.pointerId, keyId };
    handle.closest(".key-editor-row")?.classList.add("dragging");
    try { handle.setPointerCapture?.(event.pointerId); } catch (_) {}
    event.preventDefault();
  }, true);

  document.addEventListener("pointermove", (event) => {
    if (!keyPointerDrag || event.pointerId !== keyPointerDrag.pointerId) return;
    const node = document.elementFromPoint(event.clientX, event.clientY);
    const row = node instanceof Element ? node.closest(".key-editor-row[data-key-id]") : null;
    const targetId = row?.getAttribute("data-key-id") || "";
    document.querySelectorAll(".key-editor-row.drop-target").forEach((item) => item.classList.remove("drop-target"));
    if (targetId && targetId !== keyPointerDrag.keyId) row?.classList.add("drop-target");
    event.preventDefault();
  }, true);

  document.addEventListener("pointerup", (event) => {
    if (!keyPointerDrag || event.pointerId !== keyPointerDrag.pointerId) return;
    const draggedId = keyPointerDrag.keyId;
    const node = document.elementFromPoint(event.clientX, event.clientY);
    const row = node instanceof Element ? node.closest(".key-editor-row[data-key-id]") : null;
    const targetId = row?.getAttribute("data-key-id") || "";
    clearKeyPointerDrag();
    if (!targetId || targetId === draggedId) return;
    event.preventDefault();
    reorderKeyFromBridge(draggedId, targetId);
    showToast("Key 顺序已调整，保存上游后生效");
  }, true);

  document.addEventListener("pointercancel", clearKeyPointerDrag, true);

  // The browser's HTML drag path is unreliable in the embedded WebView and
  // can otherwise emit a second drop after the pointer reorder has completed.
  document.addEventListener("dragstart", (event) => {
    const handle = event.target instanceof Element ? event.target.closest("[data-key-drag]") : null;
    if (!handle || !keyPointerDrag) return;
    event.preventDefault();
    event.stopPropagation();
  }, true);

  document.addEventListener("dragstart", (event) => {
    const handle = event.target instanceof Element ? event.target.closest("[data-provider-drag]") : null;
    const providerId = handle?.getAttribute("data-provider-drag") || "";
    if (!providerId) return;
    draggingProviderId = providerId;
    event.dataTransfer?.setData("application/x-atoapi-provider", providerId);
    if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
    handle.closest(".provider-row")?.classList.add("is-dragging");
  }, true);

  document.addEventListener("dragover", (event) => {
    if (!draggingProviderId) return;
    const row = event.target instanceof Element ? event.target.closest(".provider-row[data-provider-id]") : null;
    const targetId = row?.getAttribute("data-provider-id") || "";
    if (!targetId || targetId === draggingProviderId) return;
    event.preventDefault();
    document.querySelectorAll(".provider-row.is-drag-over").forEach((item) => item.classList.remove("is-drag-over"));
    row.classList.add("is-drag-over");
  }, true);

  document.addEventListener("dragleave", (event) => {
    const row = event.target instanceof Element ? event.target.closest(".provider-row.is-drag-over") : null;
    if (row && !row.contains(event.relatedTarget instanceof Node ? event.relatedTarget : null)) {
      row.classList.remove("is-drag-over");
    }
  }, true);

  document.addEventListener("drop", (event) => {
    const row = event.target instanceof Element ? event.target.closest(".key-editor-row") : null;
    const targetId = row?.dataset.keyId;
    const draggedId = event.dataTransfer?.getData("text/plain") || draggingKeyId;
    if (!targetId || !draggedId || draggedId === targetId) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    reorderKeyFromBridge(draggedId, targetId);
  }, true);

  document.addEventListener("drop", (event) => {
    const row = event.target instanceof Element ? event.target.closest(".provider-row[data-provider-id]") : null;
    const targetId = row?.getAttribute("data-provider-id") || "";
    const draggedId = event.dataTransfer?.getData("application/x-atoapi-provider") || draggingProviderId;
    if (!targetId || !draggedId || targetId === draggedId) {
      clearProviderDragState();
      return;
    }
    event.preventDefault();
    event.stopImmediatePropagation();
    reorderProvidersFromBridge(draggedId, targetId);
    clearProviderDragState();
  }, true);

  document.addEventListener("dragend", clearProviderDragState, true);

  document.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target.closest("button, [data-bind-provider], [data-edit-provider], [data-delete-provider]") : null;
    if (!target) return;
    if (target.id === "refreshButton") {
      event.preventDefault(); event.stopImmediatePropagation(); send("refresh"); return;
    }
    if (target.id === "metricsRefreshButton") {
      event.preventDefault(); event.stopImmediatePropagation();
      // The live metric snapshot and the persisted trend are independent.
      // Refresh both explicitly; the trend loader has its own stale guard.
      trendController()?.request("refresh");
      send("refresh");
      return;
    }
    if (target.id === "agentEnabledSwitch") {
      event.preventDefault(); event.stopImmediatePropagation();
      const agent = currentAgent(); send("toggle-agent", { agentId: agent?.sourceId || agent?.id || "" }); return;
    }
    if (target.id === "providerKeyPoolEnabled") {
      event.preventDefault(); event.stopImmediatePropagation();
      setKeyPoolEnabled(target.getAttribute("aria-checked") !== "true");
      return;
    }
    if (target.getAttribute("aria-label") === "智能缓存") {
      event.preventDefault(); event.stopImmediatePropagation();
      send("save-cache-enabled", { enabled: target.getAttribute("aria-checked") !== "true" }); return;
    }
    if (target.getAttribute("aria-label") === "计入冷启动和压缩") {
      event.preventDefault(); event.stopImmediatePropagation();
      send("set-include-special-requests", { enabled: target.getAttribute("aria-checked") !== "true" }); return;
    }
    if (target.getAttribute("aria-label") === "提示详细错误") {
      event.preventDefault(); event.stopImmediatePropagation();
      send("set-show-detailed-errors", { enabled: target.getAttribute("aria-checked") !== "true" }); return;
    }
    if (target.dataset.agentId) {
      const agent = agents.find((item) => item.id === target.dataset.agentId);
      if (agent) send("select-agent", { agentId: agent.sourceId || agent.id });
      return;
    }
    if (target.dataset.bindProvider) {
      event.preventDefault(); event.stopImmediatePropagation();
      const agent = currentAgent(); send("bind-provider", { agentId: agent?.sourceId || agent?.id || "", providerId: target.dataset.bindProvider }); return;
    }
    if (target.dataset.editProvider) {
      const providerId = target.dataset.editProvider;
      event.preventDefault(); event.stopImmediatePropagation();
      openProviderEditor(providerId);
      return;
    }
    if (target.dataset.deleteProvider) {
      event.preventDefault(); event.stopImmediatePropagation();
      openDeleteConfirm(target.dataset.deleteProvider);
      return;
    }
    if (target.dataset.healthProbeProvider) {
      event.preventDefault(); event.stopImmediatePropagation();
      openHealthProbe(target.dataset.healthProbeProvider);
      return;
    }
    if (target.dataset.balanceProbe) {
      event.preventDefault(); event.stopImmediatePropagation();
      send("probe-provider-balance", { providerId: target.dataset.balanceProbe });
      return;
    }
    if (target.id === "saveProviderButton") {
      event.preventDefault(); event.stopImmediatePropagation(); send("save-provider", serializeEditor()); return;
    }
    if (target.id === "addKeyButton") {
      event.preventDefault(); event.stopImmediatePropagation(); addBlankKey(); return;
    }
    if (target.id === "testAllKeysHealthButton") {
      event.preventDefault(); event.stopImmediatePropagation();
      const providerId = editingProviderId || selectedProviderId();
      if (!providerId) showToast("请先保存上游，再测活 Key 池");
      else openHealthProbe(providerId, "all_enabled");
      return;
    }
    if (target.id === "healthProbeRunButton" || target.id === "healthProbeRetryButton") {
      event.preventDefault(); event.stopImmediatePropagation(); runHealthProbe(); return;
    }
    if (target.id === "bulkAddKeysButton") {
      event.preventDefault(); event.stopImmediatePropagation(); addBulkKeysFromBridge(); return;
    }
    if (target.dataset.keyEnabled) {
      event.preventDefault(); event.stopImmediatePropagation();
      toggleKeyEnabled(target.dataset.keyEnabled);
      return;
    }
    if (target.dataset.keyRemove) {
      event.preventDefault(); event.stopImmediatePropagation(); removeKeyFromBridge(target.dataset.keyRemove); return;
    }
    if (target.dataset.keyMove) {
      event.preventDefault(); event.stopImmediatePropagation();
      moveKeyFromBridge(target.dataset.keyId || "", target.dataset.keyMove === "up" ? -1 : 1);
      return;
    }
    if (target.id === "confirmDeleteButton") {
      event.preventDefault(); event.stopImmediatePropagation();
      const agent = currentAgent(); send("delete-provider", { agentId: agent?.sourceId || agent?.id || "", providerId: deleteTargetId || "" }); return;
    }
    if (target.dataset.testProvider) {
      event.preventDefault(); event.stopImmediatePropagation(); send("test-provider", { providerId: target.dataset.testProvider }); return;
    }
    if (target.dataset.keyTest) {
      event.preventDefault(); event.stopImmediatePropagation(); send("test-provider-key", { providerId: editingProviderId || "", keyId: target.dataset.keyTest, provider: serializeEditor() }); return;
    }
    if (target.dataset.secretToggle) {
      const input = document.getElementById(target.dataset.secretToggle);
      if (!input || input.value || input.type === "text") return;
      if (target.dataset.secretToggle === "settingsLocalKeyInput") {
        event.preventDefault(); event.stopImmediatePropagation();
        send("reveal-local-key", { targetId: target.dataset.secretToggle });
        return;
      }
      const providerId = selectedProviderId();
      if (!providerId) return;
      event.preventDefault(); event.stopImmediatePropagation();
      const keyId = input.dataset.keySecret;
      send(keyId ? "reveal-provider-key" : "reveal-provider-api-key", { providerId, keyId, targetId: target.dataset.secretToggle });
      return;
    }
    if (target.matches("[data-save-settings]")) {
      event.preventDefault(); event.stopImmediatePropagation(); send("save-settings", { settings: serializeSettings() }); return;
    }
    const mockAction = target.dataset.mockAction || "";
    if (mockAction) {
      event.preventDefault(); event.stopImmediatePropagation();
      if (target.closest("#providerModels")) send("fetch-models", { provider: serializeEditor() });
      else if (target.closest("#providerKeys")) send("test-provider-key-pool", { providerId: editingProviderId || "" });
      else if (target.closest("#providerGeneral")) send("test-provider", { provider: serializeEditor() });
      else if (target.closest("#settingsProxy")) send("restart-main-proxy");
      else if (target.closest("#settingsData")) send("clear-cache");
      return;
    }
  }, true);

  document.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches("[data-saved-secret=true]") && target.value) {
      target.classList.remove("has-saved-secret");
      target.removeAttribute("data-saved-secret");
      const key = keyPool.find((item) => item.id === target.dataset.keySecret);
      if (key) key.hasSavedSecret = false;
    }
  }, true);

  function serializeSettings() {
    const proxy = $bridge("#settingsProxy");
    const app = $bridge("#settingsApp");
    return {
      host: byFieldLabel(proxy, "监听地址")?.value.trim() || "",
      port: Number(byFieldLabel(proxy, "端口")?.value) || 0,
      local_key: $bridge("#settingsLocalKeyInput")?.value || "",
      default_channel: ({ Auto: "responses", Responses: "responses", Chat: "chat", Anthropic: "anthropic" }[byFieldLabel(app, "默认通道", "select")?.value || ""] || "responses"),
      refresh_policy: $bridge("#settingsRefreshPolicy")?.value || "visible-1s",
      proxy_auto_start: $bridge('[aria-label="开机自动启动代理"]')?.getAttribute("aria-checked") === "true",
      upstream_proxy_url: $bridge("#settingsUpstreamProxyUrlInput")?.value.trim() || ""
    };
  }

  renderProviders = function() {
    const agent = currentAgent();
    providers.forEach((provider) => provider.active = provider.id === agent.provider);
    $bridge("#providerCountTag").textContent = providers.length + " 个可用上游";
    $bridge("#providerList").innerHTML = providers.length ? providers.map((provider) =>
      '<article class="provider-row ' + (provider.active ? "active" : "") + '" data-provider-id="' + escape(provider.id) + '">' +
      '<span class="provider-mark" draggable="true" data-provider-drag="' + escape(provider.id) + '" title="拖动调整上游顺序" style="cursor:grab">' + icon(provider.active ? "route" : "server", 15) + '</span>' +
      '<div class="provider-copy"><b>' + escape(provider.name) + '</b><small>' + escape(provider.url) + '</small></div>' +
      '<div class="provider-meta"><div class="provider-tags">' + (provider.mappings ? '<span class="tag">' + escape(provider.mappings + " 个映射") + '</span>' : '') + keyPoolCountBadge(provider) + '<button class="balance-probe-chip ' + balanceProbeTone(provider.balance) + '" type="button" data-balance-probe="' + escape(provider.id) + '" title="探测当前可用 Key 的余额">' + icon("wallet-cards", 12) + '<span>' + escape(balanceProbeLabel(provider.balance)) + '</span></button></div><small class="provider-latency"><span>最近连通</span><b>' + escape(provider.latency) + '</b></small></div>' +
      '<div class="provider-actions"><button class="tool-button bind-button" type="button" data-bind-provider="' + escape(provider.id) + '" title="设为当前上游">' + icon(provider.active ? "check" : "route", 14) + (provider.active ? "当前" : "使用") + '</button>' +
      '<button class="tool-button connection-test-button" type="button" data-test-provider="' + escape(provider.id) + '" aria-label="测试 ' + escape(provider.name) + ' 连通性" title="测试当前 Key 连通性">' + icon("plug-zap", 14) + '连通</button>' +
      '<button class="tool-button health-probe-button" type="button" data-health-probe-provider="' + escape(provider.id) + '" aria-label="测活 ' + escape(provider.name) + '" title="测活">' + icon("activity", 14) + '测活</button>' +
      '<button class="icon-button" type="button" data-edit-provider="' + escape(provider.id) + '" aria-label="编辑 ' + escape(provider.name) + '" title="编辑">' + icon("pencil", 14) + '</button>' +
      '<button class="icon-button" type="button" data-delete-provider="' + escape(provider.id) + '" aria-label="删除 ' + escape(provider.name) + '" title="删除">' + icon("trash-2", 14) + '</button></div></article>'
    ).join("") : '<div class="empty-row">当前没有上游配置</div>';
  };

  function requestTimingTone(value, kind) {
    if (!Number.isFinite(value) || value <= 0) return "na";
    const good = kind === "ttft" ? 5_000 : 6_000;
    const warning = kind === "ttft" ? 10_000 : 15_000;
    return value <= good ? "good" : value <= warning ? "warn" : "bad";
  }

  function requestDuration(value) {
    if (!Number.isFinite(value) || value <= 0) return "不适用";
    return (value / 1000).toFixed(2) + "s";
  }

  function requestActualModel(request) {
    const labels = String(request.model || "—").split("→");
    return labels[labels.length - 1].trim() || "—";
  }

  function requestTokens(value) {
    return Number(value || 0).toLocaleString("en-US");
  }

  function requestCacheTailSegments(shortfallTokens, newTailTokens) {
    const shortfall = Number.isFinite(Number(shortfallTokens))
      ? Math.max(0, Math.floor(Number(shortfallTokens)))
      : 0;
    const newTail = Number.isFinite(Number(newTailTokens))
      ? Math.max(0, Math.floor(Number(newTailTokens)))
      : 0;
    if (shortfall <= 0) return [];
    // When the whole shortfall is the newly appended tail, showing both
    // labels repeats the same number and makes the compact row look wrong.
    if (newTail > 0 && newTail === shortfall) {
      return ["新 " + requestTokens(newTail)];
    }
    return [
      "缺口 " + requestTokens(shortfall),
      newTail > 0 ? "新 " + requestTokens(newTail) : ""
    ].filter(Boolean);
  }

  function renderRequestScopeTabs(metricState) {
    const tabs = $bridge("#requestScopeTabs");
    if (!tabs) return;
    const scopes = Array.isArray(metricState?.scopes) ? metricState.scopes : [];
    const active = activeRequestScope(metricState);
    tabs.innerHTML = scopes.map((scope) =>
      '<button class="request-scope-button" type="button" role="tab" aria-selected="' + (scope.id === active.id) + '" data-request-scope="' + escape(scope.id) + '">' + escape(scope.label) + '</button>'
    ).join("");
    Array.from(tabs.querySelectorAll("[data-request-scope]")).forEach((button) => {
      button.onclick = () => {
        requestScopeId = button.dataset.requestScope || "all";
        requestPage = 1;
        renderRequests();
        applyMetrics(host.state);
        trendController()?.syncScope(requestScopeId);
        syncTrendController(false);
        trendController()?.request("request-scope");
      };
    });
    trendController()?.syncScopes(scopes, active.id);
  }

  renderRequests = function() {
    const list = $bridge("#requestList");
    const pager = $bridge("#requestPager");
    if (!list || !pager) return;
    const metricState = host.state?.metrics || {};
    const scope = activeRequestScope(metricState);
    renderRequestScopeTabs(metricState);
    const columnLabels = document.querySelectorAll(".request-column-head span");
    if (columnLabels[2]) columnLabels[2].textContent = "传输";
    if (columnLabels[6]) columnLabels[6].textContent = "状态";
    const scoped = scope.providerId
      ? requests.filter((request) => request.providerId === scope.providerId)
      : requests.slice();
    const totalPages = Math.min(REQUEST_PAGE_LIMIT, Math.max(1, Math.ceil(scoped.length / REQUESTS_PER_PAGE)));
    requestPage = Math.max(1, Math.min(requestPage, totalPages));
    const start = (requestPage - 1) * REQUESTS_PER_PAGE;
    const pageItems = scoped.slice(start, start + REQUESTS_PER_PAGE);
    list.innerHTML = pageItems.length ? pageItems.map((request) => {
      const ttftMs = Number(request.ttftMs || 0);
      const totalMs = Number(request.totalMs || 0);
      const generationMs = totalMs > ttftMs ? totalMs - ttftMs : Math.max(1, totalMs);
      const outputRate = Number(request.outputTokens || 0) > 0 ? Math.round(Number(request.outputTokens) / (generationMs / 1000)) : 0;
      const cachePercent = Number(request.inputTokens || 0) > 0
        ? (Number(request.cachedTokens || 0) / Number(request.inputTokens) * 100).toFixed(1) + "%"
        : "—";
      const model = requestActualModel(request);
      const rowTone = request.statusTone || (request.failed ? "error" : "complete");
      const statusDetail = request.statusDetail
        ? '<small>' + escape(request.statusDetail) + '</small>'
        : '';
      const cacheShortfallTokens = Number(request.cacheShortfallTokens || 0);
      const cacheNewTailTokens = Number(request.cacheNewTailGapTokens || 0);
      const cacheTailSegments = requestCacheTailSegments(
        cacheShortfallTokens,
        cacheNewTailTokens
      );
      const cacheTailDetail = cacheTailSegments.length
        ? cacheTailSegments.join(" · ")
        : "满桶";
      return '<article class="request-row status-' + escape(rowTone) + (request.failed ? ' failed' : '') + '" tabindex="0" data-request-id="' + escape(request.id) + '">' +
        '<div class="request-identity" title="' + escape(request.provider + " · " + request.agentLabel) + '"><b><span class="request-provider-name">' + escape(request.provider) + '</span><span class="request-agent-badge agent-' + escape(request.agentTone || "generic") + '">' + escape(request.agentLabel) + '</span></b><span>' + escape(request.time) + '</span></div>' +
        '<div class="request-model" title="' + escape(request.provider + " · " + request.model) + '"><div class="request-model-stack"><span class="request-model-name">' + escape(model) + '</span><span class="request-reasoning">' + escape(request.reasoning || "—") + '</span></div></div>' +
        '<div class="request-stream transport-' + escape(request.transportTone || "stream") + '"><b>' + escape(request.transport) + '</b><small>' + (outputRate ? outputRate + ' t/s' : '—') + '</small></div>' +
        '<div class="request-tokens"><b>' + escape(requestTokens(request.inputTokens)) + ' / ' + escape(requestTokens(request.outputTokens)) + '</b><small>缓存 ' + escape(requestTokens(request.cachedTokens)) + ' (' + escape(cachePercent) + ')</small></div>' +
        '<div class="request-time"><span class="request-time-line ' + requestTimingTone(ttftMs, "ttft") + '"><span>首字</span><b class="value">' + escape(requestDuration(ttftMs)) + '</b></span><span class="request-time-line ' + requestTimingTone(totalMs, "total") + '"><span>耗时</span><b class="value">' + escape(requestDuration(totalMs)) + '</b></span></div>' +
        '<div class="request-cache"><b>' + escape(cachePercent) + '</b><small>' + escape(cacheTailDetail) + '</small></div>' +
        '<div class="request-status request-state ' + escape(rowTone) + '"><b>' + escape(request.statusLabel || "OK") + '</b>' + statusDetail + '</div></article>';
    }).join("") : '<div class="empty-row">当前筛选下暂无请求记录</div>';
    const pageButtons = Array.from({ length: totalPages }, (_, index) => {
      const page = index + 1;
      return '<button class="request-page-button" type="button" data-request-page="' + page + '"' + (page === requestPage ? ' aria-current="page"' : '') + '>' + page + '</button>';
    }).join("");
    pager.innerHTML = '<span class="request-page-summary">' + scoped.length + ' 条 · 第 ' + requestPage + ' / ' + totalPages + ' 页</span>' + pageButtons;
    Array.from(pager.querySelectorAll("[data-request-page]")).forEach((button) => {
      button.onclick = () => {
        requestPage = Number(button.dataset.requestPage) || 1;
        renderRequests();
      };
    });
    Array.from(list.querySelectorAll("[data-request-id]")).forEach((row) => {
      const open = () => openRequestDetail(row.dataset.requestId);
      row.addEventListener("click", open);
      row.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") { event.preventDefault(); open(); }
      });
    });
  };

  renderMappings = function() {
    const allEfforts = ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"];
    const effortsFor = (mapping) => {
      const supported = Array.isArray(mapping.supportedReasoningEfforts) && mapping.supportedReasoningEfforts.length
        ? mapping.supportedReasoningEfforts
        : allEfforts;
      const current = mapping.reasoning ? [mapping.reasoning] : [];
      return [
      { value: "", label: "跟随 Agent" },
        ...Array.from(new Set([...current, ...supported])).map((value) => ({ value, label: value }))
      ];
    };
    $bridge("#mappingList").innerHTML = mappings.map((mapping, index) =>
      '<div class="mapping-row" data-mapping-index="' + index + '">' +
      '<input list="providerModelCandidates" value="' + escape(mapping.request) + '" aria-label="请求模型" autocomplete="off" />' +
      '<span class="mapping-arrow">→</span>' +
      '<input list="providerModelCandidates" value="' + escape(mapping.actual) + '" aria-label="实际模型" autocomplete="off" />' +
      '<input value="' + escape(mapping.context) + '" aria-label="上下文窗口" inputmode="numeric" autocomplete="off" />' +
      '<select aria-label="推理强度">' + effortsFor(mapping).map((effort) => '<option value="' + escape(effort.value) + '"' + (effort.value === mapping.reasoning ? " selected" : "") + '>' + escape(effort.label) + '</option>').join("") + '</select>' +
      '<button class="icon-button" type="button" data-remove-mapping="' + index + '" aria-label="删除模型映射" title="删除">' + icon("trash-2", 13) + '</button></div>'
    ).join("");
    Array.from(document.querySelectorAll("[data-remove-mapping]")).forEach((button) => button.addEventListener("click", () => {
      mappings.splice(Number(button.dataset.removeMapping), 1); renderMappings(); hydrateIcons();
    }));
  };

  const originalRenderContext = renderContext;
  renderContext = function() {
    originalRenderContext();
    const agent = currentAgent();
    if (!agent) return;
    const endpointList = $bridge("#endpointList");
    if (!endpointList) return;
    const baseUrl = agent.localBaseUrl || "";
    const endpointValue = agent.id === "proxy"
      ? '<input class="endpoint-url-input" id="proxyBaseUrlInput" type="url" value="' + escape(baseUrl) + '" inputmode="url" autocomplete="off" spellcheck="false" aria-label="Proxy Mode Base URL" />'
      : '<code>' + escape(baseUrl) + '</code>';
    endpointList.innerHTML =
      '<div class="endpoint-item ' + (agent.id === "proxy" ? "editable-endpoint" : "") + '"><span>Base URL</span>' + endpointValue + '<button class="copy-button" id="proxyBaseUrlCopy" type="button" data-copy="' + escape(baseUrl) + '" aria-label="复制 Base URL" title="复制">' + icon("copy", 14) + '</button></div>' +
      '<div class="endpoint-item"><span>Local Key</span><code>' + escape(agent.localKey || "未设置") + '</code><button class="copy-button" type="button" data-copy="' + escape(agent.localKey || "") + '" aria-label="复制本地 Key" title="复制">' + icon("copy", 14) + '</button></div>';
    bindCopyButtons();
    hydrateIcons();
  };

  const originalOpenProviderEditor = openProviderEditor;
  openProviderEditor = function(providerId = null) {
    originalOpenProviderEditor(providerId);
    applyEditorData(providerId);
    if (providerId) startProviderKeyPoolHealthSync();
  };

  const originalCloseOverlay = closeOverlay;
  closeOverlay = function(id) {
    originalCloseOverlay(id);
    if (id === "providerOverlay") stopProviderKeyPoolHealthSync();
  };

  const originalOpenDeleteConfirm = openDeleteConfirm;
  openDeleteConfirm = function(providerId) {
    originalOpenDeleteConfirm(providerId);
    const agentId = currentAgent()?.sourceId || currentAgent()?.id || "";
    const providerIsPrivate = providerId.startsWith("agent-" + String(agentId).toLowerCase().replace(/[^a-z0-9]+/g, "-") + "-");
    const provider = providers.find((item) => item.id === providerId);
    const message = providerIsPrivate
      ? "将删除当前 Agent 的独立上游配置并解除绑定。此操作不可撤销。"
      : "将从当前 Agent 隐藏并解除这个共享上游，不会影响其他 Agent。";
    $bridge("#confirmMessage").textContent = "删除 " + (provider?.name || "该上游") + "？" + message;
  };

  const addMappingButton = $bridge("#addMappingButton");
  if (addMappingButton) {
    const replacement = addMappingButton.cloneNode(true);
    addMappingButton.replaceWith(replacement);
    replacement.addEventListener("click", () => {
      mappings.push({ request: "", actual: "", context: "", reasoning: "", supportedReasoningEfforts: [] });
      renderMappings();
      hydrateIcons();
    });
  }

  const originalOpenRequestDetail = openRequestDetail;
  openRequestDetail = function(requestId) {
    const request = requests.find((item) => item.id === requestId);
    if (!request) { originalOpenRequestDetail(requestId); return; }
    $bridge("#requestDetailSubtitle").textContent = request.provider + " · " + request.model;
    const displayedTtftMs = Number(request.ttftMs || 0);
    const visibleTextTtftMs = Number(request.visibleTextTtftMs || 0);
    const upstreamFirstEventMs = Number(request.upstreamFirstEventMs || 0);
    const visibleTextTtftRow = visibleTextTtftMs > 0 && Math.abs(visibleTextTtftMs - displayedTtftMs) >= 1
      ? '<div class="detail-row"><span>可见正文首字</span><b>' + escape(requestDuration(visibleTextTtftMs)) + '</b></div>'
      : '';
    const upstreamFirstEventRow = upstreamFirstEventMs > 0 && Math.abs(upstreamFirstEventMs - displayedTtftMs) >= 1
      ? '<div class="detail-row"><span>首个上游事件</span><b>' + escape(requestDuration(upstreamFirstEventMs)) + '</b></div>'
      : '';
    $bridge("#requestDetailBody").innerHTML =
      '<div class="status-band"><span class="status-icon">' + icon(request.stateTone === "error" ? "circle-x" : "circle-check", 17) + '</span><div><b>' + escape(request.state) + '</b><small>' + escape(request.stateTone === "error" ? (request.errorDetail || request.detail) : "成功流已完整收尾并记录 usage") + '</small></div></div>' +
      '<div class="detail-list">' +
      '<div class="detail-row"><span>请求 ID</span><code>' + escape(request.id) + '</code></div>' +
      '<div class="detail-row"><span>模型首字 / 总耗时</span><b>' + escape(request.ttft) + ' / ' + escape(request.total) + '</b></div>' +
      visibleTextTtftRow +
      upstreamFirstEventRow +
      '<div class="detail-row"><span>输入 / 输出</span><b>' + escape(request.input) + ' / ' + escape(request.output) + '</b></div>' +
      '<div class="detail-row"><span>缓存命中</span><b>' + escape(request.cached) + ' · ' + escape(request.ratio) + '</b></div>' +
      '<div class="detail-row"><span>缓存细节</span><b>' + escape(request.detail) + '</b></div>' +
      '<div class="detail-row"><span>推理强度</span><code>' + escape(request.reasoning) + '</code></div>' +
      '<div class="detail-row"><span>网络路径</span><b>' + escape(request.networkPath || "—") + ' · ' + escape(request.httpVersion || "—") + '</b></div>' +
      '<div class="detail-row"><span>下游状态</span><b>' + escape(request.downstreamStatus || "—") + '</b></div>' +
      '<div class="detail-row"><span>Responses session</span><b>' + escape(request.session || "—") + '</b></div></div>';
    hydrateIcons();
    openOverlay("requestOverlay");
  };

  document.addEventListener("change", (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement) || input.id !== "proxyBaseUrlInput") return;
    try {
      const parsed = new URL(input.value.trim());
      const port = Number(parsed.port || (parsed.protocol === "https:" ? 443 : 80));
      if (!parsed.hostname || !port) throw new Error("invalid");
      send("save-proxy-mode", { host: parsed.hostname, port });
    } catch (_) {
      showToast("Proxy Mode Base URL 无效，未保存");
    }
  });

  window.parent.postMessage({ channel: CHANNEL, kind: "ready" }, "*");
})();`;

// The embedded prototype and bridge are static build assets. Keeping one
// immutable document avoids rebuilding the iframe source during 1s metric
// refreshes and preserves its DOM/event state for the entire desktop session.
const graphitePrototypeDocument = createGraphitePrototypeDocument(bridgeSource);

function protoAgentId(agent: AgentInjectionConfig): string {
  return agent.kind === "proxy-mode" ? "proxy" : agent.id;
}

function localBaseUrl(host: string | undefined, port: number | undefined, suffix: string): string {
  const rawHost = String(host || "127.0.0.1").trim();
  const clientHost = rawHost === "0.0.0.0" || rawHost === "::"
    ? "127.0.0.1"
    : rawHost;
  const bracketedHost = clientHost.includes(":") && !clientHost.startsWith("[")
    ? `[${clientHost}]`
    : clientHost;
  return `http://${bracketedHost}:${Number(port || 18883)}${suffix}`;
}

function iconForAgent(kind: AgentInjectionConfig["kind"]): string {
  const icons: Record<AgentInjectionConfig["kind"], string> = {
    "claude-code": "code-2",
    "claude-desktop": "message-square",
    codex: "square-terminal",
    gemini: "sparkles",
    "open-code": "blocks",
    "open-claw": "bot",
    hermes: "share-2",
    "proxy-mode": "waypoints"
  };
  return icons[kind];
}

function channelLabel(channel: Channel): string {
  if (channel === "responses") return "Responses";
  if (channel === "chat") return "Chat";
  return "Anthropic";
}

function formatTokens(value?: number | null): string {
  const safe = value ?? 0;
  if (safe >= 10_000) return `${(safe / 10_000).toFixed(safe >= 100_000 ? 1 : 2)} 万`;
  return safe.toLocaleString("zh-CN");
}

function formatTokenCount(value?: number | null): string {
  return (value ?? 0).toLocaleString("en-US");
}

const CACHE_DISPLAY_BUCKET_TOKENS = 128;

type RequestCacheTailDisplay = {
  shortfallTokens: number;
  newTailTokens: number;
  avoidableTokens: number;
  providerUnstableTokens: number;
  unclassifiedTokens: number;
};

function wholeTokenCount(value?: number | null): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.floor(value)
    : 0;
}

/**
 * Prefer the backend's final 128-token gap classification. Earlier releases
 * only persisted aggregate usage, so their residual remains a gap instead of
 * being relabelled as new user context.
 */
function cacheTailDisplayForRequest(
  request: Pick<
    MetricsSnapshot["recent_requests"][number],
    | "input_tokens"
    | "cache_read_tokens"
    | "cache_new_tail_gap_tokens"
    | "cache_avoidable_gap_tokens"
    | "cache_provider_unstable_gap_tokens"
  >
): RequestCacheTailDisplay {
  const inputTokens = wholeTokenCount(request.input_tokens);
  const cacheReadTokens = Math.min(wholeTokenCount(request.cache_read_tokens), inputTokens);
  const cacheableInputTokens = Math.floor(inputTokens / CACHE_DISPLAY_BUCKET_TOKENS)
    * CACHE_DISPLAY_BUCKET_TOKENS;
  const legacyBucketShortfallTokens = Math.max(
    0,
    cacheableInputTokens - Math.min(cacheReadTokens, cacheableInputTokens)
  );
  const reportedNewTailTokens = wholeTokenCount(request.cache_new_tail_gap_tokens);
  const reportedAvoidableTokens = wholeTokenCount(request.cache_avoidable_gap_tokens);
  const reportedProviderUnstableTokens = wholeTokenCount(request.cache_provider_unstable_gap_tokens);
  const hasBackendBreakdown = reportedNewTailTokens > 0 ||
    reportedAvoidableTokens > 0 ||
    reportedProviderUnstableTokens > 0;
  const shortfallTokens = hasBackendBreakdown
    ? reportedNewTailTokens + reportedAvoidableTokens + reportedProviderUnstableTokens
    : legacyBucketShortfallTokens;

  return {
    shortfallTokens,
    newTailTokens: reportedNewTailTokens,
    avoidableTokens: reportedAvoidableTokens,
    providerUnstableTokens: reportedProviderUnstableTokens,
    unclassifiedTokens: hasBackendBreakdown ? 0 : legacyBucketShortfallTokens
  };
}

function cacheGapDetail(
  shortfall: number,
  avoidable: number,
  newTail: number,
  providerUnstable: number
): string {
  const segments: string[] = [];
  const append = (label: string, value: number) => {
    const safe = Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
    if (safe > 0) segments.push(`${label} ${formatTokenCount(safe)}`);
  };
  append("缺", shortfall);
  append("可", avoidable);
  append("新", newTail);
  append("水位", providerUnstable);
  return segments.length > 0 ? segments.join(" · ") : "满桶";
}

const confirmedCompactionSources = new Set([
  "responses-compaction-v2",
  "compact",
  "compact-fallback",
  "compact-chat-compat",
  "compact-fallback-chat-compat"
]);

function requestIsConfirmedCompaction(
  request: Pick<MetricsSnapshot["recent_requests"][number], "cache_status" | "upstream_call_source">
): boolean {
  return request.cache_status?.trim().toLowerCase() === "compact" &&
    confirmedCompactionSources.has(request.upstream_call_source?.trim().toLowerCase() ?? "");
}

type CompactionTrafficTotals = {
  requests: number;
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
  cacheShortfallTokens: number;
  cacheAvoidableGapTokens: number;
  cacheNewTailGapTokens: number;
  coldStartRequests: number;
  coldStartInputTokens: number;
  coldStartOutputTokens: number;
  coldStartCachedTokens: number;
  coldStartCacheShortfallTokens: number;
  coldStartCacheAvoidableGapTokens: number;
  coldStartCacheNewTailGapTokens: number;
};

const emptyCompactionTraffic: CompactionTrafficTotals = {
  requests: 0,
  inputTokens: 0,
  outputTokens: 0,
  cachedTokens: 0,
  cacheShortfallTokens: 0,
  cacheAvoidableGapTokens: 0,
  cacheNewTailGapTokens: 0,
  coldStartRequests: 0,
  coldStartInputTokens: 0,
  coldStartOutputTokens: 0,
  coldStartCachedTokens: 0,
  coldStartCacheShortfallTokens: 0,
  coldStartCacheAvoidableGapTokens: 0,
  coldStartCacheNewTailGapTokens: 0
};

function metricCount(value?: number | null): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}

function compactionTrafficForScope(
  records: readonly AgentProviderTrafficStats[] | null | undefined,
  agentId: string | null | undefined,
  providerId: string | null
): CompactionTrafficTotals {
  if (!records || !agentId) return emptyCompactionTraffic;
  return records
    .filter((record) => record.agent_id === agentId && (!providerId || record.provider_id === providerId))
    .reduce<CompactionTrafficTotals>((total, record) => ({
      requests: total.requests + metricCount(record.compaction_requests),
      inputTokens: total.inputTokens + metricCount(record.compaction_input_tokens),
      outputTokens: total.outputTokens + metricCount(record.compaction_output_tokens),
      cachedTokens: total.cachedTokens + metricCount(record.compaction_cache_read_tokens),
      cacheShortfallTokens: total.cacheShortfallTokens + metricCount(record.compaction_cache_shortfall_tokens),
      cacheAvoidableGapTokens: total.cacheAvoidableGapTokens + metricCount(record.compaction_cache_avoidable_gap_tokens),
      cacheNewTailGapTokens: total.cacheNewTailGapTokens + metricCount(record.compaction_cache_new_tail_gap_tokens),
      coldStartRequests: total.coldStartRequests + metricCount(record.cold_start_compaction_requests),
      coldStartInputTokens: total.coldStartInputTokens + metricCount(record.cold_start_compaction_input_tokens),
      coldStartOutputTokens: total.coldStartOutputTokens + metricCount(record.cold_start_compaction_output_tokens),
      coldStartCachedTokens: total.coldStartCachedTokens + metricCount(record.cold_start_compaction_cache_read_tokens),
      coldStartCacheShortfallTokens: total.coldStartCacheShortfallTokens + metricCount(record.cold_start_compaction_cache_shortfall_tokens),
      coldStartCacheAvoidableGapTokens: total.coldStartCacheAvoidableGapTokens + metricCount(record.cold_start_compaction_cache_avoidable_gap_tokens),
      coldStartCacheNewTailGapTokens: total.coldStartCacheNewTailGapTokens + metricCount(record.cold_start_compaction_cache_new_tail_gap_tokens)
    }), emptyCompactionTraffic);
}

function filteredMetricValue(
  total: number,
  coldStart: number,
  compaction: number,
  coldStartCompaction: number,
  includeColdStarts: boolean,
  includeCompactions: boolean
): number {
  const safeTotal = metricCount(total);
  if (includeColdStarts && includeCompactions) return safeTotal;
  const cold = metricCount(coldStart);
  const compact = metricCount(compaction);
  const overlap = Math.min(cold, compact, metricCount(coldStartCompaction));
  const excluded = !includeColdStarts && !includeCompactions
    ? cold + compact - overlap
    : !includeColdStarts
      ? cold
      : compact;
  return Math.max(0, safeTotal - excluded);
}

function formatDuration(value?: number | null): string {
  if (!value || value <= 0) return "—";
  return `${(value / 1000).toFixed(2)}s`;
}

function formatPercent(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "—";
  const normalized = value <= 1 ? value * 100 : value;
  return `${normalized.toFixed(1)}%`;
}

function formatRequestTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value;
  return date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function buildState(
  config: AppConfig | null,
  metrics: MetricsSnapshot | null,
  selectedAgentId: string,
  includeColdStarts: boolean,
  includeCompactions: boolean,
  showDetailedErrors: boolean,
  providerConnectionStatus: Record<string, string>,
  providerBalanceStatus: GraphitePrototypeHostProps["providerBalanceStatus"],
  metricsRefreshPolicy: "visible-1s" | "5s" | "manual",
  proxyStatus: ProxyStatus | null,
  networkPathDiagnostic: GraphitePrototypeHostProps["networkPathDiagnostic"],
  cacheValidation: CacheValidationStatus | null,
  appVersion: string
) {
  const providers = config?.providers ?? [];
  const agentConfigs = config?.agent_injections ?? [];
  const providerMap = new Map(providers.map((provider) => [provider.id, provider]));
  const selectedAgent = agentConfigs.find((agent) => agent.id === selectedAgentId) ?? null;
  const selectedProviderOrder = selectedAgent
    ? config?.agent_provider_orders?.find((entry) => entry.agent_id === selectedAgent.id)?.provider_ids ?? []
    : [];
  const visibleProviders = selectedAgent
    ? providersForGraphiteAgent(providers, selectedAgent, selectedProviderOrder)
    : providers;
  const agents = agentConfigs.map((agent) => {
    const provider = agent.provider_id ? providerMap.get(agent.provider_id) : undefined;
    const mappingCount = provider?.models.filter((model) => model.enabled).length ?? 0;
    return {
      id: protoAgentId(agent),
      sourceId: agent.id,
      label: agent.label,
      icon: iconForAgent(agent.kind),
      enabled: agent.enabled,
      provider: provider?.id ?? "",
      route: provider
        ? `${providerDisplayName(provider, agent)} · ${channelLabel(provider.channel)} · ${mappingCount ? `${mappingCount} 个模型映射` : "模型直传"}`
        : "未选择上游",
      endpoint: agent.kind === "codex" ? "/codex/v1" : agent.kind === "proxy-mode" ? "/v1" : "/v1",
      localBaseUrl: agent.kind === "proxy-mode"
        ? localBaseUrl(config?.proxy_mode_host, config?.proxy_mode_port, "/v1")
        : localBaseUrl(config?.host, config?.port, agent.kind === "codex" ? "/codex/v1" : "/v1"),
      localKey: agent.local_key ?? config?.local_key ?? ""
    };
  });
  const providerState = visibleProviders.map((provider) => ({
    id: provider.id,
    name: providerDisplayName(provider, selectedAgent),
    url: provider.base_url,
    channel: provider.channel_mode === "auto" ? "Auto" : channelLabel(provider.channel),
    mappings: provider.models.filter((model) => model.enabled).length,
    // An enabled pool is the routing boundary. Its connection-info Key is not
    // eligible and must not be shown as usable capacity in the provider list.
    keys: provider.key_pool?.enabled
      ? provider.key_pool.available_keys
      : (provider.has_api_key ? 1 : 0),
    keyPoolEnabled: provider.key_pool?.enabled === true,
    keyPoolCount: provider.key_pool?.keys.length ?? 0,
    active: false,
    latency: providerConnectionStatus[provider.id] ?? "未检测",
    balance: providerBalanceStatus[provider.id] ?? null
  }));
  const providerDetails = Object.fromEntries(visibleProviders.map((provider) => [provider.id, {
    id: provider.id,
    name: provider.name,
    base_url: provider.base_url,
    models_url: provider.models_url ?? "",
    custom_user_agent: provider.custom_user_agent ?? "",
    channel: provider.channel,
    channel_mode: provider.channel_mode,
    has_api_key: provider.has_api_key,
    use_system_proxy: provider.use_system_proxy,
    prompt_cache_retention_enabled: provider.prompt_cache_retention_enabled,
    request_body_gzip_enabled: provider.request_body_gzip_enabled,
    non_sse_compact_compat_enabled: provider.non_sse_compact_compat_enabled,
    auto_compact_token_limit: provider.auto_compact_token_limit ?? null,
    key_pool: provider.key_pool
      ? {
          enabled: provider.key_pool.enabled,
          strategy: provider.key_pool.strategy,
          failure_threshold: provider.key_pool.failure_threshold,
          recovery_minutes: provider.key_pool.recovery_minutes
        }
      : null,
    cache_capabilities: provider.cache_capabilities ?? [],
    network_diagnostic: networkPathDiagnostic?.provider_id === provider.id ? networkPathDiagnostic : null,
    models: provider.models.map((model) => ({
      id: model.id,
      request_model_id: model.request_model_id ?? null,
      context_window: model.context_window ?? null,
      reasoning_effort: model.reasoning_effort ?? null,
      reasoning_effort_override_enabled: model.reasoning_effort_override_enabled,
      supported_reasoning_efforts: model.supported_reasoning_efforts ?? []
    })),
    keys: (provider.key_pool?.keys ?? []).map((key) => ({
      id: key.id,
      alias: key.alias ?? "",
      preview: key.preview,
      has_saved_secret: key.has_saved_secret,
      enabled: key.enabled,
      priority: key.priority,
      status: key.status
    }))
  }]));
  const providerForRequest = (providerId?: string | null, providerName?: string) => {
    if (providerId) {
      const provider = providers.find((item) => item.id === providerId);
      if (provider) return provider;
    }
    return providers.find((provider) =>
      provider.id === providerName || provider.name === providerName
    ) ?? null;
  };
  const successfulRequestSource = recordsForAgent((metrics?.recent_requests ?? [])
    .filter((request) => request.status >= 200 && request.status < 300 && request.cache_status !== "error")
    .sort((left, right) => Date.parse(right.at) - Date.parse(left.at)), selectedAgent?.id);
  const failedRequestSource = recordsForAgent((metrics?.recent_failed_requests ?? [])
    .filter((request) => request.status >= 400 || request.cache_status === "error")
    .sort((left, right) => Date.parse(right.at) - Date.parse(left.at)), selectedAgent?.id);
  type RequestRecord = MetricsSnapshot["recent_requests"][number];
  // `ttft_ms` is recorded by the relay at the first actual model output. Do
  // not substitute a raw network chunk here: Responses providers can send
  // `response.created` well before the first output-text delta.
  const modelFirstTokenMs = (request: RequestRecord): number => {
    const modelOutput = request.ttft_ms;
    if (typeof modelOutput === "number" && Number.isFinite(modelOutput) && modelOutput >= 0) return modelOutput;
    return 0;
  };
  const visibleTextFirstTokenMs = (request: RequestRecord): number => {
    const visibleText = request.visible_text_ttft_ms;
    if (typeof visibleText === "number" && Number.isFinite(visibleText) && visibleText >= 0) return visibleText;
    return 0;
  };
  const upstreamFirstEventMs = (request: RequestRecord): number => {
    const direct = request.first_byte_ms;
    if (typeof direct === "number" && Number.isFinite(direct) && direct >= 0) return direct;
    const headers = request.upstream_headers_ms;
    const firstChunk = request.upstream_first_chunk_ms;
    if (
      typeof headers === "number" && Number.isFinite(headers) && headers >= 0 &&
      typeof firstChunk === "number" && Number.isFinite(firstChunk) && firstChunk > 0
    ) {
      const derived = headers + firstChunk;
      if (derived > 0) return derived;
    }
    return modelFirstTokenMs(request);
  };
  const toRequestItem = (request: RequestRecord) => {
    const input = request.input_tokens ?? 0;
    const cached = request.cache_read_tokens ?? 0;
    const ratio = input > 0 ? `${(cached / input * 100).toFixed(1)}%` : "—";
    const requested = request.requested_model?.trim();
    const model = requested && requested !== request.model ? `${requested} → ${request.model}` : request.model;
    const provider = providerForRequest(request.provider_id, request.provider);
    const recordAgent = agentConfigs.find((agent) => agent.id === request.agent_id) ??
      agentConfigs.find((agent) => agent.label === request.agent_label) ??
      selectedAgent;
    const badge = requestAgentBadge(request.agent_id, request.agent_label, agentConfigs);
    const providerName = provider
      ? providerDisplayName(provider, recordAgent)
      : request.provider;
    const displayModelFirstTokenMs = modelFirstTokenMs(request);
    const displayVisibleTextFirstTokenMs = visibleTextFirstTokenMs(request);
    const displayUpstreamFirstEventMs = upstreamFirstEventMs(request);
    const state = requestRecordState({
      status: request.status,
      cacheStatus: request.cache_status,
      upstreamCallSource: request.upstream_call_source,
      downstreamDisconnected: request.downstream_disconnected,
      downstreamDisconnectStage: request.downstream_disconnect_stage,
      shadowAffinityLane: request.shadow_affinity_lane,
      prefixLagClassification: request.prefix_lag_classification,
      inputTokens: input,
      cacheReadTokens: cached,
      coldStart: request.cold_start
    }) ?? { label: "完成", tone: "complete" as const };
    const transport = requestTransportDisplay({
      upstreamCallKind: request.upstream_call_kind,
      cacheStatus: request.cache_status
    });
    const statusDisplay = requestRecordStatusDisplay({
      status: request.status,
      cacheStatus: request.cache_status,
      upstreamCallSource: request.upstream_call_source,
      downstreamDisconnected: request.downstream_disconnected,
      downstreamDisconnectStage: request.downstream_disconnect_stage,
      shadowAffinityLane: request.shadow_affinity_lane,
      prefixLagClassification: request.prefix_lag_classification,
      inputTokens: input,
      cacheReadTokens: cached,
      coldStart: request.cold_start
    });
    const cacheTailDisplay = cacheTailDisplayForRequest(request);
    return {
      id: request.id,
      recordedAt: request.at,
      time: formatRequestTime(request.at),
      provider: providerName,
      providerId: request.provider_id ?? provider?.id ?? null,
      model,
      agentLabel: badge.label,
      agentTone: badge.tone,
      clientChannel: request.client_channel ?? "Responses",
      channel: `${request.client_channel ?? "Responses"} · ${request.agent_id ?? "Agent"} · 流式`,
      ttft: formatDuration(displayModelFirstTokenMs),
      total: formatDuration(request.total_ms),
      ttftMs: displayModelFirstTokenMs,
      modelTtft: formatDuration(request.ttft_ms),
      modelTtftMs: request.ttft_ms,
      visibleTextTtft: displayVisibleTextFirstTokenMs > 0
        ? formatDuration(displayVisibleTextFirstTokenMs)
        : "未记录",
      visibleTextTtftMs: displayVisibleTextFirstTokenMs,
      upstreamFirstEvent: formatDuration(displayUpstreamFirstEventMs),
      upstreamFirstEventMs: displayUpstreamFirstEventMs,
      totalMs: request.total_ms,
      input: formatTokenCount(input),
      output: formatTokenCount(request.output_tokens ?? 0),
      cached: formatTokenCount(cached),
      inputTokens: input,
      outputTokens: request.output_tokens ?? 0,
      cachedTokens: cached,
      cacheShortfallTokens: cacheTailDisplay.shortfallTokens,
      cacheAvoidableGapTokens: cacheTailDisplay.avoidableTokens,
      cacheNewTailGapTokens: cacheTailDisplay.newTailTokens,
      cacheProviderUnstableGapTokens: cacheTailDisplay.providerUnstableTokens,
      reasoning: request.effective_reasoning_effort ?? request.configured_reasoning_effort ?? request.agent_reasoning_effort ?? "—",
      ratio,
      detail: cacheGapDetail(
        cacheTailDisplay.shortfallTokens,
        cacheTailDisplay.avoidableTokens,
        cacheTailDisplay.newTailTokens,
        cacheTailDisplay.providerUnstableTokens
      ),
      errorDetail: request.status >= 400 || request.cache_status === "error"
        ? `上游返回 HTTP ${request.status || "错误"}`
        : "",
      state: state.label,
      stateTone: state.tone,
      transport: transport.label,
      transportTone: transport.tone,
      statusLabel: statusDisplay.label,
      statusDetail: statusDisplay.detail,
      statusTone: statusDisplay.tone,
      failed: state.tone === "error",
      status: request.status,
      cacheStatus: request.cache_status,
      networkPath: request.upstream_network_path ?? "—",
      httpVersion: request.upstream_http_version ?? "—",
      downstreamStatus: request.downstream_disconnected ? "下游已断开（上游已收尾）" : "完整消费",
      session: request.response_session_reused
        ? "已验证增量复用"
        : request.response_session_skip_reason ?? "完整请求 · 未启用 delta"
    };
  };
  const requestItems = limitVisibleRequestRecords([
    ...successfulRequestSource.map(toRequestItem),
    ...failedRequestSource.map(toRequestItem)
  ].sort((left, right) => Date.parse(right.recordedAt) - Date.parse(left.recordedAt)), MAX_VISIBLE_REQUESTS);
  const requestScopes = scopesForSuccessfulAgentRequests(successfulRequestSource).map((scope) => {
    if (!scope.providerId) return scope;
    const provider = providerForRequest(scope.providerId);
    return provider ? { ...scope, label: providerDisplayName(provider, selectedAgent) } : scope;
  });
  const scopeIds = new Set(requestScopes.map((scope) => scope.id));
  for (const traffic of metrics?.agent_provider_stats ?? []) {
    const providerId = traffic.provider_id?.trim();
    if (
      !providerId ||
      traffic.agent_id !== selectedAgent?.id ||
      traffic.successful_requests <= 0 ||
      scopeIds.has(`provider:${providerId}`)
    ) continue;
    const provider = providerForRequest(providerId, traffic.provider);
    requestScopes.push({
      id: `provider:${providerId}`,
      label: provider ? providerDisplayName(provider, selectedAgent) : traffic.provider || providerId,
      providerId
    });
    scopeIds.add(`provider:${providerId}`);
  }
  const isForProvider = (
    request: MetricsSnapshot["recent_requests"][number],
    providerId: string | null
  ) => {
    if (!providerId) return true;
    if (request.provider_id) return request.provider_id === providerId;
    const provider = providerForRequest(null, request.provider);
    return provider?.id === providerId;
  };
  const successfulForScope = (providerId: string | null) =>
    successfulRequestSource.filter((request) => isForProvider(request, providerId));
  const consideredSuccessfulForScope = (providerId: string | null) => {
    const successes = successfulForScope(providerId);
    return successes.filter((request) =>
      (includeColdStarts || !requestRecordIsBackendColdStart({ coldStart: request.cold_start })) &&
      (includeCompactions || !requestIsConfirmedCompaction(request))
    );
  };
  const exactHistoricalScopeForProvider = (providerId: string | null) => {
    if (!providerId) return null;
    const configuredModel = selectedAgent?.model_id?.trim();
    const isEligible = (candidate: MetricsSnapshot["recent_requests"][number]) => {
      const realm = candidate.shadow_affinity_realm_id?.trim();
      const callKind = candidate.upstream_call_kind?.trim();
      return Boolean(
        realm &&
        candidate.model?.trim() &&
        candidate.client_channel?.trim() &&
        candidate.upstream_channel?.trim() &&
        (callKind === "stream" || callKind === "sync")
      );
    };
    const candidates = successfulForScope(providerId);
    const request = candidates.find((candidate) =>
      isEligible(candidate) && (!configuredModel ||
        candidate.model === configuredModel || candidate.requested_model === configuredModel)
    ) ?? candidates.find(isEligible);
    if (!request) return null;
    return {
      provider_realm_id: request.shadow_affinity_realm_id!.trim(),
      model: request.model.trim(),
      client_channel: request.client_channel.trim(),
      upstream_channel: request.upstream_channel.trim(),
      upstream_call_kind: request.upstream_call_kind!.trim(),
      stable_prefix_cohort_id: request.shadow_affinity_cohort_id?.trim() || null
    };
  };
  const metricForScope = (providerId: string | null) => {
    const failures = failedRequestSource.filter((request) => isForProvider(request, providerId));
    const aggregate = trafficForAgentScope(
      metrics?.agent_provider_stats,
      selectedAgent?.id,
      providerId
    );
    const compaction = compactionTrafficForScope(metrics?.agent_provider_stats, selectedAgent?.id, providerId);
    const inputTokens = aggregate ? filteredMetricValue(
      aggregate.inputTokens,
      aggregate.coldStartInputTokens,
      compaction.inputTokens,
      compaction.coldStartInputTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    const outputTokens = aggregate ? filteredMetricValue(
      aggregate.outputTokens,
      aggregate.coldStartOutputTokens,
      compaction.outputTokens,
      compaction.coldStartOutputTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    const cachedTokens = aggregate ? filteredMetricValue(
      aggregate.cachedTokens,
      aggregate.coldStartCachedTokens,
      compaction.cachedTokens,
      compaction.coldStartCachedTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    const cacheShortfall = aggregate ? filteredMetricValue(
      aggregate.cacheShortfallTokens,
      aggregate.coldStartCacheShortfallTokens,
      compaction.cacheShortfallTokens,
      compaction.coldStartCacheShortfallTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    const cacheAvoidable = aggregate ? filteredMetricValue(
      aggregate.cacheAvoidableGapTokens,
      aggregate.coldStartCacheAvoidableGapTokens,
      compaction.cacheAvoidableGapTokens,
      compaction.coldStartCacheAvoidableGapTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    const cacheNewTail = aggregate ? filteredMetricValue(
      aggregate.cacheNewTailGapTokens,
      aggregate.coldStartCacheNewTailGapTokens,
      compaction.cacheNewTailGapTokens,
      compaction.coldStartCacheNewTailGapTokens,
      includeColdStarts,
      includeCompactions
    ) : 0;
    return {
      cacheRate: aggregate && inputTokens > 0 ? formatPercent(cachedTokens / inputTokens) : "—",
      inputTokens: aggregate ? formatTokens(inputTokens) : "—",
      outputTokens: aggregate ? formatTokens(outputTokens) : "—",
      cachedTokens: aggregate ? formatTokens(cachedTokens) : "—",
      successfulRequests: aggregate ? filteredMetricValue(
        aggregate.successfulRequests,
        aggregate.coldStartRequests,
        compaction.requests,
        compaction.coldStartRequests,
        includeColdStarts,
        includeCompactions
      ) : "—",
      errors: aggregate?.errors ?? "—",
      compactionRequests: aggregate?.compactionRequests ?? "—",
      coldStartRequests: aggregate?.coldStartRequests ?? "—",
      cacheShortfall: aggregate ? formatTokens(cacheShortfall) : "—",
      cacheAvoidable: aggregate ? formatTokens(cacheAvoidable) : "—",
      cacheNewTail: aggregate ? formatTokens(cacheNewTail) : "—"
    };
  };
  const p95FirstByte = (requests: readonly RequestRecord[]) => {
    const values = requests
      .map(modelFirstTokenMs)
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((left, right) => left - right);
    if (!values.length) return 0;
    return values[Math.min(values.length - 1, Math.max(0, Math.ceil(values.length * 0.95) - 1))];
  };
  const providerRows = requestScopes
    .filter((scope) => scope.providerId)
    .map((scope) => {
      const metric = metricForScope(scope.providerId);
      const considered = consideredSuccessfulForScope(scope.providerId);
      return {
        provider: scope.label,
        successful: metric.successfulRequests,
        input: metric.inputTokens,
        cached: metric.cachedTokens,
        ttft: formatDuration(p95FirstByte(considered)),
        ratio: metric.cacheRate
      };
    });
  const metricsState = {
    scopes: requestScopes.map((scope) => ({
      ...scope,
      ...metricForScope(scope.providerId),
      // The opaque scope comes from a real successful request. It is used only
      // to ask history for the same token-hit cohort; it never changes routing.
      historicalScope: exactHistoricalScopeForProvider(scope.providerId)
    })),
    cacheEnabled: config?.cache.enabled ?? false,
    includeColdStarts,
    includeCompactions,
    showDetailedErrors,
    providerRows
  };
  const settings = {
    host: config?.host ?? "127.0.0.1",
    port: String(config?.port ?? 18883),
    hasLocalKey: Boolean(config?.local_key),
    defaultChannel: config?.default_channel ?? "responses",
    proxyAutoStart: config?.proxy_auto_start ?? false,
    upstreamProxyUrl: config?.upstream_proxy_url ?? "",
    refreshPolicy: metricsRefreshPolicy,
    workspaceFingerprint: config?.workspace_fingerprint ?? "",
    updatedAt: config?.updated_at ?? "",
    persistEncrypted: config?.cache.persist_encrypted ?? false,
    appVersion
  };
  const selectedProtoId = agents.find((agent) => agent.sourceId === selectedAgentId)?.id ?? agents[0]?.id ?? "codex";
  return {
    agents,
    providers: providerState,
    providerDetails,
    requests: requestItems,
    metrics: metricsState,
    cacheValidation,
    settings,
    proxyStatus,
    appVersion,
    selectedAgentId: selectedProtoId
  };
}

interface GraphiteBridgeSync {
  config: GraphitePrototypeHostProps["config"];
  selectedAgentId: string;
  includeColdStarts: boolean;
  includeCompactions: boolean;
  showDetailedErrors: boolean;
  providerConnectionStatus: GraphitePrototypeHostProps["providerConnectionStatus"];
  providerBalanceStatus: GraphitePrototypeHostProps["providerBalanceStatus"];
  metricsRefreshPolicy: GraphitePrototypeHostProps["metricsRefreshPolicy"];
  proxyStatus: GraphitePrototypeHostProps["proxyStatus"];
  networkPathDiagnostic: GraphitePrototypeHostProps["networkPathDiagnostic"];
  cacheValidation: GraphitePrototypeHostProps["cacheValidation"];
  appVersion: string;
  metricsFingerprint: string;
  requestsFingerprint: string;
}

function requiresFullGraphiteState(
  previous: GraphiteBridgeSync | null,
  props: GraphitePrototypeHostProps
): boolean {
  return previous === null ||
    previous.config !== props.config ||
    previous.selectedAgentId !== props.selectedAgentId ||
    previous.includeColdStarts !== props.includeColdStarts ||
    previous.includeCompactions !== props.includeCompactions ||
    previous.showDetailedErrors !== props.showDetailedErrors ||
    previous.providerConnectionStatus !== props.providerConnectionStatus ||
    previous.providerBalanceStatus !== props.providerBalanceStatus ||
    previous.metricsRefreshPolicy !== props.metricsRefreshPolicy ||
    previous.proxyStatus !== props.proxyStatus ||
    previous.networkPathDiagnostic !== props.networkPathDiagnostic ||
    previous.cacheValidation !== props.cacheValidation ||
    previous.appVersion !== props.appVersion;
}

export function GraphitePrototypeHost(props: GraphitePrototypeHostProps) {
  const frameRef = useRef<HTMLIFrameElement>(null);
  const [ready, setReady] = useState(false);
  const [renderSuspended, setRenderSuspended] = useState(false);
  const onBridgeActionRef = useRef(props.onBridgeAction);
  const bridgeSyncRef = useRef<GraphiteBridgeSync | null>(null);
  const renderResumeTimerRef = useRef<number | null>(null);
  onBridgeActionRef.current = props.onBridgeAction;
  const state = useMemo(
    () => buildState(props.config, props.metrics, props.selectedAgentId, props.includeColdStarts, props.includeCompactions, props.showDetailedErrors, props.providerConnectionStatus, props.providerBalanceStatus, props.metricsRefreshPolicy, props.proxyStatus, props.networkPathDiagnostic, props.cacheValidation, props.appVersion),
    [props.appVersion, props.cacheValidation, props.config, props.includeColdStarts, props.includeCompactions, props.metrics, props.metricsRefreshPolicy, props.networkPathDiagnostic, props.providerBalanceStatus, props.providerConnectionStatus, props.proxyStatus, props.selectedAgentId, props.showDetailedErrors]
  );

  const send = useCallback((message: Record<string, unknown>) => {
    frameRef.current?.contentWindow?.postMessage({ channel: GRAPHITE_BRIDGE_CHANNEL, ...message }, "*");
  }, []);

  const metricsFingerprint = useMemo(() => JSON.stringify(state.metrics), [state.metrics]);
  const requestsFingerprint = useMemo(() => JSON.stringify(state.requests), [state.requests]);

  useEffect(() => {
    const appWindow = getCurrentWindow();
    const unlisten: Array<() => void> = [];
    let disposed = false;

    const clearResumeTimer = () => {
      if (renderResumeTimerRef.current === null) return;
      window.clearTimeout(renderResumeTimerRef.current);
      renderResumeTimerRef.current = null;
    };
    const settleAfterWindowActivity = () => {
      if (disposed) return;
      setRenderSuspended(true);
      clearResumeTimer();
      renderResumeTimerRef.current = window.setTimeout(() => {
        renderResumeTimerRef.current = null;
        void appWindow.isMinimized().then(
          (minimized) => {
            if (!disposed) setRenderSuspended(minimized);
          },
          () => {
            if (!disposed) setRenderSuspended(false);
          }
        );
      }, 220);
    };

    void Promise.all([
      appWindow.onMoved(settleAfterWindowActivity),
      appWindow.onResized(settleAfterWindowActivity),
      appWindow.onFocusChanged(({ payload: focused }) => {
        if (!focused) {
          clearResumeTimer();
          setRenderSuspended(true);
          return;
        }
        settleAfterWindowActivity();
      })
    ]).then(
      (listeners) => {
        if (disposed) {
          listeners.forEach((listener) => listener());
          return;
        }
        unlisten.push(...listeners);
      },
      () => {
        if (!disposed) setRenderSuspended(false);
      }
    );

    return () => {
      disposed = true;
      clearResumeTimer();
      unlisten.forEach((listener) => listener());
    };
  }, []);

  useEffect(() => {
    if (ready) send({ kind: "render-suspension", suspended: renderSuspended });
  }, [ready, renderSuspended, send]);

  useEffect(() => {
    if (!ready) {
      bridgeSyncRef.current = null;
      return;
    }

    const previous = bridgeSyncRef.current;
    if (previous === null || requiresFullGraphiteState(previous, props)) {
      send({ kind: "state", state });
    } else if (
      previous.metricsFingerprint !== metricsFingerprint ||
      previous.requestsFingerprint !== requestsFingerprint
    ) {
      send({
        kind: "metrics-delta",
        metrics: state.metrics,
        ...(previous.requestsFingerprint !== requestsFingerprint ? { requests: state.requests } : {})
      });
    }

    bridgeSyncRef.current = {
      config: props.config,
      selectedAgentId: props.selectedAgentId,
      includeColdStarts: props.includeColdStarts,
      includeCompactions: props.includeCompactions,
      showDetailedErrors: props.showDetailedErrors,
      providerConnectionStatus: props.providerConnectionStatus,
      providerBalanceStatus: props.providerBalanceStatus,
      metricsRefreshPolicy: props.metricsRefreshPolicy,
      proxyStatus: props.proxyStatus,
      networkPathDiagnostic: props.networkPathDiagnostic,
      cacheValidation: props.cacheValidation,
      appVersion: props.appVersion,
      metricsFingerprint,
      requestsFingerprint
    };
  }, [
    metricsFingerprint,
    props,
    ready,
    requestsFingerprint,
    send,
    state
  ]);

  useEffect(() => {
    const onMessage = (event: MessageEvent<GraphiteMessage>) => {
      if (event.source !== frameRef.current?.contentWindow || event.data?.channel !== GRAPHITE_BRIDGE_CHANNEL) return;
      if (event.data.kind === "ready") {
        setReady(true);
        return;
      }
      if (event.data.kind !== "action" || !event.data.action) return;
      const action = event.data.action;
      const payload = event.data.payload ?? {};
      const acknowledge = (response?: GraphiteBridgeResponse) =>
        send({ kind: "ack", requestId: event.data.requestId, ...response });
      const run = async () => {
        if (action === "load-release-champion") {
          const sequence = Number(payload.sequence ?? 0);
          const contextKey = String(payload.contextKey ?? "");
          const input = payload.input as unknown as ReleaseChampionQueryInput | undefined;
          try {
            if (!input?.agent_id || !contextKey) {
              throw new Error("版本命中对比范围不完整");
            }
            const data = await command<ReleaseChampionSnapshot>("get_release_champion", { input });
            acknowledge({ payload: { releaseChampion: { sequence, contextKey, data } } });
          } catch (error) {
            acknowledge({
              payload: {
                releaseChampion: {
                  sequence,
                  contextKey,
                  error: error instanceof Error ? error.message : String(error)
                }
              }
            });
          }
          return;
        }
        if (action === "load-metrics-trend") {
          const sequence = Number(payload.sequence ?? 0);
          const rangeKey = String(payload.rangeKey ?? "");
          const input = payload.input as unknown as MetricsTrendInput | undefined;
          try {
            if (!input?.start_utc || !input.end_utc || !input.agent_id || !rangeKey) {
              throw new Error("缓存趋势查询范围不完整");
            }
            const data = await command<MetricsTrendSnapshot>("get_metrics_trend", { input });
            acknowledge({ payload: { metricsTrend: { sequence, rangeKey, data } } });
          } catch (error) {
            acknowledge({
              payload: {
                metricsTrend: {
                  sequence,
                  rangeKey,
                  error: error instanceof Error ? error.message : String(error)
                }
              }
            });
          }
          return;
        }
        try {
          const response = await onBridgeActionRef.current(action, payload);
          acknowledge(response ?? undefined);
        } catch (error) {
          acknowledge({ error: String(error) });
        }
      };
      void run();
    };
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [send]);

  return (
    <iframe
      className="graphite-prototype-frame"
      ref={frameRef}
      srcDoc={graphitePrototypeDocument}
      title="Atoapi Graphite Control Desk"
    />
  );
}
