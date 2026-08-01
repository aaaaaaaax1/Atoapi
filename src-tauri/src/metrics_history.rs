use crate::persistence::{WriteBehindCoordinator, WriteOperation};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const METRICS_HISTORY_VERSION: u32 = 1;
const HOUR_SECONDS: i64 = 60 * 60;
const RETENTION_HOURS: i64 = 32 * 24;
const MAX_QUERY_HOURS: i64 = 30 * 24;
const MIN_RELEASE_CHAMPION_REQUESTS: u64 = 10;
const MIN_RELEASE_CHAMPION_INPUT_TOKENS: u64 = 128_000;
// This only runs at final shutdown, never on the request settlement path.
const METRICS_HISTORY_FLUSH_RETRIES: usize = 2;

type MetricsHistoryWriteJob =
    dyn Fn(&Path, &PersistedMetricsHistory) -> Result<()> + Send + Sync + 'static;

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsTrendQueryInput {
    pub start_utc: String,
    pub end_utc: String,
    pub agent_id: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub include_cold_starts: bool,
    /// Compatibility requests from an older UI omit this field.  Those
    /// requests must retain the historical all-traffic view rather than
    /// silently filtering a category they do not know about.
    #[serde(default = "default_include_compactions")]
    pub include_compactions: bool,
    /// Optional exact historical affinity scope. When all fields are present,
    /// trend queries never mix different Provider/Model/Key/request-family
    /// cohorts. Legacy callers may omit these fields and receive the
    /// explicitly labelled provider aggregate for compatibility.
    #[serde(default)]
    pub provider_realm_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub client_channel: Option<String>,
    #[serde(default)]
    pub upstream_channel: Option<String>,
    #[serde(default)]
    pub upstream_call_kind: Option<String>,
    /// Optional opaque stable-prefix family. When present with the complete
    /// route scope, the trend is limited to one comparable token-history
    /// family instead of mixing unrelated conversation prefixes.
    #[serde(default)]
    pub stable_prefix_cohort_id: Option<String>,
}

fn default_include_compactions() -> bool {
    true
}

/// Identifies the exact executable which wrote a release-cohort observation.
/// The user-facing comparison never relies on the package version alone: a
/// rebuilt binary with the same version remains a separate candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReleaseBuildIdentity {
    pub app_version: String,
    pub git_commit: String,
    pub executable_sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ReleaseRuntimeIdentity {
    pub build: ReleaseBuildIdentity,
    pub process_started_at: DateTime<Utc>,
}

impl ReleaseRuntimeIdentity {
    pub(crate) fn current(process_started_at: DateTime<Utc>) -> Self {
        Self {
            build: ReleaseBuildIdentity {
                app_version: env!("CARGO_PKG_VERSION").to_string(),
                git_commit: env!("ATOAPI_GIT_COMMIT").to_string(),
                executable_sha256: current_executable_sha256(),
            },
            process_started_at,
        }
    }
}

/// A key-safe comparison scope. `provider_realm_id` is already a one-way
/// affinity identity over deployment, channel, model, and selected Key; it
/// deliberately never contains the raw URL or credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReleaseCohortScope {
    pub agent_id: String,
    pub provider_id: String,
    pub provider_realm_id: String,
    pub model: String,
    pub client_channel: String,
    pub upstream_channel: String,
    pub upstream_call_kind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseChampionQueryInput {
    pub agent_id: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub include_cold_starts: bool,
    #[serde(default = "default_include_compactions")]
    pub include_compactions: bool,
    #[serde(default)]
    pub provider_realm_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub client_channel: Option<String>,
    #[serde(default)]
    pub upstream_channel: Option<String>,
    #[serde(default)]
    pub upstream_call_kind: Option<String>,
    /// Optional opaque stable-prefix family. When supplied, a release
    /// comparison is limited to the same historical token-hit family rather
    /// than combining unrelated instructions/tool-schema prefixes.
    #[serde(default)]
    pub stable_prefix_cohort_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChampionStatus {
    Improving,
    Regressed,
    Tied,
    InsufficientCurrentSamples,
    NoComparableChampion,
    AwaitingCurrentCohort,
    LegacyHistoryUnattributed,
    IncompleteFilter,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReleaseCohortSummary {
    pub app_version: String,
    pub git_commit: String,
    pub executable_sha256: String,
    pub process_started_at: Option<String>,
    pub provider_id: String,
    pub model: String,
    pub request_family: String,
    pub key_realm_fingerprint: String,
    pub values: MetricsTrendValues,
    pub sample_eligible: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReleaseChampionSnapshot {
    pub status: ReleaseChampionStatus,
    pub reason: String,
    pub current: Option<ReleaseCohortSummary>,
    pub champion: Option<ReleaseCohortSummary>,
    pub delta_cache_hit_rate: Option<f64>,
    pub minimum_successful_requests: u64,
    pub minimum_input_tokens: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct MetricsTrendValues {
    pub successful_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_miss_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_shortfall_tokens: u64,
    pub cache_avoidable_gap_tokens: u64,
    pub cache_new_tail_gap_tokens: u64,
    pub compaction_requests: u64,
    pub cold_start_requests: u64,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsTrendPoint {
    pub start_utc: String,
    #[serde(flatten)]
    pub values: MetricsTrendValues,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsTrendSnapshot {
    pub start_utc: String,
    pub end_utc: String,
    pub agent_id: String,
    pub provider_id: Option<String>,
    /// `exact_prefix_family` means one stable-prefix family inside an exact
    /// route scope; `exact` means the route scope is exact but includes its
    /// different stable prefixes;
    /// `provider_aggregate` is the backwards-compatible mixed view.
    pub scope_mode: String,
    /// `false` only when a selected legacy hour contains compaction traffic
    /// recorded before compaction token sub-buckets existed.  We never fake a
    /// subtraction for those old records.
    pub compaction_filter_complete: bool,
    pub summary: MetricsTrendValues,
    pub points: Vec<MetricsTrendPoint>,
}

#[derive(Debug, Clone)]
pub(crate) struct MetricsHistoryObservation {
    pub at: DateTime<Utc>,
    pub agent_id: String,
    pub provider_id: String,
    /// Present only when the terminal request supplied an attested, key-safe
    /// affinity realm. Legacy history remains available for trends but is
    /// intentionally excluded from version-champion comparisons.
    pub release_scope: Option<ReleaseCohortScope>,
    /// Opaque stable-prefix family from the final frozen request. It never
    /// contains user text and is used only for historical token-hit grouping.
    pub stable_prefix_cohort_id: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_shortfall_tokens: u64,
    pub cache_avoidable_gap_tokens: u64,
    pub cache_new_tail_gap_tokens: u64,
    pub compaction: bool,
    pub cold_start: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
struct TrendCounters {
    successful_requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_miss_tokens: u64,
    cache_creation_tokens: u64,
    cache_shortfall_tokens: u64,
    cache_avoidable_gap_tokens: u64,
    cache_new_tail_gap_tokens: u64,
    compaction_requests: u64,
    cold_start_requests: u64,
}

impl TrendCounters {
    fn from_observation(observation: &MetricsHistoryObservation) -> Self {
        let cache_read_tokens = observation.cache_read_tokens.min(observation.input_tokens);
        Self {
            successful_requests: 1,
            input_tokens: observation.input_tokens,
            output_tokens: observation.output_tokens,
            cache_read_tokens,
            cache_miss_tokens: observation.input_tokens.saturating_sub(cache_read_tokens),
            cache_creation_tokens: observation.cache_creation_tokens,
            cache_shortfall_tokens: observation.cache_shortfall_tokens,
            cache_avoidable_gap_tokens: observation.cache_avoidable_gap_tokens,
            cache_new_tail_gap_tokens: observation.cache_new_tail_gap_tokens,
            compaction_requests: u64::from(observation.compaction),
            cold_start_requests: u64::from(observation.cold_start),
        }
    }

    fn add_assign(&mut self, other: Self) {
        self.successful_requests = self
            .successful_requests
            .saturating_add(other.successful_requests);
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(other.cache_read_tokens);
        self.cache_miss_tokens = self
            .cache_miss_tokens
            .saturating_add(other.cache_miss_tokens);
        self.cache_creation_tokens = self
            .cache_creation_tokens
            .saturating_add(other.cache_creation_tokens);
        self.cache_shortfall_tokens = self
            .cache_shortfall_tokens
            .saturating_add(other.cache_shortfall_tokens);
        self.cache_avoidable_gap_tokens = self
            .cache_avoidable_gap_tokens
            .saturating_add(other.cache_avoidable_gap_tokens);
        self.cache_new_tail_gap_tokens = self
            .cache_new_tail_gap_tokens
            .saturating_add(other.cache_new_tail_gap_tokens);
        self.compaction_requests = self
            .compaction_requests
            .saturating_add(other.compaction_requests);
        self.cold_start_requests = self
            .cold_start_requests
            .saturating_add(other.cold_start_requests);
    }

    fn saturating_sub(self, other: Self) -> Self {
        Self {
            successful_requests: self
                .successful_requests
                .saturating_sub(other.successful_requests),
            input_tokens: self.input_tokens.saturating_sub(other.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(other.output_tokens),
            cache_read_tokens: self
                .cache_read_tokens
                .saturating_sub(other.cache_read_tokens),
            cache_miss_tokens: self
                .cache_miss_tokens
                .saturating_sub(other.cache_miss_tokens),
            cache_creation_tokens: self
                .cache_creation_tokens
                .saturating_sub(other.cache_creation_tokens),
            cache_shortfall_tokens: self
                .cache_shortfall_tokens
                .saturating_sub(other.cache_shortfall_tokens),
            cache_avoidable_gap_tokens: self
                .cache_avoidable_gap_tokens
                .saturating_sub(other.cache_avoidable_gap_tokens),
            cache_new_tail_gap_tokens: self
                .cache_new_tail_gap_tokens
                .saturating_sub(other.cache_new_tail_gap_tokens),
            compaction_requests: self
                .compaction_requests
                .saturating_sub(other.compaction_requests),
            cold_start_requests: self
                .cold_start_requests
                .saturating_sub(other.cold_start_requests),
        }
    }

    fn into_values(self) -> MetricsTrendValues {
        MetricsTrendValues {
            successful_requests: self.successful_requests,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_miss_tokens: self.cache_miss_tokens,
            cache_creation_tokens: self.cache_creation_tokens,
            cache_shortfall_tokens: self.cache_shortfall_tokens,
            cache_avoidable_gap_tokens: self.cache_avoidable_gap_tokens,
            cache_new_tail_gap_tokens: self.cache_new_tail_gap_tokens,
            compaction_requests: self.compaction_requests,
            cold_start_requests: self.cold_start_requests,
            cache_hit_rate: ratio(self.cache_read_tokens, self.input_tokens),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ScopeCounters {
    #[serde(default)]
    total: TrendCounters,
    #[serde(default)]
    cold_start: TrendCounters,
    #[serde(default)]
    compaction: TrendCounters,
    #[serde(default)]
    cold_start_compaction: TrendCounters,
    /// Older persisted history only has `total` and `cold_start`, so its
    /// compaction request count cannot be split back into precise token
    /// values.  New scopes set this before their first observation.
    #[serde(default)]
    compaction_breakdown_complete: bool,
}

impl ScopeCounters {
    fn observe(&mut self, counters: TrendCounters, cold_start: bool, compaction: bool) {
        if self.total.successful_requests == 0
            && self.cold_start.successful_requests == 0
            && self.compaction.successful_requests == 0
            && self.cold_start_compaction.successful_requests == 0
        {
            self.compaction_breakdown_complete = true;
        }
        self.total.add_assign(counters);
        if cold_start {
            self.cold_start.add_assign(counters);
        }
        if compaction {
            self.compaction.add_assign(counters);
            if cold_start {
                self.cold_start_compaction.add_assign(counters);
            }
        }
    }

    fn add_assign(&mut self, other: &Self) {
        let was_empty = self.total.successful_requests == 0
            && self.cold_start.successful_requests == 0
            && self.compaction.successful_requests == 0
            && self.cold_start_compaction.successful_requests == 0;
        self.total.add_assign(other.total);
        self.cold_start.add_assign(other.cold_start);
        self.compaction.add_assign(other.compaction);
        self.cold_start_compaction
            .add_assign(other.cold_start_compaction);
        if was_empty {
            self.compaction_breakdown_complete = other.compaction_breakdown_complete;
        } else {
            self.compaction_breakdown_complete &= other.compaction_breakdown_complete;
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HourBucket {
    #[serde(default)]
    by_agent_provider: BTreeMap<String, BTreeMap<String, ScopeCounters>>,
    /// Exact key-safe historical scopes. This is additive so older history
    /// remains readable and is never silently promoted into an exact scope.
    #[serde(default)]
    by_affinity_scope: BTreeMap<String, ScopeCounters>,
    /// A stricter, opaque stable-prefix-family split inside an exact route
    /// scope. Older history intentionally has no synthetic backfill.
    #[serde(default)]
    by_affinity_prefix_family: BTreeMap<String, ScopeCounters>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReleaseCohortRun {
    #[serde(default)]
    counters: ScopeCounters,
    #[serde(default)]
    last_observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReleaseCohortEntry {
    build: ReleaseBuildIdentity,
    scope: ReleaseCohortScope,
    /// Absent only for legacy route-level cohorts or requests for which a
    /// stable-prefix identity could not be derived. It is opaque and never
    /// stores user text.
    #[serde(default)]
    stable_prefix_cohort_id: Option<String>,
    #[serde(default)]
    runs: BTreeMap<String, ReleaseCohortRun>,
    #[serde(default)]
    last_observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMetricsHistory {
    version: u32,
    #[serde(default)]
    buckets: BTreeMap<i64, HourBucket>,
    /// Added without changing the v1 trend schema. Older files deserialize
    /// with an empty map and therefore fail closed as un-attributable rather
    /// than fabricating a historical version champion.
    #[serde(default)]
    release_cohorts: BTreeMap<String, ReleaseCohortEntry>,
}

impl Default for PersistedMetricsHistory {
    fn default() -> Self {
        Self {
            version: METRICS_HISTORY_VERSION,
            buckets: BTreeMap::new(),
            release_cohorts: BTreeMap::new(),
        }
    }
}

impl PersistedMetricsHistory {
    fn observe(
        &mut self,
        observation: MetricsHistoryObservation,
        runtime: Option<&ReleaseRuntimeIdentity>,
        now: DateTime<Utc>,
    ) -> bool {
        let agent_id = observation.agent_id.trim();
        let provider_id = observation.provider_id.trim();
        if agent_id.is_empty() || provider_id.is_empty() {
            return false;
        }
        self.prune(now);
        let bucket_start = hour_start_timestamp(observation.at);
        let oldest = hour_start_timestamp(now - Duration::hours(RETENTION_HOURS));
        if bucket_start < oldest {
            return false;
        }

        let counters = TrendCounters::from_observation(&observation);
        let stable_prefix_cohort_id = observation
            .stable_prefix_cohort_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let bucket = self.buckets.entry(bucket_start).or_default();
        let scope = bucket
            .by_agent_provider
            .entry(agent_id.to_string())
            .or_default()
            .entry(provider_id.to_string())
            .or_default();
        scope.observe(counters, observation.cold_start, observation.compaction);
        if let Some(release_scope) = observation.release_scope.as_ref() {
            bucket
                .by_affinity_scope
                .entry(affinity_scope_id(release_scope))
                .or_default()
                .observe(counters, observation.cold_start, observation.compaction);
            if let Some(stable_prefix_cohort_id) = stable_prefix_cohort_id {
                bucket
                    .by_affinity_prefix_family
                    .entry(affinity_prefix_family_scope_id(
                        release_scope,
                        stable_prefix_cohort_id,
                    ))
                    .or_default()
                    .observe(counters, observation.cold_start, observation.compaction);
            }
        }
        if let (Some(runtime), Some(release_scope)) = (runtime, observation.release_scope.as_ref())
        {
            self.observe_release_cohort(
                runtime,
                release_scope,
                stable_prefix_cohort_id,
                counters,
                observation.cold_start,
                observation.compaction,
                now,
            );
        }
        true
    }

    fn observe_release_cohort(
        &mut self,
        runtime: &ReleaseRuntimeIdentity,
        scope: &ReleaseCohortScope,
        stable_prefix_cohort_id: Option<&str>,
        counters: TrendCounters,
        cold_start: bool,
        compaction: bool,
        now: DateTime<Utc>,
    ) {
        let stable_prefix_cohort_id = stable_prefix_cohort_id
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let cohort_id = release_cohort_id(&runtime.build, scope, stable_prefix_cohort_id);
        let entry = self
            .release_cohorts
            .entry(cohort_id)
            .or_insert_with(|| ReleaseCohortEntry {
                build: runtime.build.clone(),
                scope: scope.clone(),
                stable_prefix_cohort_id: stable_prefix_cohort_id.map(ToOwned::to_owned),
                runs: BTreeMap::new(),
                last_observed_at: None,
            });
        let process_started_at = runtime
            .process_started_at
            .to_rfc3339_opts(SecondsFormat::Secs, true);
        let run = entry.runs.entry(process_started_at).or_default();
        run.counters.observe(counters, cold_start, compaction);
        run.last_observed_at = Some(now);
        entry.last_observed_at = Some(now);
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let oldest = hour_start_timestamp(now - Duration::hours(RETENTION_HOURS));
        self.buckets.retain(|start, _| *start >= oldest);
        let oldest_at = DateTime::<Utc>::from_timestamp(oldest, 0)
            .expect("metrics history retention boundary must be representable");
        self.release_cohorts.retain(|_, cohort| {
            cohort.runs.retain(|_, run| {
                run.last_observed_at
                    .is_some_and(|observed_at| observed_at >= oldest_at)
            });
            !cohort.runs.is_empty()
                && cohort
                    .last_observed_at
                    .is_some_and(|observed_at| observed_at >= oldest_at)
        });
    }

    fn query(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        agent_id: &str,
        provider_id: Option<&str>,
        exact_scope: Option<&ReleaseCohortScope>,
        stable_prefix_cohort_id: Option<&str>,
        include_cold_starts: bool,
        include_compactions: bool,
    ) -> MetricsTrendSnapshot {
        let mut summary = TrendCounters::default();
        let mut compaction_filter_complete = true;
        let scope_id = exact_scope.map(affinity_scope_id);
        let prefix_family_scope_id =
            exact_scope
                .zip(stable_prefix_cohort_id)
                .map(|(scope, stable_prefix_cohort_id)| {
                    affinity_prefix_family_scope_id(scope, stable_prefix_cohort_id)
                });
        let scope_mode = if prefix_family_scope_id.is_some() {
            "exact_prefix_family"
        } else if scope_id.is_some() {
            "exact"
        } else {
            "provider_aggregate"
        }
        .to_string();
        let mut points = Vec::with_capacity(((end - start).num_hours().max(0)) as usize);
        let mut bucket_start = start.timestamp();
        while bucket_start < end.timestamp() {
            let mut counters = TrendCounters::default();
            if let Some(bucket) = self.buckets.get(&bucket_start) {
                if let Some(scope_id) = prefix_family_scope_id.as_deref() {
                    if let Some(scope) = bucket.by_affinity_prefix_family.get(scope_id) {
                        let (effective, complete) =
                            effective_counters(scope, include_cold_starts, include_compactions);
                        counters.add_assign(effective);
                        compaction_filter_complete &= complete;
                    }
                } else if let Some(scope_id) = scope_id.as_deref() {
                    if let Some(scope) = bucket.by_affinity_scope.get(scope_id) {
                        let (effective, complete) =
                            effective_counters(scope, include_cold_starts, include_compactions);
                        counters.add_assign(effective);
                        compaction_filter_complete &= complete;
                    }
                } else if let Some(agent_scopes) = bucket.by_agent_provider.get(agent_id) {
                    if let Some(provider_id) = provider_id {
                        if let Some(scope) = agent_scopes.get(provider_id) {
                            let (effective, complete) =
                                effective_counters(scope, include_cold_starts, include_compactions);
                            counters.add_assign(effective);
                            compaction_filter_complete &= complete;
                        }
                    } else {
                        for scope in agent_scopes.values() {
                            let (effective, complete) =
                                effective_counters(scope, include_cold_starts, include_compactions);
                            counters.add_assign(effective);
                            compaction_filter_complete &= complete;
                        }
                    }
                }
            }
            summary.add_assign(counters);
            points.push(MetricsTrendPoint {
                start_utc: timestamp_to_rfc3339(bucket_start),
                values: counters.into_values(),
            });
            bucket_start = bucket_start.saturating_add(HOUR_SECONDS);
        }

        MetricsTrendSnapshot {
            start_utc: timestamp_to_rfc3339(start.timestamp()),
            end_utc: timestamp_to_rfc3339(end.timestamp()),
            agent_id: agent_id.to_string(),
            provider_id: provider_id.map(str::to_string),
            scope_mode,
            compaction_filter_complete,
            summary: summary.into_values(),
            points,
        }
    }

    fn release_champion(
        &self,
        runtime: Option<&ReleaseRuntimeIdentity>,
        input: &ReleaseChampionQueryInput,
    ) -> Result<ReleaseChampionSnapshot> {
        let agent_id = input.agent_id.trim();
        if agent_id.is_empty() {
            return Err(anyhow!("agent_id is required"));
        }
        let provider_id = input
            .provider_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let requested_exact_scope = exact_scope_from_query(
            agent_id,
            provider_id,
            input.provider_realm_id.as_deref(),
            input.model.as_deref(),
            input.client_channel.as_deref(),
            input.upstream_channel.as_deref(),
            input.upstream_call_kind.as_deref(),
        );
        let requested_stable_prefix_cohort_id = requested_exact_scope
            .as_ref()
            .and_then(|_| optional_scope_part(input.stable_prefix_cohort_id.as_deref()));
        let Some(runtime) = runtime else {
            return Ok(empty_release_champion(
                ReleaseChampionStatus::AwaitingCurrentCohort,
                "当前运行未提供可验证 build identity".to_string(),
            ));
        };
        let process_started_at = runtime
            .process_started_at
            .to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut current_candidates: Vec<(&ReleaseCohortEntry, &ReleaseCohortRun)> = Vec::new();
        for entry in self.release_cohorts.values() {
            if entry.build != runtime.build
                || entry.scope.agent_id != agent_id
                || provider_id.is_some_and(|provider_id| entry.scope.provider_id != provider_id)
                || requested_exact_scope
                    .as_ref()
                    .is_some_and(|requested| !scope_matches_query(&entry.scope, requested))
                || requested_stable_prefix_cohort_id
                    .as_ref()
                    .is_some_and(|requested| {
                        entry.stable_prefix_cohort_id.as_deref() != Some(requested.as_str())
                    })
            {
                continue;
            }
            let Some(run) = entry.runs.get(&process_started_at) else {
                continue;
            };
            current_candidates.push((entry, run));
        }
        if requested_exact_scope.is_none() || requested_stable_prefix_cohort_id.is_none() {
            let mut distinct_scopes: Vec<(&ReleaseCohortScope, Option<&str>)> = Vec::new();
            for (entry, _) in &current_candidates {
                let stable_prefix = entry.stable_prefix_cohort_id.as_deref();
                if !distinct_scopes
                    .iter()
                    .any(|(scope, prefix)| *scope == &entry.scope && *prefix == stable_prefix)
                {
                    distinct_scopes.push((&entry.scope, stable_prefix));
                }
            }
            if distinct_scopes.len() > 1 {
                return Ok(empty_release_champion(
                    ReleaseChampionStatus::IncompleteFilter,
                    "当前运行包含多个 token-history 亲和 scope；必须提供同一 Key realm、模型、请求族与稳定前缀，不能任取最新 scope"
                        .to_string(),
                ));
            }
        }
        let current = current_candidates
            .into_iter()
            .max_by_key(|(_, run)| run.last_observed_at);
        let Some((current_entry, current_run)) = current else {
            let reason = if self.release_cohorts.is_empty() {
                "历史记录未带版本 cohort；从本版本开始积累可比样本".to_string()
            } else {
                "当前所选 Provider 尚无带 key-realm 的版本 cohort 请求".to_string()
            };
            return Ok(empty_release_champion(
                if self.release_cohorts.is_empty() {
                    ReleaseChampionStatus::LegacyHistoryUnattributed
                } else {
                    ReleaseChampionStatus::AwaitingCurrentCohort
                },
                reason,
            ));
        };

        let (current_counters, current_complete) = effective_counters(
            &current_run.counters,
            input.include_cold_starts,
            input.include_compactions,
        );
        let current_summary = release_summary(
            &current_entry.build,
            &current_entry.scope,
            current_counters,
            Some(runtime.process_started_at),
        );
        if !current_complete {
            return Ok(ReleaseChampionSnapshot {
                status: ReleaseChampionStatus::IncompleteFilter,
                reason: "当前 cohort 含旧式压缩统计，不能精确应用所选过滤条件".to_string(),
                current: Some(current_summary),
                champion: None,
                delta_cache_hit_rate: None,
                minimum_successful_requests: MIN_RELEASE_CHAMPION_REQUESTS,
                minimum_input_tokens: MIN_RELEASE_CHAMPION_INPUT_TOKENS,
            });
        }

        let mut champion: Option<ReleaseCohortSummary> = None;
        for entry in self.release_cohorts.values() {
            if entry.build == runtime.build
                || entry.scope != current_entry.scope
                || entry.stable_prefix_cohort_id != current_entry.stable_prefix_cohort_id
            {
                continue;
            }
            let combined = combined_release_counters(entry);
            let (counters, complete) = effective_counters(
                &combined,
                input.include_cold_starts,
                input.include_compactions,
            );
            if !complete {
                continue;
            }
            let summary = release_summary(&entry.build, &entry.scope, counters, None);
            if !summary.sample_eligible || release_summary_beats(&summary, champion.as_ref()) {
                champion = Some(summary);
            }
        }

        if !current_summary.sample_eligible {
            return Ok(ReleaseChampionSnapshot {
                status: ReleaseChampionStatus::InsufficientCurrentSamples,
                reason: format!(
                    "当前 build 需要至少 {MIN_RELEASE_CHAMPION_REQUESTS} 条成功请求且 {MIN_RELEASE_CHAMPION_INPUT_TOKENS} input tokens 才参与冠军判断"
                ),
                current: Some(current_summary),
                champion,
                delta_cache_hit_rate: None,
                minimum_successful_requests: MIN_RELEASE_CHAMPION_REQUESTS,
                minimum_input_tokens: MIN_RELEASE_CHAMPION_INPUT_TOKENS,
            });
        }

        let Some(champion) = champion else {
            return Ok(ReleaseChampionSnapshot {
                status: if self.release_cohorts.is_empty() {
                    ReleaseChampionStatus::LegacyHistoryUnattributed
                } else {
                    ReleaseChampionStatus::NoComparableChampion
                },
                reason:
                    "未找到同 Provider、同 Key realm、同模型、同请求族、同稳定前缀的历史 build 冠军"
                        .to_string(),
                current: Some(current_summary),
                champion: None,
                delta_cache_hit_rate: None,
                minimum_successful_requests: MIN_RELEASE_CHAMPION_REQUESTS,
                minimum_input_tokens: MIN_RELEASE_CHAMPION_INPUT_TOKENS,
            });
        };
        let delta_cache_hit_rate =
            current_summary.values.cache_hit_rate - champion.values.cache_hit_rate;
        let status = if delta_cache_hit_rate > 0.000_01 {
            ReleaseChampionStatus::Improving
        } else if delta_cache_hit_rate < -0.000_01 {
            ReleaseChampionStatus::Regressed
        } else {
            ReleaseChampionStatus::Tied
        };
        Ok(ReleaseChampionSnapshot {
            status,
            reason:
                "当前与冠军使用相同的 Provider、Key realm、模型、请求族和稳定前缀；统计未伪造冷启动或压缩"
                    .to_string(),
            current: Some(current_summary),
            champion: Some(champion),
            delta_cache_hit_rate: Some(delta_cache_hit_rate),
            minimum_successful_requests: MIN_RELEASE_CHAMPION_REQUESTS,
            minimum_input_tokens: MIN_RELEASE_CHAMPION_INPUT_TOKENS,
        })
    }
}

fn empty_release_champion(
    status: ReleaseChampionStatus,
    reason: String,
) -> ReleaseChampionSnapshot {
    ReleaseChampionSnapshot {
        status,
        reason,
        current: None,
        champion: None,
        delta_cache_hit_rate: None,
        minimum_successful_requests: MIN_RELEASE_CHAMPION_REQUESTS,
        minimum_input_tokens: MIN_RELEASE_CHAMPION_INPUT_TOKENS,
    }
}

fn combined_release_counters(entry: &ReleaseCohortEntry) -> ScopeCounters {
    let mut combined = ScopeCounters::default();
    for run in entry.runs.values() {
        combined.add_assign(&run.counters);
    }
    combined
}

fn release_summary(
    build: &ReleaseBuildIdentity,
    scope: &ReleaseCohortScope,
    counters: TrendCounters,
    process_started_at: Option<DateTime<Utc>>,
) -> ReleaseCohortSummary {
    let values = counters.into_values();
    ReleaseCohortSummary {
        app_version: build.app_version.clone(),
        git_commit: build.git_commit.clone(),
        executable_sha256: build.executable_sha256.clone(),
        process_started_at: process_started_at
            .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true)),
        provider_id: scope.provider_id.clone(),
        model: scope.model.clone(),
        request_family: format!(
            "{}:{}:{}",
            scope.client_channel, scope.upstream_channel, scope.upstream_call_kind
        ),
        key_realm_fingerprint: scope.provider_realm_id.clone(),
        sample_eligible: values.successful_requests >= MIN_RELEASE_CHAMPION_REQUESTS
            && values.input_tokens >= MIN_RELEASE_CHAMPION_INPUT_TOKENS,
        values,
    }
}

fn release_summary_beats(
    candidate: &ReleaseCohortSummary,
    current: Option<&ReleaseCohortSummary>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    candidate
        .values
        .cache_hit_rate
        .total_cmp(&current.values.cache_hit_rate)
        .then_with(|| {
            candidate
                .values
                .input_tokens
                .cmp(&current.values.input_tokens)
        })
        .is_gt()
}

fn release_cohort_id(
    build: &ReleaseBuildIdentity,
    scope: &ReleaseCohortScope,
    stable_prefix_cohort_id: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    let mut parts = vec![
        if stable_prefix_cohort_id.is_some() {
            "atoapi-release-cohort-v2"
        } else {
            // Preserve the existing v1 identity for a legacy route-level
            // cohort. It remains readable, but is never silently promoted
            // into a stable-prefix comparison.
            "atoapi-release-cohort-v1"
        },
        build.app_version.as_str(),
        build.git_commit.as_str(),
        build.executable_sha256.as_str(),
        scope.agent_id.as_str(),
        scope.provider_id.as_str(),
        scope.provider_realm_id.as_str(),
        scope.model.as_str(),
        scope.client_channel.as_str(),
        scope.upstream_channel.as_str(),
        scope.upstream_call_kind.as_str(),
    ];
    if let Some(stable_prefix_cohort_id) = stable_prefix_cohort_id {
        parts.push(stable_prefix_cohort_id);
    }
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

/// A stable, opaque key for the exact historical hit cohort. It deliberately
/// includes the selected Key realm and request family, but never stores raw
/// URLs, credentials, or user content.
fn affinity_scope_id(scope: &ReleaseCohortScope) -> String {
    let mut hasher = Sha256::new();
    for part in [
        "atoapi-history-affinity-scope-v1",
        scope.agent_id.as_str(),
        scope.provider_id.as_str(),
        scope.provider_realm_id.as_str(),
        scope.model.as_str(),
        scope.client_channel.as_str(),
        scope.upstream_channel.as_str(),
        scope.upstream_call_kind.as_str(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

fn affinity_prefix_family_scope_id(
    scope: &ReleaseCohortScope,
    stable_prefix_cohort_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        "atoapi-history-affinity-prefix-family-v1",
        scope.agent_id.as_str(),
        scope.provider_id.as_str(),
        scope.provider_realm_id.as_str(),
        scope.model.as_str(),
        scope.client_channel.as_str(),
        scope.upstream_channel.as_str(),
        scope.upstream_call_kind.as_str(),
        stable_prefix_cohort_id,
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

fn optional_scope_part(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn exact_scope_from_query(
    agent_id: &str,
    provider_id: Option<&str>,
    provider_realm_id: Option<&str>,
    model: Option<&str>,
    client_channel: Option<&str>,
    upstream_channel: Option<&str>,
    upstream_call_kind: Option<&str>,
) -> Option<ReleaseCohortScope> {
    Some(ReleaseCohortScope {
        agent_id: agent_id.to_string(),
        provider_id: optional_scope_part(provider_id)?,
        provider_realm_id: optional_scope_part(provider_realm_id)?,
        model: optional_scope_part(model)?,
        client_channel: optional_scope_part(client_channel)?,
        upstream_channel: optional_scope_part(upstream_channel)?,
        upstream_call_kind: optional_scope_part(upstream_call_kind)?,
    })
}

fn scope_matches_query(scope: &ReleaseCohortScope, requested: &ReleaseCohortScope) -> bool {
    scope == requested
}

fn current_executable_sha256() -> String {
    let Ok(path) = std::env::current_exe() else {
        return "unavailable".to_string();
    };
    let Ok(file) = fs::File::open(path) else {
        return "unavailable".to_string();
    };
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(_) => return "unavailable".to_string(),
        }
    }
    format!("{:x}", hasher.finalize())
}

fn effective_counters(
    scope: &ScopeCounters,
    include_cold_starts: bool,
    include_compactions: bool,
) -> (TrendCounters, bool) {
    let mut effective = scope.total;
    if !include_cold_starts {
        effective = effective.saturating_sub(scope.cold_start);
    }

    // Compaction and cold-start are overlapping request categories.  Apply
    // inclusion-exclusion so a compacted cold read is never deducted twice.
    let compaction_filter_complete = include_compactions
        || scope.compaction_breakdown_complete
        || scope.total.compaction_requests == 0;
    if !include_compactions && scope.compaction_breakdown_complete {
        effective = effective.saturating_sub(scope.compaction);
        if !include_cold_starts {
            effective.add_assign(scope.cold_start_compaction);
        }
    }
    // Category counters describe the selected population, not the source
    // population.  The union add-back above restores ordinary usage only;
    // neither excluded category should remain visible as included work.
    if !include_cold_starts {
        effective.cold_start_requests = 0;
    }
    if !include_compactions && scope.compaction_breakdown_complete {
        effective.compaction_requests = 0;
    }
    (effective, compaction_filter_complete)
}

#[derive(Clone)]
pub(crate) struct MetricsHistory {
    /// The live view is the only source used by trend queries. The background
    /// writer never serializes it while holding this lock.
    state: Arc<Mutex<PersistedMetricsHistory>>,
    /// Successful observations waiting to be merged into the writer-owned
    /// snapshot. Taking this vector is O(1), so a slow disk cannot hold the
    /// live trend mutex or the MetricsStore settlement path.
    pending: Option<Arc<Mutex<Vec<MetricsHistoryObservation>>>>,
    writer: Option<WriteBehindCoordinator>,
    runtime: Option<ReleaseRuntimeIdentity>,
}

impl std::fmt::Debug for MetricsHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetricsHistory")
            .field("persistent", &self.writer.is_some())
            .finish()
    }
}

impl MetricsHistory {
    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        Self {
            state: Arc::new(Mutex::new(PersistedMetricsHistory::default())),
            pending: None,
            writer: None,
            runtime: None,
        }
    }

    pub(crate) fn load(path: PathBuf) -> Self {
        let write_job: Arc<MetricsHistoryWriteJob> = Arc::new(save_metrics_history);
        Self::load_with_write_job(path, write_job, None)
    }

    pub(crate) fn load_with_runtime(path: PathBuf, runtime: ReleaseRuntimeIdentity) -> Self {
        let write_job: Arc<MetricsHistoryWriteJob> = Arc::new(save_metrics_history);
        Self::load_with_write_job(path, write_job, Some(runtime))
    }

    fn load_with_write_job(
        path: PathBuf,
        write_job: Arc<MetricsHistoryWriteJob>,
        runtime: Option<ReleaseRuntimeIdentity>,
    ) -> Self {
        let history = match load_metrics_history(&path) {
            Ok(history) => history,
            Err(error) => {
                let backup = preserve_invalid_history(&path);
                eprintln!(
                    "Atoapi metrics history was ignored and reset because it could not be loaded: {error:#}; backup={}",
                    backup
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "unavailable".to_string())
                );
                PersistedMetricsHistory::default()
            }
        };
        let state = Arc::new(Mutex::new(history.clone()));
        let pending = Arc::new(Mutex::new(Vec::<MetricsHistoryObservation>::new()));
        // Keep the serialization snapshot separate from the queried live
        // state. This is intentionally cloned once at startup, not once per
        // request; later writes merge only the short pending observation list.
        let writer_state = Arc::new(Mutex::new(history));
        let pending_for_writer = pending.clone();
        let writer_state_for_writer = writer_state.clone();
        let write_job_for_writer = write_job.clone();
        let runtime_for_writer = runtime.clone();
        let writer = WriteBehindCoordinator::new("metrics_history_save", move |operation| {
            debug_assert_eq!(operation, WriteOperation::Snapshot);
            let pending = {
                let mut pending = pending_for_writer
                    .lock()
                    .expect("metrics history pending lock must not be poisoned");
                std::mem::take(&mut *pending)
            };
            let snapshot = {
                let mut writer_state = writer_state_for_writer
                    .lock()
                    .expect("metrics history writer lock must not be poisoned");
                if !pending.is_empty() {
                    let now = Utc::now();
                    for observation in pending {
                        let _ = writer_state.observe(observation, runtime_for_writer.as_ref(), now);
                    }
                }
                writer_state.clone()
            };
            write_job_for_writer(&path, &snapshot)
        });
        Self {
            state,
            pending: Some(pending),
            writer: Some(writer),
            runtime,
        }
    }

    #[cfg(test)]
    fn load_with_persistence_job(
        path: PathBuf,
        write_job: impl Fn(&Path, &PersistedMetricsHistory) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self::load_with_write_job(path, Arc::new(write_job), None)
    }

    pub(crate) fn observe(&self, observation: MetricsHistoryObservation) {
        let pending_observation = observation.clone();
        let changed = self
            .state
            .lock()
            .expect("metrics history lock must not be poisoned")
            .observe(observation, self.runtime.as_ref(), Utc::now());
        if changed {
            if let (Some(pending), Some(writer)) = (&self.pending, &self.writer) {
                pending
                    .lock()
                    .expect("metrics history pending lock must not be poisoned")
                    .push(pending_observation);
                writer.mark_dirty(WriteOperation::Snapshot);
            }
        }
    }

    pub(crate) fn query(&self, input: MetricsTrendQueryInput) -> Result<MetricsTrendSnapshot> {
        let requested_start = parse_query_timestamp("start_utc", &input.start_utc)?;
        let requested_end = parse_query_timestamp("end_utc", &input.end_utc)?;
        if requested_end <= requested_start {
            return Err(anyhow!("end_utc must be later than start_utc"));
        }
        if requested_end - requested_start > Duration::hours(MAX_QUERY_HOURS) {
            return Err(anyhow!("metrics trend range cannot exceed 30 days"));
        }
        let start = floor_to_hour(requested_start);
        let end = ceil_to_hour(requested_end);
        let agent_id = input.agent_id.trim();
        if agent_id.is_empty() {
            return Err(anyhow!("agent_id is required"));
        }
        let provider_id = input
            .provider_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let exact_scope = exact_scope_from_query(
            agent_id,
            provider_id,
            input.provider_realm_id.as_deref(),
            input.model.as_deref(),
            input.client_channel.as_deref(),
            input.upstream_channel.as_deref(),
            input.upstream_call_kind.as_deref(),
        );
        let stable_prefix_cohort_id = exact_scope
            .as_ref()
            .and_then(|_| optional_scope_part(input.stable_prefix_cohort_id.as_deref()));
        Ok(self
            .state
            .lock()
            .expect("metrics history lock must not be poisoned")
            .query(
                start,
                end,
                agent_id,
                provider_id,
                exact_scope.as_ref(),
                stable_prefix_cohort_id.as_deref(),
                input.include_cold_starts,
                input.include_compactions,
            ))
    }

    pub(crate) fn release_champion(
        &self,
        input: ReleaseChampionQueryInput,
    ) -> Result<ReleaseChampionSnapshot> {
        self.state
            .lock()
            .expect("metrics history lock must not be poisoned")
            .release_champion(self.runtime.as_ref(), &input)
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        match &self.writer {
            Some(writer) => {
                let mut last_error = match writer.flush_latest().await {
                    Ok(()) => return Ok(()),
                    Err(error) => error,
                };
                for _ in 0..METRICS_HISTORY_FLUSH_RETRIES {
                    match writer.retry_latest().await {
                        Ok(()) => return Ok(()),
                        Err(error) => last_error = error,
                    }
                }
                Err(last_error)
            }
            None => Ok(()),
        }
    }
}

pub(crate) fn metrics_history_path(config_dir: &Path) -> PathBuf {
    config_dir.join("metrics-history.json")
}

fn parse_query_timestamp(field: &str, value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value.trim())
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{field} must be an RFC3339 timestamp"))
}

fn hour_start_timestamp(value: DateTime<Utc>) -> i64 {
    value.timestamp().div_euclid(HOUR_SECONDS) * HOUR_SECONDS
}

fn floor_to_hour(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(hour_start_timestamp(value), 0)
        .expect("an hourly metrics timestamp must be representable")
}

fn ceil_to_hour(value: DateTime<Utc>) -> DateTime<Utc> {
    let floor = floor_to_hour(value);
    if floor == value {
        floor
    } else {
        floor + Duration::hours(1)
    }
}

fn timestamp_to_rfc3339(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .expect("an hourly metrics timestamp must be representable")
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn load_metrics_history(path: &Path) -> Result<PersistedMetricsHistory> {
    if !path.exists() {
        return Ok(PersistedMetricsHistory::default());
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut history: PersistedMetricsHistory = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    if history.version != METRICS_HISTORY_VERSION {
        return Err(anyhow!(
            "unsupported metrics history version {}",
            history.version
        ));
    }
    history.prune(Utc::now());
    Ok(history)
}

fn save_metrics_history(path: &Path, history: &PersistedMetricsHistory) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("json.tmp");
    let raw = format!("{}\n", serde_json::to_string_pretty(history)?);
    fs::write(&temp, raw).with_context(|| format!("failed to write {}", temp.display()))?;
    if fs::rename(&temp, path).is_err() {
        // Windows does not atomically replace an existing target with
        // `rename`. Stage the old file and restore it if the final move fails,
        // so a transient filesystem error cannot erase the last good history.
        let previous = path.with_extension("json.previous");
        if previous.exists() {
            fs::remove_file(&previous)
                .with_context(|| format!("failed to remove {}", previous.display()))?;
        }
        if path.exists() {
            fs::rename(path, &previous)
                .with_context(|| format!("failed to stage {} for replacement", path.display()))?;
        }
        if let Err(error) = fs::rename(&temp, path) {
            if previous.exists() {
                let _ = fs::rename(&previous, path);
            }
            return Err(error).with_context(|| format!("failed to replace {}", path.display()));
        }
        if previous.exists() {
            let _ = fs::remove_file(previous);
        }
    }
    Ok(())
}

fn preserve_invalid_history(path: &Path) -> Option<PathBuf> {
    if !path.exists() {
        return None;
    }
    let file_name = path.file_name()?.to_string_lossy();
    let backup = path.with_file_name(format!(
        "{file_name}.corrupt-{}",
        Utc::now().format("%Y%m%dT%H%M%SZ")
    ));
    fs::rename(path, &backup)
        .or_else(|_| {
            fs::copy(path, &backup)?;
            fs::remove_file(path)
        })
        .ok()?;
    Some(backup)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use uuid::Uuid;

    fn hour_now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(hour_start_timestamp(Utc::now()), 0).unwrap()
    }

    fn observation(
        at: DateTime<Utc>,
        provider_id: &str,
        cold_start: bool,
    ) -> MetricsHistoryObservation {
        MetricsHistoryObservation {
            at,
            agent_id: "codex".to_string(),
            provider_id: provider_id.to_string(),
            release_scope: None,
            stable_prefix_cohort_id: None,
            input_tokens: 1_000,
            output_tokens: 25,
            cache_read_tokens: 900,
            cache_creation_tokens: 128,
            cache_shortfall_tokens: 100,
            cache_avoidable_gap_tokens: 40,
            cache_new_tail_gap_tokens: 60,
            compaction: false,
            cold_start,
        }
    }

    fn query(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        provider_id: Option<&str>,
        include_cold_starts: bool,
    ) -> MetricsTrendQueryInput {
        MetricsTrendQueryInput {
            start_utc: start.to_rfc3339(),
            end_utc: end.to_rfc3339(),
            agent_id: "codex".to_string(),
            provider_id: provider_id.map(str::to_string),
            include_cold_starts,
            include_compactions: true,
            provider_realm_id: None,
            model: None,
            client_channel: None,
            upstream_channel: None,
            upstream_call_kind: None,
            stable_prefix_cohort_id: None,
        }
    }

    fn query_with_filters(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        include_cold_starts: bool,
        include_compactions: bool,
    ) -> MetricsTrendQueryInput {
        MetricsTrendQueryInput {
            include_compactions,
            ..query(start, end, None, include_cold_starts)
        }
    }

    fn release_runtime(
        version: &str,
        commit: &str,
        executable_sha256: &str,
        process_started_at: DateTime<Utc>,
    ) -> ReleaseRuntimeIdentity {
        ReleaseRuntimeIdentity {
            build: ReleaseBuildIdentity {
                app_version: version.to_string(),
                git_commit: commit.to_string(),
                executable_sha256: executable_sha256.to_string(),
            },
            process_started_at,
        }
    }

    fn release_observation(
        at: DateTime<Utc>,
        realm: &str,
        cache_read_tokens: u64,
    ) -> MetricsHistoryObservation {
        MetricsHistoryObservation {
            at,
            agent_id: "codex".to_string(),
            provider_id: "provider-a".to_string(),
            release_scope: Some(ReleaseCohortScope {
                agent_id: "codex".to_string(),
                provider_id: "provider-a".to_string(),
                provider_realm_id: realm.to_string(),
                model: "gpt-test".to_string(),
                client_channel: "responses".to_string(),
                upstream_channel: "responses".to_string(),
                upstream_call_kind: "stream".to_string(),
            }),
            stable_prefix_cohort_id: None,
            input_tokens: 20_000,
            output_tokens: 25,
            cache_read_tokens,
            cache_creation_tokens: 0,
            cache_shortfall_tokens: 20_000_u64.saturating_sub(cache_read_tokens),
            cache_avoidable_gap_tokens: 0,
            cache_new_tail_gap_tokens: 20_000_u64.saturating_sub(cache_read_tokens),
            compaction: false,
            cold_start: false,
        }
    }

    #[test]
    fn queries_scoped_hour_buckets_and_fills_missing_hours() {
        let history = MetricsHistory::in_memory();
        let start = hour_now() - Duration::hours(2);
        history.observe(observation(start, "provider-a", false));
        let mut second = observation(start + Duration::hours(1), "provider-b", false);
        second.input_tokens = 500;
        second.cache_read_tokens = 700;
        second.compaction = true;
        history.observe(second);

        let snapshot = history
            .query(query(start, start + Duration::hours(3), None, true))
            .unwrap();
        assert_eq!(snapshot.points.len(), 3);
        assert_eq!(snapshot.summary.successful_requests, 2);
        assert_eq!(snapshot.summary.input_tokens, 1_500);
        assert_eq!(snapshot.summary.cache_read_tokens, 1_400);
        assert_eq!(snapshot.summary.cache_miss_tokens, 100);
        assert_eq!(snapshot.summary.compaction_requests, 1);
        assert_eq!(snapshot.points[2].values.successful_requests, 0);

        let provider = history
            .query(query(
                start,
                start + Duration::hours(3),
                Some("provider-a"),
                true,
            ))
            .unwrap();
        assert_eq!(provider.summary.successful_requests, 1);
        assert_eq!(provider.summary.input_tokens, 1_000);
    }

    #[test]
    fn release_champion_compares_only_the_same_key_safe_cohort() {
        let start = hour_now() - Duration::hours(1);
        let champion_runtime =
            release_runtime("1.4.9", "commit-champion", "a".repeat(64).as_str(), start);
        let candidate_runtime = release_runtime(
            "1.4.10",
            "commit-candidate",
            "b".repeat(64).as_str(),
            start + Duration::minutes(10),
        );
        let mut history = PersistedMetricsHistory::default();
        for index in 0..10 {
            let at = start + Duration::seconds(index);
            assert!(history.observe(
                release_observation(at, "realm-a", 19_800),
                Some(&champion_runtime),
                at,
            ));
            // A perfect but different selected Key/endpoint realm must never
            // be allowed to become the candidate's apparent champion.
            assert!(history.observe(
                release_observation(at, "realm-other", 20_000),
                Some(&champion_runtime),
                at,
            ));
        }
        for index in 0..10 {
            let at = candidate_runtime.process_started_at + Duration::seconds(index);
            assert!(history.observe(
                release_observation(at, "realm-a", 19_000),
                Some(&candidate_runtime),
                at,
            ));
        }

        let comparison = history
            .release_champion(
                Some(&candidate_runtime),
                &ReleaseChampionQueryInput {
                    agent_id: "codex".to_string(),
                    provider_id: Some("provider-a".to_string()),
                    include_cold_starts: true,
                    include_compactions: true,
                    provider_realm_id: Some("realm-a".to_string()),
                    model: Some("gpt-test".to_string()),
                    client_channel: Some("responses".to_string()),
                    upstream_channel: Some("responses".to_string()),
                    upstream_call_kind: Some("stream".to_string()),
                    stable_prefix_cohort_id: None,
                },
            )
            .expect("the release cohort query must be valid");
        assert_eq!(comparison.status, ReleaseChampionStatus::Regressed);
        assert!(comparison
            .delta_cache_hit_rate
            .is_some_and(|delta| delta < 0.0));
        assert_eq!(
            comparison
                .champion
                .as_ref()
                .expect("same-realm champion must exist")
                .key_realm_fingerprint,
            "realm-a"
        );
        assert_eq!(
            comparison
                .champion
                .as_ref()
                .expect("same-realm champion must exist")
                .app_version,
            "1.4.9"
        );
        assert!(
            comparison
                .current
                .as_ref()
                .expect("current candidate must be reported")
                .sample_eligible
        );
    }

    #[test]
    fn exact_trend_scope_never_mixes_key_realms_or_request_families() {
        let start = hour_now() - Duration::hours(1);
        let history = MetricsHistory::in_memory();

        history.observe(release_observation(start, "realm-a", 19_800));
        history.observe(release_observation(
            start + Duration::minutes(1),
            "realm-other",
            20_000,
        ));
        let mut sync = release_observation(start + Duration::minutes(2), "realm-a", 10_000);
        sync.release_scope
            .as_mut()
            .expect("release fixture has an exact scope")
            .upstream_call_kind = "sync".to_string();
        history.observe(sync);

        let provider_aggregate = history
            .query(query(
                start,
                start + Duration::hours(1),
                Some("provider-a"),
                true,
            ))
            .expect("provider aggregate query must remain available");
        assert_eq!(provider_aggregate.scope_mode, "provider_aggregate");
        assert_eq!(provider_aggregate.summary.successful_requests, 3);

        let mut exact_input = query(start, start + Duration::hours(1), Some("provider-a"), true);
        exact_input.provider_realm_id = Some("realm-a".to_string());
        exact_input.model = Some("gpt-test".to_string());
        exact_input.client_channel = Some("responses".to_string());
        exact_input.upstream_channel = Some("responses".to_string());
        exact_input.upstream_call_kind = Some("stream".to_string());
        let exact = history
            .query(exact_input)
            .expect("exact historical query must be valid");

        assert_eq!(exact.scope_mode, "exact");
        assert_eq!(exact.summary.successful_requests, 1);
        assert_eq!(exact.summary.input_tokens, 20_000);
        assert_eq!(exact.summary.cache_read_tokens, 19_800);
    }

    #[test]
    fn exact_prefix_family_trend_never_mixes_unrelated_stable_prefixes() {
        let start = hour_now() - Duration::hours(1);
        let history = MetricsHistory::in_memory();
        let mut family_a = release_observation(start, "realm-a", 19_800);
        family_a.stable_prefix_cohort_id = Some("stable-family-a".to_string());
        history.observe(family_a);
        let mut family_b = release_observation(start + Duration::minutes(1), "realm-a", 12_000);
        family_b.stable_prefix_cohort_id = Some("stable-family-b".to_string());
        history.observe(family_b);

        let mut exact_input = query(start, start + Duration::hours(1), Some("provider-a"), true);
        exact_input.provider_realm_id = Some("realm-a".to_string());
        exact_input.model = Some("gpt-test".to_string());
        exact_input.client_channel = Some("responses".to_string());
        exact_input.upstream_channel = Some("responses".to_string());
        exact_input.upstream_call_kind = Some("stream".to_string());
        exact_input.stable_prefix_cohort_id = Some("stable-family-a".to_string());
        let exact = history
            .query(exact_input)
            .expect("exact stable-prefix family query must be valid");

        assert_eq!(exact.scope_mode, "exact_prefix_family");
        assert_eq!(exact.summary.successful_requests, 1);
        assert_eq!(exact.summary.input_tokens, 20_000);
        assert_eq!(exact.summary.cache_read_tokens, 19_800);
    }

    #[test]
    fn release_champion_never_mixes_stable_prefix_token_history_families() {
        let start = hour_now() - Duration::hours(1);
        let champion_runtime =
            release_runtime("1.4.16", "commit-champion", "c".repeat(64).as_str(), start);
        let candidate_runtime = release_runtime(
            "1.4.18",
            "commit-candidate",
            "d".repeat(64).as_str(),
            start + Duration::minutes(10),
        );
        let mut history = PersistedMetricsHistory::default();

        for index in 0..10 {
            let at = start + Duration::seconds(index);
            let mut family_a = release_observation(at, "realm-a", 19_200);
            family_a.stable_prefix_cohort_id = Some("stable-family-a".to_string());
            assert!(history.observe(family_a, Some(&champion_runtime), at));

            // This unrelated stable prefix is better, but it must not raise
            // the apparent champion for family A.
            let mut family_b = release_observation(at, "realm-a", 20_000);
            family_b.stable_prefix_cohort_id = Some("stable-family-b".to_string());
            assert!(history.observe(family_b, Some(&champion_runtime), at));
        }
        for index in 0..10 {
            let at = candidate_runtime.process_started_at + Duration::seconds(index);
            let mut family_a = release_observation(at, "realm-a", 19_400);
            family_a.stable_prefix_cohort_id = Some("stable-family-a".to_string());
            assert!(history.observe(family_a, Some(&candidate_runtime), at));
        }

        let comparison = history
            .release_champion(
                Some(&candidate_runtime),
                &ReleaseChampionQueryInput {
                    agent_id: "codex".to_string(),
                    provider_id: Some("provider-a".to_string()),
                    include_cold_starts: true,
                    include_compactions: true,
                    provider_realm_id: Some("realm-a".to_string()),
                    model: Some("gpt-test".to_string()),
                    client_channel: Some("responses".to_string()),
                    upstream_channel: Some("responses".to_string()),
                    upstream_call_kind: Some("stream".to_string()),
                    stable_prefix_cohort_id: Some("stable-family-a".to_string()),
                },
            )
            .expect("stable-prefix champion query must be valid");

        assert_eq!(comparison.status, ReleaseChampionStatus::Improving);
        assert!(comparison
            .delta_cache_hit_rate
            .is_some_and(|delta| delta > 0.0));
        assert_eq!(
            comparison
                .champion
                .as_ref()
                .expect("same stable-prefix champion must exist")
                .values
                .cache_hit_rate,
            0.96
        );
    }

    #[test]
    fn release_champion_fails_closed_for_unattributed_legacy_history() {
        let start = hour_now();
        let runtime = release_runtime("1.4.10", "commit", &"c".repeat(64), start);
        let mut history = PersistedMetricsHistory::default();
        assert!(history.observe(observation(start, "provider-a", false), None, start));

        let comparison = history
            .release_champion(
                Some(&runtime),
                &ReleaseChampionQueryInput {
                    agent_id: "codex".to_string(),
                    provider_id: Some("provider-a".to_string()),
                    include_cold_starts: true,
                    include_compactions: true,
                    provider_realm_id: None,
                    model: None,
                    client_channel: None,
                    upstream_channel: None,
                    upstream_call_kind: None,
                    stable_prefix_cohort_id: None,
                },
            )
            .expect("legacy history must remain readable");
        assert_eq!(
            comparison.status,
            ReleaseChampionStatus::LegacyHistoryUnattributed
        );
        assert!(comparison.current.is_none());
        assert!(comparison.champion.is_none());
    }

    #[test]
    fn cold_start_filter_subtracts_every_counter() {
        let history = MetricsHistory::in_memory();
        let start = hour_now() - Duration::hours(1);
        history.observe(observation(start, "provider-a", true));
        history.observe(observation(start, "provider-a", false));

        let included = history
            .query(query(start, start + Duration::hours(1), None, true))
            .unwrap();
        assert_eq!(included.summary.successful_requests, 2);
        assert_eq!(included.summary.cold_start_requests, 1);
        assert_eq!(included.summary.cache_creation_tokens, 256);

        let excluded = history
            .query(query(start, start + Duration::hours(1), None, false))
            .unwrap();
        assert_eq!(excluded.summary.successful_requests, 1);
        assert_eq!(excluded.summary.cold_start_requests, 0);
        assert_eq!(excluded.summary.cache_creation_tokens, 128);
    }

    #[test]
    fn cold_start_and_compaction_filters_use_exact_union() {
        let history = MetricsHistory::in_memory();
        let start = hour_now() - Duration::hours(1);

        let normal = observation(start, "provider-a", false);
        let cold_only = observation(start, "provider-a", true);
        let mut compact_only = observation(start, "provider-a", false);
        compact_only.compaction = true;
        let mut compact_cold = observation(start, "provider-a", true);
        compact_cold.compaction = true;

        history.observe(normal);
        history.observe(cold_only);
        history.observe(compact_only);
        history.observe(compact_cold);

        let all = history
            .query(query_with_filters(
                start,
                start + Duration::hours(1),
                true,
                true,
            ))
            .unwrap();
        assert_eq!(all.summary.successful_requests, 4);
        assert_eq!(all.summary.input_tokens, 4_000);
        assert_eq!(all.summary.compaction_requests, 2);

        let without_cold = history
            .query(query_with_filters(
                start,
                start + Duration::hours(1),
                false,
                true,
            ))
            .unwrap();
        assert_eq!(without_cold.summary.successful_requests, 2);
        assert_eq!(without_cold.summary.input_tokens, 2_000);
        assert_eq!(without_cold.summary.compaction_requests, 1);

        let without_compaction = history
            .query(query_with_filters(
                start,
                start + Duration::hours(1),
                true,
                false,
            ))
            .unwrap();
        assert_eq!(without_compaction.summary.successful_requests, 2);
        assert_eq!(without_compaction.summary.input_tokens, 2_000);
        assert_eq!(without_compaction.summary.compaction_requests, 0);

        let without_union = history
            .query(query_with_filters(
                start,
                start + Duration::hours(1),
                false,
                false,
            ))
            .unwrap();
        assert_eq!(without_union.summary.successful_requests, 1);
        assert_eq!(without_union.summary.input_tokens, 1_000);
        assert_eq!(without_union.summary.compaction_requests, 0);
        assert!(without_union.compaction_filter_complete);
    }

    #[test]
    fn legacy_compaction_totals_are_not_falsely_subtracted() {
        let start = hour_now() - Duration::hours(1);
        let mut history = PersistedMetricsHistory::default();
        let mut total = TrendCounters::default();
        total.successful_requests = 1;
        total.input_tokens = 1_000;
        total.cache_read_tokens = 100;
        total.cache_miss_tokens = 900;
        total.compaction_requests = 1;
        history
            .buckets
            .entry(hour_start_timestamp(start))
            .or_default()
            .by_agent_provider
            .entry("codex".to_string())
            .or_default()
            .insert(
                "provider-a".to_string(),
                ScopeCounters {
                    total,
                    // This is what an existing v1 JSON record deserializes
                    // to: no compaction token subset and no completeness bit.
                    ..ScopeCounters::default()
                },
            );

        let snapshot = history.query(
            start,
            start + Duration::hours(1),
            "codex",
            None,
            None,
            None,
            true,
            false,
        );
        assert_eq!(snapshot.summary.successful_requests, 1);
        assert_eq!(snapshot.summary.input_tokens, 1_000);
        assert_eq!(snapshot.summary.compaction_requests, 1);
        assert!(!snapshot.compaction_filter_complete);
    }

    #[test]
    fn deserialized_legacy_history_and_omitted_filter_stay_conservative() {
        let start = hour_now() - Duration::hours(1);
        let timestamp = hour_start_timestamp(start);
        let raw = format!(
            r#"{{"version":1,"buckets":{{"{timestamp}":{{"by_agent_provider":{{"codex":{{"provider-a":{{"total":{{"successful_requests":1,"input_tokens":1000,"output_tokens":0,"cache_read_tokens":100,"cache_miss_tokens":900,"cache_creation_tokens":0,"cache_shortfall_tokens":900,"cache_avoidable_gap_tokens":0,"cache_new_tail_gap_tokens":900,"compaction_requests":1,"cold_start_requests":0}}}}}}}}}}}}}}"#
        );
        let history: PersistedMetricsHistory = serde_json::from_str(&raw)
            .expect("a pre-compaction-split history record must deserialize");
        let snapshot = history.query(
            start,
            start + Duration::hours(1),
            "codex",
            None,
            None,
            None,
            true,
            false,
        );
        assert_eq!(snapshot.summary.input_tokens, 1_000);
        assert!(!snapshot.compaction_filter_complete);

        let query: MetricsTrendQueryInput = serde_json::from_str(&format!(
            r#"{{"start_utc":"{}","end_utc":"{}","agent_id":"codex","include_cold_starts":true}}"#,
            start.to_rfc3339(),
            (start + Duration::hours(1)).to_rfc3339()
        ))
        .expect("an older UI query must deserialize");
        assert!(query.include_compactions);
    }

    #[test]
    fn validates_range_and_normalizes_partial_hours() {
        let history = MetricsHistory::in_memory();
        let start = hour_now();
        assert!(history
            .query(query(
                start,
                start + Duration::days(30) + Duration::hours(1),
                None,
                true,
            ))
            .is_err());
        let normalized = history
            .query(query(
                start + Duration::minutes(1),
                start + Duration::hours(1) + Duration::minutes(1),
                None,
                true,
            ))
            .unwrap();
        assert_eq!(
            normalized.start_utc,
            start.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        assert_eq!(
            normalized.end_utc,
            (start + Duration::hours(2)).to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        assert_eq!(normalized.points.len(), 2);
    }

    #[tokio::test]
    async fn persists_recovers_and_ignores_corrupt_or_unknown_files() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-metrics-history-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("metrics-history.json");
        let start = hour_now() - Duration::hours(1);

        let history = MetricsHistory::load(path.clone());
        history.observe(observation(start, "provider-a", false));
        history.flush().await.unwrap();
        let reloaded = MetricsHistory::load(path.clone());
        let snapshot = reloaded
            .query(query(start, start + Duration::hours(1), None, true))
            .unwrap();
        assert_eq!(snapshot.summary.successful_requests, 1);

        fs::write(&path, "not-json").unwrap();
        let corrupt = MetricsHistory::load(path.clone());
        assert_eq!(
            corrupt
                .query(query(start, start + Duration::hours(1), None, true))
                .unwrap()
                .summary
                .successful_requests,
            0
        );
        assert!(fs::read_dir(&dir).unwrap().any(|entry| {
            entry
                .ok()
                .and_then(|entry| entry.file_name().into_string().ok())
                .is_some_and(|name| name.contains("metrics-history.json.corrupt-"))
        }));

        fs::write(&path, r#"{"version":99,"buckets":{}}"#).unwrap();
        let unknown = MetricsHistory::load(path.clone());
        assert_eq!(
            unknown
                .query(query(start, start + Duration::hours(1), None, true))
                .unwrap()
                .summary
                .successful_requests,
            0
        );
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn persists_release_cohorts_across_builds_without_promoting_legacy_buckets() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-release-cohort-history-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("metrics-history.json");
        let start = hour_now() - Duration::hours(1);
        let champion_sha = "a".repeat(64);
        let candidate_sha = "b".repeat(64);
        let champion_runtime = release_runtime("1.4.9", "commit-a", &champion_sha, start);
        let candidate_runtime = release_runtime(
            "1.4.10",
            "commit-b",
            &candidate_sha,
            start + Duration::minutes(10),
        );

        let champion_history = MetricsHistory::load_with_runtime(path.clone(), champion_runtime);
        for index in 0..10 {
            champion_history.observe(release_observation(
                start + Duration::seconds(index),
                "realm-a",
                19_800,
            ));
        }
        champion_history.flush().await.unwrap();

        let candidate_history = MetricsHistory::load_with_runtime(path.clone(), candidate_runtime);
        for index in 0..10 {
            candidate_history.observe(release_observation(
                start + Duration::minutes(10) + Duration::seconds(index),
                "realm-a",
                19_000,
            ));
        }
        candidate_history.flush().await.unwrap();
        let comparison = candidate_history
            .release_champion(ReleaseChampionQueryInput {
                agent_id: "codex".to_string(),
                provider_id: Some("provider-a".to_string()),
                include_cold_starts: true,
                include_compactions: true,
                provider_realm_id: Some("realm-a".to_string()),
                model: Some("gpt-test".to_string()),
                client_channel: Some("responses".to_string()),
                upstream_channel: Some("responses".to_string()),
                upstream_call_kind: Some("stream".to_string()),
                stable_prefix_cohort_id: None,
            })
            .unwrap();
        assert_eq!(comparison.status, ReleaseChampionStatus::Regressed);
        assert_eq!(
            comparison
                .champion
                .as_ref()
                .expect("persisted champion must be restored")
                .executable_sha256,
            champion_sha
        );
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn flush_retries_transient_write_failures_without_losing_history() {
        let dir = std::env::temp_dir().join(format!(
            "atoapi-metrics-history-transient-write-{}",
            Uuid::new_v4().simple()
        ));
        let path = dir.join("metrics-history.json");
        let start = hour_now() - Duration::hours(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_job = attempts.clone();
        let history =
            MetricsHistory::load_with_persistence_job(path.clone(), move |path, snapshot| {
                if attempts_for_job.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(anyhow!("simulated transient metrics history write failure"));
                }
                save_metrics_history(path, snapshot)
            });

        history.observe(observation(start, "provider-a", false));
        history
            .flush()
            .await
            .expect("shutdown flush should retry temporary history write failures");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        let reloaded = MetricsHistory::load(path.clone());
        let snapshot = reloaded
            .query(query(start, start + Duration::hours(1), None, true))
            .unwrap();
        assert_eq!(snapshot.summary.successful_requests, 1);
        fs::remove_dir_all(dir).ok();
    }
}
