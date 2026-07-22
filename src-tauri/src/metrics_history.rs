use crate::persistence::{WriteBehindCoordinator, WriteOperation};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const METRICS_HISTORY_VERSION: u32 = 1;
const HOUR_SECONDS: i64 = 60 * 60;
const RETENTION_HOURS: i64 = 32 * 24;
const MAX_QUERY_HOURS: i64 = 30 * 24;
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
    pub summary: MetricsTrendValues,
    pub points: Vec<MetricsTrendPoint>,
}

#[derive(Debug, Clone)]
pub(crate) struct MetricsHistoryObservation {
    pub at: DateTime<Utc>,
    pub agent_id: String,
    pub provider_id: String,
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HourBucket {
    #[serde(default)]
    by_agent_provider: BTreeMap<String, BTreeMap<String, ScopeCounters>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMetricsHistory {
    version: u32,
    #[serde(default)]
    buckets: BTreeMap<i64, HourBucket>,
}

impl Default for PersistedMetricsHistory {
    fn default() -> Self {
        Self {
            version: METRICS_HISTORY_VERSION,
            buckets: BTreeMap::new(),
        }
    }
}

impl PersistedMetricsHistory {
    fn observe(&mut self, observation: MetricsHistoryObservation, now: DateTime<Utc>) -> bool {
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
        let scope = self
            .buckets
            .entry(bucket_start)
            .or_default()
            .by_agent_provider
            .entry(agent_id.to_string())
            .or_default()
            .entry(provider_id.to_string())
            .or_default();
        scope.total.add_assign(counters);
        if observation.cold_start {
            scope.cold_start.add_assign(counters);
        }
        true
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let oldest = hour_start_timestamp(now - Duration::hours(RETENTION_HOURS));
        self.buckets.retain(|start, _| *start >= oldest);
    }

    fn query(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        agent_id: &str,
        provider_id: Option<&str>,
        include_cold_starts: bool,
    ) -> MetricsTrendSnapshot {
        let mut summary = TrendCounters::default();
        let mut points = Vec::with_capacity(((end - start).num_hours().max(0)) as usize);
        let mut bucket_start = start.timestamp();
        while bucket_start < end.timestamp() {
            let mut counters = TrendCounters::default();
            if let Some(agent_scopes) = self
                .buckets
                .get(&bucket_start)
                .and_then(|bucket| bucket.by_agent_provider.get(agent_id))
            {
                if let Some(provider_id) = provider_id {
                    if let Some(scope) = agent_scopes.get(provider_id) {
                        counters.add_assign(effective_counters(scope, include_cold_starts));
                    }
                } else {
                    for scope in agent_scopes.values() {
                        counters.add_assign(effective_counters(scope, include_cold_starts));
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
            summary: summary.into_values(),
            points,
        }
    }
}

fn effective_counters(scope: &ScopeCounters, include_cold_starts: bool) -> TrendCounters {
    if include_cold_starts {
        scope.total
    } else {
        scope.total.saturating_sub(scope.cold_start)
    }
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
        }
    }

    pub(crate) fn load(path: PathBuf) -> Self {
        let write_job: Arc<MetricsHistoryWriteJob> = Arc::new(save_metrics_history);
        Self::load_with_write_job(path, write_job)
    }

    fn load_with_write_job(path: PathBuf, write_job: Arc<MetricsHistoryWriteJob>) -> Self {
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
                        let _ = writer_state.observe(observation, now);
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
        }
    }

    #[cfg(test)]
    fn load_with_persistence_job(
        path: PathBuf,
        write_job: impl Fn(&Path, &PersistedMetricsHistory) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self::load_with_write_job(path, Arc::new(write_job))
    }

    pub(crate) fn observe(&self, observation: MetricsHistoryObservation) {
        let pending_observation = observation.clone();
        let changed = self
            .state
            .lock()
            .expect("metrics history lock must not be poisoned")
            .observe(observation, Utc::now());
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
        Ok(self
            .state
            .lock()
            .expect("metrics history lock must not be poisoned")
            .query(start, end, agent_id, provider_id, input.include_cold_starts))
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
