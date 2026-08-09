use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const CHAMPION_RELEASE_DIR: &str = "v1.4.33-exact-sent-waterline-maturity-20260807";
const CHAMPION_EXE_NAME: &str = "Atoapi.exe";
const PROVIDER_ID: &str = "agent-codex-provider-2";
const MODEL_ID: &str = "gpt-5.6-terra";
const KEY_REALM_HASH: &str = "4574f5c28bcca32c7845a8625bed88d421bcdf03b48a4550d5109d3e2e25b407";
const SCENARIO: &str = "dynamic-tail-mix";
const EMBEDDED_VERIFIER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../scripts/verify-release-champion.mjs"
));

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChampionRunPhase {
    Idle,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReleaseChampionRunStatus {
    pub phase: ReleaseChampionRunPhase,
    pub scenario: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub output_path: Option<String>,
    pub exit_code: Option<i32>,
    pub pass: Option<bool>,
    pub fail_closed: Option<bool>,
    pub error: Option<String>,
}

impl Default for ReleaseChampionRunStatus {
    fn default() -> Self {
        Self {
            phase: ReleaseChampionRunPhase::Idle,
            scenario: SCENARIO.to_string(),
            started_at: None,
            finished_at: None,
            output_path: None,
            exit_code: None,
            pass: None,
            fail_closed: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReleaseChampionRunner {
    status: Arc<Mutex<ReleaseChampionRunStatus>>,
}

#[derive(Debug, Clone)]
struct ReleaseChampionRunPlan {
    workspace_root: PathBuf,
    output_path: PathBuf,
    started_at: DateTime<Utc>,
    arguments: Vec<String>,
}

impl ReleaseChampionRunner {
    pub async fn status(&self) -> ReleaseChampionRunStatus {
        self.status.lock().await.clone()
    }

    /// Starts one fixed, same-principal release comparison. This intentionally
    /// accepts no caller-controlled command, executable, Provider, Key, model,
    /// or output path. It can only compare the accepted v1.4.33 champion with
    /// the currently running package on the selected Codex Provider-2 realm.
    pub async fn start(&self, config_path: &Path) -> Result<ReleaseChampionRunStatus> {
        let mut status = self.status.lock().await;
        if status.phase == ReleaseChampionRunPhase::Running {
            bail!("release champion comparison is already running");
        }

        let started_at = Utc::now();
        let plan = ReleaseChampionRunPlan::prepare(config_path, started_at)?;
        *status = ReleaseChampionRunStatus {
            phase: ReleaseChampionRunPhase::Running,
            scenario: SCENARIO.to_string(),
            started_at: Some(started_at),
            finished_at: None,
            output_path: Some(plan.output_path.display().to_string()),
            exit_code: None,
            pass: None,
            fail_closed: None,
            error: None,
        };
        let started_status = status.clone();
        drop(status);

        let shared_status = self.status.clone();
        tauri::async_runtime::spawn(async move {
            let completed = match tokio::task::spawn_blocking(move || run_plan(plan)).await {
                Ok(Ok(status)) => status,
                Ok(Err(error)) => failed_status(error.to_string()),
                Err(error) => {
                    failed_status(format!("release champion worker join failed: {error}"))
                }
            };
            *shared_status.lock().await = completed;
        });

        Ok(started_status)
    }
}

impl ReleaseChampionRunPlan {
    fn prepare(config_path: &Path, started_at: DateTime<Utc>) -> Result<Self> {
        let config_dir = config_path
            .parent()
            .context("Atoapi config path has no parent directory")?;
        if !config_path.is_file() {
            bail!("Atoapi config.toml is unavailable for release champion verification");
        }

        let candidate_exe = std::env::current_exe()
            .context("cannot resolve the currently running Atoapi executable")?;
        let workspace_root = workspace_root_from_release_exe(&candidate_exe)?;
        let champion_exe = workspace_root
            .join("releases")
            .join(CHAMPION_RELEASE_DIR)
            .join(CHAMPION_EXE_NAME);
        if !champion_exe.is_file() {
            bail!(
                "accepted v1.4.33 champion executable is missing: {}",
                champion_exe.display()
            );
        }

        let output_dir = config_dir.join("release").join("release-champion");
        fs::create_dir_all(&output_dir)
            .context("failed to create release champion artifact directory")?;
        let script_path = materialize_embedded_verifier(&output_dir)?;
        let run_id = format!(
            "{}-{}",
            Utc::now().format("%Y%m%d-%H%M%S"),
            Uuid::new_v4().simple()
        );
        let output_path = output_dir.join(format!("v1433-current-{SCENARIO}-{run_id}.json"));
        let prompt_cache_key_prefix = format!("atoapi-release-champion-{run_id}");
        let arguments = vec![
            script_path.display().to_string(),
            "--live".to_string(),
            "--champion-exe".to_string(),
            champion_exe.display().to_string(),
            "--candidate-exe".to_string(),
            candidate_exe.display().to_string(),
            "--source-config-dir".to_string(),
            config_dir.display().to_string(),
            "--model".to_string(),
            MODEL_ID.to_string(),
            "--key-realm-hash".to_string(),
            KEY_REALM_HASH.to_string(),
            "--provider-id".to_string(),
            PROVIDER_ID.to_string(),
            "--scenario".to_string(),
            SCENARIO.to_string(),
            "--pairs".to_string(),
            "1".to_string(),
            "--turns".to_string(),
            "11".to_string(),
            "--max-output-tokens".to_string(),
            "16".to_string(),
            "--stable-instruction-chars".to_string(),
            "16384".to_string(),
            "--seed-context-chars".to_string(),
            "2350000".to_string(),
            "--minimum-seed-input-tokens".to_string(),
            "450000".to_string(),
            "--tool-chars".to_string(),
            "131072".to_string(),
            "--tool-calls".to_string(),
            "2".to_string(),
            "--tool-output-shape".to_string(),
            "natural".to_string(),
            "--fixture-profile".to_string(),
            "natural".to_string(),
            "--max-local-proxy-overhead-regression-ms".to_string(),
            "500".to_string(),
            "--isolate-upstream-cache".to_string(),
            "--prompt-cache-key-prefix".to_string(),
            prompt_cache_key_prefix,
            "--output".to_string(),
            output_path.display().to_string(),
        ];
        Ok(Self {
            workspace_root,
            output_path,
            started_at,
            arguments,
        })
    }
}

fn workspace_root_from_release_exe(candidate_exe: &Path) -> Result<PathBuf> {
    let release_dir = candidate_exe
        .parent()
        .context("candidate executable has no release directory")?;
    let releases_dir = release_dir
        .parent()
        .context("candidate executable is not inside a releases directory")?;
    if releases_dir.file_name().and_then(|value| value.to_str()) != Some("releases") {
        bail!(
            "current executable must run from the workspace releases directory to launch this fixed verification"
        );
    }
    let workspace_root = releases_dir
        .parent()
        .context("releases directory has no workspace parent")?;
    if !workspace_root.join("src-tauri").is_dir() {
        bail!("workspace release champion scripts are unavailable beside the current executable");
    }
    Ok(workspace_root.to_path_buf())
}

fn materialize_embedded_verifier(output_dir: &Path) -> Result<PathBuf> {
    let digest = Sha256::digest(EMBEDDED_VERIFIER.as_bytes());
    let path = output_dir.join(format!("verify-release-champion-{digest:x}.mjs"));
    let needs_write = fs::read_to_string(&path)
        .map(|existing| existing != EMBEDDED_VERIFIER)
        .unwrap_or(true);
    if needs_write {
        fs::write(&path, EMBEDDED_VERIFIER)
            .with_context(|| format!("failed to materialize verifier at {}", path.display()))?;
    }
    Ok(path)
}

fn run_plan(plan: ReleaseChampionRunPlan) -> Result<ReleaseChampionRunStatus> {
    let exit = Command::new("node")
        .args(&plan.arguments)
        .current_dir(&plan.workspace_root)
        .env(
            "ATOAPI_RELEASE_CHAMPION_WORKSPACE_ROOT",
            &plan.workspace_root,
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to start the embedded release champion verifier with Node.js")?;
    let finished_at = Utc::now();
    let exit_code = exit.code();
    let report = fs::read_to_string(&plan.output_path).with_context(|| {
        format!(
            "release champion report was not written: {}",
            plan.output_path.display()
        )
    })?;
    let report: Value =
        serde_json::from_str(&report).context("release champion report is invalid JSON")?;
    Ok(ReleaseChampionRunStatus {
        phase: ReleaseChampionRunPhase::Completed,
        scenario: SCENARIO.to_string(),
        started_at: Some(plan.started_at),
        finished_at: Some(finished_at),
        output_path: Some(plan.output_path.display().to_string()),
        exit_code,
        pass: report.get("pass").and_then(Value::as_bool),
        fail_closed: report.get("fail_closed").and_then(Value::as_bool),
        error: None,
    })
}

fn failed_status(error: String) -> ReleaseChampionRunStatus {
    ReleaseChampionRunStatus {
        phase: ReleaseChampionRunPhase::Failed,
        scenario: SCENARIO.to_string(),
        started_at: None,
        finished_at: Some(Utc::now()),
        output_path: None,
        exit_code: None,
        pass: None,
        fail_closed: None,
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn release_executable_resolves_workspace_root() {
        let root = workspace_root_from_release_exe(Path::new(
            r"G:\Atoapi\releases\v1.4.37-multikey-count\Atoapi.exe",
        ))
        .expect("release layout must resolve");
        assert_eq!(root, PathBuf::from(r"G:\Atoapi"));
    }

    #[test]
    fn non_release_executable_is_rejected() {
        let error =
            workspace_root_from_release_exe(Path::new(r"C:\Program Files\Atoapi\Atoapi.exe"))
                .expect_err("installed path has no workspace release anchor");
        assert!(error.to_string().contains("releases directory"));
    }

    #[test]
    fn embedded_verifier_is_the_live_fail_closed_script() {
        assert!(EMBEDDED_VERIFIER.contains("release-champion-comparison"));
        assert!(EMBEDDED_VERIFIER.contains("--isolate-upstream-cache"));
        assert!(EMBEDDED_VERIFIER.contains("dynamic-tail-mix"));
    }
}
