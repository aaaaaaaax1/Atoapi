use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
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
const SCENARIO: &str = "dynamic-tail-mix";
const COMMON_UPSTREAM_USER_AGENT: &str = "Atoapi-ReleaseChampion-1";

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
    /// or output path. It compares the fixed v1.4.33 hit-rate comparator with
    /// the currently running development package on the latest completed
    /// Codex scope. The wrapper resolves and pins the actual saved multi-Key
    /// realm before starting either isolated arm.
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
                "v1.4.33 hit-rate comparator executable is missing: {}",
                champion_exe.display()
            );
        }

        let runner_script = workspace_root
            .join("scripts")
            .join("run-release-champion-interactive.ps1");
        if !runner_script.is_file() {
            bail!(
                "interactive release champion runner is missing: {}",
                runner_script.display()
            );
        }

        let output_dir = config_dir.join("release").join("release-champion");
        fs::create_dir_all(&output_dir)
            .context("failed to create release champion artifact directory")?;
        let run_id = format!(
            "{}-{}",
            Utc::now().format("%Y%m%d-%H%M%S"),
            Uuid::new_v4().simple()
        );
        let output_path = output_dir.join(format!(
            "development-v1438-vs-champion-v1433-{SCENARIO}-{run_id}.json"
        ));
        let arguments = interactive_runner_arguments(
            &runner_script,
            &champion_exe,
            &candidate_exe,
            config_dir,
            &output_path,
        );
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

fn interactive_runner_arguments(
    runner_script: &Path,
    champion_exe: &Path,
    candidate_exe: &Path,
    config_dir: &Path,
    output_path: &Path,
) -> Vec<String> {
    vec![
        "-NoProfile".to_string(),
        "-ExecutionPolicy".to_string(),
        "Bypass".to_string(),
        "-File".to_string(),
        runner_script.display().to_string(),
        "-Scenario".to_string(),
        SCENARIO.to_string(),
        "-Pairs".to_string(),
        "1".to_string(),
        "-ProviderScope".to_string(),
        "codex-agent".to_string(),
        "-ChampionExe".to_string(),
        champion_exe.display().to_string(),
        "-CandidateExe".to_string(),
        candidate_exe.display().to_string(),
        "-ConfigDir".to_string(),
        config_dir.display().to_string(),
        "-Output".to_string(),
        output_path.display().to_string(),
        "-UpstreamUserAgent".to_string(),
        COMMON_UPSTREAM_USER_AGENT.to_string(),
    ]
}

fn run_plan(plan: ReleaseChampionRunPlan) -> Result<ReleaseChampionRunStatus> {
    let exit = Command::new("powershell.exe")
        .args(&plan.arguments)
        .current_dir(&plan.workspace_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to start the interactive release champion runner")?;
    let finished_at = Utc::now();
    let exit_code = exit.code();
    if !plan.output_path.is_file() {
        let exit_detail = exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        bail!("release champion runner exited before writing a report (exit code {exit_detail})");
    }
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
    fn runner_delegates_scope_refresh_and_user_agent_parity_to_interactive_wrapper() {
        let arguments = interactive_runner_arguments(
            Path::new(r"G:\Atoapi\scripts\run-release-champion-interactive.ps1"),
            Path::new(r"G:\Atoapi\releases\v1.4.33\Atoapi.exe"),
            Path::new(r"G:\Atoapi\releases\v1.4.38\Atoapi.exe"),
            Path::new(r"C:\Users\MSJ\AppData\Roaming\Atoapi"),
            Path::new(r"C:\Users\MSJ\AppData\Roaming\Atoapi\release\report.json"),
        );
        assert!(arguments
            .windows(2)
            .any(|pair| { pair[0] == "-ProviderScope" && pair[1] == "codex-agent" }));
        assert!(arguments.windows(2).any(|pair| {
            pair[0] == "-UpstreamUserAgent" && pair[1] == COMMON_UPSTREAM_USER_AGENT
        }));
        assert!(!arguments
            .iter()
            .any(|argument| argument == "agent-codex-provider-2"));
        assert!(!arguments
            .iter()
            .any(|argument| argument.starts_with("4574f5")));
    }
}
