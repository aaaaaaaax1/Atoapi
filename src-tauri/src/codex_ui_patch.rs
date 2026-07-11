use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

use crate::config::app_config_dir;

const PATCHER_SOURCE: &[u8] = include_bytes!("../resources/codex-ui-patcher.mjs");
const ELEVATED_REPLACE_SCRIPT: &str = include_str!("../resources/codex-ui-replace.ps1");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PatchManifest {
    target_path: PathBuf,
    original_sha256: String,
    patched_sha256: String,
}

pub fn set_enabled(enabled: bool) -> Result<String> {
    #[cfg(target_os = "windows")]
    {
        if enabled {
            enable_patch()
        } else {
            disable_patch()
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = enabled;
        Err(anyhow!("Codex UI patching is only supported on Windows"))
    }
}

#[cfg(target_os = "windows")]
fn enable_patch() -> Result<String> {
    let target = locate_codex_asar()?;
    validate_codex_target(&target)?;
    let root = patch_root()?;
    fs::create_dir_all(&root)?;

    let manifest_path = root.join("manifest.json");
    let original_path = root.join("app.asar.original");
    let patched_path = root.join("app.asar.patched");
    let patcher_path = root.join("codex-ui-patcher.mjs");
    let replace_script_path = root.join("codex-ui-replace.ps1");
    let current_hash = sha256_file(&target)?;
    let existing = read_manifest(&manifest_path)?;

    if let Some(manifest) = existing.as_ref() {
        if manifest.target_path == target && current_hash == manifest.patched_sha256 {
            return Ok("Codex UI 补丁已启用".to_string());
        }
        if manifest.target_path == target && current_hash != manifest.original_sha256 {
            return Err(anyhow!(
                "Codex app.asar changed outside Atoapi; refusing to overwrite it"
            ));
        }
    }
    ensure_codex_closed()?;
    ensure_node_available()?;

    let reuse_original = existing.as_ref().is_some_and(|manifest| {
        manifest.target_path == target
            && current_hash == manifest.original_sha256
            && original_path.exists()
            && sha256_file(&original_path)
                .map(|hash| hash == manifest.original_sha256)
                .unwrap_or(false)
    });
    if !reuse_original {
        fs::copy(&target, &original_path)
            .with_context(|| format!("failed to back up {}", target.display()))?;
    }
    let original_hash = sha256_file(&original_path)?;
    if current_hash != original_hash {
        return Err(anyhow!(
            "Codex app.asar changed outside Atoapi; refusing to overwrite it"
        ));
    }

    write_if_changed(&patcher_path, PATCHER_SOURCE)?;
    write_if_changed(&replace_script_path, ELEVATED_REPLACE_SCRIPT.as_bytes())?;
    if patched_path.exists() {
        fs::remove_file(&patched_path)?;
    }
    run_patcher(&patcher_path, &original_path, &patched_path)?;
    let patched_hash = sha256_file(&patched_path)?;
    if patched_hash == original_hash {
        return Err(anyhow!("Codex UI patch produced no changes"));
    }

    elevated_replace(
        &replace_script_path,
        &target,
        &patched_path,
        &patched_hash,
        &root,
    )?;
    let installed_hash = sha256_file(&target)?;
    if installed_hash != patched_hash {
        return Err(anyhow!(
            "Codex UI patch verification failed after elevation"
        ));
    }

    write_manifest(
        &manifest_path,
        &PatchManifest {
            target_path: target,
            original_sha256: original_hash,
            patched_sha256: patched_hash,
        },
    )?;
    fs::remove_file(patched_path).ok();
    Ok("Codex UI 已显示 GPT-5.6、Max/Ultra 与 Fast".to_string())
}

#[cfg(target_os = "windows")]
fn disable_patch() -> Result<String> {
    let root = patch_root()?;
    let manifest_path = root.join("manifest.json");
    let original_path = root.join("app.asar.original");
    let replace_script_path = root.join("codex-ui-replace.ps1");
    let Some(manifest) = read_manifest(&manifest_path)? else {
        return Ok("Codex UI 使用默认状态".to_string());
    };
    if !manifest.target_path.exists() {
        cleanup_patch_state(&manifest_path, &original_path);
        return Ok("Codex 已更新，旧 UI 补丁状态已清理".to_string());
    }

    let current_hash = sha256_file(&manifest.target_path)?;
    if current_hash == manifest.original_sha256 {
        cleanup_patch_state(&manifest_path, &original_path);
        return Ok("Codex UI 使用默认状态".to_string());
    }
    if current_hash != manifest.patched_sha256 {
        return Err(anyhow!(
            "Codex app.asar no longer matches the Atoapi patch; refusing to restore an older file"
        ));
    }
    ensure_codex_closed()?;
    if !original_path.exists() || sha256_file(&original_path)? != manifest.original_sha256 {
        return Err(anyhow!("Codex UI original backup is missing or invalid"));
    }

    write_if_changed(&replace_script_path, ELEVATED_REPLACE_SCRIPT.as_bytes())?;
    elevated_replace(
        &replace_script_path,
        &manifest.target_path,
        &original_path,
        &manifest.original_sha256,
        &root,
    )?;
    if sha256_file(&manifest.target_path)? != manifest.original_sha256 {
        return Err(anyhow!("Codex UI restore verification failed"));
    }

    cleanup_patch_state(&manifest_path, &original_path);
    Ok("Codex UI 已恢复默认过滤".to_string())
}

#[cfg(target_os = "windows")]
fn locate_codex_asar() -> Result<PathBuf> {
    let script = concat!(
        "$package = Get-AppxPackage -Name 'OpenAI.Codex' | ",
        "Sort-Object Version -Descending | Select-Object -First 1; ",
        "if ($null -eq $package) { exit 3 }; ",
        "[Console]::Out.Write((Join-Path $package.InstallLocation 'app\\resources\\app.asar'))"
    );
    let output = hidden_command("powershell.exe")
        .args(["-NoProfile", "-Command", script])
        .output()
        .context("failed to locate the installed Codex package")?;
    if !output.status.success() {
        return Err(anyhow!("OpenAI Codex App was not found"));
    }
    let path = String::from_utf8(output.stdout)
        .context("Codex package path was not valid UTF-8")?
        .trim()
        .to_string();
    if path.is_empty() {
        return Err(anyhow!("OpenAI Codex App returned an empty install path"));
    }
    let path = PathBuf::from(path);
    if !path.exists() {
        return Err(anyhow!(
            "Codex app.asar was not found at {}",
            path.display()
        ));
    }
    Ok(path)
}

#[cfg(target_os = "windows")]
fn validate_codex_target(path: &Path) -> Result<()> {
    let normalized = path
        .to_string_lossy()
        .replace('/', "\\")
        .to_ascii_lowercase();
    if normalized.contains("\\windowsapps\\openai.codex_")
        && normalized.ends_with("\\app\\resources\\app.asar")
    {
        Ok(())
    } else {
        Err(anyhow!(
            "refusing to patch an unexpected Codex path: {}",
            path.display()
        ))
    }
}

#[cfg(target_os = "windows")]
fn ensure_codex_closed() -> Result<()> {
    let script = concat!(
        "$process = Get-Process -Name 'Codex','ChatGPT' -ErrorAction SilentlyContinue | ",
        "Select-Object -First 1; if ($null -ne $process) { exit 7 }"
    );
    let status = hidden_command("powershell.exe")
        .args(["-NoProfile", "-Command", script])
        .status()
        .context("failed to check whether Codex is running")?;
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("请先完全关闭 Codex，再切换 Codex Agent 注入开关"))
    }
}

#[cfg(target_os = "windows")]
fn ensure_node_available() -> Result<()> {
    let output = hidden_command("node")
        .arg("--version")
        .output()
        .context("Node.js is required to patch Codex app.asar")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Node.js 22.12 or newer is required to patch Codex app.asar"
        ));
    }
    let version =
        String::from_utf8(output.stdout).context("Node.js returned an invalid version string")?;
    if node_version_is_supported(version.trim()) {
        return Ok(());
    }
    Err(anyhow!(
        "Node.js 22.12 or newer is required to patch Codex app.asar; found {}",
        version.trim()
    ))
}

fn node_version_is_supported(version: &str) -> bool {
    let mut parts = version.trim_start_matches('v').split('.');
    let major = parts.next().and_then(|value| value.parse::<u64>().ok());
    let minor = parts.next().and_then(|value| value.parse::<u64>().ok());
    matches!(
        (major, minor),
        (Some(major), Some(minor)) if major > 22 || (major == 22 && minor >= 12)
    )
}

#[cfg(target_os = "windows")]
fn run_patcher(patcher: &Path, input: &Path, output: &Path) -> Result<()> {
    let result = hidden_command("node")
        .arg(patcher)
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(output)
        .output()
        .context("failed to run the Codex UI patcher")?;
    if result.status.success() && output.exists() {
        return Ok(());
    }
    let error = String::from_utf8_lossy(&result.stderr).trim().to_string();
    Err(anyhow!(
        "Codex UI patcher failed{}",
        if error.is_empty() {
            String::new()
        } else {
            format!(": {error}")
        }
    ))
}

#[cfg(target_os = "windows")]
fn elevated_replace(
    script: &Path,
    target: &Path,
    source: &Path,
    expected_hash: &str,
    source_root: &Path,
) -> Result<()> {
    let result_path = source_root.join("elevated-result.txt");
    fs::remove_file(&result_path).ok();
    let parameters = [
        "-NoProfile".to_string(),
        "-ExecutionPolicy".to_string(),
        "Bypass".to_string(),
        "-File".to_string(),
        script.to_string_lossy().into_owned(),
        "-Target".to_string(),
        target.to_string_lossy().into_owned(),
        "-Source".to_string(),
        source.to_string_lossy().into_owned(),
        "-ExpectedSha256".to_string(),
        expected_hash.to_string(),
        "-Result".to_string(),
        result_path.to_string_lossy().into_owned(),
        "-AllowedSourceRoot".to_string(),
        source_root.to_string_lossy().into_owned(),
    ]
    .into_iter()
    .map(|value| quote_windows_argument(&value))
    .collect::<Vec<_>>()
    .join(" ");

    run_elevated("powershell.exe", &parameters)?;
    let result = fs::read_to_string(&result_path)
        .context("Codex UI elevated helper did not return a result")?;
    fs::remove_file(result_path).ok();
    if result.trim() == "ok" {
        Ok(())
    } else {
        Err(anyhow!(result.trim().to_string()))
    }
}

#[cfg(target_os = "windows")]
fn run_elevated(program: &str, parameters: &str) -> Result<()> {
    use std::{mem::size_of, ptr::null_mut};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, GetLastError},
        System::Threading::{GetExitCodeProcess, WaitForSingleObject, INFINITE},
        UI::{
            Shell::{ShellExecuteExW, SEE_MASK_NOCLOSEPROCESS, SHELLEXECUTEINFOW},
            WindowsAndMessaging::SW_HIDE,
        },
    };

    let verb = wide("runas");
    let program = wide(program);
    let parameters = wide(parameters);
    let mut info: SHELLEXECUTEINFOW = unsafe { std::mem::zeroed() };
    info.cbSize = size_of::<SHELLEXECUTEINFOW>() as u32;
    info.fMask = SEE_MASK_NOCLOSEPROCESS;
    info.lpVerb = verb.as_ptr();
    info.lpFile = program.as_ptr();
    info.lpParameters = parameters.as_ptr();
    info.nShow = SW_HIDE;
    info.hwnd = null_mut();

    if unsafe { ShellExecuteExW(&mut info) } == 0 {
        return Err(anyhow!(
            "Codex UI elevation was cancelled or failed: {}",
            unsafe { GetLastError() }
        ));
    }
    if info.hProcess.is_null() {
        return Err(anyhow!("Codex UI elevation returned no process handle"));
    }
    unsafe {
        WaitForSingleObject(info.hProcess, INFINITE);
    }
    let mut exit_code = 1u32;
    let read_exit = unsafe { GetExitCodeProcess(info.hProcess, &mut exit_code) };
    unsafe {
        CloseHandle(info.hProcess);
    }
    if read_exit == 0 || exit_code != 0 {
        Err(anyhow!(
            "Codex UI elevated helper failed with exit code {exit_code}"
        ))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "windows")]
fn hidden_command(program: &str) -> Command {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x08000000;
    let mut command = Command::new(program);
    command.creation_flags(CREATE_NO_WINDOW);
    command
}

fn patch_root() -> Result<PathBuf> {
    Ok(app_config_dir()?.join("codex-ui-patch"))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:X}", digest.finalize()))
}

fn read_manifest(path: &Path) -> Result<Option<PatchManifest>> {
    if !path.exists() {
        return Ok(None);
    }
    let value = serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(value))
}

fn write_manifest(path: &Path, manifest: &PatchManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    write_if_changed(path, &bytes)
}

fn write_if_changed(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() && fs::read(path).ok().as_deref() == Some(bytes) {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn cleanup_patch_state(manifest: &Path, original: &Path) {
    fs::remove_file(manifest).ok();
    fs::remove_file(original).ok();
}

#[cfg(target_os = "windows")]
fn quote_windows_argument(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\\\""))
}

#[cfg(target_os = "windows")]
fn wide(value: &str) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    std::ffi::OsStr::new(value)
        .encode_wide()
        .chain(Some(0))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "windows")]
    fn accepts_only_codex_windows_apps_asar_path() {
        assert!(validate_codex_target(Path::new(
            r"C:\Program Files\WindowsApps\OpenAI.Codex_1.2.3.0_x64__id\app\resources\app.asar"
        ))
        .is_ok());
        assert!(validate_codex_target(Path::new(r"C:\Temp\app.asar")).is_err());
    }

    #[test]
    fn bundled_patcher_contains_required_features() {
        let text = std::str::from_utf8(PATCHER_SOURCE).unwrap();
        assert!(text.contains("gpt-5\\\\.6-(?:sol|terra|luna)"));
        assert!(text.contains("API Key Fast UI"));
        assert!(text.contains("Max and Ultra reasoning"));
    }

    #[test]
    fn requires_node_22_12_or_newer() {
        assert!(!node_version_is_supported("v22.11.0"));
        assert!(node_version_is_supported("v22.12.0"));
        assert!(node_version_is_supported("v23.0.0"));
        assert!(!node_version_is_supported("not-a-version"));
    }
}
