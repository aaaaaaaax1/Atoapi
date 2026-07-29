use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=ATOAPI_GIT_COMMIT");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");

    // A release cohort must be attributable even when two local builds share
    // the same package version.  The final executable hash is recorded at
    // runtime; this compile-time commit is the human-readable companion.
    let commit = env::var("ATOAPI_GIT_COMMIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=ATOAPI_GIT_COMMIT={commit}");

    tauri_build::build();
}

fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_string();
    (!commit.is_empty()).then_some(commit)
}
