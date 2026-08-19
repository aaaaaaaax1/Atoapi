mod admin;
mod agent_injection;
mod cache;
mod codex_ui_patch;
mod config;
mod continuation_lineage;
mod crypto;
mod metrics;
mod metrics_history;
mod persistence;
mod proxy;
mod release_champion_runner;
mod state;

/// Default upstream product identity.
///
/// Keep this aligned with the executable's actual Cargo package version. A
/// Provider's user-configured `custom_user_agent` still takes precedence.
pub(crate) const ATOAPI_USER_AGENT: &str = concat!("Atoapi/", env!("CARGO_PKG_VERSION"));

use admin::{
    add_or_update_model, add_or_update_provider, apply_agent_injection,
    apply_enabled_agent_injections, clear_cache, clone_provider_for_agent, delete_model,
    delete_provider, diagnose_provider_network_paths, fetch_provider_health_models,
    fetch_provider_models, get_agent_injections, get_cache_validation_status, get_config,
    get_metrics, get_metrics_trend, get_provider_key_pool_health, get_proxy_mode_status,
    get_proxy_status, get_release_champion, probe_provider_balance,
    probe_provider_cache_capabilities, probe_provider_health, reload_config,
    reorder_agent_providers, reveal_provider_api_key, reveal_provider_key, save_cache_policy,
    save_config, save_proxy_mode_config, select_provider, set_agent_injection_enabled,
    set_cache_validation_mode, start_proxy, stop_proxy, test_active_provider_key,
    test_provider_connection_paths, test_provider_key, test_provider_key_pool,
    update_agent_injection_route,
};
use config::{isolated_test_instance, isolated_test_listen_port, release_webview2_user_data_dir};
use state::AppState;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tauri::{Manager, RunEvent};

#[derive(Default)]
struct ExitCoordinator {
    shutdown_started: AtomicBool,
    final_exit_ready: AtomicBool,
}

impl ExitCoordinator {
    fn begin_shutdown(&self) -> bool {
        !self.shutdown_started.swap(true, Ordering::AcqRel)
    }

    fn allow_final_exit(&self) {
        self.final_exit_ready.store(true, Ordering::Release);
    }

    fn final_exit_is_ready(&self) -> bool {
        self.final_exit_ready.load(Ordering::Acquire)
    }
}

fn spawn_exit_shutdown(
    state: Arc<AppState>,
    app_handle: tauri::AppHandle,
    exit_coordinator: Arc<ExitCoordinator>,
) {
    tauri::async_runtime::spawn(async move {
        if let Err(err) = state.shutdown_for_exit().await {
            state
                .metrics
                .record_error("shutdown", &err.to_string())
                .await;
        }
        exit_coordinator.allow_final_exit();
        app_handle.exit(0);
    });
}

/// Chooses a browser profile before Tauri creates the first WebView2
/// environment. A caller-provided folder always wins. Isolated release tests
/// stay process-scoped; ordinary releases share only their own stable v2
/// profile instead of inheriting the legacy Tauri-default profile.
fn webview2_profile_for_launch(
    caller_supplied: bool,
    isolated: bool,
    isolated_port: Option<u16>,
    process_id: u32,
    release_profile: Option<PathBuf>,
) -> Option<PathBuf> {
    if caller_supplied {
        return None;
    }
    if isolated {
        let port = isolated_port.unwrap_or_default();
        return Some(
            std::env::temp_dir().join(format!("atoapi-isolated-webview2-{port}-{process_id}")),
        );
    }
    release_profile
}

/// Prepare the selected WebView2 data folder without migrating or deleting a
/// prior profile. If the local location cannot be prepared, safely leave
/// WebView2 at Tauri's default rather than preventing the desktop app from
/// opening.
fn configure_webview2_profile() {
    let caller_supplied = std::env::var_os("WEBVIEW2_USER_DATA_FOLDER").is_some();
    let isolated = isolated_test_instance();
    let release_profile = if caller_supplied || isolated {
        None
    } else {
        match release_webview2_user_data_dir() {
            Ok(profile) => Some(profile),
            Err(err) => {
                eprintln!("failed to locate release WebView2 profile: {err}");
                None
            }
        }
    };
    let Some(profile) = webview2_profile_for_launch(
        caller_supplied,
        isolated,
        isolated_test_listen_port(),
        std::process::id(),
        release_profile,
    ) else {
        return;
    };
    if let Err(err) = std::fs::create_dir_all(&profile) {
        eprintln!("failed to prepare WebView2 profile {profile:?}: {err}");
        return;
    }
    std::env::set_var("WEBVIEW2_USER_DATA_FOLDER", profile);
}

/// The release-comparison runner is intentionally port-isolated and never
/// touches the live desktop instance.  In a non-interactive test host there
/// may be no usable WebView2 desktop at all, though the proxy itself is fully
/// headless.  This explicit double opt-in starts only that isolated proxy
/// surface and leaves ordinary launches unchanged.
fn headless_isolated_test_requested() -> bool {
    headless_isolated_test_requested_with_env(
        isolated_test_instance(),
        std::env::var("ATOAPI_HEADLESS_ISOLATED_TEST")
            .ok()
            .as_deref(),
    )
}

fn headless_isolated_test_requested_with_env(isolated: bool, value: Option<&str>) -> bool {
    isolated && value.is_some_and(|value| matches!(value.trim(), "1" | "true" | "on" | "enabled"))
}

fn run_headless_isolated_test() {
    let state = Arc::new(
        AppState::load()
            .unwrap_or_else(|err| panic!("failed to initialize isolated test state: {err:?}")),
    );
    let startup_state = state.clone();
    let startup = tauri::async_runtime::block_on(async move {
        if let Err(err) = startup_state.cache.load_from_disk().await {
            startup_state
                .metrics
                .record_error("cache_load", &err.to_string())
                .await;
        }
        startup_state.start_proxy().await
    });
    if let Err(err) = startup {
        panic!("failed to start isolated headless proxy: {err:?}");
    }

    // Keep the state and its server handles alive until the verifier ends the
    // disposable child process.  Normal desktop shutdown remains unchanged.
    let _state = state;
    loop {
        std::thread::park();
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    if headless_isolated_test_requested() {
        run_headless_isolated_test();
        return;
    }
    configure_webview2_profile();
    let state = Arc::new(
        AppState::load()
            .unwrap_or_else(|err| panic!("failed to initialize application state: {err:?}")),
    );
    let exit_coordinator = Arc::new(ExitCoordinator::default());

    let app = tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_config,
            get_provider_key_pool_health,
            save_config,
            select_provider,
            clone_provider_for_agent,
            add_or_update_provider,
            reorder_agent_providers,
            delete_provider,
            reveal_provider_api_key,
            reveal_provider_key,
            fetch_provider_models,
            fetch_provider_health_models,
            diagnose_provider_network_paths,
            test_active_provider_key,
            test_provider_connection_paths,
            test_provider_key,
            test_provider_key_pool,
            probe_provider_health,
            probe_provider_balance,
            probe_provider_cache_capabilities,
            add_or_update_model,
            delete_model,
            start_proxy,
            stop_proxy,
            get_proxy_status,
            get_metrics,
            get_metrics_trend,
            get_release_champion,
            get_cache_validation_status,
            set_cache_validation_mode,
            reload_config,
            save_cache_policy,
            get_agent_injections,
            set_agent_injection_enabled,
            update_agent_injection_route,
            apply_agent_injection,
            apply_enabled_agent_injections,
            get_proxy_mode_status,
            save_proxy_mode_config,
            clear_cache
        ])
        .setup(|app| {
            let state = app.state::<Arc<AppState>>().inner().clone();
            tauri::async_runtime::spawn(async move {
                let isolated_test_instance = isolated_test_instance();
                if let Err(err) = state.cache.load_from_disk().await {
                    state
                        .metrics
                        .record_error("cache_load", &err.to_string())
                        .await;
                }
                if !isolated_test_instance {
                    if let Err(err) = state.apply_enabled_agent_injections_on_startup().await {
                        state
                            .metrics
                            .record_error("startup_agent_injection", &err.to_string())
                            .await;
                    }
                }
                let (should_start_main_proxy, should_start_proxy_mode) = {
                    let config = state.config.read().await;
                    let proxy_mode_enabled = !isolated_test_instance
                        && config
                            .agent_injections
                            .iter()
                            .any(|item| item.enabled && item.id == "proxy-mode");
                    let non_proxy_agent_enabled = config
                        .agent_injections
                        .iter()
                        .any(|item| item.enabled && item.id != "proxy-mode");
                    (
                        config.proxy_auto_start || non_proxy_agent_enabled,
                        proxy_mode_enabled,
                    )
                };
                if should_start_main_proxy {
                    if let Err(err) = state.start_proxy().await {
                        state
                            .metrics
                            .record_error("startup", &err.to_string())
                            .await;
                    }
                }
                if should_start_proxy_mode {
                    if let Err(err) = state.start_proxy_mode_proxy().await {
                        state
                            .metrics
                            .record_error("startup_proxy_mode", &err.to_string())
                            .await;
                    }
                }
            });
            Ok(())
        })
        .on_window_event({
            let exit_coordinator = exit_coordinator.clone();
            move |window, event| {
                let tauri::WindowEvent::CloseRequested { api, .. } = event else {
                    return;
                };
                api.prevent_close();
                if !exit_coordinator.begin_shutdown() {
                    return;
                }
                let state = window.state::<Arc<AppState>>().inner().clone();
                let app_handle = window.app_handle().clone();
                spawn_exit_shutdown(state, app_handle, exit_coordinator.clone());
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building Atoapi");

    app.run({
        let exit_coordinator = exit_coordinator.clone();
        move |app_handle, event| {
            let RunEvent::ExitRequested { api, .. } = event else {
                return;
            };
            if exit_coordinator.final_exit_is_ready() {
                return;
            }
            api.prevent_exit();
            if !exit_coordinator.begin_shutdown() {
                return;
            }
            let state = app_handle.state::<Arc<AppState>>().inner().clone();
            spawn_exit_shutdown(state, app_handle.clone(), exit_coordinator.clone());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        headless_isolated_test_requested_with_env, webview2_profile_for_launch, ExitCoordinator,
    };
    use std::path::PathBuf;

    #[test]
    fn exit_coordinator_runs_cleanup_once_before_allowing_exit() {
        let coordinator = ExitCoordinator::default();
        assert!(!coordinator.final_exit_is_ready());
        assert!(coordinator.begin_shutdown());
        assert!(!coordinator.begin_shutdown());
        coordinator.allow_final_exit();
        assert!(coordinator.final_exit_is_ready());
    }

    #[test]
    fn headless_isolated_mode_requires_both_explicit_test_flags() {
        assert!(headless_isolated_test_requested_with_env(true, Some("1")));
        assert!(headless_isolated_test_requested_with_env(
            true,
            Some("enabled")
        ));
        assert!(!headless_isolated_test_requested_with_env(false, Some("1")));
        assert!(!headless_isolated_test_requested_with_env(true, Some("0")));
        assert!(!headless_isolated_test_requested_with_env(true, None));
    }

    #[test]
    fn webview2_profile_selection_preserves_caller_override_and_isolates_launch_modes() {
        let release = PathBuf::from(r"C:\Users\tester\AppData\Local\Atoapi\webview2-udf-v2");
        assert_eq!(
            webview2_profile_for_launch(true, false, None, 42, Some(release.clone())),
            None,
            "an explicit caller profile must never be replaced"
        );
        assert_eq!(
            webview2_profile_for_launch(false, false, None, 42, Some(release.clone())),
            Some(release.clone()),
            "normal releases must use the stable local v2 profile"
        );
        assert_ne!(
            release.file_name().and_then(|value| value.to_str()),
            Some("local.atoapi.desktop"),
            "the new normal profile must not reuse Tauri's legacy default"
        );
        assert_eq!(
            webview2_profile_for_launch(false, true, Some(18883), 42, Some(release)),
            Some(std::env::temp_dir().join("atoapi-isolated-webview2-18883-42")),
            "isolated validation must remain per-port and per-process"
        );
        assert_eq!(
            webview2_profile_for_launch(false, false, None, 42, None),
            None,
            "an unavailable local directory must fail open to Tauri's default"
        );
    }
}
