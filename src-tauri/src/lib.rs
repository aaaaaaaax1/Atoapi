mod admin;
mod agent_injection;
mod cache;
mod codex_ui_patch;
mod config;
mod crypto;
mod metrics;
mod persistence;
mod proxy;
mod state;

pub(crate) const ATOAPI_USER_AGENT: &str = concat!("Atoapi/", env!("CARGO_PKG_VERSION"));

use admin::{
    add_or_update_model, add_or_update_provider, apply_agent_injection,
    apply_enabled_agent_injections, clear_cache, clone_provider_for_agent, delete_model,
    delete_provider, diagnose_provider_network_paths, fetch_provider_models, get_agent_injections,
    get_cache_validation_status, get_config, get_metrics, get_proxy_mode_status, get_proxy_status,
    probe_provider_response_session_reuse, reload_config, reveal_provider_api_key,
    reveal_provider_key, save_cache_policy, save_config, save_proxy_mode_config, select_provider,
    set_agent_injection_enabled, set_cache_validation_mode,
    set_provider_response_session_reuse_enabled, start_proxy, stop_proxy, test_provider_key,
    test_provider_key_pool, update_agent_injection_route,
};
use config::isolated_test_instance;
use state::AppState;
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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = Arc::new(
        AppState::load()
            .unwrap_or_else(|err| panic!("failed to initialize application state: {err:?}")),
    );
    let exit_coordinator = Arc::new(ExitCoordinator::default());

    let app = tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            select_provider,
            clone_provider_for_agent,
            add_or_update_provider,
            delete_provider,
            reveal_provider_api_key,
            reveal_provider_key,
            fetch_provider_models,
            diagnose_provider_network_paths,
            test_provider_key,
            test_provider_key_pool,
            probe_provider_response_session_reuse,
            set_provider_response_session_reuse_enabled,
            add_or_update_model,
            delete_model,
            start_proxy,
            stop_proxy,
            get_proxy_status,
            get_metrics,
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
    use super::ExitCoordinator;

    #[test]
    fn exit_coordinator_runs_cleanup_once_before_allowing_exit() {
        let coordinator = ExitCoordinator::default();
        assert!(!coordinator.final_exit_is_ready());
        assert!(coordinator.begin_shutdown());
        assert!(!coordinator.begin_shutdown());
        coordinator.allow_final_exit();
        assert!(coordinator.final_exit_is_ready());
    }
}
