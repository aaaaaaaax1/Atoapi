mod admin;
mod agent_injection;
mod cache;
mod config;
mod crypto;
mod metrics;
mod proxy;
mod state;

use admin::{
    add_or_update_model, add_or_update_provider, apply_agent_injection,
    apply_enabled_agent_injections, clear_cache, delete_model, delete_provider,
    fetch_provider_models, get_agent_injections, get_config, get_metrics, get_proxy_status,
    reload_config, reveal_provider_api_key, save_cache_policy, save_config, select_provider,
    set_agent_injection_enabled, start_proxy, stop_proxy, update_agent_injection_route,
};
use state::AppState;
use std::sync::Arc;
use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = Arc::new(
        AppState::load()
            .unwrap_or_else(|err| panic!("failed to initialize application state: {err:?}")),
    );

    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            select_provider,
            add_or_update_provider,
            delete_provider,
            reveal_provider_api_key,
            fetch_provider_models,
            add_or_update_model,
            delete_model,
            start_proxy,
            stop_proxy,
            get_proxy_status,
            get_metrics,
            reload_config,
            save_cache_policy,
            get_agent_injections,
            set_agent_injection_enabled,
            update_agent_injection_route,
            apply_agent_injection,
            apply_enabled_agent_injections,
            clear_cache
        ])
        .setup(|app| {
            let state = app.state::<Arc<AppState>>().inner().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(err) = state.cache.load_from_disk().await {
                    state
                        .metrics
                        .record_error("cache_load", &err.to_string())
                        .await;
                }
                if let Err(err) = state.apply_enabled_agent_injections_on_startup().await {
                    state
                        .metrics
                        .record_error("startup_agent_injection", &err.to_string())
                        .await;
                }
                let should_auto_start = state.config.read().await.proxy_auto_start;
                if !should_auto_start {
                    return;
                }
                if let Err(err) = state.start_proxy().await {
                    state
                        .metrics
                        .record_error("startup", &err.to_string())
                        .await;
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Atoapi");
}
