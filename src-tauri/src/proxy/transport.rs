use std::{collections::HashMap, sync::Mutex, time::Duration};

use anyhow::Result;

#[derive(Debug)]
pub(crate) struct TransportClients {
    user_agent: String,
    direct: reqwest::Client,
    system_proxy: reqwest::Client,
    agent_direct: reqwest::Client,
    agent_system_proxy: reqwest::Client,
    explicit_proxy: Mutex<HashMap<ExplicitProxyKey, reqwest::Client>>,
}

const EXPLICIT_PROXY_POOL_LIMIT: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExplicitProxyKey {
    url: String,
    redirect_policy: RedirectPolicy,
}

impl TransportClients {
    pub(crate) fn new(user_agent: &str) -> Result<Self> {
        Ok(Self {
            user_agent: user_agent.to_string(),
            direct: build_client(user_agent, NetworkPath::Direct, RedirectPolicy::Baseline)?,
            system_proxy: build_client(
                user_agent,
                NetworkPath::SystemProxy,
                RedirectPolicy::Baseline,
            )?,
            agent_direct: build_client(
                user_agent,
                NetworkPath::Direct,
                RedirectPolicy::StrictOneShot,
            )?,
            agent_system_proxy: build_client(
                user_agent,
                NetworkPath::SystemProxy,
                RedirectPolicy::StrictOneShot,
            )?,
            explicit_proxy: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn client(&self, use_system_proxy: bool) -> &reqwest::Client {
        if use_system_proxy {
            &self.system_proxy
        } else {
            &self.direct
        }
    }

    pub(crate) fn agent_client(&self, use_system_proxy: bool) -> &reqwest::Client {
        if use_system_proxy {
            &self.agent_system_proxy
        } else {
            &self.agent_direct
        }
    }

    pub(crate) fn explicit_proxy_client(
        &self,
        proxy_url: &str,
        agent_transport: bool,
    ) -> Result<reqwest::Client> {
        let redirect_policy = if agent_transport {
            RedirectPolicy::StrictOneShot
        } else {
            RedirectPolicy::Baseline
        };
        let key = ExplicitProxyKey {
            url: proxy_url.to_string(),
            redirect_policy,
        };
        if let Some(client) = self
            .explicit_proxy
            .lock()
            .expect("explicit proxy pool lock must not be poisoned")
            .get(&key)
            .cloned()
        {
            return Ok(client);
        }

        let client = build_explicit_proxy_client(&self.user_agent, proxy_url, redirect_policy)?;
        let mut pool = self
            .explicit_proxy
            .lock()
            .expect("explicit proxy pool lock must not be poisoned");
        if let Some(existing) = pool.get(&key) {
            return Ok(existing.clone());
        }
        if pool.len() >= EXPLICIT_PROXY_POOL_LIMIT {
            if let Some(evicted_key) = pool.keys().next().cloned() {
                pool.remove(&evicted_key);
            }
        }
        pool.insert(key, client.clone());
        Ok(client)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkPath {
    Direct,
    SystemProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RedirectPolicy {
    Baseline,
    StrictOneShot,
}

fn build_client(
    user_agent: &str,
    path: NetworkPath,
    redirect_policy: RedirectPolicy,
) -> Result<reqwest::Client> {
    let builder = base_client_builder(user_agent, redirect_policy);
    let builder = match path {
        // Direct traffic should keep reqwest's protocol negotiation. Forcing
        // HTTP/1.1 here makes streaming requests pay extra latency on
        // providers that support HTTP/2 multiplexing.
        NetworkPath::Direct => builder.no_proxy(),
        NetworkPath::SystemProxy => builder,
    };
    Ok(builder.build()?)
}

fn build_explicit_proxy_client(
    user_agent: &str,
    proxy_url: &str,
    redirect_policy: RedirectPolicy,
) -> Result<reqwest::Client> {
    let proxy = reqwest::Proxy::all(proxy_url)?;
    Ok(base_client_builder(user_agent, redirect_policy)
        .no_proxy()
        .proxy(proxy)
        .build()?)
}

fn base_client_builder(
    user_agent: &str,
    redirect_policy: RedirectPolicy,
) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .user_agent(user_agent)
        .connect_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(10 * 60))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true);
    let builder = match redirect_policy {
        // Ordinary API and administrator traffic retains reqwest's baseline
        // redirect behavior. It is outside the Agent generation attempt gate.
        RedirectPolicy::Baseline => builder,
        // A redirect can issue another HTTP request, so Agent generation must
        // return the original 30x response without following it. Reqwest also
        // retries selected low-level protocol NACKs by default; disable that
        // policy here so the transport cannot create a hidden second POST.
        RedirectPolicy::StrictOneShot => builder
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never()),
    };
    builder
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{
        http::StatusCode,
        response::Redirect,
        routing::{any, get, post},
        Router,
    };
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    async fn redirect_server() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>, JoinHandle<()>) {
        let redirect_hits = Arc::new(AtomicUsize::new(0));
        let redirect_hits_for_get = redirect_hits.clone();
        let redirect_hits_for_temporary_post = redirect_hits.clone();
        let redirect_hits_for_permanent_post = redirect_hits.clone();
        let target_hits = Arc::new(AtomicUsize::new(0));
        let target_hits_for_get = target_hits.clone();
        let target_hits_for_post = target_hits.clone();
        let app = Router::new()
            .route(
                "/redirect",
                get(move || {
                    let redirect_hits = redirect_hits_for_get.clone();
                    async move {
                        redirect_hits.fetch_add(1, Ordering::SeqCst);
                        Redirect::temporary("/target")
                    }
                }),
            )
            .route(
                "/post-temporary-redirect",
                post(move || {
                    let redirect_hits = redirect_hits_for_temporary_post.clone();
                    async move {
                        redirect_hits.fetch_add(1, Ordering::SeqCst);
                        Redirect::temporary("/post-target")
                    }
                }),
            )
            .route(
                "/post-permanent-redirect",
                post(move || {
                    let redirect_hits = redirect_hits_for_permanent_post.clone();
                    async move {
                        redirect_hits.fetch_add(1, Ordering::SeqCst);
                        Redirect::permanent("/post-target")
                    }
                }),
            )
            .route(
                "/target",
                get(move || {
                    let target_hits = target_hits_for_get.clone();
                    async move {
                        target_hits.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            );
        let app = app.route(
            "/post-target",
            post(move || {
                let target_hits = target_hits_for_post.clone();
                async move {
                    target_hits.fetch_add(1, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            format!("http://{address}"),
            redirect_hits,
            target_hits,
            task,
        )
    }

    async fn explicit_proxy_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_route = hits.clone();
        let app = Router::new().fallback(any(move || {
            let hits = hits_for_route.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}"), hits, task)
    }

    async fn connection_drop_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let connections_for_task = connections.clone();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            connections_for_task.fetch_add(1, Ordering::SeqCst);
            drop(socket);
            if let Ok(Ok((socket, _))) =
                tokio::time::timeout(Duration::from_millis(300), listener.accept()).await
            {
                connections_for_task.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });
        (format!("http://{address}/responses"), connections, task)
    }

    #[test]
    fn builds_both_transport_adapters_without_global_state() {
        let clients = TransportClients::new("AtoapiTransportTest/0.1").unwrap();
        let direct = clients.client(false) as *const reqwest::Client;
        let proxied = clients.client(true) as *const reqwest::Client;
        let agent_direct = clients.agent_client(false) as *const reqwest::Client;
        let agent_proxied = clients.agent_client(true) as *const reqwest::Client;
        assert_ne!(direct, proxied);
        assert_ne!(agent_direct, agent_proxied);
        assert_ne!(direct, agent_direct);
        assert_ne!(proxied, agent_proxied);
    }

    #[tokio::test]
    async fn explicit_proxy_client_is_reused_without_touching_default_paths() {
        let (proxy_url, hits, server) = explicit_proxy_server().await;
        let clients = TransportClients::new("AtoapiExplicitProxyTest/0.1").unwrap();

        for _ in 0..2 {
            let client = clients.explicit_proxy_client(&proxy_url, false).unwrap();
            let response = client
                .get("http://atoapi-explicit-proxy.invalid/v1/models")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
        }

        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(
            clients
                .explicit_proxy
                .lock()
                .expect("explicit proxy pool lock")
                .len(),
            1
        );
        clients
            .explicit_proxy_client(&proxy_url, true)
            .expect("Agent transport should receive a separate strict client");
        assert_eq!(
            clients
                .explicit_proxy
                .lock()
                .expect("explicit proxy pool lock")
                .len(),
            2
        );
        assert!(clients
            .explicit_proxy_client("not a proxy URL", false)
            .is_err());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn agent_transport_does_not_retry_a_dropped_protocol_connection() {
        let (url, connections, server) = connection_drop_server().await;
        let clients = TransportClients::new("AtoapiAgentNoRetryTest/0.1").unwrap();

        let result = clients
            .agent_client(false)
            .post(url)
            .body("{}")
            .send()
            .await;

        assert!(result.is_err());
        server.await.unwrap();
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn agent_client_does_not_follow_redirects() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiAgentRedirectTest/0.1").unwrap();

        let response = clients
            .agent_client(false)
            .get(format!("{base_url}/redirect"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(redirect_hits.load(Ordering::SeqCst), 1);
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn ordinary_client_retains_baseline_redirect_behavior() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiOrdinaryRedirectTest/0.1").unwrap();

        let response = clients
            .client(false)
            .get(format!("{base_url}/redirect"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(redirect_hits.load(Ordering::SeqCst), 1);
        assert_eq!(target_hits.load(Ordering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn agent_client_does_not_follow_post_307_or_308_redirects() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiOrdinaryPostRedirectTest/0.1").unwrap();

        let temporary = clients
            .agent_client(false)
            .post(format!("{base_url}/post-temporary-redirect"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(temporary.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);

        let permanent = clients
            .agent_client(false)
            .post(format!("{base_url}/post-permanent-redirect"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(permanent.status(), reqwest::StatusCode::PERMANENT_REDIRECT);
        assert_eq!(redirect_hits.load(Ordering::SeqCst), 2);
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn ordinary_client_follows_post_307_and_308_redirects() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiOrdinaryPostRedirectTest/0.1").unwrap();

        let temporary = clients
            .client(false)
            .post(format!("{base_url}/post-temporary-redirect"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(temporary.status(), reqwest::StatusCode::OK);

        let permanent = clients
            .client(false)
            .post(format!("{base_url}/post-permanent-redirect"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(permanent.status(), reqwest::StatusCode::OK);
        assert_eq!(redirect_hits.load(Ordering::SeqCst), 2);
        assert_eq!(target_hits.load(Ordering::SeqCst), 2);
        server.abort();
        let _ = server.await;
    }
}
