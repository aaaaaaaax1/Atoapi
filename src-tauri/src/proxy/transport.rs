use std::time::Duration;

use anyhow::Result;

#[derive(Debug)]
pub(crate) struct TransportClients {
    direct: reqwest::Client,
    system_proxy: reqwest::Client,
    agent_direct: reqwest::Client,
    agent_system_proxy: reqwest::Client,
}

impl TransportClients {
    pub(crate) fn new(user_agent: &str) -> Result<Self> {
        Ok(Self {
            direct: build_client(user_agent, NetworkPath::Direct)?,
            system_proxy: build_client(user_agent, NetworkPath::SystemProxy)?,
            agent_direct: build_client(user_agent, NetworkPath::Direct)?,
            agent_system_proxy: build_client(user_agent, NetworkPath::SystemProxy)?,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkPath {
    Direct,
    SystemProxy,
}

fn build_client(user_agent: &str, path: NetworkPath) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder()
        .user_agent(user_agent)
        // A redirect can cause reqwest to issue a second request, which violates
        // the proxy data plane's strict one-inbound/one-upstream contract.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(10 * 60))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true);
    let builder = match path {
        // Direct traffic should keep reqwest's protocol negotiation. Forcing
        // HTTP/1.1 here makes streaming requests pay extra latency on
        // providers that support HTTP/2 multiplexing.
        NetworkPath::Direct => builder.no_proxy(),
        NetworkPath::SystemProxy => builder,
    };
    Ok(builder.build()?)
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
        routing::{get, post},
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
    async fn ordinary_client_does_not_follow_redirects() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiOrdinaryRedirectTest/0.1").unwrap();

        let response = clients
            .client(false)
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
    async fn ordinary_client_does_not_follow_post_307_or_308_redirects() {
        let (base_url, redirect_hits, target_hits, server) = redirect_server().await;
        let clients = TransportClients::new("AtoapiOrdinaryPostRedirectTest/0.1").unwrap();

        let temporary = clients
            .client(false)
            .post(format!("{base_url}/post-temporary-redirect"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(temporary.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);

        let permanent = clients
            .client(false)
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
}
