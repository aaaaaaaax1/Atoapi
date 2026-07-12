use std::time::Duration;

use anyhow::Result;

#[derive(Debug)]
pub(crate) struct TransportClients {
    direct: reqwest::Client,
    system_proxy: reqwest::Client,
}

impl TransportClients {
    pub(crate) fn new(user_agent: &str) -> Result<Self> {
        Ok(Self {
            direct: build_client(user_agent, NetworkPath::Direct)?,
            system_proxy: build_client(user_agent, NetworkPath::SystemProxy)?,
        })
    }

    pub(crate) fn client(&self, use_system_proxy: bool) -> &reqwest::Client {
        if use_system_proxy {
            &self.system_proxy
        } else {
            &self.direct
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
    use super::*;

    #[test]
    fn builds_both_transport_adapters_without_global_state() {
        let clients = TransportClients::new("AtoapiTransportTest/0.1").unwrap();
        let direct = clients.client(false) as *const reqwest::Client;
        let proxied = clients.client(true) as *const reqwest::Client;
        assert_ne!(direct, proxied);
    }
}
