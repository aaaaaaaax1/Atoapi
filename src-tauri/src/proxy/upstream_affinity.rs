use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    time::{Duration, Instant},
};

use axum::http::{header, HeaderMap, HeaderValue};

use super::action_scope::CompositeActionScope;

// Keep this process-only state deliberately small. A load-balancer placement
// cookie is not configuration, user history, or a general-purpose cookie jar.
const UPSTREAM_AFFINITY_ENTRY_LIMIT: usize = 256;
const UPSTREAM_AFFINITY_TTL: Duration = Duration::from_secs(20 * 60);
const UPSTREAM_AFFINITY_COOKIE_HEADER_MAX_BYTES: usize = 2_048;

/// Opaque scope for one selected endpoint/model/channel/Key realm and one
/// trusted Codex session. Its source already includes a boot epoch, so a
/// process restart deliberately starts without any learned upstream affinity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct UpstreamAffinityScope {
    anchor_key: String,
}

impl UpstreamAffinityScope {
    pub(super) fn from_action_scope(scope: &CompositeActionScope) -> Self {
        Self {
            anchor_key: scope.anchor_key.clone(),
        }
    }

    pub(super) fn from_anchor_key(anchor_key: &str) -> Option<Self> {
        let anchor_key = anchor_key.trim();
        (!anchor_key.is_empty()).then(|| Self {
            anchor_key: anchor_key.to_string(),
        })
    }

    #[cfg(test)]
    pub(super) fn for_test(anchor_key: impl Into<String>) -> Self {
        Self {
            anchor_key: anchor_key.into(),
        }
    }
}

impl fmt::Debug for UpstreamAffinityScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UpstreamAffinityScope([opaque])")
    }
}

/// Bounded, non-persistent upstream placement state. Values are never
/// serialized, logged, included in metrics, or exposed to the downstream
/// client. It only becomes non-empty when an upstream explicitly returns a
/// narrowly recognized load-balancer cookie.
pub(crate) struct UpstreamCacheAffinity {
    entries: HashMap<UpstreamAffinityScope, UpstreamAffinityEntry>,
}

impl Default for UpstreamCacheAffinity {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl fmt::Debug for UpstreamCacheAffinity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamCacheAffinity")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

struct UpstreamAffinityEntry {
    cookies: BTreeMap<AffinityCookieName, String>,
    expires_at: Instant,
    last_used: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum AffinityCookieName {
    AwsAlb,
    AwsAlbCors,
    AwsAlbTg,
    AwsAlbTgCors,
    AzureArrAffinity,
    AzureArrAffinitySameSite,
}

impl AffinityCookieName {
    fn parse(value: &str) -> Option<Self> {
        match value {
            // AWS Application Load Balancer duration/target-group stickiness.
            "AWSALB" => Some(Self::AwsAlb),
            "AWSALBCORS" => Some(Self::AwsAlbCors),
            "AWSALBTG" => Some(Self::AwsAlbTg),
            "AWSALBTGCORS" => Some(Self::AwsAlbTgCors),
            // Azure App Service front-end affinity.
            "ARRAffinity" => Some(Self::AzureArrAffinity),
            "ARRAffinitySameSite" => Some(Self::AzureArrAffinitySameSite),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::AwsAlb => "AWSALB",
            Self::AwsAlbCors => "AWSALBCORS",
            Self::AwsAlbTg => "AWSALBTG",
            Self::AwsAlbTgCors => "AWSALBTGCORS",
            Self::AzureArrAffinity => "ARRAffinity",
            Self::AzureArrAffinitySameSite => "ARRAffinitySameSite",
        }
    }
}

struct CapturedAffinityCookie {
    name: AffinityCookieName,
    pair: Option<String>,
}

impl UpstreamCacheAffinity {
    /// Adds a previously learned upstream load-balancer cookie only when the
    /// caller did not supply its own Cookie header. This never blocks the
    /// outbound request: the caller obtains the store with `try_lock`.
    pub(super) fn inject_if_available(
        &mut self,
        scope: &UpstreamAffinityScope,
        outbound_headers: &mut HeaderMap,
        now: Instant,
    ) -> bool {
        if outbound_headers.contains_key(header::COOKIE) {
            return false;
        }
        self.prune_expired(now);
        let Some(entry) = self.entries.get_mut(scope) else {
            return false;
        };
        let Some(cookie_header) = entry.cookie_header() else {
            return false;
        };
        let Ok(cookie_header) = HeaderValue::from_str(&cookie_header) else {
            self.entries.remove(scope);
            return false;
        };
        entry.last_used = now;
        outbound_headers.insert(header::COOKIE, cookie_header);
        true
    }

    /// Learns only recognized load-balancer cookies from a successful response
    /// head. Generic, auth, session, and Cloudflare cookies are intentionally
    /// ignored. The caller keeps this store process-local and non-blocking.
    pub(super) fn learn_from_success_response(
        &mut self,
        scope: &UpstreamAffinityScope,
        response_headers: &HeaderMap,
        now: Instant,
    ) -> bool {
        let captured = response_headers
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(parse_recognized_affinity_cookie)
            .collect::<Vec<_>>();
        if captured.is_empty() {
            return false;
        }

        self.prune_expired(now);
        let entry = self
            .entries
            .entry(scope.clone())
            .or_insert_with(|| UpstreamAffinityEntry {
                cookies: BTreeMap::new(),
                expires_at: now + UPSTREAM_AFFINITY_TTL,
                last_used: now,
            });
        for cookie in captured {
            match cookie.pair {
                Some(pair) => {
                    entry.cookies.insert(cookie.name, pair);
                }
                None => {
                    entry.cookies.remove(&cookie.name);
                }
            }
        }
        if entry.cookies.is_empty() || entry.cookie_header().is_none() {
            self.entries.remove(scope);
            return false;
        }
        entry.expires_at = now + UPSTREAM_AFFINITY_TTL;
        entry.last_used = now;
        self.evict_to_limit();
        true
    }

    /// An injected placement cookie which leads to a non-2xx response may be
    /// stale or harmful for this exact route. Forget it for a future inbound;
    /// never retry the current inbound request.
    pub(super) fn clear(&mut self, scope: &UpstreamAffinityScope) {
        self.entries.remove(scope);
    }

    fn prune_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    fn evict_to_limit(&mut self) {
        while self.entries.len() > UPSTREAM_AFFINITY_ENTRY_LIMIT {
            let Some(oldest_scope) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(scope, _)| scope.clone())
            else {
                return;
            };
            self.entries.remove(&oldest_scope);
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, scope: &UpstreamAffinityScope) -> bool {
        self.entries.contains_key(scope)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

impl UpstreamAffinityEntry {
    fn cookie_header(&self) -> Option<String> {
        let header = self
            .cookies
            .values()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("; ");
        (!header.is_empty() && header.len() <= UPSTREAM_AFFINITY_COOKIE_HEADER_MAX_BYTES)
            .then_some(header)
    }
}

fn parse_recognized_affinity_cookie(value: &HeaderValue) -> Option<CapturedAffinityCookie> {
    let raw = value.to_str().ok()?;
    let mut segments = raw.split(';');
    let pair = segments.next()?.trim();
    let (name, value) = pair.split_once('=')?;
    let name = AffinityCookieName::parse(name.trim())?;
    let value = value.trim();
    let delete = segments.any(|segment| {
        let (attribute, value) = segment.trim().split_once('=').unwrap_or(("", ""));
        attribute.trim().eq_ignore_ascii_case("max-age")
            && value
                .trim()
                .parse::<i64>()
                .is_ok_and(|seconds| seconds <= 0)
    });
    if delete {
        return Some(CapturedAffinityCookie { name, pair: None });
    }
    if value.is_empty() {
        return None;
    }
    let pair = format!("{}={value}", name.as_str());
    (pair.len() <= UPSTREAM_AFFINITY_COOKIE_HEADER_MAX_BYTES
        && HeaderValue::from_str(&pair).is_ok())
    .then_some(CapturedAffinityCookie {
        name,
        pair: Some(pair),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(header::SET_COOKIE, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn recognized_cookie_replays_only_to_its_exact_scope() {
        let now = Instant::now();
        let scope = UpstreamAffinityScope::for_test("scope-a");
        let other_scope = UpstreamAffinityScope::for_test("scope-b");
        let mut store = UpstreamCacheAffinity::default();
        store.learn_from_success_response(
            &scope,
            &headers(&["AWSALB=opaque-affinity; Path=/; HttpOnly"]),
            now,
        );

        let mut same_scope = HeaderMap::new();
        assert!(store.inject_if_available(&scope, &mut same_scope, now));
        assert_eq!(
            same_scope.get(header::COOKIE).unwrap(),
            "AWSALB=opaque-affinity"
        );

        let mut different_scope = HeaderMap::new();
        assert!(!store.inject_if_available(&other_scope, &mut different_scope, now));
        assert!(!different_scope.contains_key(header::COOKIE));
    }

    #[test]
    fn generic_auth_and_cloudflare_cookies_are_ignored() {
        let now = Instant::now();
        let scope = UpstreamAffinityScope::for_test("scope-a");
        let mut store = UpstreamCacheAffinity::default();
        store.learn_from_success_response(
            &scope,
            &headers(&[
                "session=secret-session; Path=/; HttpOnly",
                "__cf_bm=cloudflare-value; Path=/; HttpOnly",
                "Authorization=not-a-cookie-we-own; Path=/",
            ]),
            now,
        );

        assert!(!store.contains(&scope));
        assert_eq!(store.len(), 0);
        assert!(!format!("{store:?}").contains("secret-session"));
    }

    #[test]
    fn caller_cookie_is_never_overwritten() {
        let now = Instant::now();
        let scope = UpstreamAffinityScope::for_test("scope-a");
        let mut store = UpstreamCacheAffinity::default();
        store.learn_from_success_response(
            &scope,
            &headers(&["ARRAffinity=opaque-affinity; Path=/; HttpOnly"]),
            now,
        );
        let mut outbound = HeaderMap::new();
        outbound.insert(header::COOKIE, HeaderValue::from_static("caller=value"));

        assert!(!store.inject_if_available(&scope, &mut outbound, now));
        assert_eq!(outbound.get(header::COOKIE).unwrap(), "caller=value");
    }

    #[test]
    fn expiry_delete_and_bounded_eviction_do_not_retain_old_affinity() {
        let now = Instant::now();
        let first = UpstreamAffinityScope::for_test("scope-first");
        let mut store = UpstreamCacheAffinity::default();
        store.learn_from_success_response(&first, &headers(&["AWSALB=first; Path=/"]), now);
        let mut expired_headers = HeaderMap::new();
        assert!(!store.inject_if_available(
            &first,
            &mut expired_headers,
            now + UPSTREAM_AFFINITY_TTL,
        ));
        assert!(!store.contains(&first));

        store.learn_from_success_response(&first, &headers(&["AWSALB=first; Path=/"]), now);
        store.learn_from_success_response(
            &first,
            &headers(&["AWSALB=deleted; Max-Age=0; Path=/"]),
            now,
        );
        assert!(!store.contains(&first));

        for index in 0..=UPSTREAM_AFFINITY_ENTRY_LIMIT {
            let scope = UpstreamAffinityScope::for_test(format!("scope-{index}"));
            store.learn_from_success_response(
                &scope,
                &headers(&["ARRAffinity=bounded; Path=/"]),
                now + Duration::from_millis(index as u64),
            );
        }
        assert!(store.len() <= UPSTREAM_AFFINITY_ENTRY_LIMIT);
    }

    #[test]
    fn paired_affinity_cookies_are_replayed_in_stable_name_order() {
        let now = Instant::now();
        let scope = UpstreamAffinityScope::for_test("scope-a");
        let mut store = UpstreamCacheAffinity::default();
        store.learn_from_success_response(
            &scope,
            &headers(&[
                "AWSALBCORS=cors-placement; Path=/; Secure",
                "AWSALB=base-placement; Path=/; Secure",
            ]),
            now,
        );
        let mut outbound = HeaderMap::new();
        assert!(store.inject_if_available(&scope, &mut outbound, now));
        assert_eq!(
            outbound.get(header::COOKIE).unwrap(),
            "AWSALB=base-placement; AWSALBCORS=cors-placement"
        );
    }
}
