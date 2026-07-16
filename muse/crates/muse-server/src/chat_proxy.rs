//! Security boundary for the optional inference-test proxy.
//!
//! The caller may choose an endpoint path and, when needed, an external gateway.
//! Keep URL policy and HTTP-client construction together so every outbound request
//! gets the same scheme, host, redirect, TLS, and timeout guarantees.

use std::{path::Path, sync::Arc, time::Duration};

use reqwest::{redirect::Policy as RedirectPolicy, Client, ClientBuilder, Response, Url};
use serde_json::Value;

/// Configured policy for one chat-proxy client. Durations have no binary default;
/// callers construct this only from explicit deployment configuration.
#[derive(Clone, Debug)]
pub struct ChatProxyConfig {
    pub timeout: Duration,
    pub max_response_bytes: usize,
    pub connect_timeout: Option<Duration>,
    pub client_grace: Option<Duration>,
    pub allowed_hosts: Vec<String>,
    pub allowed_host_suffixes: Vec<String>,
    pub allow_http: bool,
}

/// Optional mTLS material for the chat proxy. An invalid configured identity or CA
/// disables the proxy instead of silently falling back to a less-secure client.
#[derive(Clone, Debug, Default)]
pub struct ChatTls {
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub ca_cert: Option<String>,
}

#[derive(Clone, Debug)]
struct AllowedHost {
    host: String,
    port: Option<u16>,
}

#[derive(Clone, Debug)]
struct TargetPolicy {
    allowed_hosts: Arc<[AllowedHost]>,
    allowed_host_suffixes: Arc<[String]>,
    allow_http: bool,
}

/// Ready-to-use proxy client plus its immutable target policy.
#[derive(Clone)]
pub struct ChatProxy {
    client: Client,
    timeout_secs: u64,
    max_response_bytes: usize,
    client_grace_secs: Option<u64>,
    targets: TargetPolicy,
}

impl ChatProxy {
    pub fn new(config: ChatProxyConfig, tls: ChatTls) -> Result<Self, String> {
        if config.timeout.is_zero() {
            return Err("chat timeout must be greater than zero".to_owned());
        }
        if config.max_response_bytes == 0 {
            return Err("chat maximum response size must be greater than zero".to_owned());
        }
        if config.connect_timeout.is_some_and(|d| d.is_zero()) {
            return Err("chat connect timeout must be greater than zero".to_owned());
        }

        let targets = TargetPolicy::new(
            config.allowed_hosts,
            config.allowed_host_suffixes,
            config.allow_http,
        )?;
        // Never follow a gateway redirect automatically. A redirect target has not
        // passed this proxy's allow-list checks, so following it would reopen SSRF.
        let mut builder = Client::builder()
            .timeout(config.timeout)
            .https_only(!config.allow_http)
            .redirect(RedirectPolicy::none());
        if let Some(timeout) = config.connect_timeout {
            builder = builder.connect_timeout(timeout);
        }
        let client = tls
            .apply(builder)?
            .build()
            .map_err(|e| format!("build chat proxy client: {e}"))?;

        Ok(Self {
            client,
            timeout_secs: config.timeout.as_secs(),
            max_response_bytes: config.max_response_bytes,
            client_grace_secs: config.client_grace.map(|grace| grace.as_secs()),
            targets,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    pub fn client_grace_secs(&self) -> Option<u64> {
        self.client_grace_secs
    }

    /// Read and parse one upstream JSON response without ever accumulating more
    /// than the configured body limit. Content-Length is an early rejection only;
    /// the streaming count remains authoritative when it is absent or incorrect.
    pub async fn read_json_response(&self, mut response: Response) -> Result<Value, String> {
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(self.response_too_large());
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| format!("read upstream response: {e}"))?
        {
            let remaining = self.max_response_bytes.saturating_sub(body.len());
            if chunk.len() > remaining {
                return Err(self.response_too_large());
            }
            body.extend_from_slice(&chunk);
        }

        serde_json::from_slice(&body).map_err(|e| format!("parse upstream response JSON: {e}"))
    }

    fn response_too_large(&self) -> String {
        format!(
            "upstream response exceeds configured limit of {} bytes",
            self.max_response_bytes
        )
    }

    /// Resolve the outbound URL. The cached status URL is the trusted default.
    /// A caller override is accepted only for the exact status origin or an
    /// explicitly configured host/suffix; no parent domain is derived from either
    /// hostname. Every resulting URL is HTTPS unless HTTP was explicitly enabled.
    pub fn target_url(
        &self,
        status_url: &str,
        requested_base: Option<&str>,
        endpoint_path: &str,
    ) -> Result<Url, String> {
        self.targets
            .target_url(status_url, requested_base, endpoint_path)
    }
}

impl TargetPolicy {
    fn new(hosts: Vec<String>, suffixes: Vec<String>, allow_http: bool) -> Result<Self, String> {
        let allowed_hosts = hosts
            .into_iter()
            .map(|h| AllowedHost::parse(&h))
            .collect::<Result<Vec<_>, _>>()?;
        let allowed_host_suffixes = suffixes
            .into_iter()
            .map(|s| normalize_suffix(&s))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            allowed_hosts: allowed_hosts.into(),
            allowed_host_suffixes: allowed_host_suffixes.into(),
            allow_http,
        })
    }

    fn target_url(
        &self,
        status_url: &str,
        requested_base: Option<&str>,
        endpoint_path: &str,
    ) -> Result<Url, String> {
        let status = self.parse_base(status_url)?;
        let mut base = match requested_base.map(str::trim).filter(|s| !s.is_empty()) {
            None => status.clone(),
            Some(requested) => {
                let requested = self.parse_base(requested)?;
                if !same_origin(&status, &requested) && !self.explicitly_allowed(&requested) {
                    return Err(format!(
                        "target host {} is not allowed",
                        requested.host_str().unwrap_or("<missing>")
                    ));
                }
                requested
            }
        };

        validate_endpoint_path(endpoint_path)?;
        let prefix = base.path().trim_end_matches('/');
        let path = if prefix.is_empty() {
            endpoint_path.to_owned()
        } else {
            format!("{prefix}{endpoint_path}")
        };
        base.set_path(&path);
        base.set_query(None);
        base.set_fragment(None);
        Ok(base)
    }

    fn parse_base(&self, raw: &str) -> Result<Url, String> {
        let url = Url::parse(raw.trim()).map_err(|_| "invalid target URL".to_owned())?;
        let scheme_ok = url.scheme() == "https" || (self.allow_http && url.scheme() == "http");
        if !scheme_ok {
            return Err(if self.allow_http {
                "target URL must use HTTP or HTTPS".to_owned()
            } else {
                "target URL must use HTTPS".to_owned()
            });
        }
        if url.host_str().is_none() {
            return Err("target URL has no host".to_owned());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("target URL must not contain credentials".to_owned());
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err("target base URL must not contain a query or fragment".to_owned());
        }
        Ok(url)
    }

    fn explicitly_allowed(&self, url: &Url) -> bool {
        if self
            .allowed_hosts
            .iter()
            .any(|allowed| allowed.matches(url))
        {
            return true;
        }
        // Suffix rules deliberately match only the default scheme port. A gateway
        // on a non-standard port must be an exact `host:port` allow-list entry.
        if url.port().is_some() {
            return false;
        }
        let Some(host) = url.host_str().map(normalize_url_host) else {
            return false;
        };
        self.allowed_host_suffixes
            .iter()
            .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
    }
}

impl AllowedHost {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty()
            || raw.contains("://")
            || raw.contains('/')
            || raw.contains('?')
            || raw.contains('#')
            || raw.contains('@')
        {
            return Err(format!("invalid allowed chat host: {raw}"));
        }
        let probe = Url::parse(&format!("https://{raw}/"))
            .map_err(|_| format!("invalid allowed chat host: {raw}"))?;
        let host = probe
            .host_str()
            .map(normalize_url_host)
            .ok_or_else(|| format!("invalid allowed chat host: {raw}"))?;
        Ok(Self {
            host,
            port: probe.port(),
        })
    }

    fn matches(&self, url: &Url) -> bool {
        let Some(host) = url.host_str().map(normalize_url_host) else {
            return false;
        };
        if host != self.host {
            return false;
        }
        match self.port {
            Some(port) => url.port_or_known_default() == Some(port),
            None => url.port().is_none(),
        }
    }
}

impl ChatTls {
    fn apply(&self, mut builder: ClientBuilder) -> Result<ClientBuilder, String> {
        match (&self.client_cert, &self.client_key, &self.ca_cert) {
            (None, None, None) => {}
            (Some(cert), Some(key), Some(ca)) => {
                builder = builder
                    .identity(load_identity(cert, key)?)
                    .add_root_certificate(load_ca(ca)?);
            }
            _ => {
                return Err(
                    "chat mTLS requires client certificate, client key, and CA certificate together"
                        .to_owned(),
                );
            }
        }
        Ok(builder)
    }
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str().map(normalize_url_host) == b.host_str().map(normalize_url_host)
        && a.port_or_known_default() == b.port_or_known_default()
}

fn normalize_suffix(raw: &str) -> Result<String, String> {
    let suffix = raw.trim().trim_start_matches('.').trim_end_matches('.');
    let parsed = AllowedHost::parse(suffix)
        .map_err(|_| format!("invalid allowed chat host suffix: {raw}"))?;
    if parsed.port.is_some() || parsed.host.split('.').count() < 2 {
        return Err(format!("invalid allowed chat host suffix: {raw}"));
    }
    Ok(parsed.host)
}

fn normalize_url_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

fn validate_endpoint_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|part| part == "." || part == "..")
    {
        return Err(format!("invalid endpoint path: {path}"));
    }
    Ok(())
}

fn load_identity(cert_path: &str, key_path: &str) -> Result<reqwest::Identity, String> {
    let mut pem = std::fs::read(cert_path).map_err(|e| format!("read {cert_path}: {e}"))?;
    pem.push(b'\n');
    pem.extend(std::fs::read(key_path).map_err(|e| format!("read {key_path}: {e}"))?);
    reqwest::Identity::from_pem(&pem).map_err(|e| format!("parse client identity: {e}"))
}

fn load_ca(path: impl AsRef<Path>) -> Result<reqwest::Certificate, String> {
    let path = path.as_ref();
    let pem = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    reqwest::Certificate::from_pem(&pem).map_err(|e| format!("parse CA cert: {e}"))
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use axum::{
        body::{Body, Bytes},
        extract::State,
        http::{
            header::{CONTENT_TYPE, LOCATION},
            Response, StatusCode,
        },
        routing::post,
        Router,
    };

    use super::{ChatProxy, ChatProxyConfig, ChatTls};

    fn proxy(hosts: &[&str], suffixes: &[&str], allow_http: bool, timeout: Duration) -> ChatProxy {
        proxy_with_limit(hosts, suffixes, allow_http, timeout, 1024)
    }

    fn proxy_with_limit(
        hosts: &[&str],
        suffixes: &[&str],
        allow_http: bool,
        timeout: Duration,
        max_response_bytes: usize,
    ) -> ChatProxy {
        ChatProxy::new(
            ChatProxyConfig {
                timeout,
                max_response_bytes,
                connect_timeout: None,
                client_grace: None,
                allowed_hosts: hosts.iter().map(|s| (*s).to_owned()).collect(),
                allowed_host_suffixes: suffixes.iter().map(|s| (*s).to_owned()).collect(),
                allow_http,
            },
            ChatTls::default(),
        )
        .unwrap()
    }

    #[test]
    fn exact_status_origin_is_allowed_without_deriving_a_parent_domain() {
        let proxy = proxy(&[], &[], false, Duration::from_secs(1));
        let status = "https://model.ns.int-gw.cluster.example";

        let exact = proxy
            .target_url(status, Some(status), "/v1/chat/completions")
            .unwrap();
        assert_eq!(
            exact.as_str(),
            "https://model.ns.int-gw.cluster.example/v1/chat/completions"
        );
        let err = proxy
            .target_url(
                status,
                Some("https://attacker.cluster.example"),
                "/generate",
            )
            .unwrap_err();
        assert!(err.contains("not allowed"));
    }

    #[test]
    fn configured_exact_host_and_anchored_suffix_are_allowed() {
        let proxy = proxy(
            &["gateway.example:8443"],
            &["serving.example"],
            false,
            Duration::from_secs(1),
        );
        let status = "https://model.internal.example";

        assert!(proxy
            .target_url(status, Some("https://gateway.example:8443"), "/generate")
            .is_ok());
        assert!(proxy
            .target_url(status, Some("https://team.serving.example"), "/generate")
            .is_ok());
        assert!(proxy
            .target_url(
                status,
                Some("https://team.serving.example.evil.test"),
                "/generate"
            )
            .is_err());
        assert!(proxy
            .target_url(
                status,
                Some("https://team.serving.example:8443"),
                "/generate"
            )
            .is_err());
    }

    #[test]
    fn https_is_required_unless_http_is_explicitly_enabled() {
        let strict = proxy(&[], &[], false, Duration::from_secs(1));
        assert!(strict
            .target_url("http://model.internal.example", None, "/generate")
            .is_err());

        let development = proxy(&[], &[], true, Duration::from_secs(1));
        assert!(development
            .target_url("http://model.internal.example", None, "/generate")
            .is_ok());
    }

    #[test]
    fn invalid_base_and_endpoint_forms_are_rejected() {
        let proxy = proxy(&["gateway.example"], &[], false, Duration::from_secs(1));
        let status = "https://model.internal.example";
        assert!(proxy
            .target_url(
                status,
                Some("https://user:secret@gateway.example"),
                "/generate"
            )
            .is_err());
        assert!(proxy
            .target_url(status, Some("file:///etc/passwd"), "/generate")
            .is_err());
        assert!(proxy
            .target_url(status, None, "//attacker.example/generate")
            .is_err());
        assert!(proxy.target_url(status, None, "/v1/../admin").is_err());
    }

    #[test]
    fn broad_single_label_suffix_is_rejected() {
        let result = ChatProxy::new(
            ChatProxyConfig {
                timeout: Duration::from_secs(1),
                max_response_bytes: 1024,
                connect_timeout: None,
                client_grace: None,
                allowed_hosts: Vec::new(),
                allowed_host_suffixes: vec!["com".to_owned()],
                allow_http: false,
            },
            ChatTls::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn every_partial_mtls_combination_is_rejected() {
        let partial = [
            (Some("cert"), None, None),
            (None, Some("key"), None),
            (None, None, Some("ca")),
            (Some("cert"), Some("key"), None),
            (Some("cert"), None, Some("ca")),
            (None, Some("key"), Some("ca")),
        ];

        for (cert, key, ca) in partial {
            let result = ChatProxy::new(
                ChatProxyConfig {
                    timeout: Duration::from_secs(1),
                    max_response_bytes: 1024,
                    connect_timeout: None,
                    client_grace: None,
                    allowed_hosts: Vec::new(),
                    allowed_host_suffixes: Vec::new(),
                    allow_http: false,
                },
                ChatTls {
                    client_cert: cert.map(str::to_owned),
                    client_key: key.map(str::to_owned),
                    ca_cert: ca.map(str::to_owned),
                },
            );
            let err = result.err().expect("partial mTLS must fail closed");
            assert!(err.contains("requires client certificate, client key, and CA certificate"));
        }
    }

    async fn redirect(
        State(hits): State<Arc<AtomicUsize>>,
    ) -> (StatusCode, [(axum::http::HeaderName, &'static str); 1]) {
        let _ = hits;
        (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, "/destination")])
    }

    async fn destination(State(hits): State<Arc<AtomicUsize>>) -> StatusCode {
        hits.fetch_add(1, Ordering::SeqCst);
        StatusCode::OK
    }

    async fn slow() -> StatusCode {
        tokio::time::sleep(Duration::from_millis(200)).await;
        StatusCode::OK
    }

    fn streamed_json(chunks: &[&'static [u8]]) -> Response<Body> {
        let chunks = chunks
            .iter()
            .map(|chunk| Ok::<Bytes, Infallible>(Bytes::from_static(chunk)))
            .collect::<Vec<_>>();
        Response::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from_stream(tokio_stream::iter(chunks)))
            .unwrap()
    }

    async fn small_chunked_json() -> Response<Body> {
        streamed_json(&[br#"{"answer":"#, br#""ok"}"#])
    }

    async fn large_chunked_json() -> Response<Body> {
        streamed_json(&[
            br#"{"answer":"#,
            br#""this response is larger than the configured cap"}"#,
        ])
    }

    async fn spawn(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn outbound_client_does_not_follow_redirects() {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/redirect", post(redirect))
            .route("/destination", post(destination))
            .with_state(hits.clone());
        let base = spawn(app).await;
        let proxy = proxy(&[], &[], true, Duration::from_secs(1));

        let response = proxy
            .client()
            .post(format!("{base}/redirect"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn outbound_client_enforces_the_configured_total_timeout() {
        let app = Router::new().route("/slow", post(slow));
        let base = spawn(app).await;
        let proxy = proxy(&[], &[], true, Duration::from_millis(20));

        let err = proxy
            .client()
            .post(format!("{base}/slow"))
            .send()
            .await
            .unwrap_err();
        assert!(err.is_timeout());
    }

    #[tokio::test]
    async fn bounded_reader_accepts_chunked_json_without_content_length() {
        let app = Router::new().route("/json", post(small_chunked_json));
        let base = spawn(app).await;
        let proxy = proxy_with_limit(&[], &[], true, Duration::from_secs(1), 64);
        let response = proxy
            .client()
            .post(format!("{base}/json"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.content_length(), None);
        let payload = proxy.read_json_response(response).await.unwrap();
        assert_eq!(payload["answer"], "ok");
    }

    #[tokio::test]
    async fn bounded_reader_rejects_chunked_body_over_limit_without_content_length() {
        let app = Router::new().route("/json", post(large_chunked_json));
        let base = spawn(app).await;
        let proxy = proxy_with_limit(&[], &[], true, Duration::from_secs(1), 24);
        let response = proxy
            .client()
            .post(format!("{base}/json"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.content_length(), None);
        let err = proxy.read_json_response(response).await.unwrap_err();
        assert_eq!(
            err,
            "upstream response exceeds configured limit of 24 bytes"
        );
    }
}
