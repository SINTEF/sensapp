//! The guard of a server that runs without authentication (`SENSAPP_AUTH_DISABLED`).
//!
//! Tokens travel in a header, so with authentication a web page cannot act on SensApp in the name
//! of a visitor. Without it, any page the visitor opens could: a `text/plain` POST to `/publish`
//! or a POST without body to the vacuum needs no preflight. And a page whose domain is made to
//! resolve to the address of the server (DNS rebinding) is same-origin with it. So an open server
//! refuses requests from another origin, and the names it was not told about:
//!
//! - with `SENSAPP_ALLOWED_HOSTS` (`host[:port]`, comma separated: the names the clients and the
//!   proxy in front use), a `Host` must be one of them, or a loopback name, on any address. An
//!   `Origin` must be one of them, or the `Host` itself.
//! - without it, on a loopback address, the `Host` must be a loopback name. On another address
//!   nothing says which names are legitimate and rebinding is not prevented (a warning at startup).

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct OpenServerGuard {
    /// Lower case `host[:port]`
    allowed_hosts: Arc<[String]>,
    listens_on_loopback: bool,
}

impl OpenServerGuard {
    pub fn new(allowed_hosts: &[String], listens_on_loopback: bool) -> Self {
        Self {
            allowed_hosts: allowed_hosts
                .iter()
                .map(|host| host.trim().to_ascii_lowercase())
                .collect(),
            listens_on_loopback,
        }
    }

    /// Log what the guard does not prevent.
    pub fn warn_about_rebinding(&self) {
        if self.allowed_hosts.is_empty() && !self.listens_on_loopback {
            tracing::warn!(
                "SensApp runs without authentication on a non-loopback address and without \
                 SENSAPP_ALLOWED_HOSTS: a web page cannot write from another origin, but a \
                 domain made to resolve to this server (DNS rebinding) is not refused. Set \
                 SENSAPP_ALLOWED_HOSTS to the names of the server."
            );
        }
    }

    fn is_allowed(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        self.allowed_hosts.contains(&host)
    }

    /// Why a request is refused, if it is.
    fn refusal(&self, request: &Request) -> Option<&'static str> {
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .or_else(|| {
                request
                    .uri()
                    .authority()
                    .map(|authority| authority.as_str())
            });

        if self.allowed_hosts.is_empty() {
            if self.listens_on_loopback
                && let Some(host) = host
                && !is_loopback_name(host)
            {
                return Some(
                    "this server runs without authentication and only answers loopback names",
                );
            }
        } else if !host.is_some_and(|host| self.is_allowed(host) || is_loopback_name(host)) {
            return Some(
                "this server runs without authentication and only answers the names of SENSAPP_ALLOWED_HOSTS",
            );
        }

        // Browsers send `Origin` with every cross-origin request, and with same-origin writes
        let origin = request.headers().get(header::ORIGIN)?;
        let allowed = origin
            .to_str()
            .ok()
            .and_then(|origin| origin.split_once("://"))
            .is_some_and(|(_, authority)| {
                self.is_allowed(authority)
                    || host.is_some_and(|host| authority.eq_ignore_ascii_case(host))
            });
        (!allowed).then_some(
            "this server runs without authentication and refuses requests from other origins",
        )
    }
}

/// Wrap the router of a server that runs without authentication.
pub fn guard_open_server<S: Clone + Send + Sync + 'static>(
    router: Router<S>,
    guard: OpenServerGuard,
) -> Router<S> {
    router.layer(axum::middleware::from_fn_with_state(
        guard,
        refuse_foreign_requests,
    ))
}

async fn refuse_foreign_requests(
    State(guard): State<OpenServerGuard>,
    request: Request,
    next: Next,
) -> Response {
    match guard.refusal(&request) {
        Some(reason) => {
            tracing::warn!("Refused a request to the open server: {reason}");
            (StatusCode::FORBIDDEN, reason).into_response()
        }
        None => next.run(request).await,
    }
}

/// `localhost`, a name under `.localhost`, `127.x.x.x` or `[::1]`, with or without a port.
fn is_loopback_name(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        // [::1]:8080
        Some(rest) => rest.split(']').next().unwrap_or_default(),
        None => host.rsplit_once(':').map_or(host, |(name, _port)| name),
    };
    let name = name.to_ascii_lowercase();
    name == "localhost"
        || name.ends_with(".localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::post;
    use tower::ServiceExt;

    async fn respond(
        allowed_hosts: &[&str],
        listens_on_loopback: bool,
        uri: &str,
        headers: &[(&str, &str)],
    ) -> StatusCode {
        let allowed_hosts: Vec<String> =
            allowed_hosts.iter().map(|host| host.to_string()).collect();
        let router = guard_open_server(
            Router::new().route("/publish", post(|| async { "stored" })),
            OpenServerGuard::new(&allowed_hosts, listens_on_loopback),
        );
        let mut request = Request::post(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        router
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    async fn status(listens_on_loopback: bool, headers: &[(&str, &str)]) -> StatusCode {
        respond(&[], listens_on_loopback, "/publish", headers).await
    }

    #[tokio::test]
    async fn requests_from_another_origin_are_refused() {
        let host = ("host", "sensapp.internal:3000");
        assert_eq!(
            status(false, &[host]).await,
            StatusCode::OK,
            "curl, Telegraf"
        );
        assert_eq!(
            status(false, &[host, ("origin", "http://sensapp.internal:3000")]).await,
            StatusCode::OK,
            "the UI"
        );
        for origin in [
            "https://evil.example",
            "null",
            "http://sensapp.internal:3001",
        ] {
            assert_eq!(
                status(false, &[host, ("origin", origin)]).await,
                StatusCode::FORBIDDEN,
                "{origin}"
            );
        }
    }

    #[tokio::test]
    async fn a_loopback_server_only_answers_loopback_names() {
        for host in [
            "localhost:3000",
            "127.0.0.1:3000",
            "[::1]:3000",
            "ui.localhost",
        ] {
            assert_eq!(
                status(true, &[("host", host)]).await,
                StatusCode::OK,
                "{host}"
            );
        }
        // A name that was made to resolve to 127.0.0.1, with its own page as the origin
        let rebound = [
            ("host", "evil.example:3000"),
            ("origin", "http://evil.example:3000"),
        ];
        assert_eq!(status(true, &rebound).await, StatusCode::FORBIDDEN);
        // Without SENSAPP_ALLOWED_HOSTS nothing tells the names of a non-loopback server
        assert_eq!(status(false, &rebound).await, StatusCode::OK);
    }

    /// A proxy that authenticates in front, SensApp on loopback.
    #[tokio::test]
    async fn the_proxy_setups_work_with_the_names_declared() {
        let proxy = ["sensapp.example.com"];
        // The proxy keeps the Host: refused without the setting, served with it
        let kept = [("host", "sensapp.example.com")];
        assert_eq!(
            respond(&[], true, "/publish", &kept).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            respond(&proxy, true, "/publish", &kept).await,
            StatusCode::OK
        );
        // The browser write through it: its Origin is the public name
        let kept_write = [
            ("host", "sensapp.example.com"),
            ("origin", "https://sensapp.example.com"),
        ];
        assert_eq!(
            respond(&proxy, true, "/publish", &kept_write).await,
            StatusCode::OK
        );
        // The proxy rewrites the Host (nginx proxy_pass): the Origin is the declared name
        let rewritten = [
            ("host", "127.0.0.1:3000"),
            ("origin", "https://sensapp.example.com"),
        ];
        assert_eq!(
            respond(&proxy, true, "/publish", &rewritten).await,
            StatusCode::OK
        );
        // ... and the Origin of another page is not
        let other_page = [
            ("host", "127.0.0.1:3000"),
            ("origin", "https://evil.example"),
        ];
        assert_eq!(
            respond(&proxy, true, "/publish", &other_page).await,
            StatusCode::FORBIDDEN
        );
    }

    /// The Docker image listens on 0.0.0.0, where the loopback rule does not apply.
    #[tokio::test]
    async fn rebinding_is_refused_on_any_address_when_the_names_are_declared() {
        let names = ["sensapp.internal:3000", "Other.Internal:3000"];
        let rebound = [
            ("host", "evil.example:3000"),
            ("origin", "http://evil.example:3000"),
        ];
        assert_eq!(
            respond(&names, false, "/publish", &rebound).await,
            StatusCode::FORBIDDEN
        );
        for host in [
            "sensapp.internal:3000",
            "other.internal:3000",
            "localhost:3000",
        ] {
            assert_eq!(
                respond(&names, false, "/publish", &[("host", host)]).await,
                StatusCode::OK,
                "{host}"
            );
        }
        // The port is part of the name
        assert_eq!(
            respond(
                &names,
                false,
                "/publish",
                &[("host", "sensapp.internal:80")]
            )
            .await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn a_request_without_a_host_is_refused_when_the_names_are_declared() {
        // HTTP/1.0 without Host, and HTTP/2, which has the authority in the URI
        assert_eq!(
            respond(&["sensapp.internal"], false, "/publish", &[]).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            respond(&[], true, "/publish", &[]).await,
            StatusCode::OK,
            "as before without the setting"
        );
        assert_eq!(
            respond(
                &["sensapp.internal"],
                false,
                "http://sensapp.internal/publish",
                &[]
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            respond(
                &["sensapp.internal"],
                false,
                "http://evil.example/publish",
                &[]
            )
            .await,
            StatusCode::FORBIDDEN
        );
    }
}
