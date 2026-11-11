//! The guard of a server that runs without authentication (`SENSAPP_AUTH_DISABLED`).
//!
//! Tokens travel in a header, so with authentication a web page cannot act on SensApp in the name
//! of a visitor. Without it, any page the visitor opens could: a `text/plain` POST to `/publish`
//! or a POST without body to the vacuum needs no preflight. And a page whose domain is made to
//! resolve to `127.0.0.1` (DNS rebinding) is same-origin with a server on the visitor's machine.
//! So an open server refuses requests from another origin, and one that listens on a loopback
//! address refuses the names that are not loopback ones.

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Wrap the router of a server that runs without authentication.
pub fn guard_open_server(router: Router, listens_on_loopback: bool) -> Router {
    router.layer(axum::middleware::from_fn_with_state(
        listens_on_loopback,
        refuse_foreign_requests,
    ))
}

async fn refuse_foreign_requests(
    State(listens_on_loopback): State<bool>,
    request: Request,
    next: Next,
) -> Response {
    match foreign_request(&request, listens_on_loopback) {
        Some(reason) => {
            tracing::warn!("Refused a request to the open server: {reason}");
            (StatusCode::FORBIDDEN, reason).into_response()
        }
        None => next.run(request).await,
    }
}

/// Why a request is refused, if it is.
fn foreign_request(request: &Request, listens_on_loopback: bool) -> Option<&'static str> {
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

    if listens_on_loopback
        && let Some(host) = host
        && !is_loopback_name(host)
    {
        return Some("this server runs without authentication and only answers loopback names");
    }

    // Browsers send `Origin` with every cross-origin request, and with same-origin writes
    let origin = request.headers().get(header::ORIGIN)?;
    let same_origin = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .is_some_and(|(_, authority)| {
            host.is_some_and(|host| authority.eq_ignore_ascii_case(host))
        });
    (!same_origin).then_some(
        "this server runs without authentication and refuses requests from other origins",
    )
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

    async fn status(listens_on_loopback: bool, headers: &[(&str, &str)]) -> StatusCode {
        let router = guard_open_server(
            Router::new().route("/publish", post(|| async { "stored" })),
            listens_on_loopback,
        );
        let mut request = Request::post("/publish");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        router
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
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
        assert_eq!(status(false, &rebound).await, StatusCode::OK);
    }
}
