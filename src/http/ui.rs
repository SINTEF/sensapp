//! The web UI: the static files built from `frontend/`, served under `/ui/`.
//!
//! The files are public, like `/docs`: they hold no data. The API calls the UI makes are protected
//! by the same JWT authentication as any other client's.

use crate::config::SensAppConfig;
use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::Redirect;
use axum::routing::get;
use std::path::{Path, PathBuf};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;

/// Where the UI lives. The frontend is built for this base path (`base` in `vite.config.ts`).
pub const UI_PATH: &str = "/ui";

/// The UI holds a token in the browser, so it only talks to its own origin, and nothing may frame it.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'";

/// The directory to serve the UI from, or `None` when the UI is turned off or was not built.
pub fn ui_directory(config: &SensAppConfig) -> Option<PathBuf> {
    if !config.ui_enabled {
        return None;
    }
    let directory = PathBuf::from(&config.ui_dir);
    if directory.join("index.html").is_file() {
        tracing::info!(directory = %directory.display(), "Serving the web UI at {UI_PATH}/");
        Some(directory)
    } else {
        tracing::warn!(
            directory = %directory.display(),
            "The web UI is enabled but {}/index.html does not exist: not serving it. \
             Build it (`npm run build` in frontend/), set SENSAPP_UI_DIR, or set SENSAPP_UI_ENABLED=false",
            directory.display()
        );
        None
    }
}

/// Serve the UI from `directory` under `/ui/`, and redirect `/` to it. Paths that are no file go to
/// `index.html`: the UI routes in the browser.
pub fn add_ui<S>(router: Router<S>, directory: &Path) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let files = ServeDir::new(directory).fallback(ServeFile::new(directory.join("index.html")));
    let files = tower::ServiceBuilder::new()
        // Revalidate always: a new version must not be served with the assets of the old one
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(CONTENT_SECURITY_POLICY),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .service(files);
    router
        .route("/", get(|| async { Redirect::temporary("/ui/") }))
        .nest_service(UI_PATH, files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    /// A directory that looks like a built frontend, removed on drop.
    struct Dist(PathBuf);

    impl Dist {
        fn new() -> Self {
            let directory =
                std::env::temp_dir().join(format!("sensapp-ui-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(directory.join("assets")).unwrap();
            std::fs::write(directory.join("index.html"), "<html>SensApp UI</html>").unwrap();
            std::fs::write(directory.join("assets/app.js"), "console.log('ui')").unwrap();
            Self(directory)
        }
    }

    impl Drop for Dist {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn get_path(router: &Router, path: &str) -> (StatusCode, axum::http::HeaderMap, String) {
        let response = router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = to_bytes(body, 1024 * 1024).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&body).into(),
        )
    }

    #[tokio::test]
    async fn serves_the_files_the_index_and_the_routes_of_the_ui() {
        let dist = Dist::new();
        let router = add_ui(Router::new(), &dist.0);

        for path in [
            "/ui",
            "/ui/",
            "/ui/index.html",
            "/ui/some/route",
            "/ui/series/abc",
        ] {
            let (status, _, body) = get_path(&router, path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(body, "<html>SensApp UI</html>", "{path}");
        }

        let (status, headers, body) = get_path(&router, "/ui/assets/app.js").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "console.log('ui')");
        assert!(
            headers["content-type"]
                .to_str()
                .unwrap()
                .contains("javascript")
        );
    }

    #[tokio::test]
    async fn redirects_the_root_to_the_ui() {
        let dist = Dist::new();
        let router = add_ui(Router::new(), &dist.0);
        let (status, headers, _) = get_path(&router, "/").await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(headers["location"], "/ui/");
    }

    #[tokio::test]
    async fn protects_the_browser_that_holds_a_token() {
        let dist = Dist::new();
        let router = add_ui(Router::new(), &dist.0);
        for path in ["/ui/", "/ui/assets/app.js"] {
            let (_, headers, _) = get_path(&router, path).await;
            assert!(
                headers["content-security-policy"]
                    .to_str()
                    .unwrap()
                    .contains("default-src 'self'")
            );
            assert_eq!(headers["x-content-type-options"], "nosniff");
            assert_eq!(headers["cache-control"], "no-cache");
        }
    }

    #[tokio::test]
    async fn leaves_the_other_routes_alone() {
        let dist = Dist::new();
        let router = add_ui(
            Router::new().route("/metrics", get(|| async { "api" })),
            &dist.0,
        );
        let (_, headers, body) = get_path(&router, "/metrics").await;
        assert_eq!(body, "api");
        assert!(!headers.contains_key("content-security-policy"));
        let (status, _, _) = get_path(&router, "/other").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn serves_only_a_built_and_enabled_ui() {
        let dist = Dist::new();
        let config = |enabled: &str, directory: &str| {
            temp_env::with_vars(
                [
                    ("SENSAPP_UI_ENABLED", Some(enabled)),
                    ("SENSAPP_UI_DIR", Some(directory)),
                ],
                || SensAppConfig::load().unwrap(),
            )
        };
        let built = dist.0.to_str().unwrap();
        assert_eq!(ui_directory(&config("true", built)), Some(dist.0.clone()));
        assert_eq!(ui_directory(&config("false", built)), None);
        assert_eq!(ui_directory(&config("true", "/no/such/directory")), None);
    }
}
