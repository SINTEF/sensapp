//! The accept loop of the HTTP server. `axum::serve` has no timeout on the request headers (hyper
//! has one, but only with a timer, which `axum::serve` does not give it) and no limit on the
//! connections: a client that opens connections and sends its headers a byte at a time would keep
//! them all.

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug)]
pub struct ServeLimits {
    /// Time a client has to send the headers of a request (HTTP/1)
    pub header_read_timeout: Duration,
    /// Connections open at the same time; the next ones wait to be accepted
    pub max_connections: usize,
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            max_connections: 4096,
        }
    }
}

/// Serve `app` until `shutdown` completes, then let the requests in flight finish.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    limits: ServeLimits,
    shutdown: impl Future<Output = ()>,
) {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout);
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let permit = tokio::select! {
            permit = connections.clone().acquire_owned() => permit.expect("the semaphore is never closed"),
            () = &mut shutdown => break,
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    // Out of file descriptors, usually: wait instead of spinning
                    tracing::warn!("Failed to accept a connection: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let connection = builder
            .serve_connection_with_upgrades(
                TokioIo::new(stream),
                TowerToHyperService::new(app.clone()),
            )
            .into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::trace!("Connection ended with an error: {error:#}");
            }
            drop(permit);
        });
    }

    drop(listener);
    graceful.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn server(
        limits: ServeLimits,
    ) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let app = Router::new().route("/", get(|| async { "ok" }));
        tokio::spawn(serve(listener, app, limits, async {
            let _ = stopped.await;
        }));
        (address, stop)
    }

    async fn get_root(stream: &mut TcpStream) -> String {
        stream
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        answer
    }

    #[tokio::test]
    async fn a_client_that_does_not_finish_its_headers_is_dropped() {
        let (address, _stop) = server(ServeLimits {
            header_read_timeout: Duration::from_millis(200),
            ..ServeLimits::default()
        })
        .await;
        let mut slow = TcpStream::connect(address).await.unwrap();
        slow.write_all(b"GET / HTTP/1.1\r\nhost: loc")
            .await
            .unwrap();
        let mut rest = Vec::new();
        let closed =
            tokio::time::timeout(Duration::from_secs(5), slow.read_to_end(&mut rest)).await;
        assert!(closed.is_ok(), "the connection is still open");

        let mut client = TcpStream::connect(address).await.unwrap();
        assert!(get_root(&mut client).await.starts_with("HTTP/1.1 200"));
    }

    #[tokio::test]
    async fn connections_over_the_limit_wait_for_one_to_close() {
        let (address, _stop) = server(ServeLimits {
            max_connections: 1,
            ..ServeLimits::default()
        })
        .await;
        let idle = TcpStream::connect(address).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let waiting = tokio::spawn(async move {
            let mut client = TcpStream::connect(address).await.unwrap();
            get_root(&mut client).await
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiting.is_finished(), "answered over the limit");

        drop(idle);
        let answer = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("not answered once a connection closed")
            .unwrap();
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    }

    #[tokio::test]
    async fn the_requests_in_flight_finish_after_the_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let app = Router::new().route(
            "/",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                "ok"
            }),
        );
        let server = tokio::spawn(serve(listener, app, ServeLimits::default(), async {
            let _ = stopped.await;
        }));
        let request = tokio::spawn(async move {
            let mut client = TcpStream::connect(address).await.unwrap();
            get_root(&mut client).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(()).unwrap();
        assert!(request.await.unwrap().starts_with("HTTP/1.1 200"));
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the server did not stop")
            .unwrap();
        assert!(
            TcpStream::connect(address).await.is_err(),
            "still accepting"
        );
    }
}
