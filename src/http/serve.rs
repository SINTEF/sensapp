//! The accept loop of the HTTP server. `axum::serve` has no timeout on the request headers (hyper
//! has one, but only with a timer, which `axum::serve` does not give it) and no limit on the
//! connections: a client that opens connections and sends its headers a byte at a time would keep
//! them all.
//!
//! HTTP/1 only. The auto builder of hyper-util starts every connection by waiting for its first
//! bytes without any timer, and a client that sends the HTTP/2 preface and then nothing is not
//! timed either: 4,096 silent connections would hold every permit. The proxies and load
//! balancers in front of SensApp speak HTTP/1.1 upstream.

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug)]
pub struct ServeLimits {
    /// Time a client has to send the headers of a request. It is also the time a connection may
    /// stay idle between two requests (hyper starts the same timer after each response), so it
    /// must be longer than the idle timeout of the proxies in front, or they reuse connections
    /// that SensApp just closed.
    pub header_read_timeout: Duration,
    /// Connections open at the same time; the next ones wait to be accepted
    pub max_connections: usize,
    /// Time the requests in flight have to finish after the shutdown signal
    pub shutdown_grace: Duration,
}

impl ServeLimits {
    pub fn from_config(config: &crate::config::SensAppConfig) -> Self {
        Self {
            header_read_timeout: Duration::from_secs(config.http_header_read_timeout_seconds),
            shutdown_grace: Duration::from_secs(config.http_shutdown_grace_seconds),
            ..Self::default()
        }
    }
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(75),
            max_connections: 4096,
            shutdown_grace: Duration::from_secs(300),
        }
    }
}

/// Serve `app` until `shutdown` completes, then let the requests in flight finish: for at most
/// `limits.shutdown_grace`, or until `force` completes (a second signal).
pub async fn serve(
    listener: TcpListener,
    app: Router,
    limits: ServeLimits,
    shutdown: impl Future<Output = ()>,
    force: impl Future<Output = ()>,
) {
    let mut builder = http1::Builder::new();
    builder
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
        let connection =
            builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(app.clone()));
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::trace!("Connection ended with an error: {error:#}");
            }
            drop(permit);
        });
    }

    drop(listener);
    // The timeouts of the handlers do not bound the write of a response: a client that stops
    // reading would keep the process until it is killed
    tokio::select! {
        () = graceful.shutdown() => {}
        () = tokio::time::sleep(limits.shutdown_grace) => {
            tracing::warn!(
                "Shutdown: the requests in flight did not finish within {} seconds, they are cut",
                limits.shutdown_grace.as_secs()
            );
        }
        () = force => {
            tracing::warn!("Shutdown: second signal, the requests in flight are cut");
        }
    }
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
        tokio::spawn(serve(
            listener,
            app,
            limits,
            async {
                let _ = stopped.await;
            },
            std::future::pending(),
        ));
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

    /// The HTTP/1 header timer only starts after the first byte when the version is still to be
    /// detected: 100 connections that send nothing held the 100 permits for ever.
    #[tokio::test]
    async fn connections_that_send_nothing_are_dropped_and_free_their_permits() {
        let (address, _stop) = server(ServeLimits {
            header_read_timeout: Duration::from_millis(300),
            max_connections: 100,
            ..ServeLimits::default()
        })
        .await;
        let mut silent = Vec::new();
        for _ in 0..100 {
            silent.push(TcpStream::connect(address).await.unwrap());
        }
        // Every permit is held: the next client is not served yet...
        let waiting = tokio::spawn(async move {
            let mut client = TcpStream::connect(address).await.unwrap();
            get_root(&mut client).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiting.is_finished(), "served over the limit");
        // ... and is once the silent ones are dropped by the timeout
        let answer = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the silent connections still hold every permit")
            .unwrap();
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
        drop(silent);
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
        let server = tokio::spawn(serve(
            listener,
            app,
            ServeLimits::default(),
            async {
                let _ = stopped.await;
            },
            std::future::pending(),
        ));
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

    /// A keep-alive connection is closed after the header timeout, which is also its idle time:
    /// it must stay open until then, not shorter.
    #[tokio::test]
    async fn an_idle_keep_alive_connection_stays_open_for_the_header_timeout() {
        let (address, _stop) = server(ServeLimits {
            header_read_timeout: Duration::from_millis(600),
            ..ServeLimits::default()
        })
        .await;
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut answer = [0_u8; 1024];
        let read = client.read(&mut answer).await.unwrap();
        assert!(String::from_utf8_lossy(&answer[..read]).starts_with("HTTP/1.1 200"));

        tokio::time::sleep(Duration::from_millis(300)).await;
        // Still open: a second request on the same connection is answered
        client
            .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut rest = String::new();
        client.read_to_string(&mut rest).await.unwrap();
        assert!(rest.starts_with("HTTP/1.1 200"), "{rest}");
    }

    /// A client that stops reading its response must not keep the process after the grace
    /// period, and a second signal ends the wait at once.
    #[tokio::test]
    async fn the_drain_is_bounded_by_the_grace_period_and_by_a_second_signal() {
        for second_signal in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let (force, forced) = tokio::sync::oneshot::channel::<()>();
            let app = Router::new().route("/", get(|| async { vec![0_u8; 64 * 1024 * 1024] }));
            let limits = ServeLimits {
                shutdown_grace: if second_signal {
                    Duration::from_secs(3600)
                } else {
                    Duration::from_millis(300)
                },
                ..ServeLimits::default()
            };
            let server = tokio::spawn(serve(
                listener,
                app,
                limits,
                async {
                    let _ = stopped.await;
                },
                async {
                    let _ = forced.await;
                },
            ));
            // Asks for 64 MiB and reads none of it
            let mut client = TcpStream::connect(address).await.unwrap();
            client
                .write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            stop.send(()).unwrap();
            if second_signal {
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert!(!server.is_finished(), "stopped before the second signal");
                force.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .expect("the server still waits for a client that does not read")
                .unwrap();
            drop(client);
        }
    }
}
