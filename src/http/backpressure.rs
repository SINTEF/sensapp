use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// Smallest and largest `Retry-After` (seconds) the server will ever suggest.
const MIN_RETRY_AFTER_SECONDS: u64 = 1;
const MAX_RETRY_AFTER_SECONDS: u64 = 30;

/// Bounds the number of write requests handled at the same time, and keeps a moving average of
/// how long an accepted write holds its slot. That average is the best available estimate of
/// how long a rejected client should wait before a slot frees up.
#[derive(Clone, Debug)]
pub struct WriteLimiter {
    slots: Arc<Semaphore>,
    /// Exponential moving average of the write duration in milliseconds, 0 until the first write.
    average_write_millis: Arc<AtomicU64>,
}

impl WriteLimiter {
    /// `max_concurrent_writes == 0` disables the limit.
    pub fn new(max_concurrent_writes: usize) -> Self {
        let permits = if max_concurrent_writes == 0 {
            Semaphore::MAX_PERMITS
        } else {
            max_concurrent_writes.min(Semaphore::MAX_PERMITS)
        };
        Self {
            slots: Arc::new(Semaphore::new(permits)),
            average_write_millis: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_write(&self, duration: Duration) {
        let sample = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let average = self.average_write_millis.load(Ordering::Relaxed);
        // Weight 1/8 for the new sample. A lost update under contention only makes the average
        // slightly stale, which is fine for a hint.
        let updated = if average == 0 {
            sample
        } else {
            (average - average / 8).saturating_add(sample / 8)
        };
        self.average_write_millis.store(updated, Ordering::Relaxed);
    }

    /// Full jitter between the minimum and twice the average write duration: rejected clients
    /// are spread over the time it takes for the slots to turn over, instead of all returning
    /// at once. Capped so a very slow write cannot ask clients to stay away for minutes.
    fn retry_after_seconds(&self) -> u64 {
        let average_seconds = self
            .average_write_millis
            .load(Ordering::Relaxed)
            .div_ceil(1000);
        let upper = average_seconds
            .saturating_mul(2)
            .clamp(MIN_RETRY_AFTER_SECONDS + 1, MAX_RETRY_AFTER_SECONDS);
        fastrand::u64(MIN_RETRY_AFTER_SECONDS..=upper)
    }
}

/// Take a slot before the request body is read. When none is free, shed the load at once:
/// `503 Service Unavailable` with a randomised `Retry-After`. Nothing is queued, so latency
/// stays flat under overload and the rejected request costs almost nothing.
///
/// The `Retry-After` header is also the signal that the request was not processed at all, so
/// clients can safely resend it. Other `503` responses (storage unavailable) do not carry it.
pub async fn limit_concurrent_writes(
    State(limiter): State<WriteLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(_permit) = limiter.slots.clone().try_acquire_owned() else {
        let mut response = (
            StatusCode::SERVICE_UNAVAILABLE,
            "SensApp is busy writing data, retry later",
        )
            .into_response();
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from(limiter.retry_after_seconds()),
        );
        return response;
    };
    // The permit is held until the response is built, then released on drop.
    let started = Instant::now();
    let response = next.run(request).await;
    limiter.record_write(started.elapsed());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use std::collections::HashSet;
    use tower::ServiceExt;

    fn app(limiter: &WriteLimiter, handler_delay: Duration) -> Router {
        Router::new()
            .route(
                "/write",
                post(move || async move {
                    tokio::time::sleep(handler_delay).await;
                    "ok"
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                limiter.clone(),
                limit_concurrent_writes,
            ))
    }

    async fn write(app: Router) -> Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/write")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    fn retry_after(response: &Response) -> u64 {
        response.headers()[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn sampled_retry_afters(limiter: &WriteLimiter) -> HashSet<u64> {
        (0..500).map(|_| limiter.retry_after_seconds()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn sheds_excess_writes_immediately() {
        let started = Instant::now();
        let limiter = WriteLimiter::new(2);
        let app = app(&limiter, Duration::from_secs(10));
        let handles: Vec<_> = (0..5).map(|_| tokio::spawn(write(app.clone()))).collect();

        let mut ok = 0;
        let mut rejected = 0;
        for handle in handles {
            let response = handle.await.unwrap();
            match response.status() {
                StatusCode::OK => ok += 1,
                StatusCode::SERVICE_UNAVAILABLE => {
                    rejected += 1;
                    assert!(retry_after(&response) >= MIN_RETRY_AFTER_SECONDS);
                }
                other => panic!("unexpected status {other}"),
            }
        }
        assert_eq!((ok, rejected), (2, 3));
        // Rejections must not wait: only the two accepted writes take the handler's 10 s.
        assert_eq!(started.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_follows_the_observed_write_duration() {
        let limiter = WriteLimiter::new(1);
        let app = app(&limiter, Duration::from_secs(10));
        assert_eq!(write(app.clone()).await.status(), StatusCode::OK);

        // Writes now take about 10 s: rejected clients are spread over up to 20 s.
        let handles: Vec<_> = (0..40).map(|_| tokio::spawn(write(app.clone()))).collect();
        let mut delays = HashSet::new();
        for handle in handles {
            let response = handle.await.unwrap();
            if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                delays.insert(retry_after(&response));
            }
        }
        assert!(delays.len() > 1, "Retry-After must be jittered: {delays:?}");
        assert!(delays.iter().all(|delay| (1..=20).contains(delay)));
    }

    #[test]
    fn retry_after_is_small_before_any_write_was_seen() {
        let delays = sampled_retry_afters(&WriteLimiter::new(1));
        assert_eq!(delays, HashSet::from([1, 2]));
    }

    #[test]
    fn retry_after_grows_with_write_duration_and_is_capped() {
        let limiter = WriteLimiter::new(1);
        limiter.record_write(Duration::from_secs(4));
        let delays = sampled_retry_afters(&limiter);
        assert_eq!(delays.iter().min(), Some(&1));
        assert_eq!(delays.iter().max(), Some(&8));

        let limiter = WriteLimiter::new(1);
        limiter.record_write(Duration::from_secs(600));
        let delays = sampled_retry_afters(&limiter);
        assert_eq!(delays.iter().max(), Some(&MAX_RETRY_AFTER_SECONDS));
    }

    #[test]
    fn average_moves_gradually_toward_new_samples() {
        let limiter = WriteLimiter::new(1);
        limiter.record_write(Duration::from_secs(8));
        limiter.record_write(Duration::from_secs(0));
        // One fast write does not erase eight seconds of history.
        assert_eq!(limiter.average_write_millis.load(Ordering::Relaxed), 7000);
    }

    #[tokio::test(start_paused = true)]
    async fn slot_is_released_after_each_request() {
        let limiter = WriteLimiter::new(1);
        let app = app(&limiter, Duration::ZERO);
        for _ in 0..5 {
            assert_eq!(write(app.clone()).await.status(), StatusCode::OK);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn zero_disables_the_limit() {
        let limiter = WriteLimiter::new(0);
        let app = app(&limiter, Duration::from_secs(10));
        let handles: Vec<_> = (0..50).map(|_| tokio::spawn(write(app.clone()))).collect();

        for handle in handles {
            assert_eq!(handle.await.unwrap().status(), StatusCode::OK);
        }
    }
}
