//! The connection to the RRDCached daemon.
//!
//! One TCP or Unix socket stream is shared by all the requests: the protocol is a conversation of
//! lines, so a request writes its commands and reads all of its replies before the next one starts.
//! The stream is replaced when it breaks, and it is **dropped when a request is cancelled** (for
//! example by the HTTP timeout) in the middle of a conversation: the next request would read the
//! reply of the cancelled one otherwise.

use async_trait::async_trait;
use futures::future::BoxFuture;
use rrdcached_client::{
    RRDCachedClient, batch_update::BatchUpdate, consolidation_function::ConsolidationFunction,
    create::CreateArguments, errors::RRDCachedClientError, fetch::FetchResponse,
};
use std::{fmt::Debug, io, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::warn;

pub type ClientResult<T> = Result<T, RRDCachedClientError>;

/// A request that takes longer is abandoned and the connection is replaced.
pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The commands of the daemon that SensApp uses.
#[async_trait]
pub trait RRDCachedClientTrait: Send + Sync + Debug {
    async fn create(&mut self, args: CreateArguments) -> ClientResult<()>;
    async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> ClientResult<()>;
    async fn list(&mut self, recursive: bool, path: Option<&str>) -> ClientResult<Vec<String>>;
    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
    ) -> ClientResult<FetchResponse>;
    async fn last(&mut self, path: &str) -> ClientResult<usize>;
    async fn ping(&mut self) -> ClientResult<()>;
}

// The TCP and the Unix socket clients are the same type over two streams
macro_rules! impl_client_trait {
    ($stream:ty) => {
        #[async_trait]
        impl RRDCachedClientTrait for RRDCachedClient<$stream> {
            async fn create(&mut self, args: CreateArguments) -> ClientResult<()> {
                RRDCachedClient::create(self, args).await
            }

            async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> ClientResult<()> {
                RRDCachedClient::batch(self, batch_updates).await
            }

            async fn list(
                &mut self,
                recursive: bool,
                path: Option<&str>,
            ) -> ClientResult<Vec<String>> {
                RRDCachedClient::list(self, recursive, path).await
            }

            async fn fetch(
                &mut self,
                path: &str,
                consolidation_function: ConsolidationFunction,
                start: Option<i64>,
                end: Option<i64>,
            ) -> ClientResult<FetchResponse> {
                RRDCachedClient::fetch(self, path, consolidation_function, start, end, None).await
            }

            async fn last(&mut self, path: &str) -> ClientResult<usize> {
                RRDCachedClient::last(self, path).await
            }

            async fn ping(&mut self) -> ClientResult<()> {
                RRDCachedClient::ping(self).await
            }
        }
    };
}

impl_client_trait!(tokio::net::TcpStream);
impl_client_trait!(tokio::net::UnixStream);

#[async_trait]
pub trait RRDCachedConnectorTrait: Send + Sync + Debug {
    async fn connect(&self) -> ClientResult<Box<dyn RRDCachedClientTrait>>;
}

#[derive(Debug, Clone)]
pub enum RRDCachedConnectionTarget {
    Tcp { address: String },
    Unix { socket_path: String },
}

#[async_trait]
impl RRDCachedConnectorTrait for RRDCachedConnectionTarget {
    async fn connect(&self) -> ClientResult<Box<dyn RRDCachedClientTrait>> {
        let connecting = async {
            let client: Box<dyn RRDCachedClientTrait> = match self {
                Self::Tcp { address } => Box::new(RRDCachedClient::connect_tcp(address).await?),
                Self::Unix { socket_path } => {
                    Box::new(RRDCachedClient::connect_unix(socket_path).await?)
                }
            };
            Ok(client)
        };
        tokio::time::timeout(CONNECT_TIMEOUT, connecting)
            .await
            .unwrap_or_else(|_| Err(timed_out("connecting")))
    }
}

fn timed_out(what: &str) -> RRDCachedClientError {
    RRDCachedClientError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("timed out {what}"),
    ))
}

/// The errors after which the stream cannot be trusted: a broken socket, a timeout, or a reply
/// that was not what was asked (the conversation is out of step).
pub fn is_connection_error(error: &RRDCachedClientError) -> bool {
    matches!(
        error,
        RRDCachedClientError::Io(_) | RRDCachedClientError::Parsing(_)
    )
}

#[derive(Debug)]
pub struct Connection {
    connector: Arc<dyn RRDCachedConnectorTrait>,
    /// `None` while a request is using the client, and after a request that did not finish.
    client: Mutex<Option<Box<dyn RRDCachedClientTrait>>>,
}

impl Connection {
    /// Connects, so that a wrong address is an error at startup.
    pub async fn connect(connector: Arc<dyn RRDCachedConnectorTrait>) -> ClientResult<Self> {
        let client = connector.connect().await?;
        Ok(Self::with_client(connector, client))
    }

    pub fn with_client(
        connector: Arc<dyn RRDCachedConnectorTrait>,
        client: Box<dyn RRDCachedClientTrait>,
    ) -> Self {
        Self {
            connector,
            client: Mutex::new(Some(client)),
        }
    }

    /// Run a conversation with the daemon. After a connection error the connection is replaced
    /// and the conversation is run once more: a request must be safe to repeat (updates of a
    /// time that is stored already are refused by the daemon, and the caller ignores that).
    pub async fn run<T>(
        &self,
        operation: &str,
        mut action: impl for<'a> FnMut(
            &'a mut dyn RRDCachedClientTrait,
        ) -> BoxFuture<'a, ClientResult<T>>,
    ) -> ClientResult<T> {
        let mut retried = false;
        loop {
            let result = {
                let mut slot = self.client.lock().await;
                // Taken out of the slot: if this future is dropped before the end, the client
                // goes with it and the next request starts from a fresh connection.
                let mut client = match slot.take() {
                    Some(client) => client,
                    None => self.connector.connect().await?,
                };
                let result =
                    match tokio::time::timeout(OPERATION_TIMEOUT, action(client.as_mut())).await {
                        Ok(result) => result,
                        Err(_) => Err(timed_out(operation)),
                    };
                if !matches!(&result, Err(error) if is_connection_error(error)) {
                    *slot = Some(client);
                }
                result
            };

            match result {
                Err(error) if !retried && is_connection_error(&error) => {
                    warn!(
                        "RRDCached {} failed on the connection ({}); reconnecting and trying once more",
                        operation, error
                    );
                    retried = true;
                }
                result => return result,
            }
        }
    }
}
