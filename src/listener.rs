// SPDX-License-Identifier: Apache-2.0

use std::error::Error;
use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};

/// Represents a TCP socket that is both bound and configured for listening. This supports
/// both async and non-async (std) methods of binding the socket, allowing for these to
/// be constructed outside a tokio runtime if need be (e.g. during early init). Both versions
/// can be converted to an async TcpListenerStream.
///
/// Because it is a bit confusing when comparing these terms to the Berkeley socket model, creating
/// a Listener calls both:
///  1) bind()
///  2) listen()
/// Therefore, it is ready to accept() connections after success here.
//
pub struct Listener {
    inner: ListenerInner,
}

enum ListenerInner {
    Async(AsyncTcpListener),
    Std(StdTcpListener),
}

impl Listener {
    pub fn listen_std(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match StdTcpListener::bind(endpoint) {
            Ok(l) => Ok(Self {
                inner: ListenerInner::Std(l),
            }),
            Err(e) => Err(format!("unable to bind to {}: {}", endpoint, e).into()),
        }
    }

    pub async fn listen_async(endpoint: SocketAddr) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match AsyncTcpListener::bind(endpoint).await {
            Ok(l) => Ok(Self {
                inner: ListenerInner::Async(l),
            }),
            Err(e) => Err(format!("unable to find to {}: {}", endpoint, e).into()),
        }
    }

    pub fn bound_address(&self) -> Result<SocketAddr, Box<dyn Error + Send + Sync>> {
        Ok(match &self.inner {
            ListenerInner::Async(inner) => inner.local_addr()?,
            ListenerInner::Std(inner) => inner.local_addr()?,
        })
    }

    pub fn into_async(self) -> Result<AsyncTcpListener, Box<dyn Error + Send + Sync>> {
        match self.inner {
            ListenerInner::Async(inner) => Ok(inner),
            ListenerInner::Std(inner) => {
                // We convert this to async by setting non-blocking
                inner.set_nonblocking(true)?;

                let listener = AsyncTcpListener::from_std(inner)?;
                Ok(listener)
            }
        }
    }

    pub fn into_stream(self) -> Result<TcpListenerStream, Box<dyn Error + Send + Sync>> {
        let l = self.into_async()?;
        Ok(TcpListenerStream::new(l))
    }

    /// Like `into_stream`, but optionally disables Nagle's algorithm (TCP_NODELAY) on every
    /// accepted connection. Servers that are handed an incoming stream (e.g. tonic's
    /// `serve_with_incoming`) do not apply their own nodelay setting.
    pub fn into_stream_with_nodelay(
        self,
        nodelay: bool,
    ) -> Result<impl Stream<Item = std::io::Result<TcpStream>>, Box<dyn Error + Send + Sync>> {
        Ok(self.into_stream()?.map(move |conn| {
            conn.inspect(|stream| {
                if nodelay {
                    let _ = stream.set_nodelay(true);
                }
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn accepted_nodelay(nodelay: bool) -> bool {
        let listener = Listener::listen_async("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.bound_address().unwrap();
        let mut incoming = Box::pin(listener.into_stream_with_nodelay(nodelay).unwrap());

        let _client = TcpStream::connect(addr).await.unwrap();
        let accepted = incoming.next().await.unwrap().unwrap();
        accepted.nodelay().unwrap()
    }

    #[tokio::test]
    async fn stream_with_nodelay_sets_tcp_nodelay() {
        assert!(accepted_nodelay(true).await);
    }

    #[tokio::test]
    async fn stream_without_nodelay_keeps_default() {
        assert!(!accepted_nodelay(false).await);
    }
}
