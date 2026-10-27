// SPDX-License-Identifier: Apache-2.0

use flume::r#async::SendFut;
use flume::{Receiver, Sender};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Semaphore;

/// Optional second bound on a channel: the total weight (e.g. number of telemetry items) of the
/// queued entries. An entry's weight is capped at `max_weight`, so a single entry that is larger
/// than the limit can still be sent once the queue has drained.
struct WeightLimit<T> {
    permits: Arc<Semaphore>,
    max_weight: usize,
    weigh: fn(&T) -> usize,
}

impl<T> WeightLimit<T> {
    fn weight(&self, item: &T) -> u32 {
        (self.weigh)(item).min(self.max_weight) as u32
    }

    fn release(&self, item: &T) {
        self.permits.add_permits(self.weight(item) as usize);
    }
}

// Closes the weight limit's semaphore once every receiver (and stream) is gone, so that a sender
// waiting for room fails as it would on a full, disconnected channel instead of waiting forever.
struct ReceiversAlive(Arc<Semaphore>);

impl Drop for ReceiversAlive {
    fn drop(&mut self) {
        self.0.close();
    }
}

pub struct BoundedSender<T> {
    tx: Sender<T>,
    limit: Option<Arc<WeightLimit<T>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    Disconnected,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Disconnected => write!(f, "channel disconnected"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The channel is disconnected.
    Disconnected(T),
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "channel full"),
            TrySendError::Disconnected(_) => write!(f, "channel disconnected"),
        }
    }
}

impl<T> BoundedSender<T> {
    /// Send, waiting until the channel has room (and, for a weight-limited channel, until the
    /// item's weight fits within the limit).
    pub async fn send(&self, item: T) -> Result<(), SendError> {
        let permit = match &self.limit {
            None => None,
            Some(limit) => {
                let weight = limit.weight(&item);
                match limit.permits.clone().acquire_many_owned(weight).await {
                    Ok(permit) => Some(permit),
                    Err(_) => return Err(SendError::Disconnected),
                }
            }
        };

        match self.tx.send_async(item).await {
            Ok(()) => {
                // The receiver returns the permits when it takes the item off the queue
                if let Some(permit) = permit {
                    permit.forget();
                }
                Ok(())
            }
            Err(_e) => Err(SendError::Disconnected), // receiver closed
        }
    }

    /// Blocking send - blocks until there is capacity in the channel.
    /// Use this from non-async contexts (e.g., dedicated OS threads).
    /// Not supported on weight-limited channels.
    pub fn send_blocking(&self, item: T) -> Result<(), SendError> {
        debug_assert!(
            self.limit.is_none(),
            "send_blocking on a weight-limited channel"
        );
        match self.tx.send(item) {
            Ok(()) => Ok(()),
            Err(_e) => Err(SendError::Disconnected), // receiver closed
        }
    }

    /// Not supported on weight-limited channels, use `send` instead.
    pub fn send_async(&self, item: T) -> SendFut<'_, T> {
        debug_assert!(
            self.limit.is_none(),
            "send_async on a weight-limited channel"
        );
        self.tx.send_async(item)
    }

    /// Non-blocking send - returns immediately.
    /// Returns Ok if the item was sent, Err if the channel is full or disconnected.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let permit = match &self.limit {
            None => None,
            Some(limit) => match limit
                .permits
                .clone()
                .try_acquire_many_owned(limit.weight(&item))
            {
                Ok(permit) => Some(permit),
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    return Err(TrySendError::Full(item));
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(TrySendError::Disconnected(item));
                }
            },
        };

        match self.tx.try_send(item) {
            Ok(()) => {
                if let Some(permit) = permit {
                    permit.forget();
                }
                Ok(())
            }
            Err(flume::TrySendError::Full(item)) => Err(TrySendError::Full(item)),
            Err(flume::TrySendError::Disconnected(item)) => Err(TrySendError::Disconnected(item)),
        }
    }

    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx.is_empty()
    }
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        // this could be derived, but placeholder for now
        Self {
            tx: self.tx.clone(),
            limit: self.limit.clone(),
        }
    }
}

pub struct BoundedReceiver<T> {
    rx: Receiver<T>,
    limit: Option<Arc<WeightLimit<T>>>,
    _alive: Option<Arc<ReceiversAlive>>,
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            limit: self.limit.clone(),
            _alive: self._alive.clone(),
        }
    }
}

impl<T> BoundedReceiver<T> {
    fn received(&self, item: T) -> T {
        if let Some(limit) = &self.limit {
            limit.release(&item);
        }
        item
    }

    pub async fn next(&mut self) -> Option<T> {
        match self.rx.recv_async().await {
            Ok(item) => Some(self.received(item)),
            Err(_e) => None, // disconnected
        }
    }

    /// Blocking receive - blocks until an item is available.
    /// Use this from non-async contexts (e.g., dedicated OS threads).
    pub fn recv_blocking(&self) -> Option<T> {
        match self.rx.recv() {
            Ok(item) => Some(self.received(item)),
            Err(_e) => None, // disconnected
        }
    }

    /// Non-blocking receive - returns immediately.
    /// Returns None if no item is available or channel is disconnected.
    pub fn try_recv(&self) -> Option<T> {
        self.rx.try_recv().ok().map(|item| self.received(item))
    }

    /// Blocking receive with timeout - blocks until an item is available or timeout.
    /// Returns None if timeout expires or channel is disconnected.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<T> {
        self.rx
            .recv_timeout(timeout)
            .ok()
            .map(|item| self.received(item))
    }

    pub fn stream(&self) -> RecvStream<'_, T> {
        RecvStream {
            inner: self.rx.stream(),
            limit: self.limit.clone(),
            _alive: self._alive.clone(),
        }
    }

    pub fn into_stream<'a>(self) -> RecvStream<'a, T> {
        RecvStream {
            inner: self.rx.into_stream(),
            limit: self.limit,
            _alive: self._alive,
        }
    }

    pub fn len(&self) -> usize {
        self.rx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// Stream of received items, see `BoundedReceiver::stream`/`into_stream`.
pub struct RecvStream<'a, T> {
    inner: flume::r#async::RecvStream<'a, T>,
    limit: Option<Arc<WeightLimit<T>>>,
    _alive: Option<Arc<ReceiversAlive>>,
}

impl<T> Stream for RecvStream<'_, T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = Pin::new(&mut self.inner).poll_next(cx);
        if let (Poll::Ready(Some(item)), Some(limit)) = (&res, &self.limit) {
            limit.release(item);
        }
        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub fn bounded<T>(size: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let (tx, rx) = flume::bounded::<T>(size);

    let sender = BoundedSender { tx, limit: None };
    let receiver = BoundedReceiver {
        rx,
        limit: None,
        _alive: None,
    };

    (sender, receiver)
}

/// A channel bounded both by the number of entries (`size`) and, if `max_weight` is set, by the
/// total weight of the queued entries as computed by `weigh` (for example the number of
/// telemetry items in a batch). A single entry heavier than `max_weight` counts as `max_weight`.
/// Only `send`, `try_send` and the receive methods enforce the weight limit.
pub fn bounded_with_weight_limit<T>(
    size: usize,
    max_weight: Option<usize>,
    weigh: fn(&T) -> usize,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let (tx, rx) = flume::bounded::<T>(size);

    let limit = max_weight.map(|max_weight| {
        let max_weight = max_weight.clamp(1, u32::MAX as usize);
        Arc::new(WeightLimit {
            permits: Arc::new(Semaphore::new(max_weight)),
            max_weight,
            weigh,
        })
    });

    let alive = limit
        .as_ref()
        .map(|l| Arc::new(ReceiversAlive(l.permits.clone())));
    let sender = BoundedSender {
        tx,
        limit: limit.clone(),
    };
    let receiver = BoundedReceiver {
        rx,
        limit,
        _alive: alive,
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::{SendError, TrySendError, bounded, bounded_with_weight_limit};
    use futures::StreamExt;
    use tokio_test::{assert_ok, assert_pending, assert_ready, task::spawn};

    #[tokio::test]
    async fn basics() {
        let (tx, mut rx) = bounded(3);

        let msg = 10;

        // wrap futures
        let mut send1 = spawn(async { tx.send(msg).await });
        let mut recv1 = spawn(async { rx.next().await });

        // both asleep
        assert!(!send1.is_woken());
        assert!(!recv1.is_woken());

        // receiver should be in pending state
        assert_pending!(recv1.poll());

        assert_ok!(assert_ready!(send1.poll()));

        assert!(recv1.is_woken());

        assert_eq!(Some(msg), assert_ready!(recv1.poll()));

        drop(send1);
        drop(recv1);

        let mut recv2 = spawn(async { rx.next().await });

        drop(tx);
        // receives None since send channel was closed
        assert_eq!(None, assert_ready!(recv2.poll()));
    }

    #[tokio::test]
    async fn sender_blocks_on_full() {
        let (tx, mut rx) = bounded(1);

        let msg = 10;

        // wrap futures
        let mut send1 = spawn(async { tx.send(msg).await });
        let mut recv1 = spawn(async { rx.next().await });

        // receiver should be in pending state
        assert!(!recv1.is_woken());

        assert_ok!(assert_ready!(send1.poll()));

        drop(send1);
        let mut send2 = spawn(async { tx.send(msg).await });

        // Now blocks
        assert_pending!(send2.poll());

        assert_eq!(Some(msg), assert_ready!(recv1.poll()));

        // now this is ok
        assert_ok!(assert_ready!(send2.poll()));
    }

    #[tokio::test]
    async fn sender_fails_on_rx_close() {
        let (tx, rx) = bounded(1);

        let msg = 10;

        // wrap futures
        let mut send1 = spawn(async { tx.send(msg).await });

        drop(rx);
        assert_eq!(Err(SendError::Disconnected), assert_ready!(send1.poll()));
    }

    #[allow(clippy::ptr_arg)] // must match the channel's item type
    fn vec_len(v: &Vec<u32>) -> usize {
        v.len()
    }

    #[tokio::test]
    async fn weight_limit_blocks_until_received() {
        // Room for 10 entries, but only 5 items in total
        let (tx, mut rx) = bounded_with_weight_limit(10, Some(5), vec_len);

        assert_ok!(tx.send(vec![1, 2, 3]).await);

        // 3 + 3 > 5, must wait
        let mut send2 = spawn(async { tx.send(vec![4, 5, 6]).await });
        assert_pending!(send2.poll());

        assert_eq!(Some(vec![1, 2, 3]), rx.next().await);
        assert!(send2.is_woken());
        assert_ok!(assert_ready!(send2.poll()));
        assert_eq!(Some(vec![4, 5, 6]), rx.next().await);
    }

    #[tokio::test]
    async fn weight_limit_admits_oversized_entry_when_empty() {
        let (tx, mut rx) = bounded_with_weight_limit(10, Some(2), vec_len);

        // Heavier than the whole limit: counts as the limit, so it fits an empty queue
        assert_ok!(tx.send(vec![1, 2, 3, 4]).await);
        assert!(matches!(tx.try_send(vec![5]), Err(TrySendError::Full(_))));

        assert_eq!(Some(vec![1, 2, 3, 4]), rx.next().await);
        assert_ok!(tx.try_send(vec![5]));
    }

    #[tokio::test]
    async fn weight_limit_released_by_stream() {
        let (tx, rx) = bounded_with_weight_limit(10, Some(4), vec_len);
        let mut stream = rx.into_stream();

        assert_ok!(tx.send(vec![1, 2, 3]).await);
        assert!(matches!(
            tx.try_send(vec![4, 5]),
            Err(TrySendError::Full(_))
        ));

        assert_eq!(Some(vec![1, 2, 3]), stream.next().await);
        assert_ok!(tx.try_send(vec![4, 5]));
        assert_eq!(Some(vec![4, 5]), stream.next().await);
    }

    #[tokio::test]
    async fn entry_count_still_bounds_weight_limited_channel() {
        let (tx, mut rx) = bounded_with_weight_limit(1, Some(100), vec_len);

        assert_ok!(tx.send(vec![1]).await);
        let mut send2 = spawn(async { tx.send(vec![2]).await });
        assert_pending!(send2.poll());

        assert_eq!(Some(vec![1]), rx.next().await);
        assert_ok!(assert_ready!(send2.poll()));
    }

    #[tokio::test]
    async fn weight_limited_sender_waiting_fails_on_rx_close() {
        let (tx, rx) = bounded_with_weight_limit(10, Some(2), vec_len);

        assert_ok!(tx.send(vec![1, 2]).await);
        let mut send2 = spawn(async { tx.send(vec![3]).await });
        assert_pending!(send2.poll());

        drop(rx);
        assert_eq!(Err(SendError::Disconnected), assert_ready!(send2.poll()));
    }

    #[tokio::test]
    async fn weight_limited_sender_fails_on_rx_close() {
        let (tx, rx) = bounded_with_weight_limit(1, Some(10), vec_len);
        drop(rx);
        assert_eq!(Err(SendError::Disconnected), tx.send(vec![1]).await);
    }
}
