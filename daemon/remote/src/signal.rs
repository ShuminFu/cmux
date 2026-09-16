//! Go-style "closed channel" signals built on crossbeam channels so they can
//! participate in `select!` alongside data channels.
//!
//! A [`Signal`] is a cloneable receiver that becomes permanently ready
//! (disconnected) once its [`SignalHandle`] fires. Firing is idempotent.

use std::sync::Mutex;

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};

pub struct SignalHandle {
    tx: Mutex<Option<Sender<()>>>,
}

#[derive(Clone)]
pub struct Signal {
    rx: Receiver<()>,
}

/// Create a signal pair. Nothing is ever sent on the channel; dropping the
/// sender is what "closes" it.
#[must_use]
pub fn signal() -> (SignalHandle, Signal) {
    let (tx, rx) = bounded::<()>(0);
    (SignalHandle { tx: Mutex::new(Some(tx)) }, Signal { rx })
}

impl SignalHandle {
    pub fn fire(&self) {
        self.tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
    }

    #[must_use]
    pub fn is_fired(&self) -> bool {
        self.tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none()
    }
}

impl Signal {
    #[must_use]
    pub fn is_fired(&self) -> bool {
        matches!(self.rx.try_recv(), Err(TryRecvError::Disconnected))
    }

    /// The underlying receiver, for use in `crossbeam_channel::select!`.
    #[must_use]
    pub fn receiver(&self) -> &Receiver<()> {
        &self.rx
    }

    /// Block until the signal fires.
    pub fn wait(&self) {
        let _ = self.rx.recv();
    }

    /// Block until the signal fires or `timeout` elapses; returns whether it fired.
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        matches!(
            self.rx.recv_timeout(timeout),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        )
    }
}

/// A signal that is already fired (a closed channel).
#[must_use]
pub fn fired() -> Signal {
    let (handle, sig) = signal();
    handle.fire();
    sig
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_once_and_is_observable_from_clones() {
        let (handle, sig) = signal();
        let other = sig.clone();
        assert!(!sig.is_fired());
        assert!(!other.wait_timeout(std::time::Duration::from_millis(10)));
        handle.fire();
        handle.fire();
        assert!(sig.is_fired());
        assert!(other.is_fired());
        assert!(handle.is_fired());
        other.wait();
        assert!(fired().is_fired());
    }
}
