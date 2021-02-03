use std::time::Duration;

use tokio::time::sleep;

#[derive(Debug)]
pub struct Lease {
    id: i64,
    new_ttl: tokio::sync::mpsc::Sender<i64>,
    cancel: tokio::sync::oneshot::Sender<()>,
}

impl Lease {
    pub(super) fn new(id: i64, ttl: i64, timeout: tokio::sync::oneshot::Sender<()>) -> Self {
        let (tx_new_ttl, mut rx_new_ttl) = tokio::sync::mpsc::channel(1);
        let (cancel, mut should_cancel) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut ttl = ttl;
            loop {
                tokio::select! {
                    Some(new_ttl) = rx_new_ttl.recv() => ttl = new_ttl,
                    _ = &mut should_cancel => break,
                    _ = sleep(Duration::from_secs(ttl as u64)) => {let _ = timeout.send(());break},
                    else => break,
                };
            }
        });
        Self {
            id,
            new_ttl: tx_new_ttl,
            cancel,
        }
    }

    pub(super) fn refresh(&self, ttl: i64) {
        let _ = self.new_ttl.send(ttl);
    }

    pub(super) fn revoke(self) {
        let _ = self.cancel.send(());
    }
}
