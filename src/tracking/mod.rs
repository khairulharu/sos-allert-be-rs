//! Live-tracking fan-out: one broadcast channel per alert, plus the
//! connection budgets the Go service enforces in `WSConnectionLimiter`.
//!
//! * global connection cap (`sos.ws_max_connections`)
//! * per-IP cap before the upgrade (`sos.ws_max_per_ip`)
//! * per-user cap after the in-band auth (`sos.ws_max_per_user`)

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::usecase::sos::TrackHub;

/// 500 global connections by default.
pub const DEFAULT_MAX_CONNECTIONS: usize = 500;
pub const DEFAULT_MAX_PER_IP: usize = 10;
pub const DEFAULT_MAX_PER_USER: usize = 3;

/// Broadcast buffer per alert; a slow client loses messages, it does not block
/// the publisher.
const CHANNEL_CAPACITY: usize = 64;

pub struct Hub {
    channels: Mutex<HashMap<String, broadcast::Sender<Vec<u8>>>>,
    connections: AtomicUsize,
    per_ip: Mutex<HashMap<String, usize>>,
    per_user: Mutex<HashMap<String, usize>>,
    max_connections: usize,
    max_per_ip: usize,
    max_per_user: usize,
}

impl Hub {
    pub fn new(max_connections: usize, max_per_ip: usize, max_per_user: usize) -> Arc<Self> {
        Arc::new(Self {
            channels: Mutex::new(HashMap::new()),
            connections: AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::new()),
            per_user: Mutex::new(HashMap::new()),
            max_connections: max_connections.max(1),
            max_per_ip: max_per_ip.max(1),
            max_per_user: max_per_user.max(1),
        })
    }

    /// Reserve a slot for a new socket. `None` means the cap was hit and the
    /// upgrade should answer `429`.
    pub fn acquire(self: &Arc<Self>, ip: &str) -> Option<ConnectionPermit> {
        let current = self.connections.load(Ordering::SeqCst);
        if current >= self.max_connections {
            return None;
        }
        let mut per_ip = self.per_ip.lock().expect("hub poisoned");
        let count = per_ip.entry(ip.to_owned()).or_insert(0);
        if *count >= self.max_per_ip {
            return None;
        }
        *count += 1;
        self.connections.fetch_add(1, Ordering::SeqCst);

        Some(ConnectionPermit {
            hub: self.clone(),
            ip: ip.to_owned(),
            user: None,
        })
    }

    pub fn subscribe(&self, alert_id: &str) -> broadcast::Receiver<Vec<u8>> {
        let mut channels = self.channels.lock().expect("hub poisoned");
        channels
            .entry(alert_id.to_owned())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }

    fn user_slots_available(&self, user_id: &str) -> bool {
        let per_user = self.per_user.lock().expect("hub poisoned");
        per_user.get(user_id).copied().unwrap_or(0) < self.max_per_user
    }
}

impl TrackHub for Hub {
    fn publish(&self, alert_id: &str, payload: Vec<u8>) {
        let sender = {
            let mut channels = self.channels.lock().expect("hub poisoned");
            channels
                .entry(alert_id.to_owned())
                .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
                .clone()
        };
        // No subscribers is fine: the alert has no open tracker.
        let _ = sender.send(payload);
    }
}

/// Holds one global + one per-IP slot, and optionally a per-user slot.
pub struct ConnectionPermit {
    hub: Arc<Hub>,
    ip: String,
    user: Option<String>,
}

impl ConnectionPermit {
    /// Claim the per-user budget once the in-band auth told us who this is.
    /// `false` means the user already holds the maximum sockets.
    pub fn claim_user(&mut self, user_id: &str) -> bool {
        if !self.hub.user_slots_available(user_id) {
            return false;
        }
        let mut per_user = self.hub.per_user.lock().expect("hub poisoned");
        *per_user.entry(user_id.to_owned()).or_insert(0) += 1;
        self.user = Some(user_id.to_owned());
        true
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.hub.connections.fetch_sub(1, Ordering::SeqCst);
        {
            let mut per_ip = self.hub.per_ip.lock().expect("hub poisoned");
            if let Some(count) = per_ip.get_mut(&self.ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    per_ip.remove(&self.ip);
                }
            }
        }
        if let Some(user) = &self.user {
            let mut per_user = self.hub.per_user.lock().expect("hub poisoned");
            if let Some(count) = per_user.get_mut(user) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    per_user.remove(user);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
