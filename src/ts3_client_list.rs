use rustc_hash::FxHashMap;
use std::sync::RwLock;

/// Info about a TS3 client, obtained from `clientgetids`.
#[derive(Debug, Clone)]
pub struct Ts3ClientInfo {
    pub client_id: u16,
    pub uid: Option<String>,
}

/// Thread-safe cache of resolved TS3 UID -> client_id mappings.
pub struct Ts3ClientList {
    inner: RwLock<FxHashMap<u16, Ts3ClientInfo>>,
}

impl Ts3ClientList {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(FxHashMap::default()),
        }
    }

    pub fn insert_or_update(&self, client: Ts3ClientInfo) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(client.client_id, client);
    }

    /// Build uid -> client_id map for routing (only clients with known UID).
    pub fn uid_to_client_id(&self) -> FxHashMap<String, u16> {
        let inner = self.inner.read().unwrap();
        inner
            .values()
            .filter_map(|c| c.uid.as_ref().map(|uid| (uid.clone(), c.client_id)))
            .collect()
    }

    pub fn remove(&self, client_id: u16) {
        let mut inner = self.inner.write().unwrap();
        inner.remove(&client_id);
    }
}
