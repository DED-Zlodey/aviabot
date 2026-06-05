use rustc_hash::FxHashMap;
use std::sync::RwLock;

/// Info about a TS3 client, obtained from `clientlist -uid`.
#[derive(Debug, Clone)]
pub struct Ts3ClientInfo {
    pub client_id: u16,
    pub channel_id: u64,
    pub name: String,
    pub uid: Option<String>,
}

/// Thread-safe cache of the full TS3 client list.
pub struct Ts3ClientList {
    inner: RwLock<FxHashMap<u16, Ts3ClientInfo>>,
}

impl Ts3ClientList {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(FxHashMap::default()),
        }
    }

    pub fn update(&self, clients: Vec<Ts3ClientInfo>) {
        let mut map = FxHashMap::default();
        map.reserve(clients.len());
        for c in clients {
            map.insert(c.client_id, c);
        }
        let mut inner = self.inner.write().unwrap();
        *inner = map;
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

    pub fn get_all(&self) -> FxHashMap<u16, Ts3ClientInfo> {
        self.inner.read().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}
