use rustc_hash::FxHashMap;
use std::sync::RwLock;

use serde::Deserialize;
use tracing::{debug, info, trace};

/// Object category parsed from the RabbitMQ `type` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CategoryObject {
    #[default]
    Unknown,
    Spectator,
    Aircraft,
    BotPilot,
    Engine,
    BotGunner,
    Bomb,
    Rocket,
    AircraftTurret,
    Mab,
    Paratrooper,
    RctContainer,
    Accelerator,
    Vehicle,
    VehicleCrew,
    VehicleTurret,
}

impl CategoryObject {
    pub fn parse(s: &str) -> Self {
        match s {
            "Spectator" => Self::Spectator,
            "aircraft" => Self::Aircraft,
            "BotPilot" => Self::BotPilot,
            "Engine" => Self::Engine,
            "BotGunner" => Self::BotGunner,
            "bomb" => Self::Bomb,
            "rocket" => Self::Rocket,
            "aircraft_turret" => Self::AircraftTurret,
            "MAB" => Self::Mab,
            "paratrooper" => Self::Paratrooper,
            "rct_container" => Self::RctContainer,
            "accelerator" => Self::Accelerator,
            "vehicle" => Self::Vehicle,
            "vehicle_crew" => Self::VehicleCrew,
            "vehicle_turret" => Self::VehicleTurret,
            _ => Self::Unknown,
        }
    }
}

/// Player state in the game
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlayerState {
    #[default]
    Lobby,
    Active,
}

#[derive(Debug, Clone, Default)]
pub struct PlayerPosition {
    pub id: i64,
    pub gamer_name: String,
    /// Coalition: 101 (allies), 201 (axis)
    pub country: i32,
    pub team_speak_id: Option<String>,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub state: PlayerState,
    pub category: CategoryObject,
    pub aircraft_type: String,
    pub aircraft_name: String,
}

impl PlayerPosition {
    /// True if the sender should use lobby routing (lobby players and spectators).
    pub fn is_lobby_routing(&self) -> bool {
        matches!(self.state, PlayerState::Lobby) || self.category == CategoryObject::Spectator
    }
}

/// RabbitMQ message format from Commander-IL2
#[derive(Debug, Deserialize)]
pub struct PlayerSession {
    pub event: String,
    pub id: i64,
    #[serde(rename = "gamerName")]
    pub gamer_name: String,
    pub country: i32,
    #[serde(rename = "teamSpeakId")]
    pub team_speak_id: Option<String>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub z: Option<f64>,
    #[serde(rename = "type")]
    pub aircraft_type: Option<String>,
    pub name: Option<String>,
    pub pid: Option<i64>,
}

/// Snapshot of all position dictionaries, suitable for routing decisions.
/// Keys are TS3 UIDs (teamSpeakId).
#[derive(Debug, Clone, Default)]
pub struct PositionSnapshot {
    pub lobby_allies: FxHashMap<String, PlayerPosition>,
    pub lobby_axis: FxHashMap<String, PlayerPosition>,
    pub active_allies: FxHashMap<String, PlayerPosition>,
    pub active_axis: FxHashMap<String, PlayerPosition>,
}

impl PositionSnapshot {
    fn get_lobby_dict(&self, country: i32) -> &FxHashMap<String, PlayerPosition> {
        if country == 101 { &self.lobby_allies } else { &self.lobby_axis }
    }

    fn get_active_dict(&self, country: i32) -> &FxHashMap<String, PlayerPosition> {
        if country == 101 { &self.active_allies } else { &self.active_axis }
    }

    /// Return all lobby recipients for the given coalition (broadcast).
    pub fn lobby_recipients(&self, country: i32) -> impl Iterator<Item = &PlayerPosition> {
        self.get_lobby_dict(country).values()
    }

    /// Return active players within `radius` meters of the given point for the given coalition.
    pub fn in_sphere(
        &self,
        country: i32,
        center_x: f64,
        center_y: f64,
        center_z: f64,
        radius: f64,
    ) -> impl Iterator<Item = &PlayerPosition> {
        let radius_sq = radius * radius;
        self.get_active_dict(country).values().filter(move |p| {
            let dx = p.x - center_x;
            let dy = p.y - center_y;
            let dz = p.z - center_z;
            dx * dx + dy * dy + dz * dz <= radius_sq
        })
    }

    /// Find a player by TS3 UID across all dictionaries.
    pub fn get_by_uid(&self, uid: &str) -> Option<&PlayerPosition> {
        for dict in [
            &self.lobby_allies,
            &self.lobby_axis,
            &self.active_allies,
            &self.active_axis,
        ] {
            if let Some(pos) = dict.get(uid) {
                return Some(pos);
            }
        }
        None
    }

    /// Collect all UID → PlayerPosition mappings (for TS3 client id resolution).
    pub fn to_uid_map(&self) -> FxHashMap<String, PlayerPosition> {
        let mut out = FxHashMap::default();
        for dict in [
            &self.lobby_allies,
            &self.lobby_axis,
            &self.active_allies,
            &self.active_axis,
        ] {
            for (uid, pos) in dict {
                out.insert(uid.clone(), pos.clone());
            }
        }
        out
    }
}

struct Inner {
    lobby_allies: FxHashMap<String, PlayerPosition>,
    lobby_axis: FxHashMap<String, PlayerPosition>,
    active_allies: FxHashMap<String, PlayerPosition>,
    active_axis: FxHashMap<String, PlayerPosition>,
}

/// Thread-safe storage for player positions, split by coalition and lobby/active state.
/// Keys inside each dictionary are TS3 UIDs (teamSpeakId).
pub struct PlayerPositionService {
    inner: RwLock<Inner>,
}

impl PlayerPositionService {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                lobby_allies: FxHashMap::default(),
                lobby_axis: FxHashMap::default(),
                active_allies: FxHashMap::default(),
                active_axis: FxHashMap::default(),
            }),
        }
    }

    pub fn handle_session(&self, session: PlayerSession) {
        // Ignore players without a TS3 UID: they cannot participate in radio routing.
        let uid = match session.team_speak_id {
            Some(ref uid) if !uid.trim().is_empty() => uid.clone(),
            _ => {
                trace!(
                    "RabbitMQ {} ignored: {} (id={}) has teamSpeakId=null",
                    session.event, session.gamer_name, session.id
                );
                return;
            }
        };

        let mut inner = self.inner.write().unwrap();
        match session.event.as_str() {
            "join" => Self::handle_join(&mut inner, uid, session),
            "spawn" => Self::handle_spawn(&mut inner, uid, session),
            "despawn" | "detach" => Self::move_to_lobby(&mut inner, uid, session),
            "position" => Self::handle_position(&mut inner, uid, session),
            "leave" => Self::handle_leave(&mut inner, uid),
            "clear" => Self::handle_clear(&mut inner),
            _ => {}
        }
    }

    pub fn snapshot(&self) -> PositionSnapshot {
        let inner = self.inner.read().unwrap();
        PositionSnapshot {
            lobby_allies: inner.lobby_allies.clone(),
            lobby_axis: inner.lobby_axis.clone(),
            active_allies: inner.active_allies.clone(),
            active_axis: inner.active_axis.clone(),
        }
    }

    pub fn get_by_uid(&self, uid: &str) -> Option<PlayerPosition> {
        let inner = self.inner.read().unwrap();
        inner.lobby_allies.get(uid)
            .or_else(|| inner.lobby_axis.get(uid))
            .or_else(|| inner.active_allies.get(uid))
            .or_else(|| inner.active_axis.get(uid))
            .cloned()
    }

    fn is_valid_coalition(country: i32) -> bool {
        country == 101 || country == 201
    }

    fn lobby_dict(inner: &mut Inner, country: i32) -> &mut FxHashMap<String, PlayerPosition> {
        if country == 101 { &mut inner.lobby_allies } else { &mut inner.lobby_axis }
    }

    fn active_dict(inner: &mut Inner, country: i32) -> &mut FxHashMap<String, PlayerPosition> {
        if country == 101 { &mut inner.active_allies } else { &mut inner.active_axis }
    }

    fn remove_from_all(inner: &mut Inner, uid: &str) {
        inner.lobby_allies.remove(uid);
        inner.lobby_axis.remove(uid);
        inner.active_allies.remove(uid);
        inner.active_axis.remove(uid);
    }

    fn create_position(session: &PlayerSession, uid: String, is_in_lobby: bool) -> PlayerPosition {
        let aircraft_type = session.aircraft_type.clone().unwrap_or_default();
        PlayerPosition {
            id: session.id,
            gamer_name: session.gamer_name.clone(),
            country: session.country,
            team_speak_id: Some(uid),
            x: session.x.unwrap_or(0.0),
            y: session.y.unwrap_or(0.0),
            z: session.z.unwrap_or(0.0),
            state: if is_in_lobby { PlayerState::Lobby } else { PlayerState::Active },
            category: CategoryObject::parse(&aircraft_type),
            aircraft_type,
            aircraft_name: session.name.clone().unwrap_or_default(),
        }
    }

    fn handle_join(inner: &mut Inner, uid: String, session: PlayerSession) {
        if !Self::is_valid_coalition(session.country) {
            debug!(
                "HandleJoin: ignoring invalid coalition {} for {}",
                session.country, session.gamer_name
            );
            return;
        }
        Self::remove_from_all(inner, &uid);
        let pos = Self::create_position(&session, uid.clone(), true);
        Self::lobby_dict(inner, session.country).insert(uid, pos);
        debug!(
            "RabbitMQ join: {} added to lobby (coalition={})",
            session.gamer_name, session.country
        );
    }

    fn handle_spawn(inner: &mut Inner, uid: String, session: PlayerSession) {
        if !Self::is_valid_coalition(session.country) {
            return;
        }
        // Remove from this coalition's lobby, then add to active
        Self::lobby_dict(inner, session.country).remove(&uid);
        let pos = Self::create_position(&session, uid.clone(), false);
        Self::active_dict(inner, session.country).insert(uid, pos);
        debug!(
            "RabbitMQ spawn: {} moved to active (coalition={})",
            session.gamer_name, session.country
        );
    }

    fn move_to_lobby(inner: &mut Inner, uid: String, session: PlayerSession) {
        if !Self::is_valid_coalition(session.country) {
            return;
        }
        let active = Self::active_dict(inner, session.country);
        if let Some(mut pos) = active.remove(&uid) {
            pos.state = PlayerState::Lobby;
            pos.x = 0.0;
            pos.y = 0.0;
            pos.z = 0.0;
            Self::lobby_dict(inner, session.country).insert(uid, pos);
            debug!(
                "RabbitMQ despawn/detach: {} moved from active to lobby",
                session.gamer_name
            );
        }
    }

    fn handle_position(inner: &mut Inner, uid: String, session: PlayerSession) {
        if !Self::is_valid_coalition(session.country) {
            return;
        }
        let active = Self::active_dict(inner, session.country);
        if let Some(existing) = active.get_mut(&uid) {
            existing.x = session.x.unwrap_or(existing.x);
            existing.y = session.y.unwrap_or(existing.y);
            existing.z = session.z.unwrap_or(existing.z);
            existing.team_speak_id = Some(uid);
            existing.country = session.country;
            existing.gamer_name = session.gamer_name.clone();
            existing.aircraft_type = session.aircraft_type.clone().unwrap_or_default();
            existing.aircraft_name = session.name.clone().unwrap_or_default();
            existing.category = CategoryObject::parse(&existing.aircraft_type);
            trace!(
                "RabbitMQ position: {} ({}, {}, {})",
                existing.gamer_name, existing.x, existing.y, existing.z
            );
        } else {
            // If the player is not yet active, treat position as a spawn.
            Self::handle_spawn(inner, uid, session);
        }
    }

    fn handle_leave(inner: &mut Inner, uid: String) {
        Self::remove_from_all(inner, &uid);
        debug!("RabbitMQ leave: uid={} removed from all lists", uid);
    }

    fn handle_clear(inner: &mut Inner) {
        inner.lobby_allies.clear();
        inner.lobby_axis.clear();
        inner.active_allies.clear();
        inner.active_axis.clear();
        info!("RabbitMQ clear: all players removed");
    }
}
