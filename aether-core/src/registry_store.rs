use crate::AetherError;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum RegistryStatus {
    Unknown,
    Healthy,
    Unhealthy,
}

impl RegistryStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Unhealthy => "unhealthy",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "healthy" => Self::Healthy,
            "unhealthy" => Self::Unhealthy,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegistrationEntry {
    pub instance_id: String,
    pub name: String,
    pub http_url: String,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub registered_at: String,
    pub last_health_check: Option<String>,
    pub status: RegistryStatus,
}

#[derive(Clone)]
pub struct RegistryStore {
    conn: Arc<Mutex<Connection>>,
}

impl RegistryStore {
    pub fn open(path: &str) -> Result<Self, AetherError> {
        let conn = Connection::open(path).map_err(|e| AetherError::RegistryError {
            message: e.to_string(),
        })?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| AetherError::RegistryError {
                message: e.to_string(),
            })?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agents (
                instance_id       TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                http_url          TEXT NOT NULL UNIQUE,
                capabilities      TEXT NOT NULL DEFAULT '[]',
                metadata          TEXT NOT NULL DEFAULT '{}',
                registered_at     TEXT NOT NULL,
                last_health_check TEXT,
                status            TEXT NOT NULL DEFAULT 'unknown'
            );
            CREATE INDEX IF NOT EXISTS idx_agents_name ON agents(name);
            CREATE TABLE IF NOT EXISTS events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id   TEXT NOT NULL REFERENCES agents(instance_id) ON DELETE CASCADE,
                event_type    TEXT NOT NULL,
                payload       TEXT NOT NULL,
                received_at   TEXT NOT NULL
            );",
        )
        .map_err(|e| AetherError::RegistryError {
            message: e.to_string(),
        })?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Test-only: open a fresh SQLite registry backed by a unique temp file.
    #[cfg(test)]
    pub(crate) fn open_temp() -> Self {
        Self::open(crate::temp_db_path("aether-registry").to_str().expect("utf8 temp path"))
            .expect("open temp registry store")
    }

    pub async fn register(&self, entry: RegistrationEntry) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let caps = serde_json::to_string(&entry.capabilities).unwrap_or_else(|_| "[]".to_string());
        let meta = serde_json::to_string(&entry.metadata).unwrap_or_else(|_| "{}".to_string());
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            // Same URL re-registration: remove old row first
            conn.execute(
                "DELETE FROM agents WHERE http_url = ?1 AND instance_id != ?2",
                params![entry.http_url, entry.instance_id],
            )
            .ok();
            conn.execute(
                "INSERT OR REPLACE INTO agents
                 (instance_id, name, http_url, capabilities, metadata, registered_at, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'unknown')",
                params![
                    entry.instance_id,
                    entry.name,
                    entry.http_url,
                    caps,
                    meta,
                    entry.registered_at
                ],
            )
            .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| AetherError::RegistryError {
            message: e.to_string(),
        })?
        .map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    pub async fn deregister(&self, instance_id: &str) -> Result<bool, AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let affected = tokio::task::spawn_blocking(move || {
            conn.lock()
                .unwrap_or_else(|e| e.into_inner())
                .execute("DELETE FROM agents WHERE instance_id = ?1", params![id])
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| AetherError::RegistryError {
            message: e.to_string(),
        })?
        .map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(affected > 0)
    }

    pub async fn update_health(
        &self,
        instance_id: &str,
        status: RegistryStatus,
        timestamp: &str,
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let ts = timestamp.to_string();
        let st = status.as_str().to_string();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .unwrap_or_else(|e| e.into_inner())
                .execute(
                    "UPDATE agents SET status = ?1, last_health_check = ?2 WHERE instance_id = ?3",
                    params![st, ts, id],
                )
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| AetherError::RegistryError {
            message: e.to_string(),
        })?
        .map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    pub async fn list_all(&self) -> Result<Vec<RegistrationEntry>, AetherError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = conn.prepare(
                "SELECT instance_id, name, http_url, capabilities, metadata, registered_at, last_health_check, status FROM agents"
            ).map_err(|e| e.to_string())?;
            Self::collect_entries(&mut stmt, [])
        }).await.map_err(|e| AetherError::RegistryError { message: e.to_string() })?.map_err(|e| AetherError::RegistryError { message: e })
    }

    pub async fn list_by_name(&self, name: &str) -> Result<Vec<RegistrationEntry>, AetherError> {
        let conn = Arc::clone(&self.conn);
        let n = name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = conn.prepare(
                "SELECT instance_id, name, http_url, capabilities, metadata, registered_at, last_health_check, status
                 FROM agents WHERE name = ?1"
            ).map_err(|e| e.to_string())?;
            Self::collect_entries(&mut stmt, params![n])
        }).await.map_err(|e| AetherError::RegistryError { message: e.to_string() })?.map_err(|e| AetherError::RegistryError { message: e })
    }

    pub async fn add_event(
        &self,
        instance_id: &str,
        event_type: &str,
        payload: &str,
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let id = instance_id.to_string();
        let et = event_type.to_string();
        let pl = payload.to_string();
        let now = chrono::Utc::now().to_rfc3339();
        tokio::task::spawn_blocking(move || {
            conn.lock().unwrap_or_else(|e| e.into_inner())
                .execute(
                    "INSERT INTO events (instance_id, event_type, payload, received_at) VALUES (?1, ?2, ?3, ?4)",
                    params![id, et, pl, now],
                )
                .map_err(|e| e.to_string())
        }).await.map_err(|e| AetherError::RegistryError { message: e.to_string() })?.map_err(|e| AetherError::RegistryError { message: e })?;
        Ok(())
    }

    fn collect_entries(
        stmt: &mut rusqlite::Statement<'_>,
        params: impl rusqlite::Params,
    ) -> Result<Vec<RegistrationEntry>, String> {
        let entries = stmt
            .query_map(params, |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .map(
                |(iid, name, url, caps_str, meta_str, reg_at, lhc, status_str)| RegistrationEntry {
                    instance_id: iid,
                    name,
                    http_url: url,
                    capabilities: serde_json::from_str(&caps_str).unwrap_or_default(),
                    metadata: serde_json::from_str(&meta_str).unwrap_or_default(),
                    registered_at: reg_at,
                    last_health_check: lhc,
                    status: RegistryStatus::parse(&status_str),
                },
            )
            .collect::<Vec<_>>();
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_list() {
        let store = RegistryStore::open_temp();
        let entry = RegistrationEntry {
            instance_id: "inst-1".to_string(),
            name: "calc".to_string(),
            http_url: "http://127.0.0.1:8080".to_string(),
            capabilities: vec!["calculate".to_string()],
            metadata: HashMap::new(),
            registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        };
        store.register(entry).await.unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "calc");
        assert_eq!(all[0].status, RegistryStatus::Unknown);
    }

    #[tokio::test]
    async fn deregister_removes_entry() {
        let store = RegistryStore::open_temp();
        let entry = RegistrationEntry {
            instance_id: "inst-2".to_string(),
            name: "calc".to_string(),
            http_url: "http://127.0.0.1:8081".to_string(),
            capabilities: vec![],
            metadata: HashMap::new(),
            registered_at: "2026-05-21T00:00:00Z".to_string(),
            last_health_check: None,
            status: RegistryStatus::Unknown,
        };
        store.register(entry).await.unwrap();
        let removed = store.deregister("inst-2").await.unwrap();
        assert!(removed);
        assert_eq!(store.list_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn same_url_reregister_replaces_instance() {
        let store = RegistryStore::open_temp();
        let url = "http://127.0.0.1:9000";
        store
            .register(RegistrationEntry {
                instance_id: "old-id".to_string(),
                name: "a".to_string(),
                http_url: url.to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: "2026-05-21T00:00:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        store
            .register(RegistrationEntry {
                instance_id: "new-id".to_string(),
                name: "a".to_string(),
                http_url: url.to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: "2026-05-21T00:01:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].instance_id, "new-id");
    }

    #[tokio::test]
    async fn update_health_changes_status() {
        let store = RegistryStore::open_temp();
        store
            .register(RegistrationEntry {
                instance_id: "inst-3".to_string(),
                name: "x".to_string(),
                http_url: "http://127.0.0.1:9001".to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: "2026-05-21T00:00:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        store
            .update_health("inst-3", RegistryStatus::Healthy, "2026-05-21T00:01:00Z")
            .await
            .unwrap();
        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Healthy);
        assert_eq!(
            all[0].last_health_check.as_deref(),
            Some("2026-05-21T00:01:00Z")
        );
    }

    #[tokio::test]
    async fn list_by_name_filters_correctly() {
        let store = RegistryStore::open_temp();
        store
            .register(RegistrationEntry {
                instance_id: "a1".to_string(),
                name: "calc".to_string(),
                http_url: "http://127.0.0.1:9010".to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: "2026-05-21T00:00:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        store
            .register(RegistrationEntry {
                instance_id: "b1".to_string(),
                name: "writer".to_string(),
                http_url: "http://127.0.0.1:9011".to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: "2026-05-21T00:00:00Z".to_string(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        let calcs = store.list_by_name("calc").await.unwrap();
        assert_eq!(calcs.len(), 1);
        assert_eq!(calcs[0].instance_id, "a1");
    }
}
