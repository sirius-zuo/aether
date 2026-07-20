use crate::AetherError;
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStatus {
    Running,
    Suspended,
    Succeeded,
    Failed,
}

impl ExecutionStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "suspended" => Self::Suspended,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            _ => Self::Running,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeStatus {
    Pending,
    Running,
    Done,
    Suspended,
    Failed,
}

impl NodeStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Suspended => "suspended",
            Self::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "running" => Self::Running,
            "done" => Self::Done,
            "suspended" => Self::Suspended,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionRecord {
    pub workflow_id: String,
    pub status: ExecutionStatus,
    pub workflow_spec: String,
    pub initial_payload: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionNodeRecord {
    pub workflow_id: String,
    pub node_id: String,
    pub status: NodeStatus,
    pub output: Option<String>,
    pub session_id: Option<String>,
    pub approval_id: Option<String>,
    pub kind: Option<String>,
    pub prompt: Option<String>,
    pub gate_deadline: Option<String>,
    pub updated_at: String,
}

#[derive(Clone)]
pub struct ExecutionStore {
    conn: Arc<Mutex<Connection>>,
}

fn store_err(e: impl ToString) -> AetherError {
    AetherError::WorkflowError {
        message: e.to_string(),
    }
}

impl ExecutionStore {
    pub fn open(path: &str) -> Result<Self, AetherError> {
        let conn = Connection::open(path).map_err(store_err)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(store_err)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS executions (
                workflow_id     TEXT PRIMARY KEY,
                status          TEXT NOT NULL DEFAULT 'running',
                workflow_spec   TEXT NOT NULL,
                initial_payload TEXT NOT NULL,
                result          TEXT,
                error           TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS execution_nodes (
                workflow_id   TEXT NOT NULL REFERENCES executions(workflow_id) ON DELETE CASCADE,
                node_id       TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'pending',
                output        TEXT,
                session_id    TEXT,
                approval_id   TEXT,
                kind          TEXT,
                prompt        TEXT,
                gate_deadline TEXT,
                updated_at    TEXT NOT NULL,
                PRIMARY KEY (workflow_id, node_id)
            );",
        )
        .map_err(store_err)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Test-only: open a fresh SQLite store backed by a unique temp file.
    /// Durability tests use real files (never `:memory:`) so drop+reopen can
    /// exercise a true restart.
    #[cfg(test)]
    pub(crate) fn open_temp() -> Self {
        Self::open(
            crate::temp_db_path("aether-exec")
                .to_str()
                .expect("utf8 temp path"),
        )
        .expect("open temp execution store")
    }

    pub async fn create_execution(
        &self,
        workflow_id: &str,
        workflow_spec: &str,
        initial_payload: &str,
        node_ids: &[String],
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        let wid = workflow_id.to_string();
        let spec = workflow_spec.to_string();
        let payload = initial_payload.to_string();
        let nodes = node_ids.to_vec();
        let now = chrono::Utc::now().to_rfc3339();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            tx.execute(
                "INSERT INTO executions
                 (workflow_id, status, workflow_spec, initial_payload, created_at, updated_at)
                 VALUES (?1, 'running', ?2, ?3, ?4, ?4)",
                params![wid, spec, payload, now],
            )
            .map_err(|e| e.to_string())?;
            for node_id in &nodes {
                tx.execute(
                    "INSERT INTO execution_nodes (workflow_id, node_id, status, updated_at)
                     VALUES (?1, ?2, 'pending', ?3)",
                    params![wid, node_id, now],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())
        })
        .await
        .map_err(store_err)?
        .map_err(store_err)
    }

    pub async fn load_execution(
        &self,
        workflow_id: &str,
    ) -> Result<Option<(ExecutionRecord, Vec<ExecutionNodeRecord>)>, AetherError> {
        let conn = Arc::clone(&self.conn);
        let wid = workflow_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let exec = conn
                .query_row(
                    "SELECT workflow_id, status, workflow_spec, initial_payload, result, error, created_at, updated_at
                     FROM executions WHERE workflow_id = ?1",
                    params![wid],
                    |row| {
                        Ok(ExecutionRecord {
                            workflow_id: row.get(0)?,
                            status: ExecutionStatus::parse(&row.get::<_, String>(1)?),
                            workflow_spec: row.get(2)?,
                            initial_payload: row.get(3)?,
                            result: row.get(4)?,
                            error: row.get(5)?,
                            created_at: row.get(6)?,
                            updated_at: row.get(7)?,
                        })
                    },
                )
                .ok();
            let Some(exec) = exec else { return Ok::<_, String>(None) };
            let mut stmt = conn
                .prepare(
                    "SELECT workflow_id, node_id, status, output, session_id, approval_id, kind, prompt, gate_deadline, updated_at
                     FROM execution_nodes WHERE workflow_id = ?1",
                )
                .map_err(|e| e.to_string())?;
            let nodes = stmt
                .query_map(params![wid], |row| {
                    Ok(ExecutionNodeRecord {
                        workflow_id: row.get(0)?,
                        node_id: row.get(1)?,
                        status: NodeStatus::parse(&row.get::<_, String>(2)?),
                        output: row.get(3)?,
                        session_id: row.get(4)?,
                        approval_id: row.get(5)?,
                        kind: row.get(6)?,
                        prompt: row.get(7)?,
                        gate_deadline: row.get(8)?,
                        updated_at: row.get(9)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(Some((exec, nodes)))
        })
        .await
        .map_err(store_err)?
        .map_err(store_err)
    }

    pub async fn list_active(&self) -> Result<Vec<ExecutionRecord>, AetherError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = conn
                .prepare(
                    "SELECT workflow_id, status, workflow_spec, initial_payload, result, error, created_at, updated_at
                     FROM executions WHERE status IN ('running','suspended')",
                )
                .map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(ExecutionRecord {
                        workflow_id: row.get(0)?,
                        status: ExecutionStatus::parse(&row.get::<_, String>(1)?),
                        workflow_spec: row.get(2)?,
                        initial_payload: row.get(3)?,
                        result: row.get(4)?,
                        error: row.get(5)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok::<_, String>(rows)
        })
        .await
        .map_err(store_err)?
        .map_err(store_err)
    }

    async fn exec_write(
        &self,
        sql: &'static str,
        args: Vec<Option<String>>,
    ) -> Result<(), AetherError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let params: Vec<&dyn rusqlite::ToSql> =
                args.iter().map(|a| a as &dyn rusqlite::ToSql).collect();
            conn.execute(sql, params.as_slice())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(store_err)?
        .map_err(store_err)?;
        Ok(())
    }

    pub async fn mark_node_running(
        &self,
        workflow_id: &str,
        node_id: &str,
    ) -> Result<(), AetherError> {
        let now = chrono::Utc::now().to_rfc3339();
        self.exec_write(
            "UPDATE execution_nodes SET status='running', updated_at=?3
             WHERE workflow_id=?1 AND node_id=?2",
            vec![Some(workflow_id.into()), Some(node_id.into()), Some(now)],
        )
        .await
    }

    pub async fn complete_node(
        &self,
        workflow_id: &str,
        node_id: &str,
        output_json: &str,
    ) -> Result<(), AetherError> {
        let now = chrono::Utc::now().to_rfc3339();
        self.exec_write(
            "UPDATE execution_nodes
             SET status='done', output=?3, session_id=NULL, approval_id=NULL,
                 kind=NULL, prompt=NULL, gate_deadline=NULL, updated_at=?4
             WHERE workflow_id=?1 AND node_id=?2",
            vec![
                Some(workflow_id.into()),
                Some(node_id.into()),
                Some(output_json.into()),
                Some(now),
            ],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn park_node(
        &self,
        workflow_id: &str,
        node_id: &str,
        session_id: &str,
        approval_id: &str,
        kind: &str,
        prompt: &str,
        gate_deadline: Option<&str>,
    ) -> Result<(), AetherError> {
        let now = chrono::Utc::now().to_rfc3339();
        self.exec_write(
            "UPDATE execution_nodes
             SET status='suspended', session_id=?3, approval_id=?4, kind=?5,
                 prompt=?6, gate_deadline=?7, updated_at=?8
             WHERE workflow_id=?1 AND node_id=?2",
            vec![
                Some(workflow_id.into()),
                Some(node_id.into()),
                Some(session_id.into()),
                Some(approval_id.into()),
                Some(kind.into()),
                Some(prompt.into()),
                gate_deadline.map(|s| s.to_string()),
                Some(now),
            ],
        )
        .await
    }

    pub async fn finish_execution(
        &self,
        workflow_id: &str,
        status: ExecutionStatus,
        result: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), AetherError> {
        let now = chrono::Utc::now().to_rfc3339();
        self.exec_write(
            "UPDATE executions SET status=?2, result=?3, error=?4, updated_at=?5
             WHERE workflow_id=?1",
            vec![
                Some(workflow_id.into()),
                Some(status.as_str().to_string()),
                result.map(|s| s.to_string()),
                error.map(|s| s.to_string()),
                Some(now),
            ],
        )
        .await
    }

    pub async fn expire_gates(
        &self,
        now_rfc3339: &str,
    ) -> Result<Vec<(String, String)>, AetherError> {
        let conn = Arc::clone(&self.conn);
        let now = now_rfc3339.to_string();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let expired: Vec<(String, String)> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT workflow_id, node_id FROM execution_nodes
                         WHERE status='suspended' AND gate_deadline IS NOT NULL AND gate_deadline < ?1",
                    )
                    .map_err(|e| e.to_string())?;
                let rows = stmt.query_map(params![&now], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
                    .map_err(|e| e.to_string())?
                    .filter_map(|r| r.ok())
                    .collect();
                rows
            };
            for (wid, nid) in &expired {
                tx.execute(
                    "UPDATE execution_nodes SET status='failed', updated_at=?3
                     WHERE workflow_id=?1 AND node_id=?2",
                    params![wid, nid, &now],
                )
                .map_err(|e| e.to_string())?;
                tx.execute(
                    "UPDATE executions SET status='failed', error='gate deadline expired', updated_at=?2
                     WHERE workflow_id=?1",
                    params![wid, &now],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
            Ok::<_, String>(expired)
        })
        .await
        .map_err(store_err)?
        .map_err(store_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reopen_file_preserves_execution_state() {
        let path = crate::temp_db_path("aether-exec-reopen");
        let p = path.to_str().unwrap();
        {
            let store = ExecutionStore::open(p).unwrap();
            store
                .create_execution("wf", "{}", "{}", &["a".into(), "b".into()])
                .await
                .unwrap();
            store.complete_node("wf", "a", r#"{"v":1}"#).await.unwrap();
        } // store dropped -> connection closed, simulating a restart

        let store = ExecutionStore::open(p).unwrap();
        let (exec, nodes) = store.load_execution("wf").await.unwrap().unwrap();
        assert_eq!(exec.status, ExecutionStatus::Running);
        let a = nodes.iter().find(|n| n.node_id == "a").unwrap();
        assert_eq!(a.status, NodeStatus::Done);
        assert_eq!(a.output.as_deref(), Some(r#"{"v":1}"#));
    }

    #[tokio::test]
    async fn create_then_load_execution() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution(
                "wf-1",
                r#"{"entries":["a"],"edges":[{"from":"a","to":"b"}]}"#,
                r#"{"msg":"hi"}"#,
                &["a".to_string(), "b".to_string()],
            )
            .await
            .unwrap();

        let (exec, nodes) = store.load_execution("wf-1").await.unwrap().unwrap();
        assert_eq!(exec.status, ExecutionStatus::Running);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|n| n.status == NodeStatus::Pending));
    }

    #[tokio::test]
    async fn list_active_returns_running_execution() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf-2", "{}", "{}", &["a".to_string()])
            .await
            .unwrap();
        let active = store.list_active().await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].workflow_id, "wf-2");
    }

    #[tokio::test]
    async fn load_missing_execution_returns_none() {
        let store = ExecutionStore::open_temp();
        assert!(store.load_execution("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn complete_node_sets_done_and_output() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into()])
            .await
            .unwrap();
        store.mark_node_running("wf", "a").await.unwrap();
        store.complete_node("wf", "a", r#"{"x":1}"#).await.unwrap();

        let (_e, nodes) = store.load_execution("wf").await.unwrap().unwrap();
        let a = nodes.iter().find(|n| n.node_id == "a").unwrap();
        assert_eq!(a.status, NodeStatus::Done);
        assert_eq!(a.output.as_deref(), Some(r#"{"x":1}"#));
    }

    #[tokio::test]
    async fn park_node_records_correlation() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into()])
            .await
            .unwrap();
        store
            .park_node("wf", "a", "s1", "ap1", "phase_gate", "Sign off?", None)
            .await
            .unwrap();

        let (_e, nodes) = store.load_execution("wf").await.unwrap().unwrap();
        let a = nodes.iter().find(|n| n.node_id == "a").unwrap();
        assert_eq!(a.status, NodeStatus::Suspended);
        assert_eq!(a.session_id.as_deref(), Some("s1"));
        assert_eq!(a.approval_id.as_deref(), Some("ap1"));
        assert_eq!(a.prompt.as_deref(), Some("Sign off?"));
    }

    #[tokio::test]
    async fn complete_node_clears_correlation_after_resume() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into()])
            .await
            .unwrap();
        store
            .park_node(
                "wf",
                "a",
                "s1",
                "ap1",
                "tool_approval",
                "ok?",
                Some("2026-01-01T00:00:00+00:00"),
            )
            .await
            .unwrap();
        store
            .complete_node("wf", "a", r#"{"done":true}"#)
            .await
            .unwrap();

        let (_e, nodes) = store.load_execution("wf").await.unwrap().unwrap();
        let a = nodes.iter().find(|n| n.node_id == "a").unwrap();
        assert_eq!(a.status, NodeStatus::Done);
        assert!(a.session_id.is_none());
        assert!(a.approval_id.is_none());
        assert!(a.kind.is_none());
        assert!(a.prompt.is_none());
        assert!(a.gate_deadline.is_none());
    }

    #[tokio::test]
    async fn finish_execution_sets_status_and_result() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into()])
            .await
            .unwrap();
        store
            .finish_execution("wf", ExecutionStatus::Succeeded, Some(r#"{"r":1}"#), None)
            .await
            .unwrap();

        let (exec, _n) = store.load_execution("wf").await.unwrap().unwrap();
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        assert_eq!(exec.result.as_deref(), Some(r#"{"r":1}"#));
    }

    #[tokio::test]
    async fn expire_gates_fails_past_deadline_nodes() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into()])
            .await
            .unwrap();
        store
            .park_node(
                "wf",
                "a",
                "s1",
                "a1",
                "phase_gate",
                "ok?",
                Some("2000-01-01T00:00:00+00:00"),
            )
            .await
            .unwrap();

        let expired = store
            .expire_gates("2030-01-01T00:00:00+00:00")
            .await
            .unwrap();
        assert_eq!(expired, vec![("wf".to_string(), "a".to_string())]);

        let (exec, nodes) = store.load_execution("wf").await.unwrap().unwrap();
        assert_eq!(exec.status, ExecutionStatus::Failed);
        assert_eq!(nodes[0].status, NodeStatus::Failed);
    }

    #[tokio::test]
    async fn expire_gates_ignores_future_and_null_deadlines() {
        let store = ExecutionStore::open_temp();
        store
            .create_execution("wf", "{}", "{}", &["a".into(), "b".into()])
            .await
            .unwrap();
        store
            .park_node("wf", "a", "s", "x", "k", "p", None)
            .await
            .unwrap();
        store
            .park_node(
                "wf",
                "b",
                "s",
                "y",
                "k",
                "p",
                Some("2030-01-01T00:00:00+00:00"),
            )
            .await
            .unwrap();
        let expired = store
            .expire_gates("2020-01-01T00:00:00+00:00")
            .await
            .unwrap();
        assert!(expired.is_empty());
    }
}
