//! Async job tracking for `submit_goal` / `get_result`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aether_core::Outcome;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum JobState {
    Running,
    Done { result: Outcome },
}

#[derive(Clone, Default)]
pub struct JobStore {
    jobs: Arc<Mutex<HashMap<Uuid, JobState>>>,
}

impl JobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self) -> Uuid {
        let id = Uuid::new_v4();
        self.jobs.lock().unwrap().insert(id, JobState::Running);
        id
    }

    pub fn complete(&self, id: Uuid, outcome: Outcome) {
        self.jobs.lock().unwrap().insert(id, JobState::Done { result: outcome });
    }

    pub fn get(&self, id: &Uuid) -> Option<JobState> {
        self.jobs.lock().unwrap().get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_complete_roundtrip() {
        let store = JobStore::new();
        let id = store.create();
        assert!(matches!(store.get(&id), Some(JobState::Running)));
        store.complete(id, Outcome::Success(serde_json::json!({"ok": true})));
        match store.get(&id) {
            Some(JobState::Done { result: Outcome::Success(v) }) => assert_eq!(v["ok"], true),
            other => panic!("expected Done/Success, got {other:?}"),
        }
    }

    #[test]
    fn get_unknown_returns_none() {
        let store = JobStore::new();
        assert!(store.get(&Uuid::new_v4()).is_none());
    }
}
