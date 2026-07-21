//! Async job tracking for `submit_goal` / `get_result`.

use std::collections::{HashMap, VecDeque};
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

#[derive(Default)]
struct Inner {
    jobs: HashMap<Uuid, JobState>,
    completed_order: VecDeque<Uuid>,
}

#[derive(Clone, Default)]
pub struct JobStore {
    inner: Arc<Mutex<Inner>>,
}

impl JobStore {
    /// Max retained *completed* jobs; the oldest is evicted past this. Running
    /// jobs are never counted or evicted.
    pub(crate) const MAX_COMPLETED: usize = 1024;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self) -> Uuid {
        let id = Uuid::new_v4();
        self.inner.lock().unwrap().jobs.insert(id, JobState::Running);
        id
    }

    pub fn complete(&self, id: Uuid, outcome: Outcome) {
        let mut inner = self.inner.lock().unwrap();
        inner.jobs.insert(id, JobState::Done { result: outcome });
        inner.completed_order.push_back(id);
        while inner.completed_order.len() > Self::MAX_COMPLETED {
            if let Some(old) = inner.completed_order.pop_front() {
                inner.jobs.remove(&old);
            }
        }
    }

    pub fn get(&self, id: &Uuid) -> Option<JobState> {
        self.inner.lock().unwrap().jobs.get(id).cloned()
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
            Some(JobState::Done {
                result: Outcome::Success(v),
            }) => assert_eq!(v["ok"], true),
            other => panic!("expected Done/Success, got {other:?}"),
        }
    }

    #[test]
    fn get_unknown_returns_none() {
        let store = JobStore::new();
        assert!(store.get(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn completed_jobs_are_capped_running_never_evicted() {
        let store = JobStore::new();
        let running = store.create(); // stays Running, must survive
        let mut ids = Vec::new();
        for _ in 0..=JobStore::MAX_COMPLETED {
            let id = store.create();
            store.complete(id, Outcome::Success(serde_json::json!(1)));
            ids.push(id);
        }
        // Oldest completed evicted, newest retained.
        assert!(store.get(&ids[0]).is_none(), "oldest completed job must be evicted");
        assert!(store.get(ids.last().unwrap()).is_some(), "newest completed job retained");
        assert!(matches!(store.get(&running), Some(JobState::Running)),
            "a still-running job must never be evicted");
    }
}
