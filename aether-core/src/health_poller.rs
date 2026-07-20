use crate::registry_store::{RegistryStatus, RegistryStore};
use reqwest::Client;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct HealthPoller {
    pub(crate) store: RegistryStore,
    pub(crate) interval: Duration,
    pub(crate) client: Client,
    pub(crate) failure_threshold: usize,
}

impl HealthPoller {
    pub fn new(store: RegistryStore, interval: Duration) -> Self {
        Self {
            store,
            interval,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build health check client"),
            failure_threshold: 3,
        }
    }

    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub async fn run_once(&self) {
        self.poll_once(&mut HashMap::new()).await;
    }

    async fn run(self) {
        let mut failure_counts: HashMap<String, usize> = HashMap::new();
        loop {
            tokio::time::sleep(self.interval).await;
            self.poll_once(&mut failure_counts).await;
        }
    }

    pub async fn poll_once(&self, failure_counts: &mut HashMap<String, usize>) {
        let entries = match self.store.list_all().await {
            Ok(v) => v,
            Err(_) => return,
        };
        for entry in entries {
            let url = format!("{}/health", entry.http_url.trim_end_matches('/'));
            let now = chrono::Utc::now().to_rfc3339();
            let ok = self
                .client
                .get(&url)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false);
            if ok {
                failure_counts.remove(&entry.instance_id);
                let _ = self
                    .store
                    .update_health(&entry.instance_id, RegistryStatus::Healthy, &now)
                    .await;
            } else {
                let count = failure_counts.entry(entry.instance_id.clone()).or_insert(0);
                *count += 1;
                if *count >= self.failure_threshold {
                    let _ = self
                        .store
                        .update_health(&entry.instance_id, RegistryStatus::Unhealthy, &now)
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry_store::{RegistrationEntry, RegistryStore};
    use httpmock::prelude::*;

    async fn register_agent(store: &RegistryStore, url: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        store
            .register(RegistrationEntry {
                instance_id: id.clone(),
                name: "test".to_string(),
                http_url: url.to_string(),
                capabilities: vec![],
                metadata: HashMap::new(),
                registered_at: chrono::Utc::now().to_rfc3339(),
                last_health_check: None,
                status: RegistryStatus::Unknown,
            })
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn healthy_agent_marked_healthy_after_poll() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method("GET").path("/health");
            then.status(200).body("ok");
        });

        let store = RegistryStore::open_temp();
        let _id = register_agent(&store, &server.base_url()).await;

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::new(),
            failure_threshold: 3,
        };
        poller.run_once().await;

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Healthy);
    }

    #[tokio::test]
    async fn unreachable_agent_marked_unhealthy_after_threshold() {
        let store = RegistryStore::open_temp();
        let _id = register_agent(&store, "http://127.0.0.1:1").await;

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::builder()
                .timeout(Duration::from_millis(100))
                .build()
                .unwrap(),
            failure_threshold: 3,
        };

        let mut counts = HashMap::new();
        poller.poll_once(&mut counts).await;
        poller.poll_once(&mut counts).await;
        poller.poll_once(&mut counts).await;

        let all = store.list_all().await.unwrap();
        assert_eq!(all[0].status, RegistryStatus::Unhealthy);
    }

    #[tokio::test]
    async fn recovery_resets_to_healthy() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method("GET").path("/health");
            then.status(200);
        });

        let store = RegistryStore::open_temp();
        let _id = register_agent(&store, &server.base_url()).await;

        let poller = HealthPoller {
            store: store.clone(),
            interval: Duration::from_millis(1),
            client: Client::new(),
            failure_threshold: 3,
        };
        poller.run_once().await;

        let all = store.list_all().await.unwrap();
        assert_eq!(
            all[0].status,
            RegistryStatus::Healthy,
            "one success should set healthy"
        );
    }
}
