use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::kafka::error::KafkaError;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::topic_config::ConfigEntry;

use super::cache::{CatalogCache, SubjectCache};
use super::snapshot::ClusterSnapshot;

#[derive(Debug, Clone, Default)]
pub struct CatalogReuse {
    pub metadata_hash: u64,
    pub configs: HashMap<String, Vec<ConfigEntry>>,
    pub snapshot: Option<Arc<ClusterSnapshot>>,
}

#[derive(Debug, Clone)]
pub struct CatalogAssemble {
    pub snapshot: ClusterSnapshot,
    pub metadata_hash: u64,
    pub configs: HashMap<String, Vec<ConfigEntry>>,
    pub fetched_configs: bool,
    pub reused_topology: bool,
}

pub struct CatalogPollerIntervals {
    pub catalog: Duration,
    pub subjects: Duration,
    pub configs: Duration,
}

pub struct CatalogPollerIo<FC, Observe, FS> {
    pub fetch_catalog: FC,
    pub observe: Observe,
    pub fetch_subjects: FS,
}

/// Background catalog and subject tasks per configured cluster. Dropping the
/// poller aborts them.
pub struct CatalogPoller {
    tasks: Vec<JoinHandle<()>>,
    kicks: HashMap<String, Arc<Notify>>,
}

impl Drop for CatalogPoller {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl CatalogPoller {
    pub fn start<FC, FCFut, Observe, FS, FSFut>(
        catalog: CatalogCache,
        subjects: SubjectCache,
        clusters: impl IntoIterator<Item = impl Into<String>>,
        intervals: CatalogPollerIntervals,
        io: CatalogPollerIo<FC, Observe, FS>,
    ) -> Self
    where
        FC: Fn(String, CatalogReuse, bool) -> FCFut + Send + Sync + Clone + 'static,
        FCFut: Future<Output = Result<CatalogAssemble, KafkaError>> + Send + 'static,
        Observe: Fn(&str, HashMap<String, u64>) + Send + Sync + Clone + 'static,
        FS: Fn(String) -> FSFut + Send + Sync + Clone + 'static,
        FSFut: Future<Output = Result<Vec<SchemaSubject>, KafkaError>> + Send + 'static,
    {
        let CatalogPollerIntervals {
            catalog: catalog_interval,
            subjects: subject_interval,
            configs: config_interval,
        } = intervals;
        let CatalogPollerIo {
            fetch_catalog,
            observe,
            fetch_subjects,
        } = io;
        let clusters: Vec<String> = clusters.into_iter().map(Into::into).collect();
        tracing::info!(
            clusters = ?clusters,
            catalog_interval_secs = catalog_interval.as_secs(),
            subject_interval_secs = subject_interval.as_secs(),
            config_interval_secs = config_interval.as_secs(),
            "starting catalog poller"
        );
        let kicks: HashMap<String, Arc<Notify>> = clusters
            .iter()
            .cloned()
            .map(|cluster| (cluster, Arc::new(Notify::new())))
            .collect();
        let catalog_tasks: Vec<JoinHandle<()>> = clusters
            .iter()
            .cloned()
            .map(|cluster| {
                let fetch_catalog = fetch_catalog.clone();
                let observe = observe.clone();
                let cache = catalog.clone();
                let kick = Arc::clone(kicks.get(&cluster).expect("catalog kick"));
                tokio::spawn(async move {
                    let mut reuse = CatalogReuse::default();
                    let mut last_config_fetch = None;
                    loop {
                        let fetch_configs = reuse.configs.is_empty()
                            || last_config_fetch.is_none_or(|fetched_at| {
                                tokio::time::Instant::now()
                                    .saturating_duration_since(fetched_at)
                                    >= config_interval
                            });
                        let started = tokio::time::Instant::now();
                        match fetch_catalog(cluster.clone(), reuse.clone(), fetch_configs).await {
                            Ok(assembled) => {
                                if assembled.fetched_configs {
                                    last_config_fetch = Some(tokio::time::Instant::now());
                                }
                                observe(&cluster, assembled.snapshot.message_counts());
                                let changed = reuse
                                    .snapshot
                                    .as_ref()
                                    .is_none_or(|prev| !prev.body_eq(&assembled.snapshot));
                                let missing = cache.snapshot(&cluster).is_none();
                                reuse.metadata_hash = assembled.metadata_hash;
                                reuse.configs = assembled.configs;
                                if changed || missing {
                                    let snapshot = Arc::new(assembled.snapshot);
                                    cache.store(cluster.clone(), Arc::clone(&snapshot));
                                    reuse.snapshot = Some(snapshot);
                                    tracing::debug!(cluster = %cluster, lane = "catalog", "poll updated");
                                }
                                cache.record_poll(&cluster, started.elapsed(), None);
                            }
                            Err(error) => {
                                cache.record_poll(
                                    &cluster,
                                    started.elapsed(),
                                    Some(error.to_string()),
                                );
                                tracing::warn!(cluster = %cluster, lane = "catalog", %error, "poll failed");
                            }
                        }

                        wait_for_kick_or_interval(catalog_interval, &kick).await;
                    }
                })
            })
            .collect();
        let subject_record = subjects.clone();
        let subject_tasks = Self::spawn_loop(
            clusters,
            subject_interval,
            "subjects",
            move |cluster| {
                let fetch_subjects = fetch_subjects.clone();
                let subjects = subject_record.clone();
                async move {
                    let started = tokio::time::Instant::now();
                    let result = fetch_subjects(cluster.clone()).await;
                    subjects.record_poll(
                        &cluster,
                        started.elapsed(),
                        result.as_ref().err().map(ToString::to_string),
                    );
                    result
                }
            },
            move |cluster, list| subjects.store(cluster, list),
            None,
        );

        Self {
            tasks: catalog_tasks.into_iter().chain(subject_tasks).collect(),
            kicks,
        }
    }

    pub fn start_with<F, Fut>(
        cache: CatalogCache,
        clusters: impl IntoIterator<Item = impl Into<String>>,
        interval: Duration,
        fetch: F,
    ) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<ClusterSnapshot, KafkaError>> + Send + 'static,
    {
        let clusters: Vec<String> = clusters.into_iter().map(Into::into).collect();
        let kicks: HashMap<String, Arc<Notify>> = clusters
            .iter()
            .cloned()
            .map(|cluster| (cluster, Arc::new(Notify::new())))
            .collect();
        let record = cache.clone();
        Self {
            tasks: Self::spawn_loop(
                clusters,
                interval,
                "catalog",
                move |cluster| {
                    let fetch = fetch.clone();
                    let record = record.clone();
                    async move {
                        let started = tokio::time::Instant::now();
                        let result = fetch(cluster.clone()).await;
                        record.record_poll(
                            &cluster,
                            started.elapsed(),
                            result.as_ref().err().map(ToString::to_string),
                        );
                        result
                    }
                },
                move |cluster, snapshot| cache.store(cluster, snapshot),
                Some(&kicks),
            ),
            kicks,
        }
    }

    pub fn kick(&self, cluster: &str) {
        if let Some(notify) = self.kicks.get(cluster) {
            notify.notify_one();
        }
    }

    fn spawn_loop<T, F, Fut, P>(
        clusters: impl IntoIterator<Item = impl Into<String>>,
        interval: Duration,
        lane: &'static str,
        fetch: F,
        persist: P,
        kicks: Option<&HashMap<String, Arc<Notify>>>,
    ) -> Vec<JoinHandle<()>>
    where
        T: Send + 'static,
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, KafkaError>> + Send + 'static,
        P: Fn(String, T) + Send + Sync + Clone + 'static,
    {
        clusters
            .into_iter()
            .map(Into::into)
            .map(|cluster| {
                let fetch = fetch.clone();
                let persist = persist.clone();
                let kick = kicks.and_then(|kicks| kicks.get(&cluster)).cloned();
                tokio::spawn(async move {
                    loop {
                        match fetch(cluster.clone()).await {
                            Ok(value) => {
                                persist(cluster.clone(), value);
                                tracing::debug!(cluster = %cluster, lane, "poll updated");
                            }
                            Err(error) => {
                                tracing::warn!(cluster = %cluster, lane, %error, "poll failed");
                            }
                        }

                        match &kick {
                            Some(kick) => wait_for_kick_or_interval(interval, kick).await,
                            None => tokio::time::sleep(interval).await,
                        }
                    }
                })
            })
            .collect()
    }
}

async fn wait_for_kick_or_interval(interval: Duration, kick: &Notify) {
    tokio::select! {
        _ = tokio::time::sleep(interval) => {}
        _ = kick.notified() => {}
    }
}
