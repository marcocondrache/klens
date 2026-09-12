use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;

use crate::environment::BLOCKING_SLACK;
use crate::kafka::error::KafkaError;

pub async fn run_blocking<T, F>(request: Duration, work: F) -> Result<T, KafkaError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, KafkaError> + Send + 'static,
{
    tokio::time::timeout(request + *BLOCKING_SLACK, tokio::task::spawn_blocking(work))
        .await
        .map_err(|_| KafkaError::Timeout)?
        .map_err(KafkaError::from)?
}

pub fn snapshot_cache<V>(ttl: Duration) -> Cache<(), V>
where
    V: Clone + Send + Sync + 'static,
{
    Cache::builder().max_capacity(1).time_to_live(ttl).build()
}

pub fn into_kafka_error(err: Arc<KafkaError>) -> KafkaError {
    Arc::try_unwrap(err).unwrap_or_else(|err| KafkaError::Admin(err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::task::JoinSet;

    use super::*;
    use crate::kafka::metadata::MetadataSnapshot;

    fn snapshot() -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: Some("id".into()),
            brokers: Vec::new(),
            topics: Vec::new(),
        }
    }

    #[tokio::test]
    async fn expires_metadata_after_ttl() {
        let cache = snapshot_cache(Duration::from_millis(20));
        let fetches = Arc::new(AtomicUsize::new(0));

        let first = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, KafkaError>(snapshot())
                })
                .await
                .unwrap()
        };
        assert_eq!(first.cluster_id.as_deref(), Some("id"));
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        cache
            .try_get_with((), async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, KafkaError>(snapshot())
            })
            .await
            .unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(30)).await;

        cache
            .try_get_with((), async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, KafkaError>(snapshot())
            })
            .await
            .unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn coalesces_concurrent_misses() {
        let cache = Arc::new(snapshot_cache(Duration::from_secs(5)));
        let fetches = Arc::new(AtomicUsize::new(0));
        let mut joins = JoinSet::new();

        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let fetches = Arc::clone(&fetches);
            joins.spawn(async move {
                cache
                    .try_get_with((), async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, KafkaError>(snapshot())
                    })
                    .await
            });
        }

        while let Some(result) = joins.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_cache_errors() {
        let cache: Cache<(), MetadataSnapshot> = snapshot_cache(Duration::from_secs(5));
        let fetches = Arc::new(AtomicUsize::new(0));

        let failed = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Err(KafkaError::Admin("boom".into()))
                })
                .await
                .map_err(into_kafka_error)
        };
        assert!(failed.is_err());

        let recovered = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, KafkaError>(snapshot())
                })
                .await
                .unwrap()
        };
        assert_eq!(recovered.cluster_id.as_deref(), Some("id"));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_blocking_gives_the_request_its_slack() {
        let result: Result<(), _> = run_blocking(Duration::ZERO, || {
            std::thread::sleep(Duration::from_millis(50));
            Ok(())
        })
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_blocking_reports_a_timeout() {
        let result: Result<(), _> = run_blocking(Duration::ZERO, || {
            std::thread::sleep(*BLOCKING_SLACK + Duration::from_millis(200));
            Ok(())
        })
        .await;

        let error = result.unwrap_err();
        assert!(matches!(error, KafkaError::Timeout));
        assert_eq!(error.to_string(), "kafka request timed out");
    }

    #[test]
    fn into_kafka_error_keeps_a_shared_error_as_admin() {
        let shared = Arc::new(KafkaError::Timeout);
        let first = into_kafka_error(Arc::clone(&shared));
        let unwrapped = into_kafka_error(shared);

        assert!(
            matches!(first, KafkaError::Admin(message) if message == "kafka request timed out")
        );
        assert!(matches!(unwrapped, KafkaError::Timeout));
    }
}
