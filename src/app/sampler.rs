use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

use futures::stream::{self, BoxStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::environment::SAMPLE_INTERVAL;

pub(crate) struct Sampler<T> {
    latest: watch::Receiver<Option<T>>,
    task: JoinHandle<()>,
}

impl<T> Drop for Sampler<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<T: Clone + Send + Sync + 'static> Sampler<T> {
    pub(crate) fn stream(self: Arc<Self>) -> BoxStream<'static, T> {
        let latest = self.latest.clone();

        Box::pin(stream::unfold(
            (self, latest, true),
            |(sampler, mut latest, first)| async move {
                if first {
                    let buffered = latest.borrow_and_update().clone();
                    if let Some(sample) = buffered {
                        return Some((sample, (sampler, latest, false)));
                    }
                }

                loop {
                    latest.changed().await.ok()?;

                    let current = latest.borrow_and_update().clone();
                    if let Some(sample) = current {
                        return Some((sample, (sampler, latest, false)));
                    }
                }
            },
        ))
    }
}

pub(crate) struct SamplerMap<K, T> {
    live: Mutex<HashMap<K, Weak<Sampler<T>>>>,
}

impl<K, T> Default for SamplerMap<K, T> {
    fn default() -> Self {
        Self {
            live: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, T> SamplerMap<K, T>
where
    K: Eq + Hash + Clone,
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn attach<F, Fut>(&self, key: K, poll: F) -> Arc<Sampler<T>>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let mut live = self.live.lock().expect("sampler registry lock");

        if let Some(sampler) = live.get(&key).and_then(Weak::upgrade) {
            return sampler;
        }

        live.retain(|_, sampler| sampler.strong_count() > 0);

        let (updates, latest) = watch::channel(None);
        let task = tokio::spawn(async move {
            loop {
                if updates.send(Some(poll().await)).is_err() {
                    break;
                }

                tokio::time::sleep(*SAMPLE_INTERVAL).await;
            }
        });

        let sampler = Arc::new(Sampler { latest, task });
        live.insert(key, Arc::downgrade(&sampler));
        sampler
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::StreamExt;

    use super::*;

    fn counting_sampler(polls: Arc<AtomicUsize>) -> impl Fn() -> futures::future::Ready<usize> {
        move || futures::future::ready(polls.fetch_add(1, Ordering::SeqCst) + 1)
    }

    #[tokio::test(start_paused = true)]
    async fn subscribers_on_the_same_key_share_one_task() {
        let polls = Arc::new(AtomicUsize::new(0));
        let samplers: SamplerMap<String, usize> = SamplerMap::default();

        let first = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));
        let second = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));

        assert!(Arc::ptr_eq(&first, &second));

        let mut stream = first.stream();
        assert_eq!(stream.next().await, Some(1));

        tokio::time::advance(*SAMPLE_INTERVAL + Duration::from_millis(1)).await;
        assert_eq!(stream.next().await, Some(2));

        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn distinct_keys_get_their_own_task() {
        let polls = Arc::new(AtomicUsize::new(0));
        let samplers: SamplerMap<String, usize> = SamplerMap::default();

        let local = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));
        let staging = samplers.attach("staging".to_owned(), counting_sampler(polls.clone()));

        assert!(!Arc::ptr_eq(&local, &staging));

        local.stream().next().await;
        staging.stream().next().await;

        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_task_stops_once_every_subscriber_disconnects() {
        let polls = Arc::new(AtomicUsize::new(0));
        let samplers: SamplerMap<String, usize> = SamplerMap::default();

        let sampler = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));
        sampler.clone().stream().next().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        drop(sampler);
        tokio::time::advance(*SAMPLE_INTERVAL * 5).await;
        tokio::task::yield_now().await;

        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_subscriber_sees_the_buffered_sample_immediately() {
        let polls = Arc::new(AtomicUsize::new(0));
        let samplers: SamplerMap<String, usize> = SamplerMap::default();

        let sampler = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));
        sampler.clone().stream().next().await;

        let joined = samplers.attach("local".to_owned(), counting_sampler(polls.clone()));
        assert_eq!(joined.stream().next().await, Some(1));
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
}
