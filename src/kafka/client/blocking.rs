use std::time::Duration;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
