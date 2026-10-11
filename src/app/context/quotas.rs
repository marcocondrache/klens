use crate::app::auth::access::AlterQuotasCap;
use crate::kafka::KafkaError;
use crate::kafka::model::{ClientQuota, QuotaValues};

use super::Granted;

impl Granted<'_, AlterQuotasCap> {
    pub(crate) async fn set_client_quota(&self, quota: &ClientQuota) -> Result<(), KafkaError> {
        self.cluster.session.alter_client_quota(quota).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            quota = quota.to_string().as_str(),
            "set client quota"
        );
        self.settle(&self.cluster.store.quotas, |listing| {
            listing
                .values(&quota.entity)
                .map_or(quota.values == QuotaValues::default(), |values| {
                    *values == quota.values
                })
        })
        .await;
        Ok(())
    }
}
