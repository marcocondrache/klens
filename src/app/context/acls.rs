use crate::app::auth::access::{CreateAclsCap, DeleteAclsCap};
use crate::kafka::KafkaError;
use crate::kafka::model::Acl;

use super::Granted;

impl Granted<'_, CreateAclsCap> {
    pub(crate) async fn create_acls(&self, acls: &[Acl]) -> Result<(), KafkaError> {
        self.cluster.session.create_acls(acls).await?;
        for acl in acls {
            tracing::info!(
                cluster = %self.cluster.store.name(),
                acl = acl.to_string().as_str(),
                "created acl"
            );
        }
        self.settle(&self.cluster.store.acls, |listing| {
            acls.iter().all(|acl| listing.contains(acl))
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteAclsCap> {
    pub(crate) async fn delete_acl(&self, acl: &Acl) -> Result<(), KafkaError> {
        self.cluster.session.delete_acl(acl).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            acl = acl.to_string().as_str(),
            "deleted acl"
        );
        self.settle(&self.cluster.store.acls, |listing| !listing.contains(acl))
            .await;
        Ok(())
    }
}
