use crate::app::auth::access::{DeleteScramCredentialsCap, SetScramCredentialsCap};
use crate::kafka::KafkaError;
use crate::kafka::model::{NewScramCredential, ScramMechanism};

use super::Granted;

impl Granted<'_, SetScramCredentialsCap> {
    pub(crate) async fn set_scram_credential(
        &self,
        credential: &NewScramCredential,
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .set_scram_credential(credential)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            scram_user = credential.user.as_str(),
            mechanism = %credential.mechanism,
            iterations = credential.iterations,
            "set scram credential"
        );
        self.settle(&self.cluster.store.scram_users, |listing| {
            listing.iterations(&credential.user, credential.mechanism)
                == Some(credential.iterations)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteScramCredentialsCap> {
    pub(crate) async fn delete_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<(), KafkaError> {
        self.cluster
            .session
            .delete_scram_credential(user, mechanism)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            scram_user = user,
            %mechanism,
            "deleted scram credential"
        );
        self.settle(&self.cluster.store.scram_users, |listing| {
            listing.iterations(user, mechanism).is_none()
        })
        .await;
        Ok(())
    }
}
