use futures::{FutureExt as _, future};

use crate::app::auth::access::{AlterBrokerConfigsCap, BrokerConfigsCap};
use crate::kafka::model::{BrokerScope, ConfigEdit};
use crate::kafka::store::reread_until;
use crate::kafka::{ConfigEntry, KafkaError};

use super::Granted;

impl Granted<'_, BrokerConfigsCap> {
    pub(crate) async fn broker_configs(&self, id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.cluster.broker_configs(id).await
    }
}

impl Granted<'_, AlterBrokerConfigsCap> {
    pub(crate) async fn alter_broker_configs(
        &self,
        scope: BrokerScope,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        if let BrokerScope::Broker(id) = scope {
            self.cluster.known_broker(id)?;
        }
        self.cluster
            .session
            .alter_broker_configs(scope, edit)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            %scope,
            set = ?edit.set.keys().collect::<Vec<_>>(),
            reset = ?edit.reset,
            "altered broker configs"
        );
        let brokers = match scope {
            BrokerScope::Broker(id) => vec![id],
            BrokerScope::Cluster => self
                .cluster
                .store
                .topology
                .load()
                .map(|topology| topology.brokers.keys().copied().collect())
                .unwrap_or_default(),
        };
        let source = scope.source();
        let shown = brokers.into_iter().map(|id| {
            reread_until(
                move || self.cluster.session.broker_configs(id),
                |entries| edit.shows_in(entries, source),
            )
        });
        self.settle_on(future::join_all(shown).map(drop)).await;
        Ok(())
    }
}
