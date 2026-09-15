use std::ops::Deref;

use crate::AppState;
use crate::app::auth::access::{EffectiveAccess, Privilege};
use crate::kafka::KafkaError;

use super::error::GqlError;

#[derive(Clone)]
pub struct GraphQlContext {
    pub state: AppState,
    pub access: EffectiveAccess,
}

impl GraphQlContext {
    pub fn unrestricted(state: AppState) -> Self {
        Self {
            state,
            access: EffectiveAccess::Unrestricted,
        }
    }

    pub fn allow_cluster(&self, cluster: &str) -> Result<(), KafkaError> {
        if self.access.can_see_cluster(cluster) {
            Ok(())
        } else {
            Err(KafkaError::UnknownCluster(cluster.to_owned()))
        }
    }

    pub fn allow_privilege(&self, privilege: Privilege, cluster: &str) -> Result<(), GqlError> {
        self.allow_cluster(cluster)?;
        if self.access.allows(privilege, cluster) {
            Ok(())
        } else {
            Err(GqlError::Forbidden)
        }
    }
}

impl Deref for GraphQlContext {
    type Target = AppState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl juniper::Context for GraphQlContext {}
