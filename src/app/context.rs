use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterAccess, EffectiveAccess, PrivilegeSet};
use crate::kafka::KafkaError;
use crate::kafka::store::ClusterStore;

use super::error::ApiError;

pub(crate) struct Session {
    pub state: AppState,
    pub access: EffectiveAccess,
    pub guard: SessionGuard,
}

pub(crate) struct ClusterHandle<'a> {
    pub access: ClusterAccess<'a>,
    pub store: &'a Arc<ClusterStore>,
}

impl ClusterHandle<'_> {
    pub(crate) fn name(&self) -> &str {
        self.access.cluster()
    }
}

impl Session {
    pub(crate) fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterHandle<'a>, ApiError> {
        let access = self.access.cluster(name)?;
        let store = self.state.cluster(access.cluster())?;
        let ceiling = if store.identity.read_only {
            PrivilegeSet::READ
        } else {
            PrivilegeSet::ALL
        };
        Ok(ClusterHandle {
            access: access.capped(ceiling),
            store,
        })
    }
}

impl Session {
    pub(crate) fn audit<T>(
        &self,
        cluster: &str,
        action: &'static str,
        target: &str,
        outcome: &Result<T, KafkaError>,
    ) {
        let subject = self.guard.subject().unwrap_or("anonymous");
        match outcome {
            Ok(_) => tracing::info!(
                target: "klens::audit",
                subject,
                cluster,
                action,
                target,
                "change applied"
            ),
            Err(error) => tracing::warn!(
                target: "klens::audit",
                subject,
                cluster,
                action,
                target,
                code = error.code(),
                %error,
                "change failed"
            ),
        }
    }
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(access) = parts.extensions.remove::<EffectiveAccess>() else {
            return Err(ApiError::Unauthorized);
        };
        let Some(guard) = parts.extensions.remove::<SessionGuard>() else {
            return Err(ApiError::Unauthorized);
        };
        Ok(Self {
            state: state.clone(),
            access,
            guard,
        })
    }
}
