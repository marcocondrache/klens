use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterAccess, EffectiveAccess};
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
        Ok(ClusterHandle {
            store: self.state.cluster(access.cluster())?,
            access,
        })
    }
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(access) = parts.extensions.get::<EffectiveAccess>().cloned() else {
            return Err(ApiError::Unauthorized);
        };
        let Some(guard) = parts.extensions.get::<SessionGuard>().cloned() else {
            return Err(ApiError::Unauthorized);
        };
        Ok(Self {
            state: state.clone(),
            access,
            guard,
        })
    }
}
