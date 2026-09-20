use std::ops::Deref;
use std::sync::Arc;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterAccess, EffectiveAccess};
use crate::kafka::store::ClusterStore;

use super::error::GqlError;

#[derive(Clone)]
pub struct GraphQlContext {
    pub state: AppState,
    pub access: EffectiveAccess,
    pub guard: SessionGuard,
}

pub struct ClusterHandle<'a> {
    pub access: ClusterAccess<'a>,
    pub store: &'a Arc<ClusterStore>,
}

impl ClusterHandle<'_> {
    pub fn name(&self) -> &str {
        self.access.cluster()
    }
}

impl GraphQlContext {
    pub fn unrestricted(state: AppState) -> Self {
        Self {
            state,
            access: EffectiveAccess::Unrestricted,
            guard: SessionGuard::open(),
        }
    }

    pub fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterHandle<'a>, GqlError> {
        let access = self.access.cluster(name)?;
        Ok(ClusterHandle {
            store: self.state.cluster(access.cluster())?,
            access,
        })
    }
}

impl Deref for GraphQlContext {
    type Target = AppState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl juniper::Context for GraphQlContext {}
