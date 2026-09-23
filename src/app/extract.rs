use axum::extract::FromRequestParts;
use axum::extract::rejection::PathRejection;
use axum::http::request::Parts;
use axum_extra::extract::QueryRejection;
use serde::de::DeserializeOwned;

use super::error::ApiError;

pub(crate) struct Query<T>(pub T);

impl<T, S> FromRequestParts<S> for Query<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum_extra::extract::Query(value) =
            axum_extra::extract::Query::from_request_parts(parts, state).await?;
        Ok(Self(value))
    }
}

pub(crate) struct Path<T>(pub T);

impl<T, S> FromRequestParts<S> for Path<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Path(value) =
            axum::extract::Path::from_request_parts(parts, state).await?;
        Ok(Self(value))
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        Self::InvalidRequest {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl From<PathRejection> for ApiError {
    fn from(rejection: PathRejection) -> Self {
        Self::InvalidRequest {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}
