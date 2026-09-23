use axum::Json;
use axum::Router;
use axum::routing::post;

use crate::AppState;
use crate::kafka::store::GroupRow;
use crate::kafka::{ClusterSession, KafkaError, OffsetMove, ResetScope, plan_reset};

use super::context::{ClusterHandle, Session};
use super::error::ApiError;
use super::extract::{JsonBody, Path};
use super::writes::{Audit, confirm};

pub mod types;

#[cfg(test)]
mod tests;

use types::{DeleteOffsetsRequest, DeletedOffsets, OffsetChange, OffsetReset, ResetOffsetsRequest};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/reset", post(reset))
        .route("/delete", post(delete))
}

async fn reset(
    session: Session,
    Path(name): Path<String>,
    JsonBody(request): JsonBody<ResetOffsetsRequest>,
) -> Result<Json<OffsetReset>, ApiError> {
    Audit::new(&session, &name, "group_offsets.reset", &request.group)
        .dry_run(request.dry_run)
        .record(reset_offsets(&session, &name, &request).await)
        .map(Json)
}

async fn reset_offsets(
    session: &Session,
    name: &str,
    request: &ResetOffsetsRequest,
) -> Result<OffsetReset, ApiError> {
    let cluster = session.cluster(name)?;
    let capability = cluster.access.reset_offsets()?;
    let group = known_group(&cluster, &request.group)?;
    if !request.dry_run && group.state.has_members() {
        return Err(group_not_empty(&cluster, &request.group));
    }

    let scope = match (&request.topic, &request.partitions) {
        (None, Some(_)) => {
            return Err(
                KafkaError::InvalidRequest("partitions need a topic to belong to".into()).into(),
            );
        }
        (None, None) => ResetScope::Committed,
        (Some(topic), partitions) => ResetScope::Topic {
            topic: topic.clone(),
            partitions: partitions.clone(),
        },
    };
    let kafka = kafka(session, capability.cluster())?;
    let plan = plan_reset(kafka, &request.group, &scope, request.to.into()).await?;

    if !request.dry_run {
        let offsets: Vec<_> = plan.iter().map(OffsetMove::committed).collect();
        kafka.commit_group_offsets(&request.group, &offsets).await?;
        cluster.store.offsets.kick();
    }

    Ok(OffsetReset {
        group: request.group.clone(),
        applied: !request.dry_run,
        partitions: plan.into_iter().map(OffsetChange::from).collect(),
    })
}

async fn delete(
    session: Session,
    Path(name): Path<String>,
    JsonBody(request): JsonBody<DeleteOffsetsRequest>,
) -> Result<Json<DeletedOffsets>, ApiError> {
    Audit::new(&session, &name, "group_offsets.delete", &request.group)
        .record(delete_offsets(&session, &name, &request).await)
        .map(Json)
}

async fn delete_offsets(
    session: &Session,
    name: &str,
    request: &DeleteOffsetsRequest,
) -> Result<DeletedOffsets, ApiError> {
    let cluster = session.cluster(name)?;
    let capability = cluster.access.delete_group_offsets()?;
    known_group(&cluster, &request.group)?;
    confirm(&request.confirm, &request.group, "group id")?;

    let kafka = kafka(session, capability.cluster())?;
    let mut partitions = match &request.partitions {
        Some(partitions) if partitions.is_empty() => {
            return Err(KafkaError::InvalidRequest(
                "partitions must not be empty; omit it to cover every committed partition".into(),
            )
            .into());
        }
        Some(partitions) => partitions.clone(),
        None => kafka
            .committed_offsets(&request.group, None)
            .await?
            .into_iter()
            .filter(|offset| offset.topic == request.topic)
            .map(|offset| offset.partition)
            .collect(),
    };
    partitions.sort_unstable();
    partitions.dedup();

    let refs: Vec<(String, i32)> = partitions
        .iter()
        .map(|partition| (request.topic.clone(), *partition))
        .collect();
    kafka.delete_group_offsets(&request.group, &refs).await?;
    cluster.store.offsets.kick();

    Ok(DeletedOffsets {
        group: request.group.clone(),
        topic: request.topic.clone(),
        partitions,
    })
}

fn known_group(cluster: &ClusterHandle<'_>, group: &str) -> Result<GroupRow, ApiError> {
    cluster.store.group_row(group).ok_or_else(|| {
        KafkaError::UnknownGroup {
            cluster: cluster.name().to_owned(),
            group: group.to_owned(),
        }
        .into()
    })
}

fn group_not_empty(cluster: &ClusterHandle<'_>, group: &str) -> ApiError {
    KafkaError::GroupNotEmpty {
        cluster: cluster.name().to_owned(),
        group: group.to_owned(),
    }
    .into()
}

fn kafka<'a>(session: &'a Session, cluster: &str) -> Result<&'a dyn ClusterSession, ApiError> {
    Ok(session.state.sessions.session(cluster)?)
}
