use std::collections::HashMap;

use krafka::admin::ConsumerGroupDescription;
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::protocol::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, VersionedDecode, VersionedEncode, versions,
};

use crate::kafka::error::KafkaError;
use crate::kafka::group::{GroupSnapshot, MemberAssignment, is_internal_group};

use super::convert::member_assignments;

pub(super) fn snapshots_from_descriptions(
    descriptions: Vec<ConsumerGroupDescription>,
) -> Vec<GroupSnapshot> {
    descriptions
        .into_iter()
        .filter(|description| {
            description.error.is_none()
                && !description.state.eq_ignore_ascii_case("dead")
                && !is_internal_group(&description.group_id)
        })
        .map(GroupSnapshot::from_krafka)
        .collect()
}

/// krafka's admin wrapper drops classic `member_assignment` bytes. Fetch them
/// with DescribeGroups and decode the consumer-protocol blob.
pub(super) async fn fill_classic_assignments(
    client: &KrafkaSharedClient,
    groups: &mut [GroupSnapshot],
) -> Result<(), KafkaError> {
    for group in groups.iter_mut() {
        if group
            .members
            .iter()
            .all(|member| !member.assignments.is_empty())
        {
            continue;
        }
        if group.members.is_empty() {
            continue;
        }
        let assigned = classic_assignments(client, &group.id).await?;
        for member in &mut group.members {
            if member.assignments.is_empty()
                && let Some(partitions) = assigned.get(&member.id)
            {
                member.assignments = partitions.clone();
            }
        }
    }
    Ok(())
}

async fn classic_assignments(
    client: &KrafkaSharedClient,
    group_id: &str,
) -> Result<HashMap<String, Vec<MemberAssignment>>, KafkaError> {
    let Some(broker) = client.metadata().brokers().into_iter().next() else {
        return Err(KafkaError::Admin("no brokers available".into()));
    };
    let any = client.pool().get_connection(broker.address()).await?;
    let find_version = any
        .negotiate_api_version(
            ApiKey::FindCoordinator,
            versions::FIND_COORDINATOR_MAX,
            versions::FIND_COORDINATOR_MIN,
        )
        .ok_or_else(|| KafkaError::Admin("FindCoordinator is not supported".into()))?;
    let find_bytes = any
        .send_request(ApiKey::FindCoordinator, find_version, |buf| {
            FindCoordinatorRequest::for_group(group_id).encode_versioned(find_version, buf)
        })
        .await?;
    let mut find_buf = find_bytes;
    let found = FindCoordinatorResponse::decode_versioned(find_version, &mut find_buf)?;
    if !found.error_code.is_ok() {
        return Err(KafkaError::Admin(format!(
            "FindCoordinator for '{group_id}': {:?}",
            found.error_code
        )));
    }

    let coordinator = client
        .pool()
        .get_connection(&format!("{}:{}", found.host, found.port))
        .await?;
    let describe_version = coordinator
        .negotiate_api_version(
            ApiKey::DescribeGroups,
            versions::DESCRIBE_GROUPS_MAX,
            versions::DESCRIBE_GROUPS_MIN,
        )
        .ok_or_else(|| KafkaError::Admin("DescribeGroups is not supported".into()))?;
    let describe_bytes = coordinator
        .send_request(ApiKey::DescribeGroups, describe_version, |buf| {
            DescribeGroupsRequest {
                groups: vec![group_id.to_owned()],
                include_authorized_operations: false,
            }
            .encode_versioned(describe_version, buf)
        })
        .await?;
    let mut describe_buf = describe_bytes;
    let described = DescribeGroupsResponse::decode_versioned(describe_version, &mut describe_buf)?;

    let mut assigned = HashMap::new();
    for group in described.groups {
        for member in group.members {
            assigned.insert(
                member.member_id,
                member_assignments(&member.member_assignment),
            );
        }
    }
    Ok(assigned)
}
