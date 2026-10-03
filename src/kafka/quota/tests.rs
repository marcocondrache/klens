use super::*;

fn described(entity: &[(&str, Option<&str>)], values: &[(&str, f64)]) -> DescribedQuota {
    DescribedQuota {
        entity: entity
            .iter()
            .map(|(entity_type, name)| ((*entity_type).to_owned(), name.map(str::to_owned)))
            .collect(),
        values: values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), *value))
            .collect(),
    }
}

fn described_quotas(entries: Vec<DescribedQuota>) -> Vec<ClientQuota> {
    match QuotaListing::from_describe("local", None, entries).expect("listing") {
        QuotaListing::Described(quotas) => quotas,
        QuotaListing::Denied => panic!("listing was denied"),
    }
}

fn part(entity_type: QuotaEntityType, name: Option<&str>) -> QuotaEntity {
    QuotaEntity {
        entity_type,
        name: name.map(str::to_owned),
    }
}

#[test]
fn each_entity_type_maps_from_its_wire_name() {
    let quotas = described_quotas(vec![
        described(&[("user", Some("alice"))], &[]),
        described(&[("client-id", Some("checkout"))], &[]),
        described(&[("ip", Some("10.0.0.7"))], &[]),
    ]);

    assert_eq!(
        quotas
            .iter()
            .map(|quota| &quota.entity[0])
            .collect::<Vec<_>>(),
        vec![
            &part(QuotaEntityType::User, Some("alice")),
            &part(QuotaEntityType::ClientId, Some("checkout")),
            &part(QuotaEntityType::Ip, Some("10.0.0.7")),
        ]
    );
}

#[test]
fn entity_parts_are_ordered_user_then_client_id() {
    let quotas = described_quotas(vec![described(
        &[("client-id", None), ("user", Some("alice"))],
        &[],
    )]);

    assert_eq!(
        quotas[0].entity,
        vec![
            part(QuotaEntityType::User, Some("alice")),
            part(QuotaEntityType::ClientId, None),
        ]
    );
}

#[test]
fn every_known_key_fills_its_own_value() {
    let quotas = described_quotas(vec![described(
        &[("user", Some("alice"))],
        &[
            ("producer_byte_rate", 1.0),
            ("consumer_byte_rate", 2.0),
            ("request_percentage", 3.0),
            ("controller_mutation_rate", 4.0),
            ("connection_creation_rate", 5.0),
        ],
    )]);

    assert_eq!(
        quotas[0].values,
        QuotaValues {
            producer_byte_rate: Some(1.0),
            consumer_byte_rate: Some(2.0),
            request_percentage: Some(3.0),
            controller_mutation_rate: Some(4.0),
            connection_creation_rate: Some(5.0),
        }
    );
}

#[test]
fn unknown_keys_are_dropped_and_known_ones_kept() {
    let quotas = described_quotas(vec![described(
        &[("user", Some("alice"))],
        &[("future_rate", 9.0), ("producer_byte_rate", 1.0)],
    )]);

    assert_eq!(
        quotas[0].values,
        QuotaValues {
            producer_byte_rate: Some(1.0),
            ..QuotaValues::default()
        }
    );
}

#[test]
fn an_entry_with_an_unknown_entity_type_is_dropped() {
    let quotas = described_quotas(vec![
        described(&[("user", Some("alice")), ("tenant", Some("blue"))], &[]),
        described(&[("user", Some("bob"))], &[]),
    ]);

    assert_eq!(quotas.len(), 1);
    assert_eq!(
        quotas[0].entity,
        vec![part(QuotaEntityType::User, Some("bob"))]
    );
}

#[test]
fn a_cluster_authorization_failure_is_a_denied_listing() {
    for message in [
        "Cluster authorization failed.",
        "ClusterAuthorizationFailed",
    ] {
        let listing = QuotaListing::from_describe("local", Some(message), Vec::new());

        assert_eq!(listing.expect("listing"), QuotaListing::Denied, "{message}");
    }
}

#[test]
fn any_other_describe_error_fails_the_call() {
    let error = QuotaListing::from_describe("local", Some("UnknownServerError"), Vec::new())
        .expect_err("error");

    assert!(
        matches!(&error, KafkaError::Admin(message) if message == "UnknownServerError"),
        "{error:?}"
    );
}
