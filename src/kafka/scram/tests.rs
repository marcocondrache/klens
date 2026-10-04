use secrecy::SecretString;

use super::*;

fn described(name: &str, credentials: &[(WireMechanism, i32)]) -> DescribedUser {
    DescribedUser {
        name: name.to_owned(),
        credentials: credentials.to_vec(),
    }
}

fn credential(mechanism: ScramMechanism, iterations: i32) -> ScramCredential {
    ScramCredential {
        mechanism,
        iterations,
    }
}

#[test]
fn users_are_listed_by_name_with_credentials_by_mechanism() {
    let listing = ScramListing::from_describe(
        None,
        vec![
            described("bob", &[(WireMechanism::Sha512, 4096)]),
            described(
                "alice",
                &[(WireMechanism::Sha512, 8192), (WireMechanism::Sha256, 4096)],
            ),
        ],
    );

    assert_eq!(
        listing.expect("listing"),
        ScramListing::Described(vec![
            ScramUser {
                name: "alice".to_owned(),
                credentials: vec![
                    credential(ScramMechanism::Sha256, 4096),
                    credential(ScramMechanism::Sha512, 8192),
                ],
            },
            ScramUser {
                name: "bob".to_owned(),
                credentials: vec![credential(ScramMechanism::Sha512, 4096)],
            },
        ])
    );
}

#[test]
fn a_cluster_authorization_failure_is_a_denied_listing() {
    for message in [
        "Cluster authorization failed.",
        "ClusterAuthorizationFailed",
    ] {
        let listing = ScramListing::from_describe(Some(message), Vec::new());

        assert_eq!(listing.expect("listing"), ScramListing::Denied, "{message}");
    }
}

#[test]
fn any_other_describe_error_fails_the_call() {
    let error =
        ScramListing::from_describe(Some("UnknownServerError"), Vec::new()).expect_err("error");

    assert!(
        matches!(&error, KafkaError::Admin(message) if message == "UnknownServerError"),
        "{error:?}"
    );
}

fn listing() -> ScramListing {
    ScramListing::from_describe(
        None,
        vec![described(
            "alice",
            &[(WireMechanism::Sha256, 4096), (WireMechanism::Sha512, 8192)],
        )],
    )
    .expect("listing")
}

#[test]
fn a_listing_finds_the_iterations_of_one_users_credential() {
    let listing = listing();

    assert_eq!(
        listing.iterations("alice", ScramMechanism::Sha512),
        Some(8192)
    );
    assert_eq!(
        listing.iterations("alice", ScramMechanism::Sha256),
        Some(4096)
    );
    assert_eq!(listing.iterations("bob", ScramMechanism::Sha256), None);
    assert_eq!(
        ScramListing::Denied.iterations("alice", ScramMechanism::Sha256),
        None
    );
}

#[test]
fn a_user_without_a_mechanism_has_no_iterations_for_it() {
    let listing = ScramListing::from_describe(
        None,
        vec![described("bob", &[(WireMechanism::Sha512, 4096)])],
    )
    .expect("listing");

    assert_eq!(listing.iterations("bob", ScramMechanism::Sha256), None);
}

#[test]
fn each_mechanism_reads_as_its_sasl_name() {
    assert_eq!(ScramMechanism::Sha256.to_string(), "SCRAM-SHA-256");
    assert_eq!(ScramMechanism::Sha512.to_string(), "SCRAM-SHA-512");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn pencil(mechanism: ScramMechanism) -> NewScramCredential {
    NewScramCredential {
        user: "alice".to_owned(),
        mechanism,
        iterations: 4096,
        password: SecretString::from("pencil"),
    }
}

#[test]
fn a_password_is_salted_with_pbkdf2_over_the_mechanisms_hash() {
    for (mechanism, expected) in [
        (
            ScramMechanism::Sha256,
            "45b4c001e16908ebf7c5fd588dbafae4920ea80aed2819664fdc76842268cc2c",
        ),
        (
            ScramMechanism::Sha512,
            "3160839726bdf455bf7547ac071dffcb0ca70047613b1650b55e47c263067970\
             393ea81677a2896190024e87531f0b09da994075c8b01cd03509b63c77547a48",
        ),
    ] {
        let upsertion = pencil(mechanism).salted_with(Zeroizing::new(b"NaCl".to_vec()));

        assert_eq!(hex(&upsertion.salted_password), expected, "{mechanism}");
        assert_eq!(upsertion.name, "alice");
        assert_eq!(upsertion.mechanism, mechanism.wire());
        assert_eq!(upsertion.iterations, 4096);
        assert_eq!(*upsertion.salt, b"NaCl");
    }
}

#[test]
fn every_upsertion_draws_a_fresh_salt() {
    let credential = pencil(ScramMechanism::Sha256);

    let first = credential.upsertion();
    let second = credential.upsertion();

    assert_eq!(first.salt.len(), SALT_LEN);
    assert_ne!(first.salt, second.salt);
    assert_ne!(first.salted_password, second.salted_password);
}
