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
