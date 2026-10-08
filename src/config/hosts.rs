use std::fmt::{self, Display, Formatter};
use std::net::{Ipv4Addr, Ipv6Addr};

use axum::http::uri::Authority;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use url::Host;

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "String")]
pub struct AllowedHost {
    host: String,
    port: Option<u16>,
}

impl AllowedHost {
    pub(super) fn loopback() -> Vec<Self> {
        ["localhost", "127.0.0.1", "[::1]"]
            .map(|host| Self {
                host: host.to_owned(),
                port: None,
            })
            .into()
    }

    pub fn allows(&self, authority: &Authority) -> bool {
        authority.host().eq_ignore_ascii_case(&self.host)
            && self
                .port
                .is_none_or(|port| authority.port_u16() == Some(port))
    }
}

impl TryFrom<String> for AllowedHost {
    type Error = String;

    fn try_from(entry: String) -> Result<Self, String> {
        if let Ok(address) = entry.parse::<Ipv6Addr>() {
            return Ok(Self {
                host: format!("[{address}]"),
                port: None,
            });
        }
        if entry.contains(['/', '@']) {
            return Err(format!(
                "'{entry}' must be a host and an optional port, with no scheme, path or user"
            ));
        }
        if entry.contains('*') {
            return Err(format!(
                "'{entry}' has a wildcard, but each host must be listed in full"
            ));
        }
        let (host, port) = match entry.rsplit_once(':') {
            Some((host, port)) if !entry.ends_with(']') => (host, Some(port)),
            _ => (entry.as_str(), None),
        };
        if host.contains(':') && !host.starts_with('[') {
            return Err(format!(
                "'{entry}' needs brackets around its IPv6 address, like [::1]:8080"
            ));
        }
        let port = port
            .map(str::parse)
            .transpose()
            .map_err(|_| format!("'{entry}' has an invalid port"))?;
        let parsed = Host::parse(host).map_err(|error| format!("'{entry}': {error}"))?;
        // URL parsing reads 10.1 as 10.0.0.1, so a dropped octet would
        // silently name another address.
        if matches!(parsed, Host::Ipv4(_)) && host.parse::<Ipv4Addr>().is_err() {
            return Err(format!(
                "'{entry}' must write its IPv4 address as four decimal numbers"
            ));
        }
        Ok(Self {
            host: parsed.to_string(),
            port,
        })
    }
}

impl Display for AllowedHost {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.port {
            Some(port) => write!(formatter, "{}:{port}", self.host),
            None => formatter.write_str(&self.host),
        }
    }
}

pub(super) fn at_least_one<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<AllowedHost>, D::Error> {
    let hosts = Vec::deserialize(deserializer)?;
    if hosts.is_empty() {
        return Err(D::Error::custom("must name at least one host"));
    }
    Ok(hosts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{yaml, yaml_err};

    #[test]
    fn an_entry_reads_as_a_browser_names_its_host() {
        let hosts: Vec<AllowedHost> = yaml(
            "[KLENS.Example.com, 'klens.example.com:8443', 127.0.0.1, '10.0.0.7:8080', \
             '::1', '[::1]', '[0:0::1]:8080', 'b\u{fc}cher.example']",
        );

        assert_eq!(
            hosts.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "klens.example.com",
                "klens.example.com:8443",
                "127.0.0.1",
                "10.0.0.7:8080",
                "[::1]",
                "[::1]",
                "[::1]:8080",
                "xn--bcher-kva.example",
            ]
        );
    }

    #[test]
    fn an_entry_that_names_no_single_host_is_refused() {
        for (entry, expected) in [
            (
                "'http://localhost'",
                "'http://localhost' must be a host and an optional port, with no scheme, path or user",
            ),
            (
                "'localhost/klens'",
                "'localhost/klens' must be a host and an optional port",
            ),
            (
                "'admin@localhost'",
                "'admin@localhost' must be a host and an optional port",
            ),
            (
                "'*.example.com'",
                "'*.example.com' has a wildcard, but each host must be listed in full",
            ),
            (
                "'fe80::1:65535'",
                "'fe80::1:65535' needs brackets around its IPv6 address, like [::1]:8080",
            ),
            ("'localhost:'", "'localhost:' has an invalid port"),
            ("'localhost:http'", "'localhost:http' has an invalid port"),
            ("'localhost:65536'", "'localhost:65536' has an invalid port"),
            ("'[::1]:'", "'[::1]:' has an invalid port"),
            ("'[::g]'", "'[::g]': invalid IPv6 address"),
            ("'256.0.0.1'", "'256.0.0.1': invalid IPv4 address"),
            (
                "'10.1'",
                "'10.1' must write its IPv4 address as four decimal numbers",
            ),
            (
                "'0x7f.0.0.1:8080'",
                "'0x7f.0.0.1:8080' must write its IPv4 address as four decimal numbers",
            ),
            (
                "'127.000.000.001'",
                "'127.000.000.001' must write its IPv4 address as four decimal numbers",
            ),
            (
                "'8080'",
                "'8080' must write its IPv4 address as four decimal numbers",
            ),
            (
                "'local host'",
                "'local host': invalid international domain name",
            ),
            ("''", "'': empty host"),
        ] {
            let error = yaml_err::<AllowedHost>(entry);
            assert!(error.starts_with(expected), "{entry}: {error}");
        }
    }

    #[test]
    fn a_host_matches_by_name_or_address_and_by_port_only_when_it_names_one() {
        for (entry, authority, expected) in [
            ("localhost", "localhost", true),
            ("localhost", "localhost:8080", true),
            ("localhost", "LocalHost:9000", true),
            ("localhost", "localhost.example", false),
            ("localhost", "127.0.0.1", false),
            ("'klens.example.com:8443'", "klens.example.com:8443", true),
            ("'klens.example.com:8443'", "klens.example.com:8080", false),
            ("'klens.example.com:8443'", "klens.example.com", false),
            ("'klens.example.com:8443'", "other.example.com:8443", false),
            ("'b\u{fc}cher.example'", "XN--BCHER-KVA.example", true),
            ("127.0.0.1", "127.0.0.1:8080", true),
            ("127.0.0.1", "127.1", false),
            ("'::1'", "[::1]:8080", true),
            ("'::1'", "[0:0::1]", false),
            ("'::1'", "[::2]", false),
            ("'[::1]:8080'", "[::1]:8080", true),
            ("'[::1]:8080'", "[::1]:8081", false),
        ] {
            let host: AllowedHost = yaml(entry);
            let authority: Authority = authority.parse().expect("an authority");

            assert_eq!(host.allows(&authority), expected, "{entry} for {authority}");
        }
    }
}
