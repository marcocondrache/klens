use rdkafka::config::ClientConfig;

use crate::config::{ClusterConfig, Config, SaslConfig, SecurityConfig, TlsConfig};
use crate::environment::{
    API_VERSION_REQUEST_TIMEOUT_MS, CLIENT_ID_PREFIX, SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
};

/// Kafka client settings derived from a cluster config node.
#[derive(Debug, Clone)]
pub struct KafkaClusterConfig {
    client: ClientConfig,
}

impl KafkaClusterConfig {
    pub fn from_config(config: &Config) -> Vec<Self> {
        config.clusters.iter().map(Self::from).collect()
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub fn into_client_config(self) -> ClientConfig {
        self.client
    }
}

impl From<&ClusterConfig> for KafkaClusterConfig {
    fn from(cluster: &ClusterConfig) -> Self {
        let mut client = ClientConfig::new();
        client.set("bootstrap.servers", cluster.bootstrap_servers.join(","));
        client.set("client.id", format!("{CLIENT_ID_PREFIX}-{}", cluster.name));
        client.set(
            "socket.connection.setup.timeout.ms",
            SOCKET_CONNECTION_SETUP_TIMEOUT_MS.to_string(),
        );
        client.set(
            "api.version.request.timeout.ms",
            API_VERSION_REQUEST_TIMEOUT_MS.to_string(),
        );

        if let Some(security) = &cluster.security {
            apply_security(&mut client, security);
        }

        for (key, value) in &cluster.properties {
            client.set(key, value);
        }

        Self { client }
    }
}

fn apply_security(client: &mut ClientConfig, security: &SecurityConfig) {
    client.set("security.protocol", security.protocol.as_str());

    if let Some(sasl) = &security.sasl {
        apply_sasl(client, sasl);
    }

    if let Some(tls) = &security.tls {
        apply_tls(client, tls);
    }
}

fn apply_sasl(client: &mut ClientConfig, sasl: &SaslConfig) {
    client.set("sasl.mechanisms", sasl.mechanism.as_str());
    client.set("sasl.username", &sasl.username);
    client.set("sasl.password", &sasl.password);
}

fn apply_tls(client: &mut ClientConfig, tls: &TlsConfig) {
    if let Some(ca_cert) = &tls.ca_cert {
        client.set("ssl.ca.location", ca_cert.to_string_lossy().as_ref());
    }

    if let Some(client_cert) = &tls.client_cert {
        client.set(
            "ssl.certificate.location",
            client_cert.to_string_lossy().as_ref(),
        );
    }

    if let Some(client_key) = &tls.client_key {
        client.set("ssl.key.location", client_key.to_string_lossy().as_ref());
    }

    if tls.insecure_skip_verify {
        client.set("enable.ssl.certificate.verification", "false");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(yaml: &str) -> ClusterConfig {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn derives_plaintext_client_settings() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - broker-1:9092
              - broker-2:9092
            properties:
              request.timeout.ms: '10000'
            ",
        );

        let kafka = KafkaClusterConfig::from(&cluster);
        let client = kafka.client_config();

        assert_eq!(
            client.get("bootstrap.servers"),
            Some("broker-1:9092,broker-2:9092")
        );
        assert_eq!(client.get("client.id"), Some("klens-local"));
        assert_eq!(client.get("request.timeout.ms"), Some("10000"));
        assert_eq!(
            client.get("socket.connection.setup.timeout.ms"),
            Some("10000")
        );
        assert_eq!(client.get("api.version.request.timeout.ms"), Some("10000"));
        assert_eq!(client.get("security.protocol"), None);
    }

    #[test]
    fn derives_sasl_ssl_client_settings() {
        let cluster = cluster(
            "
            name: secure
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SASL_SSL
              sasl:
                mechanism: SCRAM-SHA-512
                username: admin
                password: secret
              tls:
                ca_cert: /etc/ca.pem
                client_cert: /etc/client.pem
                client_key: /etc/client.key
                insecure_skip_verify: true
            ",
        );

        let kafka = KafkaClusterConfig::from(&cluster);
        let client = kafka.client_config();

        assert_eq!(client.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(client.get("sasl.mechanisms"), Some("SCRAM-SHA-512"));
        assert_eq!(client.get("sasl.username"), Some("admin"));
        assert_eq!(client.get("sasl.password"), Some("secret"));
        assert_eq!(client.get("ssl.ca.location"), Some("/etc/ca.pem"));
        assert_eq!(
            client.get("ssl.certificate.location"),
            Some("/etc/client.pem")
        );
        assert_eq!(client.get("ssl.key.location"), Some("/etc/client.key"));
        assert_eq!(
            client.get("enable.ssl.certificate.verification"),
            Some("false")
        );
    }

    #[test]
    fn derives_each_cluster_from_root_config() {
        let config: Config = serde_yaml_ng::from_str(
            "
            bind: 127.0.0.1:8080
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: staging
                bootstrap_servers:
                  - staging:9092
            ",
        )
        .unwrap();

        let derived = KafkaClusterConfig::from_config(&config);
        assert_eq!(derived.len(), 2);
        assert_eq!(
            derived[0].client_config().get("client.id"),
            Some("klens-local")
        );
        assert_eq!(
            derived[1].client_config().get("client.id"),
            Some("klens-staging")
        );
    }

    #[test]
    fn cluster_properties_override_connection_timeouts() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            properties:
              socket.connection.setup.timeout.ms: '30000'
            ",
        );

        let kafka = KafkaClusterConfig::from(&cluster);
        assert_eq!(
            kafka
                .client_config()
                .get("socket.connection.setup.timeout.ms"),
            Some("30000")
        );
    }
}
