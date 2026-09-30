use std::path::PathBuf;

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

/// A credential, read from where the config says it is: `{value: TEXT}`,
/// `{env: NAME}`, or `{file: PATH}`. A file's trailing newlines are dropped.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Source")]
pub struct Secret(SecretString);

/// Untagged so that a bare string, most likely the secret itself, fails with
/// the `expecting` text instead of being quoted back as an unknown variant.
#[derive(Deserialize)]
#[serde(
    untagged,
    deny_unknown_fields,
    expecting = "a secret source: {value: TEXT}, {env: NAME} or {file: PATH}"
)]
enum Source {
    Value { value: String },
    Env { env: String },
    File { file: PathBuf },
}

impl TryFrom<Source> for Secret {
    type Error = String;

    fn try_from(source: Source) -> Result<Self, String> {
        let text = match source {
            Source::Value { value } => value,
            Source::Env { env } => {
                std::env::var(&env).map_err(|error| format!("{env}: {error}"))?
            }
            Source::File { file } => std::fs::read_to_string(&file)
                .map_err(|error| format!("{}: {error}", file.display()))?
                .trim_end_matches(['\r', '\n'])
                .to_owned(),
        };
        Ok(Self(text.into()))
    }
}

impl ExposeSecret<str> for Secret {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

/// A [`Secret`] that keys an HMAC or signs cookies, used exactly as written.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Secret")]
pub struct KeyMaterial(Secret);

impl KeyMaterial {
    pub const MIN_BYTES: usize = 32;

    pub fn as_bytes(&self) -> &[u8] {
        self.0.expose_secret().as_bytes()
    }
}

impl TryFrom<Secret> for KeyMaterial {
    type Error = String;

    fn try_from(secret: Secret) -> Result<Self, String> {
        if secret.expose_secret().len() < Self::MIN_BYTES {
            return Err(format!("must be at least {} bytes", Self::MIN_BYTES));
        }
        Ok(Self(secret))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse;
    use crate::config::tests::TempFile;

    fn secret(source: &str) -> Result<String, String> {
        parse::<Secret>(source)
            .map(|secret| secret.expose_secret().to_owned())
            .map_err(|error| error.to_string())
    }

    #[test]
    fn a_secret_reads_from_its_value_env_or_file() {
        let file = TempFile::new("secret", "p@ss: *word #x\r\n\n");

        assert_eq!(secret("{value: 'hunter2 #tail'}").unwrap(), "hunter2 #tail");
        assert_eq!(
            secret("{env: CARGO_PKG_NAME}").unwrap(),
            env!("CARGO_PKG_NAME")
        );
        assert_eq!(
            secret(&format!("{{file: '{}'}}", file.0.display())).unwrap(),
            "p@ss: *word #x"
        );
    }

    #[test]
    fn a_source_that_cannot_be_read_names_itself() {
        assert_eq!(
            secret("{env: KLENS_TEST_UNSET}").unwrap_err(),
            "KLENS_TEST_UNSET: environment variable not found"
        );
        assert_eq!(
            secret("{file: /nonexistent/klens}").unwrap_err(),
            "/nonexistent/klens: No such file or directory (os error 2)"
        );
    }

    #[test]
    fn anything_but_one_source_is_rejected_without_echoing_it() {
        for source in [
            "hunter2",
            "[hunter2]",
            "{value: hunter2, env: HUNTER2}",
            "{password: hunter2}",
        ] {
            assert_eq!(
                secret(source).unwrap_err(),
                "a secret source: {value: TEXT}, {env: NAME} or {file: PATH}",
            );
        }
    }

    #[test]
    fn debug_output_hides_the_secret() {
        let secret: Secret = parse("{value: hunter2}").unwrap();
        let key: KeyMaterial = parse(&format!("{{value: {}}}", "k".repeat(32))).unwrap();

        assert_eq!(format!("{secret:?}"), "Secret(SecretBox<str>([REDACTED]))");
        assert!(!format!("{key:?}").contains('k'), "{key:?}");
    }

    #[test]
    fn key_material_is_used_as_written_from_thirty_two_bytes() {
        let key = |text: String| parse::<KeyMaterial>(&format!("{{value: '{text}'}}"));
        let written = format!(" {}=", "k".repeat(30));

        assert_eq!(key(written.clone()).unwrap().as_bytes(), written.as_bytes());
        assert_eq!(
            key("k".repeat(31)).unwrap_err().to_string(),
            "must be at least 32 bytes"
        );
    }
}
