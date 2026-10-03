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
    use crate::testing::{temp_file, yaml, yaml_err};

    fn exposed(source: &str) -> String {
        yaml::<Secret>(source).expose_secret().to_owned()
    }

    #[test]
    fn a_secret_reads_from_its_value_env_or_file() {
        let file = temp_file("p@ss: *word #x\r\n\n");

        assert_eq!(exposed("{value: 'hunter2 #tail'}"), "hunter2 #tail");
        assert_eq!(exposed("{env: CARGO_PKG_NAME}"), env!("CARGO_PKG_NAME"));
        assert_eq!(
            exposed(&format!("{{file: '{}'}}", file.path().display())),
            "p@ss: *word #x"
        );
    }

    #[test]
    fn a_source_that_cannot_be_read_names_itself() {
        assert_eq!(
            yaml_err::<Secret>("{env: KLENS_TEST_UNSET}"),
            "KLENS_TEST_UNSET: environment variable not found"
        );
        assert_eq!(
            yaml_err::<Secret>("{file: /nonexistent/klens}"),
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
                yaml_err::<Secret>(source),
                "a secret source: {value: TEXT}, {env: NAME} or {file: PATH}",
            );
        }
    }

    #[test]
    fn debug_output_hides_the_secret() {
        let secret: Secret = yaml("{value: hunter2}");
        let key: KeyMaterial = yaml(&format!("{{value: {}}}", "k".repeat(32)));

        assert_eq!(format!("{secret:?}"), "Secret(SecretBox<str>([REDACTED]))");
        assert!(!format!("{key:?}").contains('k'), "{key:?}");
    }

    #[test]
    fn key_material_is_used_as_written_from_thirty_two_bytes() {
        let source = |text: &str| format!("{{value: '{text}'}}");
        let written = format!(" {}=", "k".repeat(30));

        let key: KeyMaterial = yaml(&source(&written));
        assert_eq!(key.as_bytes(), written.as_bytes());
        assert_eq!(
            yaml_err::<KeyMaterial>(&source(&"k".repeat(31))),
            "must be at least 32 bytes"
        );
    }
}
