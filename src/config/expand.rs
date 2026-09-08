use std::borrow::Cow;
use std::env::VarError;

pub(crate) fn expand(raw: &str) -> Result<String, shellexpand::LookupError<VarError>> {
    expand_with(raw, |name| std::env::var(name).map(Some))
}

pub(crate) fn expand_with<C, T, E>(
    raw: &str,
    lookup: C,
) -> Result<String, shellexpand::LookupError<E>>
where
    C: FnMut(&str) -> Result<Option<T>, E>,
    T: AsRef<str>,
{
    shellexpand::env_with_context(raw, lookup).map(Cow::into_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn expand_map(
        raw: &str,
        vars: &[(&str, &str)],
    ) -> Result<String, shellexpand::LookupError<VarError>> {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        expand_with(raw, |name| match vars.get(name) {
            Some(value) => Ok(Some(*value)),
            None => Err(VarError::NotPresent),
        })
    }

    #[test]
    fn expands_braced_variable() {
        assert_eq!(
            expand_map(
                "password: ${KAFKA_PASSWORD}",
                &[("KAFKA_PASSWORD", "s3cret")]
            )
            .unwrap(),
            "password: s3cret"
        );
    }

    #[test]
    fn expands_unbraced_variable() {
        assert_eq!(
            expand_map("password: $KAFKA_PASSWORD", &[("KAFKA_PASSWORD", "s3cret")]).unwrap(),
            "password: s3cret"
        );
    }

    #[test]
    fn expands_default_when_unset() {
        assert_eq!(
            expand_map("log_level: ${KLENS_LOG_LEVEL:-info}", &[]).unwrap(),
            "log_level: info"
        );
    }

    #[test]
    fn prefers_set_value_over_default() {
        assert_eq!(
            expand_map(
                "log_level: ${KLENS_LOG_LEVEL:-info}",
                &[("KLENS_LOG_LEVEL", "debug")]
            )
            .unwrap(),
            "log_level: debug"
        );
    }

    #[test]
    fn errors_on_missing_required_variable() {
        let error = expand_map("password: ${KAFKA_PASSWORD}", &[]).unwrap_err();
        assert_eq!(error.var_name, "KAFKA_PASSWORD");
        assert_eq!(error.cause, VarError::NotPresent);
        let message = error.to_string();
        assert!(message.contains("KAFKA_PASSWORD"));
        assert!(!message.contains("s3cret"));
    }

    #[test]
    fn doubles_dollar_escape_to_literal() {
        assert_eq!(expand_map("cost: $$5", &[]).unwrap(), "cost: $5");
    }

    #[test]
    fn leaves_non_variable_dollar_signs() {
        assert_eq!(expand_map("cost: $%", &[]).unwrap(), "cost: $%");
    }
}
