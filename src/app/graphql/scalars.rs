use juniper::GraphQLScalar;

/// Signed 64-bit integer, serialized as a string.
///
/// Offsets, watermarks, lag and retained counts routinely pass 2^53, where a
/// JSON number silently loses precision in every JavaScript client. A string
/// crosses the wire intact. Input accepts either a string or an `Int`.
#[derive(Clone, Copy, Debug, GraphQLScalar, PartialEq, Eq, PartialOrd, Ord)]
#[graphql(with = int64, parse_token(i32, String))]
pub struct Int64(i64);

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for Int64 {
    fn from(value: u64) -> Self {
        Self(value as i64)
    }
}

impl From<i32> for Int64 {
    fn from(value: i32) -> Self {
        Self(i64::from(value))
    }
}

impl From<Int64> for i64 {
    fn from(value: Int64) -> Self {
        value.0
    }
}

mod int64 {
    use juniper::{Scalar, ScalarValue};

    use super::Int64;

    pub(super) fn to_output(value: &Int64) -> String {
        value.0.to_string()
    }

    pub(super) fn from_input(value: &Scalar<impl ScalarValue>) -> Result<Int64, Box<str>> {
        if let Some(number) = value.try_to_int() {
            return Ok(Int64(i64::from(number)));
        }
        value
            .try_to::<&str>()
            .map_err(|error| error.to_string().into())
            .and_then(|text| {
                text.parse::<i64>()
                    .map(Int64)
                    .map_err(|error| format!("failed to parse `Int64`: {error}").into())
            })
    }
}

#[cfg(test)]
mod tests {
    use juniper::{FromInputValue as _, InputValue, ToInputValue as _, graphql_input_value};

    use super::Int64;

    #[test]
    fn large_values_round_trip_as_strings() {
        let offset = Int64::from(9_007_199_254_740_993_i64);

        let encoded: InputValue = offset.to_input_value();
        assert_eq!(encoded, graphql_input_value!("9007199254740993"));
        assert_eq!(Int64::from_input_value(&encoded).unwrap(), offset);
    }

    #[test]
    fn small_values_may_arrive_as_numbers() {
        let encoded: InputValue = graphql_input_value!(42);

        assert_eq!(Int64::from_input_value(&encoded).unwrap(), Int64::from(42));
    }

    #[test]
    fn a_non_numeric_string_is_rejected() {
        let encoded: InputValue = graphql_input_value!("not a number");

        assert!(Int64::from_input_value(&encoded).is_err());
    }
}
