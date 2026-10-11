//! Avro decoding that walks each datum before apache-avro decodes it.
//!
//! apache-avro recurses once per schema node with no depth limit, so a
//! recursive schema lets a few hundred bytes overflow the stack. It allocates
//! a schema's fixed size and one value per array item before reading them, so
//! a few bytes can claim a terabyte or millions of nulls. Either one aborts the
//! process, so the walk reads the datum against its writer schema first and
//! refuses one that nests too deep, holds too many values, or declares more
//! bytes than it carries.

use std::collections::{HashMap, HashSet};

use apache_avro::Schema;
use apache_avro::schema::{Name, Namespace, ResolvedSchema};
use thiserror::Error;

/// Each level costs one frame of apache-avro's decoder. A debug build
/// overflows a 2 MiB stack at about 90 of them, and a release build at about
/// 1,800.
const MAX_DEPTH: usize = 64;

/// Nulls, empty records and zero-size fixed values take no bytes, so a datum
/// may hold more values than bytes, but only by this factor.
const VALUES_PER_BYTE: usize = 4;

/// Lets a small record hold a few thousand empty values whatever its size.
const MIN_VALUES: usize = 4096;

/// The longest varint Avro writes for a 64-bit integer.
const MAX_VARINT_BYTES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ShapeError {
    #[error("the value nests deeper than {MAX_DEPTH} levels")]
    TooDeep,
    #[error("the value holds more than {limit} values in {bytes} bytes")]
    TooManyValues { limit: usize, bytes: usize },
    #[error("the value does not match its schema")]
    Mismatch,
}

#[derive(Debug, Error)]
pub(crate) enum AvroError {
    #[error(transparent)]
    Shape(#[from] ShapeError),
    #[error(transparent)]
    Decode(#[from] apache_avro::Error),
}

/// A writer schema, the schemas it depends on, and every named type they hold.
#[derive(Debug)]
pub(crate) struct AvroCodec {
    root: Schema,
    /// The dependencies in order, then the root.
    schemata: Vec<Schema>,
    names: HashMap<Name, Schema>,
}

impl AvroCodec {
    /// `dependencies` must come in dependency order, as the registry client
    /// returns them. A type that two subjects reference arrives twice, and
    /// apache-avro refuses a name defined twice, so an exact copy is dropped.
    pub(crate) fn parse(schema: &str, dependencies: &[&str]) -> apache_avro::AvroResult<Self> {
        let (root, mut schemata) = Schema::parse_str_with_list(schema, distinct(dependencies))?;
        schemata.push(root.clone());

        let names = ResolvedSchema::try_from(schemata.iter().collect::<Vec<_>>())?
            .get_names()
            .iter()
            .map(|(name, schema)| (name.clone(), (*schema).clone()))
            .collect();

        Ok(Self {
            root,
            schemata,
            names,
        })
    }

    pub(crate) fn decode(&self, datum: &[u8]) -> Result<serde_json::Value, AvroError> {
        self.check(datum)?;
        let value = apache_avro::from_avro_datum_schemata(
            &self.root,
            self.schemata.iter().collect(),
            &mut &*datum,
            None,
        )?;
        Ok(serde_json::Value::try_from(value)?)
    }

    fn check(&self, datum: &[u8]) -> Result<(), ShapeError> {
        let limit = datum
            .len()
            .saturating_mul(VALUES_PER_BYTE)
            .saturating_add(MIN_VALUES);
        let mut walk = Walk {
            names: &self.names,
            rest: datum,
            values_left: limit,
            limit,
            bytes: datum.len(),
        };

        walk.value(&self.root, &None, 0)
    }
}

fn distinct<'a>(sources: &[&'a str]) -> Vec<&'a str> {
    let mut seen = HashSet::new();
    sources
        .iter()
        .copied()
        .filter(
            |source| match serde_json::from_str::<serde_json::Value>(source) {
                Ok(json) => seen.insert(json),
                Err(_) => true,
            },
        )
        .collect()
}

/// Reads a datum the way apache-avro's decoder does, one schema node per
/// level, but builds nothing.
struct Walk<'a> {
    names: &'a HashMap<Name, Schema>,
    rest: &'a [u8],
    values_left: usize,
    limit: usize,
    bytes: usize,
}

impl Walk<'_> {
    fn value(
        &mut self,
        schema: &Schema,
        namespace: &Namespace,
        depth: usize,
    ) -> Result<(), ShapeError> {
        if depth > MAX_DEPTH {
            return Err(ShapeError::TooDeep);
        }
        self.values_left = self
            .values_left
            .checked_sub(1)
            .ok_or(ShapeError::TooManyValues {
                limit: self.limit,
                bytes: self.bytes,
            })?;

        match schema {
            Schema::Null => Ok(()),
            Schema::Boolean => self.take(1),
            Schema::Int
            | Schema::Long
            | Schema::Enum(_)
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => self.long().map(drop),
            Schema::Float => self.take(4),
            Schema::Double => self.take(8),
            Schema::Duration => self.take(12),
            Schema::Fixed(fixed) => self.take(fixed.size),
            Schema::Bytes | Schema::String | Schema::BigDecimal | Schema::Uuid => self.sized(),
            Schema::Decimal(decimal) => self.value(&decimal.inner, namespace, depth + 1),
            Schema::Array(array) => {
                self.blocks(|walk| walk.value(&array.items, namespace, depth + 1))
            }
            Schema::Map(map) => self.blocks(|walk| {
                walk.sized()?;
                walk.value(&map.types, namespace, depth + 1)
            }),
            Schema::Union(union) => {
                let index = usize::try_from(self.long()?).map_err(|_| ShapeError::Mismatch)?;
                let variant = union.variants().get(index).ok_or(ShapeError::Mismatch)?;
                self.value(variant, namespace, depth + 1)
            }
            Schema::Record(record) => {
                let name = record.name.fully_qualified_name(namespace);
                record
                    .fields
                    .iter()
                    .try_for_each(|field| self.value(&field.schema, &name.namespace, depth + 1))
            }
            Schema::Ref { name } => {
                let name = name.fully_qualified_name(namespace);
                let schema = self.names.get(&name).ok_or(ShapeError::Mismatch)?;
                self.value(schema, &name.namespace, depth + 1)
            }
        }
    }

    /// apache-avro allocates a declared length before it reads a byte, so one
    /// the rest of the datum cannot hold is refused first.
    fn take(&mut self, count: usize) -> Result<(), ShapeError> {
        self.rest = self.rest.get(count..).ok_or(ShapeError::Mismatch)?;
        Ok(())
    }

    fn sized(&mut self) -> Result<(), ShapeError> {
        let length = usize::try_from(self.long()?).map_err(|_| ShapeError::Mismatch)?;
        self.take(length)
    }

    /// Arrays and maps arrive as blocks of items, each led by its count and
    /// ended by a block of zero.
    fn blocks(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<(), ShapeError>,
    ) -> Result<(), ShapeError> {
        loop {
            let count = match self.long()? {
                0 => return Ok(()),
                count if count < 0 => {
                    self.long()?;
                    count.checked_neg().ok_or(ShapeError::Mismatch)?
                }
                count => count,
            };
            for _ in 0..count {
                item(self)?;
            }
        }
    }

    /// A zigzag varint.
    fn long(&mut self) -> Result<i64, ShapeError> {
        let mut raw: u64 = 0;
        for (index, &byte) in self.rest.iter().take(MAX_VARINT_BYTES).enumerate() {
            raw |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                self.rest = &self.rest[index + 1..];
                return Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64));
            }
        }

        Err(ShapeError::Mismatch)
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::types::Value;
    use serde_json::json;

    use super::*;

    /// Each level of the tree costs an array, a reference and a record.
    const TREE: &str = r#"{"type":"record","name":"Tree","fields":[{"name":"children","type":{"type":"array","items":"Tree"}}]}"#;

    fn codec(schema: &str) -> AvroCodec {
        AvroCodec::parse(schema, &[]).expect("a valid schema")
    }

    /// A chain of single children whose innermost array is empty.
    fn chain(levels: usize) -> Vec<u8> {
        let mut datum = vec![0x02; levels];
        datum.extend(std::iter::repeat_n(0x00, levels + 1));
        datum
    }

    #[test]
    fn a_recursive_value_decodes_up_to_the_depth_limit_and_no_further() {
        let tree = codec(TREE);
        let deepest = (MAX_DEPTH - 1) / 3;

        assert!(tree.decode(&chain(deepest)).is_ok());
        assert_eq!(tree.check(&chain(deepest + 1)), Err(ShapeError::TooDeep));
    }

    #[test]
    fn a_record_that_contains_itself_is_refused_without_reading_a_byte() {
        let endless = codec(
            r#"{"type":"record","name":"Endless","fields":[{"name":"next","type":"Endless"}]}"#,
        );

        assert_eq!(endless.check(&[]), Err(ShapeError::TooDeep));
    }

    #[test]
    fn empty_items_are_refused_past_the_value_budget() {
        let nulls = codec(r#"{"type":"array","items":"null"}"#);
        let claim = |count: i64| {
            let mut datum = Vec::new();
            let mut raw = ((count << 1) ^ (count >> 63)) as u64;
            while raw >= 0x80 {
                datum.push((raw as u8) | 0x80);
                raw >>= 7;
            }
            datum.push(raw as u8);
            datum.push(0x00);
            datum
        };

        assert_eq!(nulls.check(&claim(1_000)), Ok(()));
        assert_eq!(
            nulls.check(&claim(1_000_000_000)),
            Err(ShapeError::TooManyValues {
                limit: 6 * VALUES_PER_BYTE + MIN_VALUES,
                bytes: 6,
            })
        );
    }

    #[test]
    fn a_length_the_datum_cannot_hold_is_refused_before_it_is_allocated() {
        let huge = codec(r#"{"type":"fixed","name":"Huge","size":1099511627776}"#);
        let text = codec(r#""string""#);

        assert!(matches!(
            huge.decode(&[]),
            Err(AvroError::Shape(ShapeError::Mismatch))
        ));
        assert!(matches!(
            text.decode(&[0x80, 0x80, 0x80, 0x80, 0x04]),
            Err(AvroError::Shape(ShapeError::Mismatch))
        ));
    }

    #[test]
    fn a_value_written_by_apache_avro_decodes_through_the_walk() {
        let schema = r#"{
            "type": "record", "name": "Tree", "namespace": "com.example",
            "fields": [
                {"name": "label", "type": "string"},
                {"name": "weight", "type": "double"},
                {"name": "tags", "type": {"type": "map", "values": "long"}},
                {"name": "id", "type": {"type": "fixed", "name": "Id", "size": 4}},
                {"name": "parent", "type": ["null", "com.example.Tree"]},
                {"name": "children", "type": {"type": "array", "items": "Tree"}}
            ]
        }"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let tree = |label: &str, children: Vec<Value>| {
            Value::Record(vec![
                ("label".into(), Value::String(label.into())),
                ("weight".into(), Value::Double(0.5)),
                (
                    "tags".into(),
                    Value::Map([("depth".to_owned(), Value::Long(-300))].into()),
                ),
                ("id".into(), Value::Fixed(4, vec![1, 2, 3, 4])),
                ("parent".into(), Value::Union(0, Box::new(Value::Null))),
                ("children".into(), Value::Array(children)),
            ])
        };
        let datum = apache_avro::to_avro_datum(
            &parsed,
            tree(
                "root",
                vec![tree("a", vec![tree("a1", vec![])]), tree("b", vec![])],
            ),
        )
        .unwrap();

        let decoded = codec(schema).decode(&datum).unwrap();

        assert_eq!(decoded["children"][0]["children"][0]["label"], "a1");
        assert_eq!(decoded["tags"]["depth"], -300);
    }

    #[test]
    fn a_type_two_dependencies_share_resolves_once() {
        let status = r#"{"type":"enum","name":"Status","namespace":"common","symbols":["OPEN"]}"#;
        let order = r#"{"type":"record","name":"Order","fields":[
            {"name":"status","type":"common.Status"},
            {"name":"history","type":{"type":"array","items":"common.Status"}}]}"#;
        let codec = AvroCodec::parse(order, &[status, status]).unwrap();

        assert_eq!(
            codec.decode(&[0x00, 0x04, 0x00, 0x00, 0x00]).unwrap(),
            json!({"status": "OPEN", "history": ["OPEN", "OPEN"]})
        );
    }
}
