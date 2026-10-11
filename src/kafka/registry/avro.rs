//! A walk over an Avro datum before apache-avro decodes it.
//!
//! apache-avro recurses once per schema node with no depth limit, so a
//! recursive schema lets a few hundred bytes overflow the stack. It also
//! allocates one value per array item even when an item takes no bytes, so a
//! ten-byte record can claim millions of nulls. A stack overflow aborts the
//! process, so the walk reads the datum against its writer schema without
//! allocating and refuses one that nests too deep or holds too many values.

use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::schema::{Name, Namespace, ResolvedSchema};
use thiserror::Error;

/// Each level costs one frame of apache-avro's decoder, and a debug build
/// overflows a 2 MiB worker stack at about 120 of them.
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

/// A writer schema with every named type it can reach.
#[derive(Debug)]
pub(crate) struct AvroShape {
    root: Schema,
    names: HashMap<Name, Schema>,
}

impl AvroShape {
    /// `dependencies` must come in dependency order, as the registry client
    /// returns them.
    pub(crate) fn parse(schema: &str, dependencies: &[&str]) -> apache_avro::AvroResult<Self> {
        let (root, schemata) = Schema::parse_str_with_list(schema, dependencies)?;

        let mut all: Vec<&Schema> = schemata.iter().collect();
        all.push(&root);
        let names = ResolvedSchema::try_from(all)?
            .get_names()
            .iter()
            .map(|(name, schema)| (name.clone(), (*schema).clone()))
            .collect();

        Ok(Self { root, names })
    }

    /// Mirrors apache-avro's decoder, and is at least as lenient, so a datum it
    /// refuses is one apache-avro would fail on or should not be given.
    pub(crate) fn check(&self, datum: &[u8]) -> Result<(), ShapeError> {
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
            Schema::Boolean => {
                self.skip(1);
                Ok(())
            }
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
            Schema::Float => {
                self.skip(4);
                Ok(())
            }
            Schema::Double => {
                self.skip(8);
                Ok(())
            }
            Schema::Duration => {
                self.skip(12);
                Ok(())
            }
            Schema::Fixed(fixed) => {
                self.skip(fixed.size);
                Ok(())
            }
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

    /// apache-avro reads a truncated datum as if it ended in nulls, so a short
    /// read here is not an error either.
    fn skip(&mut self, count: usize) {
        self.rest = self.rest.get(count..).unwrap_or_default();
    }

    fn sized(&mut self) -> Result<(), ShapeError> {
        let length = usize::try_from(self.long()?).map_err(|_| ShapeError::Mismatch)?;
        self.skip(length);
        Ok(())
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

    /// A zigzag varint. A datum that ends inside one reads as zero, which ends
    /// any block or union it opens.
    fn long(&mut self) -> Result<i64, ShapeError> {
        let mut raw: u64 = 0;
        for (index, &byte) in self.rest.iter().enumerate() {
            if index == MAX_VARINT_BYTES {
                return Err(ShapeError::Mismatch);
            }
            raw |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                self.rest = &self.rest[index + 1..];
                return Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64));
            }
        }

        self.rest = &[];
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::types::Value;

    use super::*;

    /// Each level of the tree costs an array, a reference and a record.
    const TREE: &str = r#"{"type":"record","name":"Tree","fields":[{"name":"children","type":{"type":"array","items":"Tree"}}]}"#;

    fn shape(schema: &str) -> AvroShape {
        AvroShape::parse(schema, &[]).expect("a valid schema")
    }

    /// A chain of single children whose innermost array is empty.
    fn chain(levels: usize) -> Vec<u8> {
        let mut datum = vec![0x02; levels];
        datum.extend(std::iter::repeat_n(0x00, levels + 1));
        datum
    }

    #[test]
    fn a_recursive_value_is_refused_past_the_depth_limit() {
        let tree = shape(TREE);
        let deepest = (MAX_DEPTH - 1) / 3;

        assert_eq!(tree.check(&chain(deepest)), Ok(()));
        assert_eq!(tree.check(&chain(deepest + 1)), Err(ShapeError::TooDeep));
    }

    #[test]
    fn a_record_that_contains_itself_is_refused_without_reading_a_byte() {
        let endless = shape(
            r#"{"type":"record","name":"Endless","fields":[{"name":"next","type":"Endless"}]}"#,
        );

        assert_eq!(endless.check(&[]), Err(ShapeError::TooDeep));
    }

    #[test]
    fn empty_items_are_refused_past_the_value_budget() {
        let nulls = shape(r#"{"type":"array","items":"null"}"#);
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
    fn a_value_written_by_apache_avro_passes_the_walk() {
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

        assert_eq!(shape(schema).check(&datum), Ok(()));
    }

    #[test]
    fn a_reference_resolves_through_the_dependencies() {
        let status = r#"{"type":"enum","name":"Status","namespace":"common","symbols":["OPEN"]}"#;
        let order = r#"{"type":"record","name":"Order","fields":[
            {"name":"status","type":"common.Status"},
            {"name":"history","type":{"type":"array","items":"common.Status"}}]}"#;
        let shape = AvroShape::parse(order, &[status]).unwrap();

        assert_eq!(shape.check(&[0x00, 0x04, 0x00, 0x00, 0x00]), Ok(()));
    }
}
