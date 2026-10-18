use apache_avro::Schema as AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::ResolvedSchema;
use serde_json::Value;

pub(crate) struct AvroCodec {
    root: AvroSchema,
    named: Vec<AvroSchema>,
}

impl AvroCodec {
    pub(crate) fn compile(
        schema: &str,
        dependencies: &[(String, String)],
    ) -> Result<Self, apache_avro::Error> {
        let (root, named) =
            AvroSchema::parse_str_with_list(schema, dependencies.iter().map(|(_, body)| body))?;
        Ok(Self { root, named })
    }

    pub(crate) fn decode(&self, mut body: &[u8]) -> Result<Value, apache_avro::Error> {
        // The reader learns names front to back, so a schema follows those it references.
        let resolved =
            ResolvedSchema::new_with_schemata(self.named.iter().chain([&self.root]).collect())?;
        let value = GenericDatumReader::builder(&self.root)
            .resolved_writer_schemata(resolved)
            .build()?
            .read_value(&mut body)?;
        Value::try_from(value)
    }
}
