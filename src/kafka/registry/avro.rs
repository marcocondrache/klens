use apache_avro::Schema as AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use ouroboros::self_referencing;
use serde_json::Value;

// The reader borrows the schemas it resolves, so the codec owns both to resolve once per schema.
#[self_referencing]
pub(crate) struct AvroCodec {
    root: AvroSchema,
    named: Vec<AvroSchema>,
    #[borrows(root, named)]
    #[covariant]
    reader: GenericDatumReader<'this>,
}

impl AvroCodec {
    pub(crate) fn compile(
        schema: &str,
        dependencies: &[(String, String)],
    ) -> Result<Self, apache_avro::Error> {
        let (root, named) =
            AvroSchema::parse_str_with_list(schema, dependencies.iter().map(|(_, body)| body))?;

        Self::try_new(root, named, |root, named| {
            // The reader learns names front to back, so a schema follows those it references.
            GenericDatumReader::builder(root)
                .writer_schemata(named.iter().chain([root]).collect())?
                .build()
        })
    }

    pub(crate) fn decode(&self, mut body: &[u8]) -> Result<Value, apache_avro::Error> {
        let value = self.borrow_reader().read_value(&mut body)?;
        Value::try_from(value)
    }
}
