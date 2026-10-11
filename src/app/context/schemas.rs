use crate::app::auth::access::{
    DeleteSchemasCap, RegisterSchemasCap, SchemaTextCap, SetCompatibilityCap,
};
use crate::kafka::KafkaError;
use crate::kafka::model::{
    NewSchema, RegisteredSchema, RegisteredVersion, SchemaCompatibility, SchemaDeletion,
};

use super::Granted;

impl Granted<'_, SchemaTextCap> {
    pub(crate) async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.cluster.session.subject_schema(subject, version).await
    }
}

impl Granted<'_, RegisterSchemasCap> {
    pub(crate) async fn register_schema(
        &self,
        schema: &NewSchema,
    ) -> Result<RegisteredVersion, KafkaError> {
        let registered = self.cluster.session.register_schema(schema).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject = schema.subject.as_str(),
            id = registered.id,
            version = registered.version,
            "registered schema"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table
                .get(&schema.subject)
                .is_some_and(|info| info.versions.contains(&registered.version))
        })
        .await;
        Ok(registered)
    }
}

impl Granted<'_, DeleteSchemasCap> {
    pub(crate) async fn delete_schema(&self, deletion: &SchemaDeletion) -> Result<(), KafkaError> {
        let subject = deletion.subject.as_str();
        self.known_subject(subject, deletion.version)?;
        self.cluster.session.delete_schema(deletion).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject,
            version = deletion.version,
            permanent = deletion.permanent,
            "deleted schema"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table.get(subject).is_none_or(|info| {
                deletion
                    .version
                    .is_some_and(|version| !info.versions.contains(&version))
            })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, SetCompatibilityCap> {
    pub(crate) async fn set_compatibility(
        &self,
        subject: &str,
        level: SchemaCompatibility,
    ) -> Result<(), KafkaError> {
        self.known_subject(subject, None)?;
        self.cluster
            .session
            .set_compatibility(subject, level)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            subject,
            compatibility = ?level,
            "set schema compatibility"
        );
        self.settle(&self.cluster.store.subjects, |table| {
            table
                .get(subject)
                .is_some_and(|info| info.compatibility == level)
        })
        .await;
        Ok(())
    }
}
