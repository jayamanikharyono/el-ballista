//! Logical-plan extension codec for Ballista.
//! distributed/table_codec.rs
//! The `rel distribute` client registers a `PostgresTableProvider` and runs
//! `DataFrame::create_physical_plan` locally — the physical plan is what gets shipped. But the
//! *logical* plan also travels (client → scheduler → planner), and Ballista's default logical
//! codec rejects provider nodes it doesn't recognize. This codec serializes our provider as JSON
//! so the scheduler can rebuild it (without opening a single connection) and plan the scan.
//! Non-Postgres nodes and providers fall back to `BallistaLogicalExtensionCodec`.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::connector::postgres::table_provider::{PostgresTableProvider, PostgresTableProviderModel};
use super::plan_codec::POSTGRES_SCAN_MAGIC;

#[derive(Debug)]
pub struct PostgresLogicalCodec {
    default: BallistaLogicalExtensionCodec,
}

impl Default for PostgresLogicalCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresLogicalCodec {
    pub fn new() -> Self {
        Self {
            default: BallistaLogicalExtensionCodec::default(),
        }
    }
}

impl LogicalExtensionCodec for PostgresLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> DataFusionResult<Extension> {
        self.default.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        self.default.try_encode(node, buf)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        if let Some(provider) = node.downcast_ref::<PostgresTableProvider>() {
            let model = provider.to_model();
            let bytes = serde_json::to_vec(&model).map_err(|e| {
                DataFusionError::External(
                    format!("cannot serialize PostgresTableProvider: {e}").into(),
                )
            })?;

            buf.extend_from_slice(POSTGRES_SCAN_MAGIC);
            buf.extend_from_slice(&bytes);
            log::debug!(
                "encoded PostgresTableProvider for {} ({} bytes payload)",
                table_ref,
                bytes.len()
            );

            Ok(())
        } else {
            self.default.try_encode_table_provider(table_ref, node, buf)
        }
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        if buf.starts_with(POSTGRES_SCAN_MAGIC) {
            let payload = &buf[POSTGRES_SCAN_MAGIC.len()..];
            let model: PostgresTableProviderModel =
                serde_json::from_slice(payload).map_err(|e| {
                    DataFusionError::External(
                        format!("cannot deserialize PostgresTableProvider: {e}").into(),
                    )
                })?;

            let provider = PostgresTableProvider::from_model(schema, model);
            log::debug!("decoded PostgresTableProvider for {}", table_ref);

            Ok(Arc::new(provider))
        } else {
            self.default.try_decode_table_provider(buf, table_ref, schema, ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::connection::PostgresConnectionDescriptor;
    use crate::types::table_metadata::TableMetadata;
    use datafusion::common::TableReference;

    fn test_model() -> PostgresTableProviderModel {
        PostgresTableProviderModel {
            descriptor: PostgresConnectionDescriptor {
                host: "db.internal".to_string(),
                port: 5433,
                user: "reader".to_string(),
                password_env: "READER_PW".to_string(),
                database: "app".to_string(),
                pool_max: 8,
                expected_workers: 2,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            table_metadata: TableMetadata {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![],
            },
            policy: crate::pushdown::PushdownPolicy::CostBased,
            deny: vec!["secret".to_string()],
            push: vec![],
            watermark_column: Some("updated_at".to_string()),
            window: None,
            batch_size: 1024,
            parallel_workers: 4,
            partition_column: Some("order_id".to_string()),
            strategy: crate::connector::postgres::parallel::ParallelStrategy::Keyset,
            enum_columns: vec!["status".to_string()],
        }
    }

    #[test]
    fn test_logical_codec_round_trip() {
        use arrow::datatypes::{DataType, Field, Schema};

        let codec = PostgresLogicalCodec::new();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            false,
        )]));
        // from_model needs no pool: decode-side providers resolve lazily.
        let provider = PostgresTableProvider::from_model(schema.clone(), test_model());
        let table_ref = TableReference::bare("orders");

        let mut buf = Vec::new();
        codec
            .try_encode_table_provider(&table_ref, Arc::new(provider), &mut buf)
            .unwrap();
        assert!(
            buf.starts_with(POSTGRES_SCAN_MAGIC),
            "our payload must carry the magic prefix"
        );

        let decoded = codec
            .try_decode_table_provider(&buf, &table_ref, schema, &TaskContext::default())
            .unwrap();
        let decoded = decoded
            .downcast_ref::<PostgresTableProvider>()
            .expect("decoded back into PostgresTableProvider");

        // Spot-check the model survived: descriptor, policy knobs, partitioning intent.
        let model = decoded.to_model();
        assert_eq!(model.descriptor.host, "db.internal");
        assert_eq!(model.descriptor.port, 5433);
        assert_eq!(model.deny, vec!["secret".to_string()]);
        assert_eq!(model.parallel_workers, 4);
        assert_eq!(model.batch_size, 1024);
        assert_eq!(model.enum_columns, vec!["status".to_string()]);
    }

    #[test]
    fn test_logical_codec_non_magic_delegates() {
        // Bytes without our magic prefix fall through to Ballista's default codec, which
        // must reject them as an error — never panic, never misdecode as ours.
        let codec = PostgresLogicalCodec::new();
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        let table_ref = TableReference::bare("orders");

        let err = codec
            .try_decode_table_provider(
                b"not-our-payload",
                &table_ref,
                schema,
                &TaskContext::default(),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("PostgresTableProvider"),
            "non-magic bytes must not reach our decoder, got: {msg}"
        );
    }
}