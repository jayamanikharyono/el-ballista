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