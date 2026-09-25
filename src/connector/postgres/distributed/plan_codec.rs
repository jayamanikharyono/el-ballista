//! Physical-plan extension codec for Ballista.
//! distributed/plan_codec.rs
//! Postgres has no Text/Wire serialization for a live `PgPool`, so a scan plan can't travel
//! from scheduler to executor as code. Instead `PostgresExecutionPlan` is carried as a JSON
//! payload (the descriptor — never a password), and each executor materializes it back into a
//! plan that resolves the process-shared pool on first execute. Overshadowing the default
//! Ballista codec REPLACES it, so every non-Postgres node (shuffle nodes, `UnknownExec`, ...)
//! must be delegated back to `BallistaPhysicalExtensionCodec`. A magic prefix disambiguates the
//! JSON payload from Ballista's own protobuf framing.

use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::connector::postgres::execution_plan::{
    PostgresExecutionPlan, PostgresExecutionPlanModel,
};

/// Distinguishes our JSON payload from anything `BallistaPhysicalExtensionCodec` would decode.
pub const POSTGRES_SCAN_MAGIC: &[u8] = b"PGSC01\0";

#[derive(Debug)]
pub struct PostgresPhysicalCodec {
    default: BallistaPhysicalExtensionCodec,
}

impl Default for PostgresPhysicalCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresPhysicalCodec {
    /// A codec that carries `PostgresExecutionPlan` as a JSON payload and delegates every other
    /// node to Ballista's default physical codec.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    /// use rust_ballista_extraction_layer::connector::postgres::distributed::PostgresPhysicalCodec;
    ///
    /// // Handed to Ballista (scheduler/executor config) in place of its default codec.
    /// let codec: Arc<dyn PhysicalExtensionCodec> = Arc::new(PostgresPhysicalCodec::new());
    /// # let _ = codec;
    /// ```
    pub fn new() -> Self {
        Self {
            default: BallistaPhysicalExtensionCodec::default(),
        }
    }
}

impl PhysicalExtensionCodec for PostgresPhysicalCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        if let Some(plan) = node.downcast_ref::<PostgresExecutionPlan>() {
            let model = plan.to_model()?;
            let bytes = serde_json::to_vec(&model).map_err(|e| {
                DataFusionError::External(
                    format!("cannot serialize PostgresExecutionPlan: {e}").into(),
                )
            })?;

            buf.extend_from_slice(POSTGRES_SCAN_MAGIC);
            buf.extend_from_slice(&bytes);
            log::debug!(
                "encoded PostgresExecutionPlan ({} bytes payload)",
                bytes.len()
            );

            Ok(())
        } else {
            self.default.try_encode(node, buf)
        }
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if buf.starts_with(POSTGRES_SCAN_MAGIC) {
            let payload = &buf[POSTGRES_SCAN_MAGIC.len()..];
            let model: PostgresExecutionPlanModel =
                serde_json::from_slice(payload).map_err(|e| {
                    DataFusionError::External(
                        format!("cannot deserialize PostgresExecutionPlan: {e}").into(),
                    )
                })?;

            let plan = PostgresExecutionPlan::from_model(model)?;
            Ok(Arc::new(plan))
        } else {
            self.default.try_decode(buf, inputs, ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::table_metadata::TableMetadata;

    fn descriptor()
    -> crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor {
        crate::connector::postgres::distributed::connection::PostgresConnectionDescriptor {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: "PGPASSWORD".to_string(),
            database: "db".to_string(),
            pool_max: 8,
            expected_workers: 2,
            statement_timeout_ms: 1000,
            application_name: "test".to_string(),
            schema: "public".to_string(),
        }
    }

    #[test]
    fn test_round_trip() -> DataFusionResult<()> {
        let model = PostgresExecutionPlanModel {
            descriptor: descriptor(),
            table_metadata: TableMetadata {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![],
            },
            pushed_filters: vec![],
            pushed_limit: None,
            batch_size: 8192,
            use_copy: false,
            copy_statement_timeout_ms: None,
            max_batch_bytes: 16 * 1024 * 1024,
            partitions: vec![],
            run_id: "r_test".to_string(),
        };

        let codec = PostgresPhysicalCodec::new();

        let mut buf = Vec::new();
        let plan = PostgresExecutionPlan::from_model(model)?;
        codec.try_encode(Arc::new(plan), &mut buf)?;

        assert!(buf.starts_with(POSTGRES_SCAN_MAGIC));

        let decoded = codec.try_decode(&buf, &[], &TaskContext::default())?;
        let decoded = decoded
            .downcast_ref::<PostgresExecutionPlan>()
            .expect("decoded back into PostgresExecutionPlan");

        assert_eq!(decoded.schema().fields.len(), 0);
        assert_eq!(
            decoded.schema().metadata.get("name"),
            None,
            "no field-level metadata expected on an empty schema"
        );

        Ok(())
    }
}
