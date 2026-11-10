use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::{
    config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv,
    session_state::SessionStateBuilder,
};

use crate::config_extensions::denormalized_config::DenormalizedConfig;
use crate::connectors::kafka::TopicReader;
use crate::connectors::ConnectorRegistry;
use crate::datastream::DataStream;
use crate::physical_optimizer::EnsureHashPartititionOnGroupByForStreamingAggregates;
use crate::query_planner::StreamingQueryPlanner;
use crate::sql::execute_sql;
use crate::state_backend::slatedb::initialize_global_slatedb;
use crate::utils::get_default_optimizer_rules;

use denormalized_common::error::{DenormalizedError, Result};

/// Thread-safe context acting as a controller.
///
/// # Note
///
/// 1. Currently serves as a bootstrap controller, not the intended final model.
/// 2. Custom connector registration may be supported through this type in the
///    future, but is currently not supported.
#[derive(Clone)]
pub struct Context {
    pub session_context: Arc<SessionContext>,
    /// Registry used to resolve connectors referenced by SQL statements.
    pub connectors: Arc<ConnectorRegistry>,
}

impl Context {
    pub fn default_config() -> SessionConfig {
        let ext_config = DenormalizedConfig::default();
        let mut config = SessionConfig::new()
            .set(
                "datafusion.execution.batch_size",
                &datafusion::common::ScalarValue::UInt64(Some(32)),
            )
            // coalesce_batches slows down the pipeline and increases latency as it tries to concat
            // small batches together so we disable it.
            .set(
                "datafusion.execution.coalesce_batches",
                &datafusion::common::ScalarValue::Boolean(Some(false)),
            );

        config.options_mut().extensions.insert(ext_config);
        config
    }

    pub fn new() -> Result<Self, DenormalizedError> {
        Context::with_config(Context::default_config())
    }

    pub fn with_config(config: SessionConfig) -> Result<Self, DenormalizedError> {
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_query_planner(Arc::new(StreamingQueryPlanner {}))
            .with_optimizer_rules(get_default_optimizer_rules())
            .with_physical_optimizer_rule(Arc::new(
                EnsureHashPartititionOnGroupByForStreamingAggregates::new(),
            ))
            .build();

        Ok(Self {
            session_context: Arc::new(SessionContext::new_with_state(state)),
            connectors: Arc::new(ConnectorRegistry::with_builtins()),
        })
    }

    pub async fn from_topic(&self, topic: TopicReader) -> Result<DataStream, DenormalizedError> {
        let topic_name = topic.0.topic.clone();
        self.register_table(topic_name.clone(), Arc::new(topic))
            .await?;
        let df = self.session_context.table(topic_name.as_str()).await?;
        let ds = DataStream::new(Arc::new(df), Arc::new(self.clone()));
        Ok(ds)
    }

    /// Creates a [`DataStream`] from a [`SessionContext`] registered table.
    ///
    /// # Errors
    ///
    /// Returns an error if table is not registered.
    pub async fn from_table(&self, table: &str) -> Result<DataStream, DenormalizedError> {
        let df = self.session_context.table(table).await?;
        let ds = DataStream::new(Arc::new(df), Arc::new(self.clone()));
        Ok(ds)
    }

    pub async fn register_table(
        &self,
        name: String,
        table: Arc<impl TableProvider + 'static>,
    ) -> Result<(), DenormalizedError> {
        self.session_context
            .register_table(name.as_str(), table.clone())?;

        Ok(())
    }

    pub async fn with_slatedb_backend(self, path: String) -> Self {
        let _ = initialize_global_slatedb(path.as_str()).await;
        self
    }

    /// Executes a single SQL statement against this [`Context`].
    ///
    /// Returns a [`DataStream`] representing the query output. For DDL
    /// statements such as `CREATE TABLE`, the returned stream is empty because
    /// the statement only registers a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be parsed, the statement is not
    /// supported, connector resolution or source creation fails, or table
    /// registration fails.
    pub async fn execute_sql(&self, sql: &str) -> Result<DataStream> {
        execute_sql(self, sql).await
    }
}
