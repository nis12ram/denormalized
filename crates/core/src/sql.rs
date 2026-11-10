//! SQL interface for denormalized streaming tables.
//!
//! The interface currently supports a single `CREATE TABLE` statement, which
//! registers a streaming source table backed by a connector.

pub mod table;

use datafusion::sql::sqlparser::{ast, dialect::AnsiDialect, parser::Parser};
use denormalized_common::{error::Result, DenormalizedError};

use crate::context::Context;
use crate::datastream::DataStream;
use crate::sql::table::create_table;

const DENORMALIZED_SQL_DIALECT: AnsiDialect = AnsiDialect {};

/// Parses and executes a single SQL statement against `ctx`.
///
/// The returned [`DataStream`] represents the statement output. For the
/// currently supported `CREATE TABLE` statement, the returned stream is empty
/// because the statement only registers a source table.
///
/// # Errors
///
/// Returns an error if:
///
/// - the SQL cannot be parsed,
/// - more than one statement, or no statement, is supplied,
/// - the statement is not a supported `CREATE TABLE`,
/// - connector resolution or source creation fails, or
/// - table registration fails.
pub async fn execute_sql(ctx: &Context, sql: &str) -> Result<DataStream> {
    let statements = Parser::parse_sql(&DENORMALIZED_SQL_DIALECT, sql)
        .map_err(|err| DenormalizedError::SQL(format!("unable to parse SQL: '{err}'")))?;

    if statements.len() > 1 {
        return Err(DenormalizedError::SQL(
            "only supports a single SQL statement".to_string(),
        ));
    }

    let Some(statement) = statements.into_iter().next() else {
        return Err(DenormalizedError::SQL(
            "single SQL statement is required".to_string(),
        ));
    };

    let ds = match statement {
        ast::Statement::CreateTable(table) => create_table(ctx, table).await?,
        other => {
            return Err(DenormalizedError::SQL(format!(
                "only CREATE TABLE is supported: '{other}'"
            )))
        }
    };

    Ok(ds)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::datasource::{empty::EmptyTable, TableProvider};

    use crate::connectors::{Connector, ConnectorRegistry, CreateSourceRequest};

    use super::*;

    const CREATE_MOCK_TABLE: &str =
        "CREATE TABLE test_table (id BIGINT, name STRING) WITH (connector = 'mock')";

    #[derive(Debug)]
    struct MockConnector;

    #[async_trait]
    impl Connector for MockConnector {
        fn name(&self) -> &'static str {
            "mock"
        }

        async fn create_source(
            &self,
            request: CreateSourceRequest,
        ) -> Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(EmptyTable::new(request.table_schema)))
        }
    }

    fn mock_context() -> Context {
        let mut ctx = Context::new().unwrap();
        ctx.connectors = Arc::new(ConnectorRegistry::with_test_connector(Arc::new(
            MockConnector,
        )));
        ctx
    }

    #[tokio::test]
    async fn executes_create_table_with_mock_connector() {
        let ctx = mock_context();

        execute_sql(&ctx, CREATE_MOCK_TABLE).await.unwrap();

        assert!(ctx.session_context.table_exist("test_table").unwrap());

        let provider = ctx
            .session_context
            .table_provider("test_table")
            .await
            .unwrap();
        assert_eq!(provider.schema().field(0).name(), "id");
        assert_eq!(provider.schema().field(1).name(), "name");
    }

    #[tokio::test]
    async fn create_table_if_not_exists_is_idempotent() {
        let ctx = mock_context();

        execute_sql(&ctx, CREATE_MOCK_TABLE).await.unwrap();
        execute_sql(
            &ctx,
            "CREATE TABLE IF NOT EXISTS test_table (id BIGINT) WITH (connector = 'mock')",
        )
        .await
        .unwrap();

        assert!(ctx.session_context.table_exist("test_table").unwrap());
    }

    #[tokio::test]
    async fn create_table_rejects_duplicate_table() {
        let ctx = mock_context();

        execute_sql(&ctx, CREATE_MOCK_TABLE).await.unwrap();

        assert!(matches!(
            execute_sql(
                &ctx,
                "CREATE TABLE test_table (id BIGINT) WITH (connector = 'mock')"
            )
            .await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn create_table_rejects_unknown_connector() {
        let ctx = Context::new().unwrap();

        assert!(matches!(
            execute_sql(
                &ctx,
                "CREATE TABLE test_table (id BIGINT) WITH (connector = 'unknown')"
            )
            .await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_table_requires_connector_option() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(
                &ctx,
                "CREATE TABLE test_table (id BIGINT) WITH (topic = 'test')"
            )
            .await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_table_requires_with_clause() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(&ctx, "CREATE TABLE test_table (id BIGINT)").await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn create_table_rejects_create_table_as_select() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(&ctx, "CREATE TABLE test_table AS SELECT 1").await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn execute_sql_rejects_unsupported_statement() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(&ctx, "SELECT 1").await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn execute_sql_rejects_multiple_statements() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(
                &ctx,
                "CREATE TABLE first_table (id BIGINT) WITH (connector = 'mock'); \
                 CREATE TABLE second_table (id BIGINT) WITH (connector = 'mock');"
            )
            .await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn execute_sql_rejects_invalid_sql() {
        let ctx = mock_context();

        assert!(matches!(
            execute_sql(&ctx, "THIS IS NOT SQL").await,
            Err(DenormalizedError::SQL(_))
        ));
    }
}
