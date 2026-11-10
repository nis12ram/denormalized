//! `CREATE TABLE` handling for the SQL interface.
//!
//! This module converts a parsed `CREATE TABLE` statement into an Arrow
//! schema and a connector-backed source table, then registers that table in
//! the [`Context`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use datafusion::{dataframe::DataFrame, logical_expr::LogicalPlanBuilder, sql::sqlparser::ast};
use denormalized_common::{error::Result, DenormalizedError};

use crate::{
    connectors::{ConnectorOptions, CreateSourceRequest},
    context::Context,
    datastream::DataStream,
};

/// Table options as key-value pairs, specified in the `WITH` clause.
///
/// Currently all options are connector options. Non-connector table options
/// are reserved for future table models.
#[derive(Debug, Clone)]
struct TableOptions {
    connector: ConnectorOptions,
    // TODO: Introduce non-connector table options when needed by future models.
}

impl TableOptions {
    /// Creates [`TableOptions`] from a slice of [`ast::SqlOption`].
    ///
    /// # Errors
    ///
    /// Returns an error if an option is not a key-value pair or if an option
    /// value is not a supported literal.
    fn new(with_opts: &[ast::SqlOption]) -> Result<Self> {
        let mut connector_opts = HashMap::new();

        for opt in with_opts {
            let ast::SqlOption::KeyValue { key, value } = opt else {
                return Err(DenormalizedError::SQL(format!(
                    "invalid with option: '{opt}'; expected an `=` delimited key-value pair"
                )));
            };

            let key = key.value.clone();
            let value = sql_expr_to_string(value)?;

            connector_opts.insert(key, value);
        }

        Ok(Self {
            connector: ConnectorOptions::new(connector_opts),
        })
    }
}

/// Creates a non-persistent streaming source table.
///
/// This is a lightweight ingress point: it only defines how to connect to an
/// external system and does not store any data. Optional persistence is a
/// desired future model, not yet implemented.
///
/// The table is registered in `ctx` under the name from the `CREATE TABLE`
/// statement. If the table already exists and `IF NOT EXISTS` was supplied,
/// this returns an empty [`DataStream`].
///
/// # Errors
///
/// Returns an error if:
///
/// - the statement uses an unsupported `CREATE TABLE` form,
/// - the table already exists without `IF NOT EXISTS`,
/// - no columns are specified,
/// - the `WITH` clause is missing or malformed,
/// - the connector is unknown,
/// - the connector fails to create the source provider, or
/// - table registration fails.
pub(super) async fn create_table(ctx: &Context, table: ast::CreateTable) -> Result<DataStream> {
    if table.or_replace
        || table.temporary
        || table.external
        || table.dynamic
        || table.transient
        || table.volatile
        || table.iceberg
        || table.snapshot
        || table.query.is_some()
        || table.like.is_some()
        || table.clone.is_some()
    {
        return Err(DenormalizedError::SQL(
            "only 'CREATE TABLE' is supported".to_string(),
        ));
    }

    let name = last_name(&table.name)?;
    if ctx.session_context.table_exist(&name)? {
        if table.if_not_exists {
            return create_empty_datastream(ctx);
        }

        return Err(DenormalizedError::SQL(format!(
            "table '{name}' already exists",
        )));
    }

    let schema = Arc::new(build_schema_from_sql_cols(&table.columns)?);

    let with_options =
        match &table.table_options {
            ast::CreateTableOptions::None => {
                return Err(DenormalizedError::SQL(
                    "'CREATE TABLE' require connector options, specified using the `WITH` keyword"
                        .to_string(),
                ))
            }
            ast::CreateTableOptions::With(with_options) => with_options.as_slice(),
            _ => return Err(DenormalizedError::SQL(
                "'CREATE TABLE' only accepts connector options specified using the `WITH` keyword"
                    .to_string(),
            )),
        };

    let table_options = TableOptions::new(with_options)?;
    let mut connector_options = table_options.connector;

    let connector_name = connector_options.pull_str("connector")?;
    let connector = ctx.connectors.resolve(&connector_name)?;
    let provider = connector
        .create_source(CreateSourceRequest {
            table_name: name.clone(),
            table_schema: schema,
            connector_options,
        })
        .await?;

    ctx.session_context.register_table(&name, provider)?;

    create_empty_datastream(ctx)
}

/// Converts a simple SQL value used in `WITH (...)` into a plain string.
///
/// # Errors
///
/// Returns an error if `expr` is not a single-quoted string, number, or boolean
/// literal.
fn sql_expr_to_string(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Value(ast::ValueWithSpan { value, .. }) => match value {
            ast::Value::SingleQuotedString(value) => Ok(value.clone()),
            ast::Value::Number(value, _) => Ok(value.to_string()),
            ast::Value::Boolean(value) => Ok(value.to_string()),
            other => Err(DenormalizedError::SQL(format!(
                "unsupported value in WITH option: '{other}'; expected a single-quoted string, number, or boolean literal"
            ))),
        },
        other => Err(DenormalizedError::SQL(format!(
            "unsupported value in WITH option: '{other}'; expected a single-quoted string, number, or boolean literal"
        ))),
    }
}

/// Returns the last identifier from an [`ast::ObjectName`].
///
/// # Errors
///
/// Returns an error if `name` is empty or its last component is not an
/// identifier.
fn last_name(name: &ast::ObjectName) -> Result<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .ok_or_else(|| DenormalizedError::SQL("empty table name".to_string()))
}

/// Builds an Arrow [`Schema`] from SQL column definitions.
///
/// # Errors
///
/// Returns an error if `cols` is empty or if a column uses an unsupported SQL
/// data type.
fn build_schema_from_sql_cols(cols: &[ast::ColumnDef]) -> Result<Schema> {
    if cols.is_empty() {
        return Err(DenormalizedError::SQL(
            "'CREATE TABLE' requires at least one column".to_string(),
        ));
    }

    let mut fields = Vec::new();
    for column in cols {
        let nullable = !column
            .options
            .iter()
            .any(|option| matches!(option.option, ast::ColumnOption::NotNull));
        let data_type = arrow_type_from_sql_type(&column.data_type)?;
        fields.push(Field::new(column.name.value.clone(), data_type, nullable));
    }

    Ok(Schema::new(fields))
}

/// Converts a SQL data type into an Arrow data type.
///
/// # Errors
///
/// Returns an error if `data_type` is not supported by the SQL interface.
fn arrow_type_from_sql_type(data_type: &ast::DataType) -> Result<ArrowDataType> {
    let result = match data_type {
        ast::DataType::TinyInt(_) => ArrowDataType::Int8,
        ast::DataType::SmallInt(_) => ArrowDataType::Int16,
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::MediumInt(_) => {
            ArrowDataType::Int32
        }
        ast::DataType::BigInt(_) => ArrowDataType::Int64,
        ast::DataType::Boolean | ast::DataType::Bool => ArrowDataType::Boolean,
        ast::DataType::Varchar(_)
        | ast::DataType::Text
        | ast::DataType::String(_)
        | ast::DataType::CharacterVarying(_)
        | ast::DataType::CharVarying(_)
        | ast::DataType::Character(_)
        | ast::DataType::Char(_) => ArrowDataType::Utf8,
        ast::DataType::Double(_) | ast::DataType::DoublePrecision => ArrowDataType::Float64,
        ast::DataType::Float(_) | ast::DataType::Real => ArrowDataType::Float32,
        ast::DataType::Date | ast::DataType::Date32 => ArrowDataType::Date32,
        ast::DataType::Datetime(_) => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        other => {
            return Err(DenormalizedError::SQL(format!(
                "column type '{other}' is not supported"
            )));
        }
    };
    Ok(result)
}

/// Creates an empty [`DataStream`] for DDL statements that produce no rows.
///
/// # Errors
///
/// Returns an error if DataFusion fails to build the empty logical plan.
fn create_empty_datastream(ctx: &Context) -> Result<DataStream> {
    let plan = LogicalPlanBuilder::empty(false)
        .build()
        .map_err(DenormalizedError::from)?;
    let empty_df = DataFrame::new(ctx.session_context.state(), plan);
    let ds = DataStream::new(Arc::new(empty_df), Arc::new(ctx.clone()));

    Ok(ds)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::{dialect::AnsiDialect, parser::Parser};

    use super::*;

    fn parse_create_table(sql: &str) -> ast::CreateTable {
        let statements =
            Parser::parse_sql(&AnsiDialect {}, sql).expect("test SQL should parse successfully");

        match statements.into_iter().next().unwrap() {
            ast::Statement::CreateTable(table) => table,
            statement => panic!("expected CREATE TABLE, got {statement:?}"),
        }
    }

    #[test]
    fn builds_schema_from_sql_columns() {
        let table = parse_create_table(
            "CREATE TABLE test_table ( \
                id BIGINT NOT NULL, \
                name STRING, \
                price INT, \
                active BOOLEAN, \
                score DOUBLE, \
                day DATE \
            )",
        );

        let schema = build_schema_from_sql_cols(&table.columns).unwrap();

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int64);
        assert!(!schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Utf8);
        assert!(schema.field(1).is_nullable());

        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Boolean);
        assert_eq!(schema.field(4).data_type(), &ArrowDataType::Float64);
        assert_eq!(schema.field(5).data_type(), &ArrowDataType::Date32);
    }

    #[test]
    fn rejects_empty_column_list() {
        assert!(matches!(
            build_schema_from_sql_cols(&[]),
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[test]
    fn rejects_unsupported_column_type() {
        let table = parse_create_table("CREATE TABLE test_table (payload BINARY)");

        assert!(matches!(
            build_schema_from_sql_cols(&table.columns),
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[test]
    fn extracts_last_table_name_component() {
        let table = parse_create_table("CREATE TABLE catalog.schema.test_table (id INT)");

        assert_eq!(last_name(&table.name).unwrap(), "test_table");
    }

    #[test]
    fn converts_with_option_literals_to_strings() {
        let table = parse_create_table(
            "CREATE TABLE test_table (id INT) \
             WITH (connector = 'mock', retries = 3, enabled = true)",
        );

        let ast::CreateTableOptions::With(options) = &table.table_options else {
            panic!("expected WITH options");
        };

        let table_options = TableOptions::new(options).unwrap();

        assert_eq!(
            table_options.connector.remaining().get("connector"),
            Some(&"mock".to_string())
        );
        assert_eq!(
            table_options.connector.remaining().get("retries"),
            Some(&"3".to_string())
        );
        assert_eq!(
            table_options.connector.remaining().get("enabled"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn rejects_unsupported_with_option_expression() {
        let table = parse_create_table(
            "CREATE TABLE test_table (id INT) WITH (connector = 'mock', retries = 1 + 2)",
        );

        let ast::CreateTableOptions::With(options) = &table.table_options else {
            panic!("expected WITH options");
        };

        assert!(matches!(
            TableOptions::new(options),
            Err(DenormalizedError::SQL(_))
        ));
    }
}
