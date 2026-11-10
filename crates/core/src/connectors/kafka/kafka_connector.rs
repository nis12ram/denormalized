use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use denormalized_common::{DenormalizedError, Result};

use crate::connectors::kafka::KafkaTopicBuilder;
use crate::connectors::{Connector, CreateSourceRequest};
use crate::physical_plan::utils::time::TimestampUnit;

/// [`Connector`] implementation for Apache Kafka source tables.
///
/// The connector maps SQL options such as `bootstrap.servers`, `topic`, and
/// `encoding` onto [`KafkaTopicBuilder`]. Additional dotted options are
/// forwarded as librdkafka configuration options.
#[derive(Debug)]
pub struct KafkaConnector;

#[async_trait]
impl Connector for KafkaConnector {
    fn name(&self) -> &'static str {
        "kafka"
    }

    /// Creates a Kafka-backed source table.
    ///
    /// Required options are `bootstrap.servers`, `topic`, and `encoding`.
    /// `timestamp_column` and `timestamp_unit` must be supplied together.
    ///
    /// # Errors
    ///
    /// Returns an error if required options are missing, an option value is
    /// invalid, or Kafka topic metadata cannot be fetched while building the
    /// reader.
    async fn create_source(&self, request: CreateSourceRequest) -> Result<Arc<dyn TableProvider>> {
        let CreateSourceRequest {
            table_schema,
            mut connector_options,
            ..
        } = request;

        let bootstrap_servers = connector_options.pull_str("bootstrap.servers")?;
        let topic_name = connector_options.pull_str("topic")?;
        let encoding = connector_options.pull_str("encoding")?;

        let timestamp_column = connector_options.pull_opt_str("timestamp_column");
        let timestamp_unit = match connector_options.pull_opt_str("timestamp_unit") {
            Some(value) => Some(parse_timestamp_unit(&value)?),
            _ => None,
        };
        match (&timestamp_column, &timestamp_unit) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(DenormalizedError::Connector(
                    "'timestamp_column' and 'timestamp_unit' must be provided together".to_string(),
                ))
            }
        }

        let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers);

        topic_builder.with_topic(topic_name);
        topic_builder.with_schema(table_schema);
        topic_builder.with_encoding(&encoding)?;
        if let (Some(col), Some(unit)) = (timestamp_column, timestamp_unit) {
            topic_builder.with_timestamp(col, unit);
        }

        let remaining_options = connector_options.into_remaining();
        // TODO: Move this validation check to the right place after refactoring `src/connectors/kafka/`.
        // NOTE: For future Kafka `create_sink` implementation, avoid duplicating this validation check for [`KafkaConnectionOpts`].
        for key in remaining_options.keys() {
            if !key.contains(".") && key != "debug" && key != "enabled_events" && key != "retries" {
                return Err(DenormalizedError::Connector(
                    format!("Invalid Kafka read connector configuration key '{key}': it is not valid for the read connector, nor does it contain `.` as librdkafka configuration options generally do (nor is it one of the few special exceptions to that rule)")
                ));
            }
        }

        let reader = topic_builder.build_reader(remaining_options).await?;

        Ok(Arc::new(reader))
    }
}

/// Parses a SQL `timestamp_unit` value into a [`TimestampUnit`].
///
/// Supported values are `millis`/`milliseconds`, `seconds`, and
/// `iso8601:<format>`.
///
/// # Errors
///
/// Returns [`DenormalizedError::SQL`] if `value` is not a supported timestamp
/// unit.
fn parse_timestamp_unit(value: &str) -> Result<TimestampUnit> {
    match value {
        "millis" | "milliseconds" => Ok(TimestampUnit::Int64Millis),
        "seconds" => Ok(TimestampUnit::Int64Seconds),
        value if value.starts_with("iso8601:") => {
            let format = value["iso8601:".len()..].to_string();
            Ok(TimestampUnit::StringIso8601(format))
        }
        other => Err(DenormalizedError::SQL(format!(
            "unsupported timestamp_unit '{other}'; expected 'millis', 'seconds', or 'iso8601:<format>'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::Schema;

    use crate::connectors::ConnectorOptions;

    use super::*;

    fn source_request(options: &[(&str, &str)]) -> CreateSourceRequest {
        let connector_options = options
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>();

        CreateSourceRequest {
            table_name: "test_table".to_string(),
            table_schema: Arc::new(Schema::empty()),
            connector_options: ConnectorOptions::new(connector_options),
        }
    }

    #[test]
    fn connector_name_is_kafka() {
        assert_eq!(KafkaConnector.name(), "kafka");
    }

    #[tokio::test]
    async fn create_source_requires_bootstrap_servers() {
        let request = source_request(&[("topic", "test"), ("encoding", "json")]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_source_requires_topic() {
        let request = source_request(&[
            ("bootstrap.servers", "localhost:9092"),
            ("encoding", "json"),
        ]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_source_requires_encoding() {
        let request = source_request(&[("bootstrap.servers", "localhost:9092"), ("topic", "test")]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_source_requires_timestamp_options_together() {
        let request = source_request(&[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "test"),
            ("encoding", "json"),
            ("timestamp_column", "occurred_at"),
        ]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[tokio::test]
    async fn create_source_rejects_invalid_timestamp_unit() {
        let request = source_request(&[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "test"),
            ("encoding", "json"),
            ("timestamp_column", "occurred_at"),
            ("timestamp_unit", "fortnights"),
        ]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::SQL(_))
        ));
    }

    #[tokio::test]
    async fn create_source_rejects_undotted_unknown_option() {
        let request = source_request(&[
            ("bootstrap.servers", "localhost:9092"),
            ("topic", "test"),
            ("encoding", "json"),
            ("badkey", "value"),
        ]);

        assert!(matches!(
            KafkaConnector.create_source(request).await,
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[test]
    fn parses_timestamp_units() {
        assert!(matches!(
            parse_timestamp_unit("millis"),
            Ok(TimestampUnit::Int64Millis)
        ));
        assert!(matches!(
            parse_timestamp_unit("milliseconds"),
            Ok(TimestampUnit::Int64Millis)
        ));
        assert!(matches!(
            parse_timestamp_unit("seconds"),
            Ok(TimestampUnit::Int64Seconds)
        ));
        assert!(matches!(
            parse_timestamp_unit("iso8601:%Y-%m-%d"),
            Ok(TimestampUnit::StringIso8601(format)) if format == "%Y-%m-%d"
        ));
    }

    #[test]
    fn rejects_unknown_timestamp_unit() {
        assert!(matches!(
            parse_timestamp_unit("fortnights"),
            Err(DenormalizedError::SQL(_))
        ));
    }
}
