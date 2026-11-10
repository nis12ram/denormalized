//! Connector abstraction for streaming source tables.
//!
//! A connector translates a SQL `CREATE TABLE ... WITH (...)` request into a
//! DataFusion [`TableProvider`] that reads from an external system.

use std::{collections::HashMap, sync::Arc};

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use denormalized_common::{DenormalizedError, Result};

use crate::connectors::kafka::KafkaConnector;

pub mod kafka;

/// Key-value options supplied to a connector.
///
/// The SQL layer converts `WITH (...)` values into strings before constructing
/// this type, so connectors do not depend on SQL parser types.
///
/// Individual connectors consume the options they understand with
/// [`ConnectorOptions::pull_str`], [`ConnectorOptions::pull_opt_str`], or
/// [`ConnectorOptions::into_remaining`].
#[derive(Debug, Clone)]
pub struct ConnectorOptions(HashMap<String, String>);

impl ConnectorOptions {
    /// Creates connector options from a map of string key-value pairs.
    pub fn new(opts: HashMap<String, String>) -> Self {
        Self(opts)
    }

    /// Consumes the options and returns all remaining key-value pairs.
    ///
    /// Connectors typically call this after pulling the options they support,
    /// for example to forward remaining properties to a client library.
    pub fn into_remaining(self) -> HashMap<String, String> {
        self.0
    }

    /// Returns all remaining key-value pairs without consuming them.
    pub fn remaining(&self) -> &HashMap<String, String> {
        &self.0
    }

    /// Removes and returns the value for `key`, if present.
    pub fn pull_opt_str(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// Removes and returns the value for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`DenormalizedError::Connector`] if `key` is not present.
    pub fn pull_str(&mut self, key: &str) -> Result<String> {
        self.pull_opt_str(key).ok_or_else(|| {
            DenormalizedError::Connector(format!("required connector option '{key}' not set"))
        })
    }
}

/// Details required by a connector to create a source table.
pub struct CreateSourceRequest {
    /// Name of the table being created.
    pub table_name: String,
    /// Arrow schema declared by the `CREATE TABLE` statement.
    pub table_schema: Arc<Schema>,
    /// Connector-specific options from the `WITH` clause.
    pub connector_options: ConnectorOptions,
}

/// Generic representation of a streaming connector.
///
/// A connector is responsible for translating a [`CreateSourceRequest`] into a
/// [`TableProvider`] backed by an external system.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Returns the name used to select this connector in SQL.
    fn name(&self) -> &'static str;

    /// Creates a source table provider for the given request.
    ///
    /// # Errors
    ///
    /// Returns an error if required options are missing, an option is invalid,
    /// or the external system cannot be reached while creating the source.
    async fn create_source(&self, request: CreateSourceRequest) -> Result<Arc<dyn TableProvider>>;

    // TODO: Introduce a `create_sink` method to support future sinks.
}

/// Connector registry mapping connector names to their implementations.
///
/// # Note
///
/// Custom connector registration may be supported in the future, but is
/// currently not supported.
pub struct ConnectorRegistry {
    connector: HashMap<String, Arc<dyn Connector>>,
}

impl ConnectorRegistry {
    /// Creates an empty connector registry
    fn new() -> Self {
        Self {
            connector: HashMap::new(),
        }
    }

    /// Creates the registry with built-in connectors
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();
        registry.register(Arc::new(KafkaConnector));
        registry
    }

    /// Register a connector in the registry
    fn register(&mut self, connector: Arc<dyn Connector>) {
        self.connector
            .insert(connector.name().to_string(), connector);
    }

    /// Returns a connector from the registry, if available
    pub fn get(&self, name: &str) -> Option<Arc<dyn Connector>> {
        self.connector.get(name).cloned()
    }

    /// Returns a connector from the registry or an error if it is not found.
    ///
    /// # Errors
    ///
    /// Returns [`DenormalizedError::Connector`] if no connector is registered
    /// under `name`.
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn Connector>> {
        self.get(name)
            .ok_or_else(|| DenormalizedError::Connector(format!("connector '{name}' not known")))
    }
}

#[cfg(test)]
impl ConnectorRegistry {
    /// Creates a registry containing only `connector`.
    pub(crate) fn with_test_connector(connector: Arc<dyn Connector>) -> Self {
        let mut registry = Self::new();
        registry.register(connector);
        registry
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn connector_options_pull_and_remaining() {
        let mut options = ConnectorOptions::new(HashMap::from([
            ("required".to_string(), "value".to_string()),
            ("optional".to_string(), "42".to_string()),
            ("left".to_string(), "yes".to_string()),
        ]));

        assert_eq!(options.pull_str("required").unwrap(), "value");
        assert_eq!(options.pull_opt_str("optional"), Some("42".to_string()));
        assert_eq!(options.pull_opt_str("missing"), None);
        assert_eq!(options.remaining().get("left"), Some(&"yes".to_string()));

        let remaining = options.into_remaining();
        assert_eq!(remaining.get("left"), Some(&"yes".to_string()));
        assert!(!remaining.contains_key("required"));
    }

    #[test]
    fn connector_options_missing_required_is_error() {
        let mut options = ConnectorOptions::new(HashMap::new());

        assert!(matches!(
            options.pull_str("required"),
            Err(DenormalizedError::Connector(_))
        ));
    }

    #[test]
    fn registry_resolves_builtin_connectors() {
        let registry = ConnectorRegistry::with_builtins();

        assert_eq!(registry.resolve("kafka").unwrap().name(), "kafka");
    }

    #[test]
    fn registry_rejects_unknown_connector() {
        let registry = ConnectorRegistry::with_builtins();

        assert!(matches!(
            registry.resolve("unknown"),
            Err(DenormalizedError::Connector(_))
        ));
    }
}
