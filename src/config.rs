//! Batching configuration and its validation.

use std::fmt;

/// Batching configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchConfig {
    /// Maximum number of events in one batch. A flush splits each buffer
    /// into chunks of at most this many events. Must be at least 1.
    pub max_batch_events: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_batch_events: 100,
        }
    }
}

impl BatchConfig {
    /// Check the configuration; see [`ConfigError`] for what can go wrong.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_batch_events < 1 {
            return Err(ConfigError::non_positive("max_batch_events"));
        }
        Ok(())
    }
}

/// A rejected [`BatchConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// Which field was rejected.
    pub field: String,
    /// Why it was rejected.
    pub reason: String,
}

impl ConfigError {
    fn non_positive(field: &str) -> Self {
        ConfigError {
            field: field.to_string(),
            reason: "must be at least 1".to_string(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {}: {}", self.field, self.reason)
    }
}

impl std::error::Error for ConfigError {}
