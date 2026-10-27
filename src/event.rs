//! One telemetry event: the unit the pipeline ingests.

/// A single telemetry event.
///
/// `scope` is the consent scope the event belongs to (for example
/// `"analytics"` or `"ads"`). It travels with the event from ingestion;
/// what the pipeline does with it depends on the batching policy.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// Consent scope the event belongs to.
    pub scope: String,
    /// Event name, e.g. `"click"` or `"purchase"`.
    pub name: String,
    /// Measured value attached to the event.
    pub value: f64,
    /// Milliseconds since the Unix epoch.
    pub ts: u64,
}

impl Event {
    /// Build an event from its parts.
    pub fn new(scope: &str, name: &str, value: f64, ts: u64) -> Self {
        Event {
            scope: scope.to_string(),
            name: name.to_string(),
            value,
            ts,
        }
    }
}
