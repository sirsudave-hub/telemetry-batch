//! Cumulative aggregates: per consent scope plus a global rollup.

use std::collections::HashMap;

use crate::aggregate::Aggregate;

/// Cumulative aggregates over everything emitted so far.
///
/// `per_scope` groups by the consent scope carried on each event, so one
/// entry exists per scope seen. `global` rolls every scope together.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Totals {
    /// Cumulative aggregate per consent scope.
    pub per_scope: HashMap<String, Aggregate>,
    /// Cumulative aggregate over all scopes.
    pub global: Aggregate,
}

impl Totals {
    /// An empty totals view.
    pub fn new() -> Self {
        Totals {
            per_scope: HashMap::new(),
            global: Aggregate::new(),
        }
    }

    /// Fold one emitted batch into the view.
    pub fn add_batch(&mut self, scope: &str, aggregate: &Aggregate) {
        self.per_scope
            .entry(scope.to_string())
            .or_default()
            .merge(aggregate);
        self.global.merge(aggregate);
    }

    /// Cumulative aggregate for one scope, if any events were seen for it.
    pub fn scope(&self, scope: &str) -> Option<&Aggregate> {
        self.per_scope.get(scope)
    }

    /// Number of scopes with cumulative data.
    pub fn scope_count(&self) -> usize {
        self.per_scope.len()
    }
}
