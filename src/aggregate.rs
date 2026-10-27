//! Per-batch aggregates: count and sum over the batch's events.

/// Aggregate over a set of events: how many, totalling what.
///
/// This is the record a batch leaves behind. It deliberately carries no
/// per-event detail: once a batch is emitted, the aggregate stands for it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Aggregate {
    /// Number of events folded into this aggregate.
    pub count: u64,
    /// Sum of the events' values.
    pub sum: f64,
}

impl Aggregate {
    /// An aggregate over no events.
    pub fn new() -> Self {
        Aggregate { count: 0, sum: 0.0 }
    }

    /// Fold one event value into the aggregate.
    pub fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
    }

    /// Fold another aggregate into this one.
    pub fn merge(&mut self, other: &Aggregate) {
        self.count += other.count;
        self.sum += other.sum;
    }

    /// Aggregate over exactly the given values.
    pub fn over(values: &[f64]) -> Self {
        let mut agg = Aggregate::new();
        for v in values {
            agg.add(*v);
        }
        agg
    }
}
