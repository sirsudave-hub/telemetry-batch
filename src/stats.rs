//! Diagnostic flush counters. These describe pipeline activity, not
//! batch contents, and take no part in batch semantics.

/// Counters over the flushes a [`Batcher`](crate::Batcher) has performed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FlushStats {
    /// How many times [`flush`](crate::Batcher::flush) ran, including
    /// flushes that found nothing buffered.
    pub flushes: u64,
    /// Total events emitted across all flushes.
    pub events_flushed: u64,
    /// Total batches emitted across all flushes.
    pub batches_emitted: u64,
}

impl FlushStats {
    /// Record one flush that emitted `events` events in `batches` batches.
    pub fn record_flush(&mut self, events: u64, batches: u64) {
        self.flushes += 1;
        self.events_flushed += events;
        self.batches_emitted += batches;
    }
}
