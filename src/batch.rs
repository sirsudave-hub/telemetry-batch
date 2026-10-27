//! The batcher: buffers events and flushes them in batches.
//!
//! [`Batcher`] is the pipeline. [`ingest`](Batcher::ingest) buffers one
//! event; [`flush`](Batcher::flush) drains the buffer into [`Batch`]
//! values, each with one [`Aggregate`](crate::Aggregate). Emitted batches
//! accumulate in [`history`](Batcher::history) and fold into
//! [`totals`](Batcher::totals).

use std::collections::HashMap;

use crate::aggregate::Aggregate;
use crate::config::{BatchConfig, ConfigError};
use crate::event::Event;
use crate::stats::FlushStats;
use crate::totals::Totals;
use crate::GLOBAL_SCOPE;

/// One emitted batch: a slice of one buffer, summarised by one aggregate.
///
/// The aggregate is the record the batch leaves behind; the batch carries
/// no per-event detail past emission.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// Sequence number of this batch across the batcher's lifetime.
    pub batch_id: u64,
    /// Consent scope the batch was flushed for. While batching is global
    /// this is always [`GLOBAL_SCOPE`](crate::GLOBAL_SCOPE).
    pub scope: String,
    /// Aggregate over the batch's events.
    pub aggregate: Aggregate,
}

/// Buffers telemetry events and flushes them in batches.
#[derive(Debug)]
pub struct Batcher {
    config: BatchConfig,
    buffer: Vec<Event>,
    history: Vec<Batch>,
    totals: Totals,
    stats: FlushStats,
    next_batch_id: u64,
}

impl Batcher {
    /// Build a batcher. Rejects an invalid `config` (see
    /// [`BatchConfig::validate`]).
    pub fn new(config: BatchConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Batcher {
            config,
            buffer: Vec::new(),
            history: Vec::new(),
            totals: Totals::new(),
            stats: FlushStats::default(),
            next_batch_id: 0,
        })
    }

    /// Buffer one event for a later flush.
    pub fn ingest(&mut self, event: Event) {
        self.buffer.push(event);
    }

    /// Drain the buffer into batches.
    ///
    /// The buffer is split into chunks of at most
    /// `max_batch_events` events; each chunk becomes one [`Batch`],
    /// appended to the history and folded into the totals. The buffer is
    /// empty afterwards: an emitted batch leaves its aggregate behind,
    /// not its events. Flushing an empty buffer emits nothing.
    ///
    /// Batches are returned in emission order with ascending `batch_id`.
    pub fn flush(&mut self) -> Vec<Batch> {
        let drained: Vec<Event> = self.buffer.drain(..).collect();
        if drained.is_empty() {
            self.stats.record_flush(0, 0);
            return Vec::new();
        }

        // Per-scope subtotals for the reporting view, accumulated first so
        // the totals update below stays a plain merge per scope.
        let mut scope_sums: HashMap<String, Aggregate> = HashMap::new();
        for event in &drained {
            scope_sums
                .entry(event.scope.clone())
                .or_default()
                .add(event.value);
        }

        let mut emitted: Vec<Batch> = Vec::new();
        let mut chunk = Aggregate::new();
        let mut chunk_len = 0usize;
        // NOTE: batching is global — one buffer, chunked in arrival order.
        // The scope on the event feeds the totals view, not the batching.
        for event in &drained {
            chunk.add(event.value);
            chunk_len += 1;
            if chunk_len == self.config.max_batch_events {
                emitted.push(self.seal_chunk(chunk));
                chunk = Aggregate::new();
                chunk_len = 0;
            }
        }
        if chunk_len > 0 {
            emitted.push(self.seal_chunk(chunk));
        }

        for (scope, agg) in &scope_sums {
            self.totals.add_batch(scope, agg);
        }
        self.stats
            .record_flush(drained.len() as u64, emitted.len() as u64);
        self.history.extend(emitted.iter().cloned());
        emitted
    }

    /// Mint the next batch id and wrap one chunk aggregate as a batch.
    fn seal_chunk(&mut self, chunk: Aggregate) -> Batch {
        let batch = Batch {
            batch_id: self.next_batch_id,
            scope: GLOBAL_SCOPE.to_string(),
            aggregate: chunk,
        };
        self.next_batch_id += 1;
        batch
    }

    /// Every batch this batcher has emitted, in emission order.
    pub fn history(&self) -> &[Batch] {
        &self.history
    }

    /// Cumulative aggregates over everything emitted so far.
    pub fn totals(&self) -> &Totals {
        &self.totals
    }

    /// Events currently buffered and not yet flushed.
    pub fn pending_count(&self) -> usize {
        self.buffer.len()
    }

    /// Diagnostic flush counters.
    pub fn stats(&self) -> &FlushStats {
        &self.stats
    }

    /// Revoke a consent scope, retroactively.
    ///
    /// Planned revocation support: purge the scope from the buffers, emit
    /// nothing further from its data, and restate the affected aggregates
    /// to exclude it.
    ///
    /// # Panics
    ///
    /// Revocation is not implemented yet.
    pub fn revoke(&mut self, _scope: &str) {
        todo!("consent revocation is not implemented yet")
    }

    /// Whether a consent scope has been revoked.
    ///
    /// # Panics
    ///
    /// Revocation is not implemented yet.
    pub fn is_revoked(&self, _scope: &str) -> bool {
        todo!("consent revocation is not implemented yet")
    }
}

impl Default for Batcher {
    /// A batcher with the default configuration.
    fn default() -> Self {
        Batcher::new(BatchConfig::default())
            .expect("default batch config is valid")
    }
}
