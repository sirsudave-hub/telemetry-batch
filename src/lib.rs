//! Buffer telemetry events and flush them in batches.
//!
//! Events arrive through [`Batcher::ingest`], accumulate in memory, and are
//! emitted with [`Batcher::flush`] as [`Batch`] values, each carrying one
//! [`Aggregate`] summarising the events it flushed. Emitted batches are kept
//! in [`Batcher::history`] and folded into the cumulative [`Totals`] view.
//!
//! Batching is currently global: one buffer holds every scope and each flush
//! emits batches scoped `"global"`. The per-event consent scope is still
//! recorded on every [`Event`] and is already used to group the [`Totals`]
//! reporting view.

mod aggregate;
mod batch;
mod config;
mod event;
mod stats;
mod totals;

pub use aggregate::Aggregate;
pub use batch::{Batch, Batcher};
pub use config::{BatchConfig, ConfigError};
pub use event::Event;
pub use stats::FlushStats;
pub use totals::Totals;

/// Scope label used for batches while batching is global.
pub const GLOBAL_SCOPE: &str = "global";
