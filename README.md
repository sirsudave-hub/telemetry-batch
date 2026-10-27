# telemetry-batch

Buffer telemetry events and flush them in batches with per-batch aggregates.

Events arrive through `Batcher::ingest`, accumulate in memory, and are
emitted with `Batcher::flush` as `Batch` values, each carrying one
`Aggregate` (count and sum) summarising the events it flushed. Emitted
batches accumulate in `Batcher::history` and fold into a cumulative
`Totals` view grouped by consent scope with a global rollup.

Batching is currently global: one buffer holds every scope and each flush
emits batches scoped `"global"`, split into chunks of at most
`max_batch_events` events. The per-event consent scope is recorded on
every `Event` and already groups the `Totals` reporting view.

The crate also exposes planned revocation entry points (`revoke`,
`is_revoked`); they are currently unimplemented stubs.

Standard library only; no dependencies.

## Example

```rust
use telemetry_batch::{BatchConfig, Batcher, Event};

let mut batcher = Batcher::new(BatchConfig { max_batch_events: 2 }).unwrap();
batcher.ingest(Event::new("analytics", "click", 1.0, 1000));
batcher.ingest(Event::new("ads", "impression", 2.0, 1001));
batcher.ingest(Event::new("analytics", "click", 4.0, 1002));

let batches = batcher.flush();
assert_eq!(batches.len(), 2); // chunked 2 + 1
assert_eq!(batcher.pending_count(), 0);
assert_eq!(batcher.totals().global.count, 3);
assert_eq!(batcher.totals().scope("analytics").unwrap().sum, 5.0);
```

## Development

```sh
cargo build
cargo test
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT) at your option.
