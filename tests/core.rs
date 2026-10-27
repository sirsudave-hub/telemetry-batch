use telemetry_batch::{Aggregate, BatchConfig, Batcher, Event, FlushStats, Totals};

fn batcher(max_batch_events: usize) -> Batcher {
    Batcher::new(BatchConfig { max_batch_events }).unwrap()
}

fn ev(scope: &str, value: f64) -> Event {
    Event::new(scope, "click", value, 0)
}

#[test]
fn aggregate_add_accumulates_count_and_sum() {
    let mut agg = Aggregate::new();
    assert_eq!(agg, Aggregate { count: 0, sum: 0.0 });
    assert_eq!(Aggregate::default(), Aggregate::new());
    agg.add(2.0);
    agg.add(3.0);
    assert_eq!(agg, Aggregate { count: 2, sum: 5.0 });
}

#[test]
fn aggregate_merge_combines_and_empty_is_identity() {
    let mut agg = Aggregate::over(&[1.0, 2.0]);
    agg.merge(&Aggregate::over(&[10.0]));
    assert_eq!(agg, Aggregate { count: 3, sum: 13.0 });
    let mut agg = Aggregate::over(&[4.0]);
    agg.merge(&Aggregate::new());
    assert_eq!(agg, Aggregate { count: 1, sum: 4.0 });
    let mut agg = Aggregate::new();
    agg.merge(&Aggregate::over(&[6.0]));
    assert_eq!(agg, Aggregate { count: 1, sum: 6.0 });
}

#[test]
fn aggregate_over_empty_and_values() {
    assert_eq!(Aggregate::over(&[]), Aggregate { count: 0, sum: 0.0 });
    assert_eq!(
        Aggregate::over(&[1.0, 2.0, 3.0]),
        Aggregate { count: 3, sum: 6.0 }
    );
    assert_eq!(Aggregate::over(&[7.0]), Aggregate { count: 1, sum: 7.0 });
}

#[test]
fn config_validate_accepts_valid_configs() {
    assert_eq!(BatchConfig::default().max_batch_events, 100);
    assert!(BatchConfig::default().validate().is_ok());
    assert!(BatchConfig { max_batch_events: 1 }.validate().is_ok());
    assert!(BatchConfig { max_batch_events: 250 }.validate().is_ok());
}

#[test]
fn config_validate_rejects_zero() {
    let err = BatchConfig { max_batch_events: 0 }.validate().unwrap_err();
    assert_eq!(err.field, "max_batch_events");
    assert!(!err.reason.is_empty());
    assert!(err.to_string().contains("max_batch_events"));
}

#[test]
fn batcher_new_rejects_invalid_config_and_default_works() {
    let err = Batcher::new(BatchConfig { max_batch_events: 0 }).unwrap_err();
    assert_eq!(err.field, "max_batch_events");
    assert!(Batcher::new(BatchConfig::default()).is_ok());
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    assert_eq!(batcher.pending_count(), 1);
}

#[test]
fn empty_flush_emits_nothing_records_stats_and_leaves_history() {
    let mut batcher = Batcher::default();
    assert!(batcher.flush().is_empty());
    assert!(batcher.history().is_empty());
    assert_eq!(batcher.stats().flushes, 1);
    assert_eq!(batcher.stats().events_flushed, 0);
    assert_eq!(batcher.stats().batches_emitted, 0);

    batcher.ingest(ev("analytics", 1.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    let history_before = batcher.history().to_vec();
    let stats_before = batcher.stats().clone();

    let empty = batcher.flush();
    assert!(empty.is_empty());
    assert_eq!(batcher.history().to_vec(), history_before);
    assert_eq!(batcher.stats().flushes, stats_before.flushes + 1);
    assert_eq!(batcher.stats().events_flushed, stats_before.events_flushed);
    assert_eq!(batcher.stats().batches_emitted, stats_before.batches_emitted);
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn single_scope_chunking_with_max_one() {
    let mut batcher = batcher(1);
    for value in [1.0, 2.0, 3.0] {
        batcher.ingest(ev("analytics", value));
    }
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    assert_eq!(emitted[0].aggregate, Aggregate { count: 1, sum: 1.0 });
    assert_eq!(emitted[1].aggregate, Aggregate { count: 1, sum: 2.0 });
    assert_eq!(emitted[2].aggregate, Aggregate { count: 1, sum: 3.0 });
    assert_eq!(batcher.pending_count(), 0);
    assert_eq!(batcher.stats().batches_emitted, 3);
    assert_eq!(batcher.stats().events_flushed, 3);
}

#[test]
fn single_scope_chunking_exact_multiple() {
    let mut batcher = batcher(2);
    for value in [1.0, 2.0, 3.0, 4.0] {
        batcher.ingest(ev("analytics", value));
    }
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert_eq!(emitted[0].aggregate, Aggregate { count: 2, sum: 3.0 });
    assert_eq!(emitted[1].aggregate, Aggregate { count: 2, sum: 7.0 });
}

#[test]
fn single_scope_chunking_remainder() {
    let mut batcher = batcher(2);
    for value in [1.0, 2.0, 3.0, 4.0, 5.0] {
        batcher.ingest(ev("analytics", value));
    }
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    assert_eq!(emitted[0].aggregate, Aggregate { count: 2, sum: 3.0 });
    assert_eq!(emitted[1].aggregate, Aggregate { count: 2, sum: 7.0 });
    assert_eq!(emitted[2].aggregate, Aggregate { count: 1, sum: 5.0 });
}

#[test]
fn pending_count_before_and_after_flush() {
    let mut batcher = batcher(10);
    assert_eq!(batcher.pending_count(), 0);
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("analytics", 3.0));
    assert_eq!(batcher.pending_count(), 3);
    batcher.flush();
    assert_eq!(batcher.pending_count(), 0);
    batcher.ingest(ev("analytics", 4.0));
    assert_eq!(batcher.pending_count(), 1);
    batcher.flush();
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn history_matches_emission_one_to_one() {
    let mut batcher = batcher(2);
    for value in [1.0, 2.0, 3.0] {
        batcher.ingest(ev("analytics", value));
    }
    let emitted = batcher.flush();
    assert_eq!(batcher.history().len(), emitted.len());
    assert_eq!(batcher.history().to_vec(), emitted);

    let history_len_before = batcher.history().len();
    batcher.ingest(ev("analytics", 4.0));
    batcher.ingest(ev("analytics", 5.0));
    let emitted = batcher.flush();
    assert_eq!(batcher.history().len(), history_len_before + emitted.len());
    assert_eq!(batcher.history()[history_len_before..].to_vec(), emitted);
}

#[test]
fn batch_id_sequence_across_two_flushes() {
    let mut batcher = batcher(1);
    for value in [1.0, 2.0, 3.0] {
        batcher.ingest(ev("analytics", value));
    }
    let first = batcher.flush();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].batch_id, 0);
    assert_eq!(first[1].batch_id, 1);
    assert_eq!(first[2].batch_id, 2);

    for value in [4.0, 5.0] {
        batcher.ingest(ev("analytics", value));
    }
    let second = batcher.flush();
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].batch_id, 3);
    assert_eq!(second[1].batch_id, 4);

    let ids: Vec<u64> = batcher.history().iter().map(|batch| batch.batch_id).collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

#[test]
fn totals_values_for_multi_scope_input() {
    let mut totals = Totals::new();
    assert_eq!(totals.scope_count(), 0);
    assert_eq!(totals.scope("analytics"), None);
    totals.add_batch("analytics", &Aggregate::over(&[1.0, 2.0]));
    totals.add_batch("ads", &Aggregate::over(&[10.0]));
    assert_eq!(
        totals.scope("analytics"),
        Some(&Aggregate { count: 2, sum: 3.0 })
    );
    assert_eq!(totals.scope("ads"), Some(&Aggregate { count: 1, sum: 10.0 }));
    assert_eq!(totals.global, Aggregate { count: 3, sum: 13.0 });
    assert_eq!(totals.scope_count(), 2);

    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("analytics", 2.0));
    let emitted = batcher.flush();
    assert_eq!(batcher.history().len(), emitted.len());
    for (i, batch) in emitted.iter().enumerate() {
        assert_eq!(batch.batch_id, i as u64);
    }
    assert_eq!(
        emitted.iter().map(|batch| batch.aggregate.count).sum::<u64>(),
        3
    );
    assert_eq!(
        emitted.iter().map(|batch| batch.aggregate.sum).sum::<f64>(),
        13.0
    );
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 2, sum: 3.0 })
    );
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 1, sum: 10.0 })
    );
    assert_eq!(batcher.totals().scope("missing"), None);
    assert_eq!(batcher.totals().global, Aggregate { count: 3, sum: 13.0 });
    assert_eq!(batcher.totals().scope_count(), 2);
}

#[test]
fn stats_accumulate_across_flushes_including_empty() {
    let mut stats = FlushStats::default();
    assert_eq!(
        stats,
        FlushStats { flushes: 0, events_flushed: 0, batches_emitted: 0 }
    );
    stats.record_flush(3, 2);
    assert_eq!(
        stats,
        FlushStats { flushes: 1, events_flushed: 3, batches_emitted: 2 }
    );

    let mut batcher = batcher(2);
    assert!(batcher.flush().is_empty());
    assert_eq!(batcher.stats().flushes, 1);
    assert_eq!(batcher.stats().events_flushed, 0);
    assert_eq!(batcher.stats().batches_emitted, 0);

    for value in [1.0, 2.0, 3.0] {
        batcher.ingest(ev("analytics", value));
    }
    batcher.flush();
    assert_eq!(
        batcher.stats(),
        &FlushStats { flushes: 2, events_flushed: 3, batches_emitted: 2 }
    );

    batcher.ingest(ev("analytics", 4.0));
    batcher.flush();
    assert_eq!(
        batcher.stats(),
        &FlushStats { flushes: 3, events_flushed: 4, batches_emitted: 3 }
    );
}
