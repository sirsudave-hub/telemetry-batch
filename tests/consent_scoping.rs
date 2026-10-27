use telemetry_batch::{Aggregate, Batch, BatchConfig, Batcher, Event};

fn batcher(max_batch_events: usize) -> Batcher {
    Batcher::new(BatchConfig { max_batch_events }).unwrap()
}

fn ev(scope: &str, value: f64) -> Event {
    Event::new(scope, "click", value, 0)
}

fn for_scope<'a>(batches: &'a [Batch], scope: &str) -> Vec<&'a Batch> {
    let mut found: Vec<&Batch> = batches
        .iter()
        .filter(|batch| batch.scope == scope)
        .collect();
    found.sort_by_key(|batch| batch.batch_id);
    found
}

fn sorted_by_scope(batches: Vec<Batch>) -> Vec<Batch> {
    let mut sorted = batches;
    sorted.sort_by(|a, b| a.scope.cmp(&b.scope).then(a.batch_id.cmp(&b.batch_id)));
    sorted
}

#[test]
fn multi_scope_batches_carry_real_scopes_and_per_scope_aggregates() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 10.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.scope != "global"));
    let by_scope = sorted_by_scope(emitted);
    assert_eq!(by_scope[0].scope, "ads");
    assert_eq!(by_scope[0].aggregate, Aggregate { count: 1, sum: 10.0 });
    assert_eq!(by_scope[1].scope, "analytics");
    assert_eq!(by_scope[1].aggregate, Aggregate { count: 2, sum: 3.0 });
}

#[test]
fn chunking_within_one_scope_is_independent_of_other_scope() {
    let mut batcher = batcher(2);
    for value in [1.0, 2.0, 3.0, 4.0, 5.0] {
        batcher.ingest(ev("analytics", value));
    }
    batcher.ingest(ev("ads", 100.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 4);
    let analytics = for_scope(&emitted, "analytics");
    assert_eq!(analytics.len(), 3);
    assert_eq!(analytics[0].aggregate, Aggregate { count: 2, sum: 3.0 });
    assert_eq!(analytics[1].aggregate, Aggregate { count: 2, sum: 7.0 });
    assert_eq!(analytics[2].aggregate, Aggregate { count: 1, sum: 5.0 });
    let ads = for_scope(&emitted, "ads");
    assert_eq!(ads.len(), 1);
    assert_eq!(ads[0].aggregate, Aggregate { count: 1, sum: 100.0 });
}

#[test]
fn history_matches_scoped_emission_one_to_one() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    assert_eq!(batcher.history().len(), emitted.len());
    assert_eq!(batcher.history().to_vec(), emitted);
    assert!(batcher.history().iter().all(|batch| batch.scope != "global"));
    assert_eq!(for_scope(batcher.history(), "ads").len(), 1);
    assert_eq!(for_scope(batcher.history(), "analytics").len(), 1);
    assert_eq!(for_scope(batcher.history(), "checkout").len(), 1);
}

#[test]
fn totals_match_scoped_batches_per_scope_and_global() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    batcher.ingest(ev("checkout", 200.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    assert_eq!(
        for_scope(&emitted, "analytics")[0].aggregate,
        Aggregate { count: 2, sum: 3.0 }
    );
    assert_eq!(
        for_scope(&emitted, "ads")[0].aggregate,
        Aggregate { count: 1, sum: 10.0 }
    );
    assert_eq!(
        for_scope(&emitted, "checkout")[0].aggregate,
        Aggregate { count: 2, sum: 300.0 }
    );
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 2, sum: 3.0 })
    );
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 1, sum: 10.0 })
    );
    assert_eq!(
        batcher.totals().scope("checkout"),
        Some(&Aggregate { count: 2, sum: 300.0 })
    );
    assert_eq!(batcher.totals().global, Aggregate { count: 5, sum: 313.0 });
    assert_eq!(batcher.totals().scope_count(), 3);
}

#[test]
fn batch_ids_ascend_from_zero_for_scoped_batches() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    let mut ids: Vec<u64> = emitted.iter().map(|batch| batch.batch_id).collect();
    ids.sort();
    assert_eq!(ids, vec![0, 1, 2]);
    let mut scopes: Vec<&str> = emitted.iter().map(|batch| batch.scope.as_str()).collect();
    scopes.sort();
    assert_eq!(scopes, vec!["ads", "analytics", "checkout"]);
    assert_eq!(for_scope(&emitted, "analytics").len(), 1);
    assert_eq!(for_scope(&emitted, "ads").len(), 1);
    assert_eq!(for_scope(&emitted, "checkout").len(), 1);
}

#[test]
fn empty_flush_after_scoped_flush_leaves_history_unchanged() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.scope != "global"));
    let history_before = batcher.history().to_vec();
    let empty = batcher.flush();
    assert!(empty.is_empty());
    assert_eq!(batcher.history().to_vec(), history_before);
    assert!(batcher.history().iter().all(|batch| batch.scope != "global"));
}

#[test]
fn second_ingest_flush_cycle_appends_with_continuing_ids() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    let first = batcher.flush();
    assert_eq!(first.len(), 2);
    let mut first_ids: Vec<u64> = first.iter().map(|batch| batch.batch_id).collect();
    first_ids.sort();
    assert_eq!(first_ids, vec![0, 1]);
    assert!(first.iter().all(|batch| batch.scope != "global"));

    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 20.0));
    let second = batcher.flush();
    assert_eq!(second.len(), 2);
    let mut second_ids: Vec<u64> = second.iter().map(|batch| batch.batch_id).collect();
    second_ids.sort();
    assert_eq!(second_ids, vec![2, 3]);
    assert!(second.iter().all(|batch| batch.scope != "global"));

    assert_eq!(batcher.history().len(), 4);
    let mut history_ids: Vec<u64> = batcher
        .history()
        .iter()
        .map(|batch| batch.batch_id)
        .collect();
    history_ids.sort();
    assert_eq!(history_ids, vec![0, 1, 2, 3]);
    let analytics: Vec<f64> = for_scope(batcher.history(), "analytics")
        .iter()
        .map(|batch| batch.aggregate.sum)
        .collect();
    assert_eq!(analytics, vec![1.0, 2.0]);
    let ads: Vec<f64> = for_scope(batcher.history(), "ads")
        .iter()
        .map(|batch| batch.aggregate.sum)
        .collect();
    assert_eq!(ads, vec![10.0, 20.0]);
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 2, sum: 3.0 })
    );
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 2, sum: 30.0 })
    );
    assert_eq!(batcher.totals().global, Aggregate { count: 4, sum: 33.0 });
}

#[test]
fn pending_count_is_zero_after_scoped_flush() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    assert_eq!(batcher.pending_count(), 3);
    let emitted = batcher.flush();
    assert_eq!(batcher.pending_count(), 0);
    assert_eq!(emitted.len(), 3);
    let mut scopes: Vec<&str> = emitted.iter().map(|batch| batch.scope.as_str()).collect();
    scopes.sort();
    assert_eq!(scopes, vec!["ads", "analytics", "checkout"]);
}

#[test]
fn every_batch_scope_is_a_real_event_scope_never_global() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 2.0));
    batcher.ingest(ev("checkout", 3.0));
    batcher.ingest(ev("payments", 4.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 4);
    let mut scopes: Vec<&str> = emitted.iter().map(|batch| batch.scope.as_str()).collect();
    scopes.sort();
    assert_eq!(scopes, vec!["ads", "analytics", "checkout", "payments"]);
    assert!(emitted.iter().all(|batch| batch.scope != "global"));
    assert_eq!(for_scope(&emitted, "payments")[0].aggregate, Aggregate { count: 1, sum: 4.0 });
}

#[test]
fn interleaved_arrival_keeps_scopes_in_separate_batches() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 20.0));
    batcher.ingest(ev("analytics", 3.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert_eq!(
        for_scope(&emitted, "analytics")[0].aggregate,
        Aggregate { count: 3, sum: 6.0 }
    );
    assert_eq!(
        for_scope(&emitted, "ads")[0].aggregate,
        Aggregate { count: 2, sum: 30.0 }
    );
    assert!(emitted.iter().all(|batch| batch.scope != "global"));
}
