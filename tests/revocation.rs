use telemetry_batch::{Aggregate, Batch, BatchConfig, Batcher, Event, Totals};

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

#[test]
fn is_revoked_false_before_true_after_other_scopes_false() {
    let mut batcher = Batcher::default();
    assert!(!batcher.is_revoked("analytics"));
    assert!(!batcher.is_revoked("ads"));
    batcher.revoke("analytics");
    assert!(batcher.is_revoked("analytics"));
    assert!(!batcher.is_revoked("ads"));
    assert!(!batcher.is_revoked("checkout"));
}

#[test]
fn revoke_purges_buffered_unflushed_events_only_for_that_scope() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("checkout", 100.0));
    assert_eq!(batcher.pending_count(), 4);
    batcher.revoke("analytics");
    assert!(batcher.is_revoked("analytics"));
    assert_eq!(batcher.pending_count(), 2);
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert_eq!(for_scope(&emitted, "ads").len(), 1);
    assert_eq!(
        for_scope(&emitted, "ads")[0].aggregate,
        Aggregate { count: 1, sum: 10.0 }
    );
    assert_eq!(for_scope(&emitted, "checkout").len(), 1);
    assert_eq!(
        for_scope(&emitted, "checkout")[0].aggregate,
        Aggregate { count: 1, sum: 100.0 }
    );
    assert_eq!(for_scope(&emitted, "analytics").len(), 0);
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 1, sum: 10.0 })
    );
    assert_eq!(
        batcher.totals().scope("checkout"),
        Some(&Aggregate { count: 1, sum: 100.0 })
    );
    assert_eq!(batcher.totals().scope("analytics"), None);
    assert_eq!(batcher.totals().global, Aggregate { count: 2, sum: 110.0 });
    assert_eq!(batcher.totals().scope_count(), 2);
}

#[test]
fn revoke_drops_history_batches_and_recomputes_totals() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 10.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert_eq!(batcher.history().len(), 2);
    assert_eq!(batcher.totals().global, Aggregate { count: 3, sum: 13.0 });

    batcher.revoke("analytics");
    assert!(batcher.is_revoked("analytics"));
    assert_eq!(batcher.history().len(), 1);
    assert_eq!(batcher.history()[0].scope, "ads");
    assert_eq!(batcher.history()[0].aggregate, Aggregate { count: 1, sum: 10.0 });
    assert_eq!(batcher.totals().scope("analytics"), None);
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 1, sum: 10.0 })
    );
    assert_eq!(batcher.totals().global, Aggregate { count: 1, sum: 10.0 });
    assert_eq!(batcher.totals().scope_count(), 1);
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn ingest_after_revoke_is_dropped() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 5.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(batcher.totals().global, Aggregate { count: 1, sum: 5.0 });

    batcher.revoke("analytics");
    assert!(batcher.history().is_empty());
    assert_eq!(batcher.totals(), &Totals::new());
    assert_eq!(batcher.totals().scope_count(), 0);

    batcher.ingest(ev("analytics", 7.0));
    assert_eq!(batcher.pending_count(), 0);
    let emitted = batcher.flush();
    assert!(emitted.is_empty());
    assert!(batcher.history().is_empty());
    assert_eq!(batcher.totals(), &Totals::new());
    assert_eq!(batcher.totals().global, Aggregate { count: 0, sum: 0.0 });
}

#[test]
fn revoke_unknown_scope_marks_it_and_drops_later_ingest() {
    let mut batcher = Batcher::default();
    assert!(!batcher.is_revoked("never_seen"));
    batcher.revoke("never_seen");
    assert!(batcher.is_revoked("never_seen"));
    assert!(!batcher.is_revoked("analytics"));

    batcher.ingest(ev("never_seen", 99.0));
    assert_eq!(batcher.pending_count(), 0);
    batcher.ingest(ev("analytics", 5.0));
    assert_eq!(batcher.pending_count(), 1);
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].scope, "analytics");
    assert_eq!(emitted[0].aggregate, Aggregate { count: 1, sum: 5.0 });
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 1, sum: 5.0 })
    );
    assert_eq!(batcher.totals().scope("never_seen"), None);
    assert_eq!(batcher.totals().global, Aggregate { count: 1, sum: 5.0 });
}

#[test]
fn double_revoke_is_byte_identical_to_single_revoke() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);

    batcher.revoke("analytics");
    let history_after_first = batcher.history().to_vec();
    let totals_after_first = batcher.totals().clone();

    batcher.revoke("analytics");
    assert!(batcher.is_revoked("analytics"));
    assert_eq!(batcher.history().to_vec(), history_after_first);
    assert_eq!(batcher.totals(), &totals_after_first);
    assert_eq!(batcher.pending_count(), 0);
}

#[test]
fn two_scopes_revoked_in_sequence_compose() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);

    batcher.revoke("analytics");
    batcher.revoke("ads");
    assert!(batcher.is_revoked("analytics"));
    assert!(batcher.is_revoked("ads"));
    assert!(!batcher.is_revoked("checkout"));
    assert_eq!(batcher.history().len(), 1);
    assert_eq!(batcher.history()[0].scope, "checkout");
    assert_eq!(
        batcher.history()[0].aggregate,
        Aggregate { count: 1, sum: 100.0 }
    );
    assert_eq!(batcher.totals().scope("analytics"), None);
    assert_eq!(batcher.totals().scope("ads"), None);
    assert_eq!(
        batcher.totals().scope("checkout"),
        Some(&Aggregate { count: 1, sum: 100.0 })
    );
    assert_eq!(batcher.totals().global, Aggregate { count: 1, sum: 100.0 });
    assert_eq!(batcher.totals().scope_count(), 1);
}

#[test]
fn convergence_under_continued_ingestion_reaches_fixed_point() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("checkout", 100.0));
    let first = batcher.flush();
    assert_eq!(first.len(), 3);

    batcher.revoke("ads");
    assert!(batcher.is_revoked("ads"));

    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 20.0));
    batcher.ingest(ev("checkout", 200.0));
    assert_eq!(batcher.pending_count(), 2);
    let second = batcher.flush();
    assert_eq!(second.len(), 2);
    assert_eq!(
        for_scope(&second, "analytics")[0].aggregate,
        Aggregate { count: 1, sum: 2.0 }
    );
    assert_eq!(
        for_scope(&second, "checkout")[0].aggregate,
        Aggregate { count: 1, sum: 200.0 }
    );
    assert_eq!(for_scope(&second, "ads").len(), 0);

    assert_eq!(batcher.history().len(), 4);
    let analytics: Vec<f64> = for_scope(batcher.history(), "analytics")
        .iter()
        .map(|batch| batch.aggregate.sum)
        .collect();
    assert_eq!(analytics, vec![1.0, 2.0]);
    let checkout: Vec<f64> = for_scope(batcher.history(), "checkout")
        .iter()
        .map(|batch| batch.aggregate.sum)
        .collect();
    assert_eq!(checkout, vec![100.0, 200.0]);
    assert_eq!(for_scope(batcher.history(), "ads").len(), 0);
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 2, sum: 3.0 })
    );
    assert_eq!(
        batcher.totals().scope("checkout"),
        Some(&Aggregate { count: 2, sum: 300.0 })
    );
    assert_eq!(batcher.totals().scope("ads"), None);
    assert_eq!(batcher.totals().global, Aggregate { count: 4, sum: 303.0 });
    assert_eq!(batcher.totals().scope_count(), 2);

    let history_before = batcher.history().to_vec();
    let totals_before = batcher.totals().clone();
    assert!(batcher.flush().is_empty());
    assert!(batcher.flush().is_empty());
    assert_eq!(batcher.history().to_vec(), history_before);
    assert_eq!(batcher.totals(), &totals_before);
}

#[test]
fn live_scopes_still_batch_per_scope_after_a_revoke() {
    let mut batcher = batcher(2);
    batcher.ingest(ev("analytics", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("analytics", 3.0));
    batcher.ingest(ev("ads", 10.0));
    batcher.ingest(ev("ads", 20.0));
    batcher.ingest(ev("ads", 30.0));
    let first = batcher.flush();
    assert_eq!(first.len(), 4);
    assert_eq!(for_scope(&first, "analytics").len(), 2);
    assert_eq!(for_scope(&first, "ads").len(), 2);

    batcher.revoke("ads");
    assert_eq!(batcher.history().len(), 2);
    assert_eq!(for_scope(batcher.history(), "ads").len(), 0);

    batcher.ingest(ev("analytics", 4.0));
    batcher.ingest(ev("analytics", 5.0));
    batcher.ingest(ev("checkout", 100.0));
    let second = batcher.flush();
    assert_eq!(second.len(), 2);
    assert_eq!(
        for_scope(&second, "analytics")[0].aggregate,
        Aggregate { count: 2, sum: 9.0 }
    );
    assert_eq!(
        for_scope(&second, "checkout")[0].aggregate,
        Aggregate { count: 1, sum: 100.0 }
    );

    let analytics: Vec<f64> = for_scope(batcher.history(), "analytics")
        .iter()
        .map(|batch| batch.aggregate.sum)
        .collect();
    assert_eq!(analytics, vec![3.0, 3.0, 9.0]);
    assert_eq!(for_scope(batcher.history(), "checkout").len(), 1);
    assert_eq!(for_scope(batcher.history(), "ads").len(), 0);
    assert_eq!(
        batcher.totals().scope("analytics"),
        Some(&Aggregate { count: 5, sum: 15.0 })
    );
    assert_eq!(
        batcher.totals().scope("checkout"),
        Some(&Aggregate { count: 1, sum: 100.0 })
    );
    assert_eq!(batcher.totals().scope("ads"), None);
    assert_eq!(batcher.totals().global, Aggregate { count: 6, sum: 115.0 });
    assert_eq!(batcher.totals().scope_count(), 2);
}

#[test]
fn revoke_purges_both_buffered_and_history_for_the_scope() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("analytics", 1.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(batcher.history().len(), 1);

    batcher.ingest(ev("analytics", 2.0));
    batcher.ingest(ev("ads", 10.0));
    assert_eq!(batcher.pending_count(), 2);

    batcher.revoke("analytics");
    assert!(batcher.is_revoked("analytics"));
    assert_eq!(batcher.pending_count(), 1);
    assert!(batcher.history().is_empty());
    assert_eq!(batcher.totals(), &Totals::new());
    assert_eq!(batcher.totals().global, Aggregate { count: 0, sum: 0.0 });

    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].scope, "ads");
    assert_eq!(emitted[0].aggregate, Aggregate { count: 1, sum: 10.0 });
    assert_eq!(
        batcher.totals().scope("ads"),
        Some(&Aggregate { count: 1, sum: 10.0 })
    );
    assert_eq!(batcher.totals().global, Aggregate { count: 1, sum: 10.0 });
}
