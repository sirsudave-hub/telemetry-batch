use telemetry_batch::{Aggregate, Batch, Batcher, Event};

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
fn initial_generation_zero_and_pre_revoke_batches_carry_zero() {
    let mut batcher = Batcher::default();
    assert_eq!(batcher.restatement_generation(), 0);
    batcher.ingest(ev("ads", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.generation == 0));
    assert_eq!(
        for_scope(&emitted, "ads")[0].aggregate,
        Aggregate { count: 1, sum: 1.0 }
    );
    assert_eq!(
        for_scope(&emitted, "analytics")[0].aggregate,
        Aggregate { count: 1, sum: 2.0 }
    );
    assert_eq!(batcher.restatement_generation(), 0);
}

#[test]
fn effective_revoke_advances_and_new_batches_carry_new_generation() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("ads", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.generation == 0));
    batcher.revoke("ads");
    assert_eq!(batcher.restatement_generation(), 1);
    batcher.ingest(ev("analytics", 3.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].scope, "analytics");
    assert_eq!(emitted[0].generation, 1);
    assert_eq!(emitted[0].aggregate, Aggregate { count: 1, sum: 3.0 });
    assert_eq!(batcher.restatement_generation(), 1);
}

#[test]
fn surviving_history_keeps_old_generation_after_revoke() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("a", 1.0));
    batcher.ingest(ev("b", 2.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.generation == 0));
    batcher.revoke("a");
    assert_eq!(batcher.restatement_generation(), 1);
    batcher.ingest(ev("b", 3.0));
    batcher.ingest(ev("c", 4.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    assert!(emitted.iter().all(|batch| batch.generation == 1));
    assert_eq!(
        for_scope(&emitted, "b")[0].aggregate,
        Aggregate { count: 1, sum: 3.0 }
    );
    assert_eq!(
        for_scope(&emitted, "c")[0].aggregate,
        Aggregate { count: 1, sum: 4.0 }
    );
    batcher.revoke("c");
    assert_eq!(batcher.restatement_generation(), 2);
    assert_eq!(batcher.history().len(), 2);
    let surviving = for_scope(batcher.history(), "b");
    assert_eq!(surviving.len(), 2);
    assert_eq!(surviving[0].generation, 0);
    assert_eq!(surviving[0].aggregate, Aggregate { count: 1, sum: 2.0 });
    assert_eq!(surviving[1].generation, 1);
    assert_eq!(surviving[1].aggregate, Aggregate { count: 1, sum: 3.0 });
}

#[test]
fn double_revoke_does_not_advance() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("ads", 1.0));
    batcher.ingest(ev("analytics", 2.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 2);
    batcher.revoke("ads");
    assert_eq!(batcher.restatement_generation(), 1);
    let snapshot = batcher.history().to_vec();
    batcher.revoke("ads");
    assert_eq!(batcher.restatement_generation(), 1);
    assert_eq!(batcher.history().to_vec(), snapshot);
}

#[test]
fn unknown_scope_revoke_without_data_does_not_advance() {
    let mut batcher = Batcher::default();
    batcher.revoke("never-seen");
    assert!(batcher.is_revoked("never-seen"));
    assert_eq!(batcher.restatement_generation(), 0);
    batcher.ingest(ev("ads", 1.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].generation, 0);
    batcher.revoke("ads");
    assert_eq!(batcher.restatement_generation(), 1);
    batcher.revoke("other-unknown");
    assert_eq!(batcher.restatement_generation(), 1);
}

#[test]
fn buffered_only_scope_revoke_advances() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("ads", 1.0));
    assert_eq!(batcher.pending_count(), 1);
    assert_eq!(batcher.restatement_generation(), 0);
    batcher.revoke("ads");
    assert_eq!(batcher.restatement_generation(), 1);
    assert_eq!(batcher.pending_count(), 0);
    assert!(batcher.is_revoked("ads"));
}

#[test]
fn two_effective_revokes_reach_two() {
    let mut batcher = Batcher::default();
    batcher.ingest(ev("a", 1.0));
    batcher.ingest(ev("b", 2.0));
    batcher.ingest(ev("c", 3.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 3);
    assert!(emitted.iter().all(|batch| batch.generation == 0));
    batcher.revoke("a");
    assert_eq!(batcher.restatement_generation(), 1);
    batcher.revoke("b");
    assert_eq!(batcher.restatement_generation(), 2);
    assert_eq!(batcher.history().len(), 1);
    assert_eq!(batcher.history()[0].scope, "c");
    assert_eq!(batcher.history()[0].generation, 0);
    assert_eq!(
        batcher.history()[0].aggregate,
        Aggregate { count: 1, sum: 3.0 }
    );
    batcher.ingest(ev("c", 4.0));
    let emitted = batcher.flush();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].scope, "c");
    assert_eq!(emitted[0].generation, 2);
    assert_eq!(emitted[0].aggregate, Aggregate { count: 1, sum: 4.0 });
}
