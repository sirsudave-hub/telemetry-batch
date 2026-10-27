use telemetry_batch::{BatchConfig, Batcher};

fn batcher() -> Batcher {
    Batcher::new(BatchConfig { max_batch_events: 10 }).unwrap()
}

#[test]
#[should_panic]
fn revoke_unimplemented_until_step_2() {
    batcher().revoke("some-scope");
}
