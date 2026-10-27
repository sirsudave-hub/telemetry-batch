use std::panic;

use telemetry_batch::{BatchConfig, Batcher};

#[test]
fn revoke_unimplemented_until_step_2() {
    let result = panic::catch_unwind(|| {
        Batcher::new(BatchConfig { max_batch_events: 10 })
            .unwrap()
            .revoke("some-scope")
    });
    assert!(result.is_err());
}
