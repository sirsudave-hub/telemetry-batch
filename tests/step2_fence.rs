use std::panic::{self, AssertUnwindSafe};

use telemetry_batch::Batcher;

// Step-2 over-execution fence for the step-3 query
// `pub fn restatement_generation(&self) -> u64` on `Batcher`.
// The file-local trait default panics; on trees without the inherent
// method it resolves and the test passes, while on trees where step 3
// landed the inherent method shadows it, returns, and the test fails.
trait Step3RestatementProbe {
    fn restatement_generation(&self) -> u64 {
        panic!("restatement_generation is not implemented until step 3");
    }
}

impl Step3RestatementProbe for Batcher {}

#[test]
fn restatement_generation_unimplemented_until_step_3() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        Batcher::default().restatement_generation()
    }));
    assert!(result.is_err());
}
