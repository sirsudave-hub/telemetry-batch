use telemetry_batch::{Batcher, Event};

#[test]
fn emitted_events_leave_no_trace_in_debug_rendering() {
    let mut batcher = Batcher::default();
    batcher.ingest(Event::new("meridian", "zephyr_signal_4815", 1.0, 8888777666555));
    batcher.ingest(Event::new("harbor", "zephyr_signal_4815", 2.0, 8888777666555));
    let emitted = batcher.flush();
    assert!(!emitted.is_empty());
    assert_eq!(batcher.pending_count(), 0);
    let rendering = format!("{:?}", batcher);
    assert!(!rendering.contains("zephyr_signal_4815"));
    assert!(!rendering.contains("8888777666555"));
}
