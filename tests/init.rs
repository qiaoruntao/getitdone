#![cfg(feature = "tracing")]
use getitdone::init_test_tracing;

#[test]
fn tracing_initializer_smoke_test() {
    // We just want to ensure it doesn't panic and returns a guard
    let guard = init_test_tracing("getitdone-tests", None);
    assert!(guard.is_ok());
}
