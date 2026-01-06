use getitdone::{init_test_tracing, TestTracingGuard};

#[test]
fn stdout_initializer_smoke_test() {
    match init_test_tracing("getitdone-tests", None) {
        Ok(TestTracingGuard::Stdout(provider)) => {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
        Ok(TestTracingGuard::Otlp(_)) => panic!("expected stdout tracing"),
        Err(e) => panic!("failed to init tracing: {e}"),
    }
}
