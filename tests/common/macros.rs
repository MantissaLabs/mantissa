#[macro_export]
macro_rules! local_test {
    ($name:ident, $body:block) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            mantissa::logger::init_for_tests();
            // Headless-node integration tests can build large async state machines.
            // Keep those states off the libtest thread stack while still running
            // inside the LocalSet required by spawn_local-based components.
            common::testkit::run_local(Box::pin(async { $body })).await
        }
    };
}
