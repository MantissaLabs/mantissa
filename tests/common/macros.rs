#[macro_export]
macro_rules! local_test {
    ($name:ident, $body:block) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            mantissa::logger::init_for_tests();
            common::testkit::run_local(async { $body }).await
        }
    };
}
