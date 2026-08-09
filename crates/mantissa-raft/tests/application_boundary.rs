use std::convert::Infallible;
use std::future;

use mantissa_raft::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplicationStateMachine,
    ApplyContext, EntryResponse, RaftApplication, SnapshotRead, TypeConfig,
};
use openraft::{EmptyNode, RaftTypeConfig};

#[derive(Debug, Eq, PartialEq)]
struct TestCommand {
    delta: u64,
}

impl ApplicationCommand for TestCommand {}

#[derive(Debug, Eq, PartialEq)]
struct TestResponse {
    value: u64,
    applied_index: u64,
}

impl ApplicationResponse for TestResponse {}

#[derive(Debug, Eq, PartialEq)]
struct TestSnapshot {
    value: u64,
}

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Returns the complete empty stream used by this type-boundary test.
    fn read_chunk(
        &mut self,
        _maximum_bytes: usize,
    ) -> impl Future<Output = Result<SnapshotRead, Self::Error>> + Send {
        future::ready(Ok(SnapshotRead::new(Vec::new(), true)))
    }

    /// Accepts no bytes because this test only checks the public type path.
    fn write_chunk(
        &mut self,
        _bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        future::ready(Ok(()))
    }

    /// Completes the unused snapshot handle.
    fn finish_write(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        future::ready(Ok(()))
    }
}

struct TestApplication;

impl RaftApplication for TestApplication {
    type Command = TestCommand;
    type Response = TestResponse;
    type Snapshot = TestSnapshot;
    type Error = Infallible;
    type NodeId = u64;
    type Node = EmptyNode;
}

#[derive(Default)]
struct CounterStateMachine {
    value: u64,
}

impl ApplicationStateMachine<TestApplication> for CounterStateMachine {
    /// Applies one typed counter update for the public-boundary test.
    fn apply(
        &mut self,
        context: ApplyContext,
        command: TestCommand,
    ) -> impl Future<Output = Result<TestResponse, Infallible>> + Send {
        self.value += command.delta;
        future::ready(Ok(TestResponse {
            value: self.value,
            applied_index: context.index(),
        }))
    }
}

/// Requires OpenRaft to observe every associated type from the application.
fn assert_openraft_config<C>()
where
    C: RaftTypeConfig<
            D = TestCommand,
            R = EntryResponse<TestResponse>,
            NodeId = u64,
            Node = EmptyNode,
            SnapshotData = TestSnapshot,
        >,
{
}

#[tokio::test]
async fn typed_application_crosses_the_public_boundary() {
    assert_openraft_config::<TypeConfig<TestApplication>>();

    let mut state_machine = CounterStateMachine::default();
    let response = match state_machine
        .apply(ApplyContext::new(3, 11), TestCommand { delta: 7 })
        .await
    {
        Ok(response) => response,
        Err(error) => match error {},
    };

    assert_eq!(
        TestResponse {
            value: 7,
            applied_index: 11,
        },
        response
    );

    let snapshot = TestSnapshot {
        value: response.value,
    };
    assert_eq!(TestSnapshot { value: 7 }, snapshot);
}
