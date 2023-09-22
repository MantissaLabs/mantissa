@0xde292e0f854316dc;

using Topology = import "topology.capnp";
using Ousterhout = import "ousterhout.capnp";
using Stat = import "stat.capnp";
using Utils = import "utils.capnp";

interface Delegate {
  # Delegate describes calls that are used to schedule tasks.
  # The server creates handles from the neighbors list to call
  # relevant methods.

  info @0 () -> (info :Stat.System);
  # Return informations on the agent.

  book @1 (req :Ousterhout.SlotRequest) -> (alloc :Ousterhout.Allocation);
  # Book slots. Takes a vector of slots in parameter with necessary workload
  # informations. Returns a promise of allocation.

  free @2 (req :Ousterhout.SlotRequest) -> ();
  # Free slots. Takes a vector of slots to release.

  schedule @3 (workload: Ousterhout.Workload) -> (allocation :Ousterhout.Allocation);
  # Schedules a task.

  run @4 (workload: Ousterhout.Workload) -> ();
  # Executes a workload from a given order.

  list @5 () -> (tasks :TaskList);
  # List tasks running on the delegate.
}

struct TaskList {
  tasks @0 :List(TaskInfo);
  # Contains a list of tasks running on a delegate.
}

struct TaskInfo {
  # A Task running on the delegate. Could be
  # a container, a package or a binary.

  uuid @0 :Text;
  # UUID of the task, it must be unique.

  name @1 :Text;
  # Name of the task.

  kind @2 :Kind;
  # The kind of "packaging" used for the task.

  replicas @3 :UInt64;
  # The number of replicas for that task.

  image @4 :Text;
  # The image used if the process is a container.

  created @5 :Utils.Date;
  # Date on which the task was created.

  machine @6 :Topology.NodeInfo;
  # Machine the task is running on. Used when listing
  # tasks from another node.

  # The kind of runnable/packaging used for the task.
  enum Kind {
    binary @0;
    container @1;
    package @2;
  }
}
