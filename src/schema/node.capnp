@0xde292e0f854316dc;

using Topology = import "topology.capnp";
using Scheduling = import "scheduling.capnp";
using Stat = import "stat.capnp";
using Utils = import "utils.capnp";

interface Node {
  # Node contains informations about the worker node as well as
  # its capabilities, which are used to schedule and execute tasks.

  info @0 () -> (info :Stat.System);
  # Returns informations about the node, its resource usage, etc.

  scheduler @1 () -> (sched :Scheduler);
  # Returns a handle to the scheduler component of the node, used
  # to book resources in order to execute a task.

  executor @2 () -> (exec :Executor);
  # Returns a handle to the executor, used to run tasks given a
  # description and resource allocation.
}

interface Executor {
  # Executor takes tasks descriptions and runs them on the local machine.

  run @0 (workload: Scheduling.Workload) -> ();
  # Executes a workload from a given order.

  list @1 () -> (tasks :TaskList);
  # List tasks running on the node.
}

interface NodeStats {
  # NodeStats contains informations about the node, its resource usage, etc.

  info @0 () -> (info :Stat.System);
}

interface Scheduler {
  # Scheduler describes calls that are used to schedule and cancel tasks.

  book @0 (req :Scheduling.SlotRequest) -> (alloc :Scheduling.Allocation);
  # Book slots. Takes a vector of slots in parameter with necessary workload
  # informations. Returns a promise of allocation.

  free @1 (req :Scheduling.SlotRequest) -> ();
  # Free slots. Takes a vector of slots to release.

  schedule @2 (workload: Scheduling.Workload) -> (allocation :Scheduling.Allocation);
  # Schedules a task.
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
