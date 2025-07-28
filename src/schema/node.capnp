@0xde292e0f854316dc;

using Topology = import "topology.capnp";
using Ousterhout = import "ousterhout.capnp";
using Stat = import "stat.capnp";
using Utils = import "utils.capnp";

# Node contains informations about the worker node as well as
# its capabilities, which are used to schedule and execute tasks.
interface Node {
  # Returns informations about the node, its resource usage, etc.
  info @0 () -> (info :Stat.System);

  # Returns a handle to the scheduler component of the node, used
  # to book resources in order to execute a task.
  scheduler @1 () -> (sched :Scheduler);

  # Returns a handle to the executor, used to run tasks given a
  # description and resource allocation.
  executor @2 () -> (exec :Executor);
}

# Executor takes tasks descriptions and runs them on the local machine.
interface Executor {
  # Executes a workload from a given order.
  run @0 (workload: Ousterhout.Workload) -> ();

  # List tasks running on the node.
  list @1 () -> (tasks :TaskList);
}

interface NodeStats {
  info @0 () -> (info :Stat.System);
}

# Scheduler describes calls that are used to schedule and cancel tasks.
interface Scheduler {
  # Book slots. Takes a vector of slots in parameter with necessary workload
  # informations. Returns a promise of allocation.
  book @0 (req :Ousterhout.SlotRequest) -> (alloc :Ousterhout.Allocation);

  # Free slots. Takes a vector of slots to release.
  free @1 (req :Ousterhout.SlotRequest) -> ();

  # Schedules a task.
  schedule @2 (workload: Ousterhout.Workload) -> (allocation :Ousterhout.Allocation);
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
