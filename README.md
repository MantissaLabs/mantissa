# Mantissa

## Introduction

Mantissa is a distributed and energy-efficient application scheduler and cluster management system. It is mainly designed to be portable across operating systems and integrate between multiple heterogeneous environments (x86, ARM).

Mantissa is self-organizing, fault tolerant and self-healing. It both considers deployments on reliable infrastructure with infiniband network interconnections as well as unreliable environments with unreliable network such as cloud infrastructures.

It supports Advanced reservation scenarios and Gang Scheduling.

The goal is to achieve reliability while allowing maximum utilization of the hardware.

# Features

Mantissa gathers all of the following into a single comprehensive binary:

- A scheduler
- Cluster state management (using CRDTs)
- Network management (Ingress, network policies)
- Storage (persistent volumes)
- Secret Management
- Service discovery
- Load balancing
- Cluster / task autoscaler
- External API

Mantissa is easy to use and maintain.


## Setup a cluster

Bootstraping a new cluster with mantissa is rather easy. Nodes taking part in a mantissa cluster are all equal, ie. there are no leader or followers. Each node is responsible for a part of the cluster state and can be used to schedule tasks.

### Initialize a new cluster

To initialize a new cluster, simply type:

```
$ mantissa bootstrap
```

This will bootstrap a single node with a topology server and an agent server. New members could be added to the cluster by linking to this node.

### Join an existing cluster

To join an existing cluster (locally, for testing), type:

```
$ mantissa --listen 127.0.0.1:6580 --anchor 127.0.0.1:6578 link
```

This will add a new member to the mantissa cluster.

### List members of the cluster

Mantissa is topology-aware and multi-region enabled by default, which means that we can have multiple sub-clusters in a single, worldwide mantissa.

To list the clusters, simply type:

```
$ mantissa clusters list
ID                   NAME          NODES
3284900234950425920  aws-region-1  100
8160031073963094169  aws-region-2  100
```

We have only one cluster available, so far so good, but let's see how many nodes we have inside that cluster:

```
$ mantissa nodes list
ID                    HOSTNAME  ENDPOINT
10142400562995393014  mantissa  127.0.0.1:6579
14018497205078696097  mantissa  127.0.0.1:6580
17777715967294288234  mantissa  127.0.0.1:6578
```

We have two processes attached to our mantissa cluster. It is ok to have multiple processes on the same node, mantissa is smart enough to detect that, in which case we can allow multiple users or group to share the same machine.

## License

Licensed under either of

* Apache License, Version 2.0, (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.

### Contribution

See [CONTRIBUTING.md](CONTRIBUTING.md) and [Code-of-Conduct.md](Code-of-Conduct.md) for more information.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this repository by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

### Authors

**Alexandre Beslic**

- [abronan.com](https://abronan.com)
- [@abronan](https://twitter.com/abronan)
