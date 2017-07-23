The key-value store supports the following functionalities:
1. Support for CRUD operations (Create, Read, Update, Delete)
2. Load-balancing (via a consistent hashing ring to hash both servers and keys)
3. Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key)
4. Quorum consistency level for both reads and writes (at least two replicas)
5. Stabilization after failure (recreate three replicas after failure)

There isn't a single leader coordinating all the reads and writes from multiple replicas of data. Instead, applications simply choose randomly a key-value store to coordinate a read or a write. However, the leader election can be exploited.

The key-value store makes use of the gossip-style membership middleware implemented in ccmp1. When a key-value store detects that some replicas are down, it runs a stabilization protocol to ensure that there are always 3 replicas for a key in the cluster.
