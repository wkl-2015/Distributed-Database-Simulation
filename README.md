# Distributed-Database-Simulation

Authors: kw1443@nyu.edu hj690@nyu.edu

The project implements a distributed database in Java, with multi-version concurrency control, deadlock avoidance, replication, and failure recovery. The database can read input from a file or via command line, and can report the status of transactions (committed, aborted, etc) to the user.
The serializability is ensured by two-phase locking and deadlock avoidance is achieved by the wait-die algorithm. Available copies algorithm is adopted to enhance fault tolerance and provide failure recovery.