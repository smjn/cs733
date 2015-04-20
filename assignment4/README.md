# Completed RAFT implementation with a KVStore behind it

## Assignment 4 for CS733, Engineering a Cloud, CS733, IIT Bombay

### Authors:
* Sushant Mahajan (133059007)
* Discussed with - Bharath Radhakrishnan (133050014)

The raft implementation is complete. commands are getting replicated across all replicas and the leader is the only one responding.

If the client tries to connect to a non-leader, he receives redirect error and connection is closed. Leader election works.

### Note:
The log files will become very large on running test cases. They are named as log{0..4}.
