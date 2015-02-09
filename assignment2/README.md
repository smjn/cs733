# RAFT layer below the KV store

Assignment 2 of course Engineering a Cloud, CS733, IIT Bombay

Author:	
* Sushant Mahajan
* Bharath Radhakrishnan
* Harshit Pande

This part of assignment implements the RAFT layer on top of the KVStore prepared in the previous assignment. This is a consensus protocol which allows coherent replication across multiple servers (here simulated by separate processes).

In our test setup we had one leader server and four followers. Currently the leader receives all the client communication and if some other server is contacted it redirects the user to the leader. The leader adds the user commands to its log and also broadcasts it to the follower server. The followers acknowledge. If the leader receives a majority (including itself), the entry is committed  and the followers notified of the commmit. The command is then executed on all replicas.

In this iteration, we've modularised the code as follows:
* raft
	* raft.go - implements the raft layer.
	* kvstore.go - the older kvstore from assignment1 with minor modifications.
* connhandler
	* connhandler.go - dedicated to handling a single client connection.
* utils
	* utils.go - functionality to encode/decode the user commands to and from a byte array.
* server.go - server code that initializes it all (the main file).
* server_test.go - test cases for all flows.

Note: The test cases are still under develpment.



