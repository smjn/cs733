# Replicated key-value store built on top of RAFT layer implementing a shared log interface

##Assignment 2 of course Engineering a Cloud, CS733, IIT Bombay

###Authors:	
* Sushant Mahajan (133059007)
* Bharath Radhakrishnan (133050014)
* Harshit Pande (133050040)

###Note: As required by the assignment this part doesn't implement full RAFT protocol but only the procedure to get majority from the followers and then updating the shared log in all the server.

This part of assignment implements the RAFT layer on top of the KVStore prepared in the assignment1 (https://git.cse.iitb.ac.in/smahajan/cs733/tree/master/assignment1). This is a consensus protocol which allows coherent replication across multiple servers (here simulated by separate processes).

In our test setup we had one leader server and four followers. Currently the leader receives all the client communication and if some other server is contacted it redirects the user to the leader. The leader adds the user commands to its log and also broadcasts it to the follower server. The followers acknowledge. If the leader receives a majority (including itself), the entry is committed  and the followers notified of the commmit. The command is then executed on all replicas.

In this iteration, we've modularised the code as follows:
* raft
	* raft.go - implements the partial raft layer.
	* kvstore.go - the older kvstore from assignment1 with minor modifications.
* connhandler
	* connhandler.go - dedicated to handling a single client connection.
* utils
	* utils.go - functionality to encode/decode the user commands to and from a byte array.
* server.go - server code that initializes it all (the main file).
* server_test.go - test cases for all flows.

Note: The test cases are still under development.

###Usage:
You need to simply run the NUM_SERVERS (which set 5 as of now in `server_test.go`) instances of `server_go`
In bash this can be done simply as:
```
#!/bin/bash

for i in {1..5}
do
	go run server.go $i 5 &
done
```

Now your client is ready to talk to these servers. The following ports are used:
####Leader is talking with client at the port 9001
####Leader is talking with followers at the port 20001
####The four followers talk to leader at the ports 20002, 20003, 20004, 20005



