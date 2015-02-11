//testing

package main

import (
	"bytes"
	//"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"raft"
	"strconv"
	"testing"
	"time"
)

//constant values used
const (
	NUM_SERVERS int = 5
)

type Testpair struct {
	to_server   []byte
	from_server []byte
}

//
func TestAll(t *testing.T) {
	//start the servers
	/*for i := 1; i <= NUM_SERVERS; i++ {
		go testServersCommunic(i, t)
	}
	//wait for some time so that servers are ready
	time.Sleep(4 * time.Second)*/

	//run client that tries connecting to the followers
	testConnectFollower(t)

	//test no reply
	testNoReply(t)

	testSet(t)

	//note: this testing function is for our own verifiaction that replication did infact
	//go through and is not part of standard client-server interface
	//should be commented out when pitted against others servers
	testReplication(t)
}

//run servers
func testServersCommunic(i int, t *testing.T) {
	cmd := exec.Command("go", "run", "server.go", strconv.Itoa(i), strconv.Itoa(NUM_SERVERS), "x")
	f, err := os.OpenFile(strconv.Itoa(i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		t.Errorf("error opening file: %v", err)
	}

	defer f.Close()
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Run()
}

//on trying to connect with the followers the client should get redirect err from the followers
func testConnectFollower(t *testing.T) {
	for i := 2; i <= NUM_SERVERS; i++ { //the followers start at second position
		server_port := raft.CLIENT_PORT + i
		conn, err := net.Dial("tcp", ":"+strconv.Itoa(server_port))
		if err != nil {
			t.Error("Error in connecting the server at port: " + strconv.Itoa(server_port))
		} else {
			//time.Sleep(time.Millisecond)
			sending := []byte("set mykey1 100 3\r\nlul\r\n")
			port := strconv.Itoa(raft.CLIENT_PORT + 1)
			expecting := []byte("ERR_REDIRECT 127.0.0.1 " + port + "\r\n")

			conn.Write(sending)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
			n := bytes.Index(buffer, []byte{0})
			if !bytes.Equal(buffer[:n], expecting) {
				t.Error(
					"For", sending, string(sending),
					"expected", expecting, string(expecting),
					"got", buffer[:n], string(buffer[:n]),
				)
			}
			conn.Close()
			//time.Sleep(time.Millisecond)
		}
	}
}

//noreply option is not more valid with set and cas
//client should get command error from the server if it sends 'no reply' option
func testNoReply(t *testing.T) {
	var noreply_cases = []Testpair{
		{[]byte("set mykey1 100 3 noreply\r\n"), []byte("ERR_CMD_ERR\r\n")},
		{[]byte("cas mykey1 0 1 6 noreply\r\n"), []byte("ERR_CMD_ERR\r\n")},
	}

	server_port := raft.CLIENT_PORT + 1

	conn, err := net.Dial("tcp", ":"+strconv.Itoa(server_port))
	if err != nil {
		t.Error("Error in connecting the server at port: " + strconv.Itoa(server_port))
	} else {
		//time.Sleep(time.Millisecond)
		for _, pair := range noreply_cases {
			conn.Write(pair.to_server)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
			n := bytes.Index(buffer, []byte{0})
			//fmt.Println(buffer)
			if !bytes.Equal(buffer[:n], pair.from_server) {
				t.Error(
					"For", pair.to_server, string(pair.to_server),
					"expected", pair.from_server, string(pair.from_server),
					"got", buffer[:n], string(buffer[:n]),
				)
			}
		}
		conn.Close()
		//time.Sleep(time.Millisecond)
	}
}

//simple test case
func testSet(t *testing.T) {
	var noreply_cases = []Testpair{
		{[]byte("set mykey1 100 3\r\nadd\r\n"), []byte("OK 1\r\n")},
	}

	server_port := raft.CLIENT_PORT + 1

	conn, err := net.Dial("tcp", ":"+strconv.Itoa(server_port))
	if err != nil {
		t.Error("Error in connecting the server at port: " + strconv.Itoa(server_port))
	} else {
		//time.Sleep(time.Millisecond)
		for _, pair := range noreply_cases {
			conn.Write(pair.to_server)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
			n := bytes.Index(buffer, []byte{0})
			//fmt.Println(buffer)
			if !bytes.Equal(buffer[:n], pair.from_server) {
				t.Error(
					"For", pair.to_server, string(pair.to_server),
					"expected", pair.from_server, string(pair.from_server),
					"got", buffer[:n], string(buffer[:n]),
				)
			}
		}
		conn.Close()
		//time.Sleep(time.Millisecond)
	}
}

//test that replication indeed occurred across all followers
//update the kvstore via leader
//but through special testing purpose functions the client should be able to
//check in the followers whether the update got replicated
//this is for testing purpose only and is not part of standard client - server interface
func testReplication(t *testing.T) {

	//set value in the leader
	leader_port := raft.CLIENT_PORT + 1
	conn, err := net.Dial("tcp", ":"+strconv.Itoa(leader_port))
	if err != nil {
		t.Error("Error in connecting the leader at port: " + strconv.Itoa(leader_port))
	} else {
		time.Sleep(time.Millisecond)
		sending := []byte("set replica 1000 3\r\nlul\r\n")
		expecting := []byte("OK 1\r\n")
		conn.Write(sending)
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		n := bytes.Index(buffer, []byte{0})
		if !bytes.Equal(buffer[:n], expecting) {
			t.Error(
				"For", sending, string(sending),
				"expected", expecting, string(expecting),
				"got", buffer[:n], string(buffer[:n]),
			)
		}
		conn.Close()
		time.Sleep(time.Millisecond)
	}

	//test if the value got updated in the followers

	presentChan := make(chan bool)
	go monitorPresentChannel(presentChan, t)

	for i := 2; i <= NUM_SERVERS; i++ {
		//for _, server := range raft.GetClusterConfig().Servers[1:] {
		go func(presentChan chan bool) {
			//client, err := rpc.Dial("tcp", server.Hostname+":"+strconv.Itoa(server.LogPort))
			client, err := rpc.Dial("tcp", "127.0.0.1:"+strconv.Itoa(raft.LOG_PORT+i))
			if err != nil {
				Info.Fatal("Dialing:", err)
			}
			reply := new(TestReply)
			reply.replica_updated = false
			args := new(TestArgs)
			args.key = "replica"
			args.value = []byte("lul")
			args.version = 1

			testerCall := client.Go("Tester.TesterRPC", args, reply, nil) //check if it is replicated
			testerCall = <-testerCall.Done
			presentChan <- reply.replica_updated
		}(presentChan)
	}
}

func monitorPresentChannel(presentChan chan bool, t *testing.T) {
	countPresent := 0
	var isPresent bool
	for i := 1; i <= 4; i++ {
		isPresent = <-presentChan
		if isPresent {
			countPresent++
		}
	}
	if countPresent != 4 {
		t.Error("The update didn't occur in all the followers")
	}
}

// Add some dummy entries in raft.ClusterConfig such that majority is not achieved
// Expected: Time out should occur after 5 sec and log entry table should not be updated
func testCommandNotCommittedWithoutMajority() {

}

// Expected: Log entry table updated with the new entry
func testCommandCommittedWithMajority() {

}

// Multiple clients sending different requests
// Expected: Log entry table updated
func testConcurrentManyClientsToLeader() {

}

// Single client sending 100 requests one after another
// Expected: Log entry table updated
func testConcurrentClientManyRequestsToLeader() {

}

// Tests similar to the previous assignment (Copy/Paste)
