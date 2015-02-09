// server.go
package main

import (
	"bytes"
	"connhandler"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"raft"
	"strconv"
)

// Logger
var Info *log.Logger

//global raft object for each server instance
var rft *raft.Raft

//Receiver fot RPC
type AppendEntries struct{}

//encapsulate the return value of RPC
//only for testing purpose
type Tester struct{}

type TestArgs struct {
	key     string
	value   []byte
	version uint64
}

type TestReply struct {
	replica_updated bool
}

//only for testing purpose
//this function checks for the key value in its kvstore and sets replica_updated true if present and false if absent
func (t *Tester) testerRPC(args *TestArgs, reply *TestReply) error {
	table := raft.GetKeyValStr()
	table.RLock()
	defer table.RUnlock()
	dic := table.GetDicKVstr()
	if v, ok := dic[args.key]; ok {
		the_val := v.GetVal()
		the_vers := v.GetVers()
		if bytes.Equal(the_val, args.value) && the_vers == args.version {
			reply.replica_updated = true
			return nil
		} else {
			return nil
		}
	} else {
		return nil
	}
}

type Reply struct {
	X int
}

//RPC for follower server. To let followers know that they can append their logs
//arguments: pointer to argument struct (has LogEntryData), pointer to reply struct
//returns: error
//receiver: pointer to AppendEntries
func (t *AppendEntries) AppendEntriesRPC(args *raft.LogEntryData, reply *Reply) error {
	Info.Println("Append Entries RPC invoked", (*args).GetLsn(), (*args).GetData(), (*args).GetCommitted())
	rft.LogArray = append(rft.LogArray, raft.NewLogEntry((*args).GetData(), (*args).GetCommitted(), nil))
	reply.X = 1
	return nil
}

//RPC for follower server. To let followers know that and entry can be committed.
//arguments: pointer to argument struct (has LogEntry), pointer to reply struct
//returns: error
//receiver: pointer to AppendEntries
func (t *AppendEntries) CommitRPC(args *raft.CommitData, reply *Reply) error {
	Info.Println("Commit RPC invoked")
	rft.LogArray[(*args).Id].SetCommitted(true)
	rft.AddToChannel(rft.LogArray[(*args).Id])
	reply.X = 1
	return nil
}

//Initialize all the things necessary for start the server for inter raft communication.
//The servers are running on ports 20000+serverId. {1..5}
//arguments: pointer to current server config, pointer to raft object, a bool channel to set to true to let
//the invoker know that the proc ended.
//returns: none
//receiver: none
func initInterServerCommunication(server *raft.ServerConfig, rft *raft.Raft, ch chan bool) {
	appendRpc := new(AppendEntries)
	rpc.Register(appendRpc)
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(server.LogPort))
	if e != nil {
		Info.Fatal("listen error:", e)
	}
	for {
		if conn, err := listener.Accept(); err != nil {
			Info.Fatal("accept error: " + err.Error())
		} else {
			Info.Printf("new connection established\n")
			go rpc.ServeConn(conn)
		}
	}
	ch <- true
}

//Simple logger that is enabled or disabled according to the command line arguments. In test cases
//it is redirected to a file per server {1..5}.
//arguments: current server id, toggle enable/disable
//return: none
//receiver: none
func initLogger(serverId int, toDebug bool) {
	// Logger Initializaion
	if !toDebug {
		Info = log.New(ioutil.Discard, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	}

	Info.Println("Initialized server.")
}

//Initialize all the things necessary for start the server for communication with client.
//The servers are running on ports 9000+serverId {1..5}.
//arguments: pointer to current server config, pointer to raft object, a bool channel to set to true to let
//the invoker know that the proc ended.
//returns: none
//receiver: none
func initClientCommunication(server *raft.ServerConfig, rft *raft.Raft, ch chan bool) {
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(server.ClientPort))
	if e != nil {
		Info.Fatal("client listen error:", e)
	}
	for {
		if conn, err := listener.Accept(); err != nil {
			Info.Fatal("client accept error: " + err.Error())
		} else {
			Info.Printf("client new connection established\n")
			go connhandler.HandleClient(conn, rft, Info)
		}
	}
	ch <- true
}

//Entry point for application. Starts all major server go routines and then waits for ever
func main() {
	sid, err := strconv.Atoi(os.Args[1])
	ch1 := make(chan bool)
	ch2 := make(chan bool)
	if err != nil {
		Info.Println("argument ", os.Args[1], "is not string")
	}

	if len(os.Args) > 3 {
		initLogger(sid, true)
	} else {
		initLogger(sid, false)
	}
	Info.Println("Starting")

	serverCount, err2 := strconv.Atoi((os.Args[2]))
	if err2 != nil {
		Info.Println("argument ", os.Args[2], "is not string")
	}

	server, _ := raft.NewServerConfig(sid)
	clusterConfig, _ := raft.NewClusterConfig(serverCount)
	commitCh := make(chan raft.LogEntry)

	rft, _ = raft.NewRaft(clusterConfig, sid, commitCh, Info)
	raft.InitKVStore(Info, sid)

	go raft.MonitorCommitChannel(commitCh) //for kvstore
	go initClientCommunication(server, rft, ch1)
	go initInterServerCommunication(server, rft, ch2)

	for <-ch1 && <-ch2 {

	}
}
