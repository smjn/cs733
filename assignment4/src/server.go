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
	"time"
)

// Logger
var Info *log.Logger

//global raft object for each server instance
var rft *raft.Raft

//Receiver for all raft related RPCs
type RaftRPCService struct{}

//RPC argument for testing the replication of keys value version across key value stores
type TestArgs struct {
	key     string
	value   []byte
	version uint64
}

// RPC argument with boolean value in the reply to confirm that indeed the replication went through across servers
type TestReply struct {
	isUpdated bool
}

//only for testing purpose
//this function checks for the key value in its kvstore and sets reply.isUpdated true if present and false if absent
//arguments: args contains the key, value, version to be matched
//reply is the reply to be sent
func (t *RaftRPCService) testerRPC(args *TestArgs, reply *TestReply) error {
	table := raft.GetKeyValStr()
	table.RLock()
	defer table.RUnlock()
	dic := table.GetDictionary()
	if v, ok := dic[args.key]; ok {
		val := v.GetVal()
		ver := v.GetVer()
		if bytes.Equal(val, args.value) && ver == args.version {
			reply.isUpdated = true
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

//RPC called by leader server. To let followers know that they can append their logs
//arguments: pointer to argument struct (has AppendRPC), pointer to reply struct AppendReply
//returns: error
//receiver: pointer to RaftRPCService
func (t *RaftRPCService) AppendRPC(args *raft.AppendRPC, reply *raft.AppendReply) error {
	Info.Println("append RPC invoked")
	rft.AddToEventChannel(args)
	temp := rft.FetchAppendReply().(*raft.AppendReply)
	reply.CurrentTerm = temp.CurrentTerm
	reply.Reply = temp.Reply
	reply.Fid = temp.Fid
	reply.LogLength = temp.LogLength
	return nil
}

//RPC for follower server. To let followers know that and entry can be committed.
//arguments: pointer to argument struct (has LogEntry), pointer to reply struct
//returns: error
//receiver: pointer to RaftRPCService
func (t *RaftRPCService) CommitRPC(args *raft.CommitData, reply *Reply) error {
	Info.Println("Commit RPC invoked")
	rft.LogArray[(*args).Id].SetCommitted(true)
	rft.AddToChannel(rft.LogArray[(*args).Id])
	reply.X = 1
	return nil
}

//RPC called by candidate server. To ask the follower for votes.
//arguments: pointer to argument struct (has VoteRequest), pointer to reply struct VoteRequestReply
//returns: error
//receiver: pointer to RaftRPCService
func (t *RaftRPCService) VoteRequestRPC(args *raft.VoteRequest, reply *raft.VoteRequestReply) error {
	Info.Println("Request Vote RPC received")
	rft.AddToEventChannel(args)
	temp := rft.FetchVoteReply().(*raft.VoteRequestReply)
	reply.CurrentTerm = temp.CurrentTerm
	reply.Reply = temp.Reply
	return nil
}

//Initialize all the things necessary for start the server for inter raft communication.
//The servers are running on ports 20000+serverId. {0..4}
//arguments: pointer to current server config, pointer to raft object, a bool channel to set to true to let
//the invoker know that the proc ended.
//returns: none
//receiver: none
func initInterServerCommunication(server *raft.ServerConfig, rft *raft.Raft, ch chan bool) {
	raftRPC := new(RaftRPCService)
	rpc.Register(raftRPC)

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
		Info = log.New(ioutil.Discard, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	} else {
		Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
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
	sid, _ := strconv.Atoi(os.Args[1])
	ch1 := make(chan bool)
	ch2 := make(chan bool)

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
	raft.InitKVStore(Info)

	go raft.MonitorCommitChannel(commitCh) //for kvstore
	go initClientCommunication(server, rft, ch1)
	go initInterServerCommunication(server, rft, ch2)

	time.Sleep(100 * time.Millisecond)
	go rft.Loop()

	for <-ch1 && <-ch2 {

	}
}
