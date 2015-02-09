// server.go
package main

import (
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

// Flag for enabling/disabling logging functionality
var DEBUG = true

type AppendEntries struct{}

type Reply struct {
	X int
}

func (t *AppendEntries) AppendEntriesRPC(args *raft.LogEntry, reply *Reply) error {
	Info.Println("RPC invoked")
	reply.X = 1
	return nil
}

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

// Initialize Logger
func initLogger(serverId int) {
	// Logger Initializaion
	if !DEBUG {
		Info = log.New(ioutil.Discard, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {

		Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	}

	Info.Println("Initialized server.")
}

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

func main() {
	sid, err := strconv.Atoi(os.Args[1])
	ch1 := make(chan bool)
	ch2 := make(chan bool)
	if err != nil {
		Info.Println("argument ", os.Args[1], "is not string")
	}

	initLogger(sid)
	Info.Println("Starting")

	serverCount, err2 := strconv.Atoi((os.Args[2]))
	if err2 != nil {
		Info.Println("argument ", os.Args[2], "is not string")
	}

	server, _ := raft.NewServerConfig(sid)
	clusterConfig, _ := raft.NewClusterConfig(serverCount)
	commitCh := make(chan raft.LogEntry)

	rft, _ := raft.NewRaft(clusterConfig, sid, commitCh)
	raft.InitKVStore(Info)

	go raft.MonitorCommitChannel(commitCh) //for kvstore
	go initClientCommunication(server, rft, ch1)
	go initInterServerCommunication(server, rft, ch2)

	for <-ch1 && <-ch2 {

	}
}