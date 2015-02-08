// server.go
package main

import (
	"cs733/assignment2/handler"
	"cs733/assignment2/kvstore"
	"cs733/assignment2/raft"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"time"
)

// Logger
var Info *log.Logger

// Flag for enabling/disabling logging functionality
var DEBUG = true

type Args struct {
	X int
}

type Reply struct {
	X int
}

type AppendEntries struct{}

func (t *AppendEntries) AppendEntriesRPC(args *Args, reply *Reply) error {
	reply.X = args.X
	return nil
}

func initInterServerCommunication(server *raft.ServerConfig, rft *raft.SharedLog) {
	appendRpc := new(AppendEntries)
	rpc.Register(appendRpc)
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(this_server.LogPort))
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
}

// Initialize Logger
func initLogger(serverId int) {
	// Logger Initializaion
	if !DEBUG {
		Info = log.New(ioutil.Discard, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {

		Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	}

	Info.Println("Initialized server")
}

func initClientCommunication(server *raft.ServerConfig, rft *raft.SharedLog) {
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(server.ClientPort))
	if e != nil {
		Info.Fatal("client listen error:", e)
	}
	for {
		if conn, err := listener.Accept(); err != nil {
			Info.Fatal("client accept error: " + err.Error())
		} else {
			Info.Printf("client new connection established\n")
			go handler.HandleClient(conn, rft)
		}
	}
}

func main() {
	sid, err := strconv.Atoi(os.Args[1])
	if err != nil {
		Info.Println("argument ", os.Args[1], "is not string")
	}

	initLogger(sid)
	Info.Println("Start")

	serverCount, err2 := strconv.Atoi((os.Args[2]))
	if err2 != nil {
		Info.Println("argument ", os.Args[2], "is not string")
	}

	server, _ := raft.NewServerConfig(sid)
	clusterConfig, _ := raft.NewClusterConfig(serverCount)
	commitCh := make(chan raft.LogEntry)

	rft := raft.NewRaft(clusterConfig, sid, commitCh)

	initClientCommunication(server, rft)
	initInterServerCommunication(server, rft)

	var dummy string
	fmt.Scanln(&dummy)
}
