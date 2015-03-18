// server.go
package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"raft"
	"strconv"
	"time"
)

// Logger
var Info *log.Logger

//global raft object for each server instance
var rft *raft.Raft

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

//Entry point for application. Starts all major server go routines and then waits for ever
func main() {
	rand.Seed(time.Now().UnixNano())
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
