// server.go
package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

var rafts map[int]*Raft

func getLogger(serverId int, toDebug bool) (l *log.Logger) {
	if !toDebug {
		l = log.New(ioutil.Discard, "INFO: ", log.Ltime|log.Lshortfile)
	} else {
		logf, _ := os.OpenFile(strconv.Itoa(serverId), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		l = log.New(logf, "INFO: ", log.Ltime|log.Lmicroseconds|log.Lshortfile)
	}

	l.Println("Initialized server.")
	return l
}

func Start(serverId int, toDebug bool) {
	eventCh := make(chan RaftEvent)
	commitCh := make(chan LogEntry)
	monitorVotesCh := make(chan bool)
	clusterConfig, _ := NewClusterConfig(5)
	rft, _ := NewRaft(clusterConfig, serverId, commitCh, eventCh, monitorVotesCh, true)
	if rafts == nil {
		rafts = make(map[int]*Raft)
	}
	rafts[serverId] = rft
	fmt.Println(len(rafts))
	rft.loop()
}
