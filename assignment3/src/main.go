package main

import (
	"fmt"
	"raft"
)

const (
	SERVERS = 5
)

func main() {
	dummyCh := make(chan bool)
	commitCh := make(chan raft.LogEntry)
	fmt.Println("Started")
	for i := 1; i <= 5; i++ {
		go raft.Start(i, commitCh, dummyCh, true)
	}
	if <-dummyCh {
		fmt.Println("khattam")
	}
}
