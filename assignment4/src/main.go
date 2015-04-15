package main

import (
	"fmt"
	"raft"
)

const (
	SERVERS = 5
)

func main() {
	dummyCh := make(chan bool, 1)
	fmt.Println("Started")
	for i := 1; i <= 5; i++ {
		go raft.Start(i, true)
	}
	if <-dummyCh {
		fmt.Println("khattam")
	}
}
