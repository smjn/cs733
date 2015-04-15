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
	for i := 1; i <= NUM_SERVERS; i++ {
		go startServers(i, t)
	}
	//wait for some time so that servers are ready
	time.Sleep(4 * time.Second)
}

//run servers
func startServers(i int, t *testing.T) {
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
