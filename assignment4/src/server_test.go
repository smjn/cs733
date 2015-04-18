//testing

package main

import (
	//"fmt"
	"os"
	"os/exec"
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
	dummy := make(chan bool)
	//start the servers
	for i := 0; i < NUM_SERVERS; i++ {
		go startServers(i, t, dummy)
	}
	//wait for some time so that servers are ready

	time.Sleep(1 * time.Second)
	testClientAppend()
	if <-dummy {

	}
}

//run servers
func startServers(i int, t *testing.T, dummy chan bool) {
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

func testClientAppend() {

}
