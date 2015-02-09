//testing

package main

import (
	"bytes"
	"net"
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
	for i := 0; i < NUM_SERVERS; i++ {
		go testServersCommunic(i, t)
	}
	//wait for some time so that servers are ready
	time.Sleep(4 * time.Second)

	//run client that tries connecting to the followers
	testConnectFollower(t)
}

//run servers
func testServersCommunic(i int, t *testing.T) {
	cmd := exec.Command("go", "run", "server.go", strconv.Itoa(i+1), strconv.Itoa(NUM_SERVERS))
	f, err := os.OpenFile(strconv.Itoa(i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		t.Errorf("error opening file: %v", err)
	}

	defer f.Close()
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Run()
}

//on trying to connect with the followers the client should get redirect err from the followers
func testConnectFollower(t *testing.T) {
	for i := 2; i < NUM_SERVERS; i++ { //the followers start at second position
		server_port := raft.LOG_PORT + i
		conn, err := net.Dial("tcp", ":"+strconv.Itoa(server_port))
		if err != nil {
			t.Error("Error in connecting the server at port: " + strconv.Itoa(server_port))
		} else {
			time.Sleep(time.Millisecond)
			sending := []byte("set mykey1 100 3\r\nlul\r\n")
			expecting := []byte("ERR_REDIRECT 127.0.0.1 " + strconv.Itoa(raft.LOG_PORT+1) + "\r\n" + "ERR_REDIRECT 127.0.0.1 " + strconv.Itoa(raft.CLIENT_PORT+1) + "\r\n")
			conn.Write(sending)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
			n := bytes.Index(buffer, []byte{0})
			if !bytes.Equal(buffer[:n], expecting) {
				t.Error(
					"For", sending, string(sending),
					"expected", expecting, string(expecting),
					"got", buffer[:n], string(buffer[:n]),
				)
			}
			conn.Close()
			time.Sleep(time.Millisecond)
		}
	}
}

//noreply option is not more valid with set and cas
//client should get command error from the server if it sends 'no reply' option
func testNoReply(t *testing.T) {
	var noreply_cases = []Testpair{
		{[]byte("set mykey1 100 3 noreply\r\n"), []byte("ERRCMDERR\r\n")},
		{[]byte("cas mykey1 0 1 6 noreply\r\n"), []byte("ERRCMDERR\r\n")},
	}

	server_port := raft.LOG_PORT + 1

	conn, err := net.Dial("tcp", ":"+strconv.Itoa(server_port))
	if err != nil {
		t.Error("Error in connecting the server at port: " + strconv.Itoa(server_port))
	} else {
		time.Sleep(time.Millisecond)
		for _, pair := range noreply_cases {
			conn.Write(pair.to_server)
			buffer := make([]byte, 1024)
			conn.Read(buffer)
			n := bytes.Index(buffer, []byte{0})
			if !bytes.Equal(buffer[:n], pair.from_server) {
				t.Error(
					"For", pair.to_server, string(pair.to_server),
					"expected", pair.from_server, string(pair.from_server),
					"got", buffer[:n], string(buffer[:n]),
				)
			}
		}
		conn.Close()
		time.Sleep(time.Millisecond)
	}
}
