//testing

package main

import (
	"bytes"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

var LeaderId int
var Info *log.Logger

//
func TestAll(t *testing.T) {
	//start the servers
	initLogger()
	for i := 0; i < NUM_SERVERS; i++ {
		go startServers(i, t)
	}
	//wait for some time so that servers are ready

	time.Sleep(8 * time.Second)
	testPerformClientConnect(t)
	testPerformSet(t, 1)
	testPerformSet(t, 2)
	testPerformSet(t, 3)
	testPerformSet(t, 4)
	//testKillLeader(t)
	killServers()
}

func killServers() {
	Info.Println("killing servers")
	cmd := exec.Command("sh", "-c", "for i in `netstat -ntlp|grep server|awk '{print $7}'`; do kill -9 ${i%%/*}; done")
	cmd.Run()
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

func probeLeader(t *testing.T) (int, error) {
	if conn, err := net.Dial("tcp", ":"+strconv.Itoa(9000)); err != nil {
		t.Errorf("Could not connect")
		return -1, err
	} else {
		sending := []byte("set probe 100 3\r\nlul\r\n")
		conn.Write(sending)
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		n := bytes.Index(buffer, []byte{0})
		str := string(buffer[:n])
		if strings.Contains(str, "ERR_REDIRECT") {
			str = strings.TrimSpace(str)
			id, _ := strconv.Atoi(strings.Fields(str)[1])
			LeaderId = id
			return id, nil
		}
		return 0, nil
	}
}

func getLeaderConn(t *testing.T) net.Conn {
	if conn, err := net.Dial("tcp", ":"+strconv.Itoa(9000+LeaderId)); err != nil {
		t.Errorf("Could not connect")
		return nil
	} else {
		return conn
	}
}

func initLogger() {
	// Logger Initializaion
	f, _ := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	Info = log.New(f, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

func testPerformClientConnect(t *testing.T) {
	id, _ := probeLeader(t)
	if id == -1 {
		t.Errorf("Could not connect")
	} else if id < 0 || id > 4 {
		t.Errorf("Invalid leader id")
	}
	Info.Println("Leader Id:", id)
}

func testPerformSet(t *testing.T, i int) {
	if conn := getLeaderConn(t); conn != nil {
		sending := []byte("set mykey" + strconv.Itoa(i) + " 100 3\r\nlul\r\n")
		conn.Write(sending)
		buffer := make([]byte, 1024)
		time.Sleep(time.Millisecond * 50)
		conn.Read(buffer)
		n := bytes.Index(buffer, []byte{0})
		str := string(buffer[:n])
		if strings.TrimSpace(str) != "OK "+strconv.Itoa(1) {
			t.Errorf("invalid reply received", str)
		} else {
			Info.Println(str)
		}
		conn.Close()
	} else {
		t.Errorf("could not get leader connection")
	}
}
