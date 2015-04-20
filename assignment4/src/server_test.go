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
	test     []byte
	expected []byte
}

var LeaderId int
var logger *log.Logger
var keyPrefix int

func getPrefix() int {
	keyPrefix++
	return keyPrefix
}

//
func TestAll(t *testing.T) {
	//start the servers
	initTestLogger()
	for i := 0; i < NUM_SERVERS; i++ {
		go startServers(i, t)
	}
	//wait for some time so that servers are ready
	time.Sleep(5 * time.Second)

	testPerformClientConnect(t)
	testCommands(t)
	killServers()
}

//kill all servers, cleanup
func killServers() {
	logger.Println("killing servers")
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

//check which server is the leader
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

//returns a connection to the leader server
func getLeaderConn(t *testing.T) net.Conn {
	if conn, err := net.Dial("tcp", ":"+strconv.Itoa(9000+LeaderId)); err != nil {
		t.Errorf("Could not connect")
		return nil
	} else {
		return conn
	}
}

//initialize logger meant for test cases
func initTestLogger() {
	f, _ := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	logger = log.New(f, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

func testPerformClientConnect(t *testing.T) {
	id, _ := probeLeader(t)
	if id == -1 {
		t.Errorf("Could not connect")
	} else if id < 0 || id > 4 {
		t.Errorf("Invalid leader id")
	}
	logger.Println("Leader Id:", id)
}

func testCommands(t *testing.T) {
	testPerformMultipleSet(t, getPrefix(), 1) //check single set
	testPerformMultipleSet(t, getPrefix(), 200)
	testPerformCas(t)
	testPerformMultipleCas(t, 200)
	testPerformMultipleGet(t, 200)
	testPerformMultipleGetm(t, 200)
	testPerformMultipleDelete(t, 100)
}

func doTest(conn net.Conn, t *testing.T, test *Testpair) {
	conn.Write(test.test)
	buf := make([]byte, 1024)
	time.Sleep(time.Millisecond * 20)
	n, _ := conn.Read(buf)

	if !bytes.Equal(test.expected, buf[:n]) {
		logger.Println("test:", string(test.test), "got:", string(buf[:n]), "expected:", string(test.expected))
		t.Errorf("invalid reply received", string(buf[:n]))
	}
}

func testPerformMultipleSet(t *testing.T, start int, times int) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := start; i < start+times; i++ {
			test := &Testpair{[]byte("set mykey" + strconv.Itoa(i) + " 0 3\r\nlul\r\n"), []byte("OK 1\r\n")}
			doTest(conn, t, test)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformCas(t *testing.T) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		test := &Testpair{[]byte("cas mykey1 1000 1 3\r\nlul\r\n"), []byte("OK 2\r\n")}
		doTest(conn, t, test)
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleCas(t *testing.T, end int) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("cas mykey2 1000 " + strconv.Itoa(i+1) + " 3\r\nlul\r\n"), []byte("OK " + strconv.Itoa(i+2) + "\r\n")}
			doTest(conn, t, test)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleGet(t *testing.T, end int) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("get mykey3\r\n"), []byte("VALUE 3\r\nlul\r\n")}
			doTest(conn, t, test)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleGetm(t *testing.T, end int) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("getm mykey4\r\n"), []byte("VALUE 1 0 3\r\nlul\r\n")}
			doTest(conn, t, test)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleDelete(t *testing.T, end int) {
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("delete mykey" + strconv.Itoa(i+1) + "\r\n"), []byte("DELETED\r\n")}
			doTest(conn, t, test)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}
