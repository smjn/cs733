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
	NUM_SERVERS     int = 5
	NORM_DELAY          = 15
	CONC_DELAY          = 30
	POST_TEST_DELAY     = 5
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
	testConcurrent(t, 10, 10)
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
	logger.Println("Probing leader")
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
	logger.Println("Getting connection to leader")
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
	logger.Println("testPerformClientConnect")
	id, _ := probeLeader(t)
	if id == -1 {
		t.Errorf("Could not connect")
	} else if id < 0 || id > 4 {
		t.Errorf("Invalid leader id")
	}
	logger.Println("Leader Id:", id)
}

func testCommands(t *testing.T) {
	logger.Println("testCommands")
	testPerformMultipleSet(t, getPrefix(), 1) //check single set
	testPerformMultipleSet(t, getPrefix(), 200)
	testPerformCas(t)
	testPerformMultipleCas(t, 200)
	testPerformMultipleGet(t, 200)
	testPerformMultipleGetm(t, 200)
	testPerformMultipleDelete(t, 100)
}

func doTest(conn net.Conn, t *testing.T, test *Testpair, delay int) {
	conn.Write(test.test)
	buf := make([]byte, 256)
	time.Sleep(time.Millisecond * time.Duration(delay))
	n, _ := conn.Read(buf)
	//logger.Println("read", buffer.Bytes())

	if !bytes.Equal(test.expected, buf[:n]) {
		logger.Println("test:", string(test.test), "got:", string(buf[:n]), "expected:", string(test.expected))
		t.Errorf("invalid reply received", string(buf[:n]))
	}
}

func testPerformMultipleSet(t *testing.T, start int, times int) {
	logger.Println("testPerformMultipleSet")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := start; i < start+times; i++ {
			test := &Testpair{[]byte("set mykey" + strconv.Itoa(i) + " 0 3\r\nlul\r\n"), []byte("OK 1\r\n")}
			doTest(conn, t, test, NORM_DELAY)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformCas(t *testing.T) {
	logger.Println("testPerformCas")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		test := &Testpair{[]byte("cas mykey1 1000 1 3\r\nlul\r\n"), []byte("OK 2\r\n")}
		doTest(conn, t, test, NORM_DELAY)
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleCas(t *testing.T, end int) {
	logger.Println("testPerformMultipleCas")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("cas mykey2 1000 " + strconv.Itoa(i+1) + " 3\r\nlul\r\n"), []byte("OK " + strconv.Itoa(i+2) + "\r\n")}
			doTest(conn, t, test, NORM_DELAY)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleGet(t *testing.T, end int) {
	logger.Println("testPerformMultipleGet")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("get mykey3\r\n"), []byte("VALUE 3\r\nlul\r\n")}
			doTest(conn, t, test, NORM_DELAY)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleGetm(t *testing.T, end int) {
	logger.Println("testPerformMultipleGetm")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("getm mykey4\r\n"), []byte("VALUE 1 0 3\r\nlul\r\n")}
			doTest(conn, t, test, NORM_DELAY)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testPerformMultipleDelete(t *testing.T, end int) {
	logger.Println("testPerformMultipleDelete")
	if conn := getLeaderConn(t); conn != nil {
		defer conn.Close()
		for i := 0; i < end; i++ {
			test := &Testpair{[]byte("delete mykey" + strconv.Itoa(i+1) + "\r\n"), []byte("DELETED\r\n")}
			doTest(conn, t, test, NORM_DELAY)
		}
	} else {
		t.Errorf("could not get leader connection")
	}
}

func testConcurrent(t *testing.T, clients int, commands int) {
	logger.Println("testConcurrent")
	ch := make(chan int)
	for c := 0; c < clients; c++ {
		if conn := getLeaderConn(t); conn != nil {
			defer conn.Close()
			logger.Println("starting routine")
			go testCommandsRoutine(conn, t, commands, ch, c+1500)
		} else {
			t.Errorf("could not get leader connection")
		}
	}
	num := 0
	for num < clients {
		//logger.Println("got", num)
		num += <-ch
	}
}

func testCommandsRoutine(conn net.Conn, t *testing.T, commands int, ch chan int, off int) {
	logger.Println("testing", commands)

	for i := 0; i < commands; i++ {
		test := &Testpair{[]byte("set mykey" + strconv.Itoa(off) + " 0 9\r\nsome data\r\n"), []byte("OK " + strconv.Itoa(i+1) + "\r\n")}
		doTest(conn, t, test, CONC_DELAY)
		time.Sleep(time.Millisecond * POST_TEST_DELAY)
	}

	for i := 0; i < commands; i++ {
		test := &Testpair{[]byte("get mykey" + strconv.Itoa(off) + "\r\n"), []byte("VALUE 9\r\nsome data\r\n")}
		doTest(conn, t, test, CONC_DELAY)
		time.Sleep(time.Millisecond * POST_TEST_DELAY)
	}

	for i := 0; i < commands; i++ {
		test := &Testpair{[]byte("getm mykey" + strconv.Itoa(off) + "\r\n"), []byte("VALUE " + strconv.Itoa(commands) + " 0 9\r\nsome data\r\n")}
		doTest(conn, t, test, CONC_DELAY)
		time.Sleep(time.Millisecond * POST_TEST_DELAY)
	}

	for i := 0; i < commands; i++ {
		test := &Testpair{
			[]byte("cas mykey" + strconv.Itoa(off) + " 1000 " + strconv.Itoa(commands+i) + " 9\r\nsome data\r\n"),
			[]byte("OK " + strconv.Itoa(commands+i+1) + "\r\n")}
		doTest(conn, t, test, CONC_DELAY)
		time.Sleep(time.Millisecond * POST_TEST_DELAY)
	}

	test := &Testpair{[]byte("delete mykey" + strconv.Itoa(off) + "\r\n"), []byte("DELETED\r\n")}
	doTest(conn, t, test, CONC_DELAY)
	time.Sleep(time.Millisecond * POST_TEST_DELAY)
	ch <- 1
}
