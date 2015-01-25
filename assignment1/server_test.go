package main

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

type TestCasePair struct {
	command  []byte
	expected []byte
}

const MAX_CLIENTS = 150
const MAX_CMD_NUM = 250

//this test function will start tests for various commands, one client at a time
func TestCommands(t *testing.T) {
	go main()
	//give some time for server to initialize
	time.Sleep(time.Second)
	testIndividualCommands(t)
	testRapidCommands(t)
	ReInitServer()
	testConcurrentCommands(t)
}

func testIndividualCommands(t *testing.T) {
	conn, err := net.Dial("tcp", ":5000")
	defer conn.Close()
	time.Sleep(time.Millisecond)
	if err != nil {
		t.Errorf("Connection Error")
	}

	testSetCommand(t, conn)
	testGetCommand(t, conn)
	testDeleteCommand(t, conn)
	testCasCommand(t, conn)
	testGetmCommand(t, conn)
}

func doTest(t *testing.T, e TestCasePair, conn net.Conn) {
	buf := make([]byte, 2048)
	conn.Write(e.command)
	n, _ := conn.Read(buf)
	if !bytes.Equal(buf[:n], e.expected) {
		fmt.Println(buf[:n], e.expected, "S:"+string(buf[:n]), "E:"+string(e.expected), "C:"+string(e.command))
		t.Errorf("Error occured for case")
	}
}

func testSetCommand(t *testing.T, conn net.Conn) {
	cases := []TestCasePair{
		/*test set command*/
		{[]byte("set k 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},                                  //fast expiring
		{[]byte("set l 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},                                  //fast expiring
		{[]byte("set m 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},                                  //fast expiring
		{[]byte("set n 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},                                  //fast expiring
		{[]byte("set a 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},                                //single length
		{[]byte("set b 200 10" + CRLF + "12345" + CRLF + "890" + CRLF), []byte("OK 1" + CRLF)},                      //"+CRLF+" in middle
		{[]byte("set c 0 10" + CRLF + "12345" + CRLF + "890" + CRLF), []byte("OK 1" + CRLF)},                        //perpetual key
		{[]byte("set \n 200 10" + CRLF + "1234567890" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)}, //newline key (error)
		{[]byte("set d 200 10" + CRLF + "12345678901" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                        //value length greater (error)
		{[]byte("set e 200 10" + CRLF + "1234\r6789" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                         //value length less (error)
		//key length high (250 bytes)
		{[]byte("set 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 200 10" + CRLF + "1234\r67890" + CRLF), []byte("OK 1" + CRLF)},
		{[]byte("set cn 100 10" + CRLF + "1234\n\n7890" + CRLF), []byte("OK 1" + CRLF)},   //contiguous newlines in value
		{[]byte("set cn 100 10" + CRLF + "12\n4\n\n78\r0" + CRLF), []byte("OK 2" + CRLF)}, //contiguous newlines in value

		//version update
		{[]byte("set changing 200 2" + CRLF + "12" + CRLF + "set changing 200 2" + CRLF + "12" + CRLF + "set changing 200 2" + CRLF + "12" + CRLF), []byte("OK 1" + CRLF + "OK 2" + CRLF + "OK 3" + CRLF)},

		////key length high (251 bytes), error
		{[]byte("set 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901 200 10" + CRLF + "1234\r67890" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},

		{[]byte("set f -1 10" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},     //invalid expiry (number)
		{[]byte("set g xyz 10" + CRLF + "1234\r6\r890" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},   //invalid expiry (string)
		{[]byte("set h 200 -1" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},    //invalid value size (number)
		{[]byte("set i 200 xyz" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},   //invalid value size (string)
		{[]byte("set j 200 10 20" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)}, //invalid number of args >
		{[]byte("set j 200" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},       //invalid number of args <

		/*set some keys to be used later*/
		{[]byte("set fe1 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //fast expiring
		{[]byte("set fe2 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //fast expiring
		{[]byte("set fe3 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //fast expiring
		{[]byte("set fe4 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //fast expiring
		{[]byte("set nk1 200 10" + CRLF + "12\r45\n7890" + CRLF), []byte("OK 1" + CRLF)}, //normal key
		{[]byte("set nk2 200 10" + CRLF + "1234\n\n7890" + CRLF), []byte("OK 1" + CRLF)}, //normal key
		{[]byte("set nk3 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},   //normal key
		{[]byte("set nk4 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},   //normal key
		{[]byte("set pk1 0 10" + CRLF + "12\r45\n7890" + CRLF), []byte("OK 1" + CRLF)},   //perpetual key
		{[]byte("set pk2 0 10" + CRLF + "1234\n\n7890" + CRLF), []byte("OK 1" + CRLF)},   //perpetual key
		{[]byte("set pk3 0 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //perpetual key
		{[]byte("set pk4 0 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)},     //perpetual key
	}

	for _, e := range cases {
		doTest(t, e, conn)
	}
}

func testGetCommand(t *testing.T, conn net.Conn) {
	cases := []TestCasePair{
		/*test get command*/
		{[]byte("get a" + CRLF), []byte("VALUE 10" + CRLF + "1234567890" + CRLF)},           //fetch key set earlier
		{[]byte("get b" + CRLF), []byte("VALUE 10" + CRLF + "12345" + CRLF + "890" + CRLF)}, //fetch key with value having "+CRLF+" set earlier
		{[]byte("get non_existant" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},                   //fetch non existant key
		{[]byte("get k" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},                              //fetch expired key (k)
		{[]byte("get a 2" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                              //invalid number of args >
		{[]byte("get" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                                  //invalid number of args <

		//invalid key size
		{[]byte("get 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901" + CRLF), []byte(ERR_CMD_ERR + CRLF)},
	}

	for _, e := range cases {
		doTest(t, e, conn)
	}
}

func testDeleteCommand(t *testing.T, conn net.Conn) {
	cases := []TestCasePair{
		/*test delete command*/
		//set some keys first
		{[]byte("set d1 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)}, //some key will be deleted
		{[]byte("set d2 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)}, //some key will be deleted
		//deletion begins
		{[]byte("delete a" + CRLF), []byte(DELETED + CRLF)},                  //delete normal key
		{[]byte("delete l" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},            //delete expired key
		{[]byte("delete non_existant" + CRLF), []byte(ERR_NOT_FOUND + CRLF)}, //delete non existant key
		{[]byte("delete d2 12" + CRLF), []byte(ERR_CMD_ERR + CRLF)},          //invalid arguments >
		{[]byte("delete" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                //invalid arguments <
	}

	for _, e := range cases {
		doTest(t, e, conn)
	}
}

func testCasCommand(t *testing.T, conn net.Conn) {
	cases := []TestCasePair{
		/*test cas command*/
		//set some keys first
		{[]byte("set c1 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)}, //some key will be deleted
		{[]byte("set c2 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 1" + CRLF)}, //some key will be deleted
		//set new value of version 15 key c1 and retrieve the result
		{[]byte("cas c1 300 1 9" + CRLF + "123456789" + CRLF), []byte("OK 2" + CRLF)},                                //cas key set earlier
		{[]byte("get c1" + CRLF), []byte("VALUE 9" + CRLF + "123456789" + CRLF)},                                     //verify cas'ed key
		{[]byte("cas m 2 1 5" + CRLF + "12345" + CRLF), []byte("ERR_NOT_FOUND" + CRLF)},                              //cas expired key
		{[]byte("cas non_existant 2 4 5" + CRLF + "12345" + CRLF), []byte("ERR_NOT_FOUND" + CRLF)},                   //cas non-existant key
		{[]byte("cas c2 100 5 5" + CRLF + "12345" + CRLF), []byte("ERR_VERSION" + CRLF)},                             //cas incorrect version (16)
		{[]byte("cas \n 300 15 9" + CRLF + "123456789" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)}, //invalid key
		{[]byte("cas c2 -1 5 5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},       //invalid expiry (int)
		{[]byte("cas c2 xyz 5 5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},      //invalid expiry (string)
		{[]byte("cas c2 100 -5 5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},     //invalid version (int)
		{[]byte("cas c2 100 xyz 5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},    //invalid version (string)
		{[]byte("cas c2 100 5 -5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},     //invalid value (int)
		{[]byte("cas c2 100 5 xyz" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},    //invalid value (string)
		{[]byte("cas c2 100 5 5 6" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},    //invalid arg num >
		{[]byte("cas c2 100 5" + CRLF + "12345" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},        //invalid arg num <
	}

	for _, e := range cases {
		doTest(t, e, conn)
	}
}

func testGetmCommand(t *testing.T, conn net.Conn) {
	cases := []TestCasePair{
		/*test getm commands*/
		{[]byte("getm c" + CRLF), []byte("VALUE 1 0 10" + CRLF + "12345" + CRLF + "890" + CRLF)}, //perpetual key
		{[]byte("getm non_existant" + CRLF), []byte("ERR_NOT_FOUND" + CRLF)},                     //non-existant key
		{[]byte("getm a" + CRLF), []byte("ERR_NOT_FOUND" + CRLF)},                                //deleted key
		{[]byte("getm c x" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                                //invalid args >
		{[]byte("getm" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                                    //invalid args <

		////invalid key size
		{[]byte("getm 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901" + CRLF), []byte(ERR_CMD_ERR + CRLF)},
	}

	for _, e := range cases {
		doTest(t, e, conn)
	}
}

func testRapidCommands(t *testing.T) {
	conn, err := net.Dial("tcp", ":5000")
	version := 0
	defer conn.Close()
	time.Sleep(time.Millisecond)
	if err != nil {
		t.Errorf("Connection Error")
	}

	for i := 0; i < 10000; i++ {
		version++
		cases := TestCasePair{[]byte("set somekey 200 10" + CRLF + "1234567890" + CRLF), []byte("OK " + strconv.Itoa(version) + CRLF)}
		doTest(t, cases, conn)
	}
	for i := 0; i < 10000; i++ {
		cases := TestCasePair{[]byte("get somekey" + CRLF), []byte("VALUE 10" + CRLF + "1234567890" + CRLF)}
		doTest(t, cases, conn)
	}
	for i := 0; i < 10000; i++ {
		cases := TestCasePair{[]byte("getm pk1" + CRLF), []byte("VALUE 1 0 10" + CRLF + "12\r45\n7890" + CRLF)}
		doTest(t, cases, conn)
	}
	version = 1
	sample := TestCasePair{[]byte("set pk5 0 10" + CRLF + "1234567890" + CRLF), []byte("OK " + strconv.Itoa(version) + CRLF)} //perpetual key
	doTest(t, sample, conn)
	for i := 0; i < 10000; i++ {
		cases := TestCasePair{[]byte("cas pk5 200 " + strconv.Itoa(version) + " 10" + CRLF + "1234567890" + CRLF), []byte("OK " + strconv.Itoa(version+1) + CRLF)}
		doTest(t, cases, conn)
		version++
	}
}

func testConcurrentCommands(t *testing.T) {
	testConcurrentSet(t)
	testConcurrentGet(t)
	testConcurrentGetm(t)
	testConcurrentCas(t)
	testConcurrentDelete(t)
}

func testConcurrentSet(t *testing.T) {
	ch := make(chan int)
	for i := 0; i < MAX_CLIENTS; i++ {
		conn, err := net.Dial("tcp", ":5000")
		defer conn.Close()
		if err != nil {
			t.Errorf("too many clients")
		}
		go runSet(t, conn, i, ch)
	}
	num := 0
	for num < MAX_CLIENTS {
		num += <-ch
		//fmt.Println(num)
	}
}

func testConcurrentGet(t *testing.T) {
	ch := make(chan int)
	for i := 0; i < MAX_CLIENTS; i++ {
		conn, err := net.Dial("tcp", ":5000")
		defer conn.Close()
		if err != nil {
			t.Errorf("too many clients")
		}
		go runGet(t, conn, i, ch)
	}
	num := 0
	for num < MAX_CLIENTS {
		num += <-ch
		//fmt.Println(num)
	}
}

func testConcurrentCas(t *testing.T) {
	ch := make(chan int)
	for i := 0; i < MAX_CLIENTS; i++ {
		conn, err := net.Dial("tcp", ":5000")
		defer conn.Close()
		if err != nil {
			t.Errorf("too many clients")
		}
		go runCas(t, conn, i, ch)
	}
	num := 0
	for num < MAX_CLIENTS {
		num += <-ch
		//fmt.Println(num)
	}
}

func testConcurrentGetm(t *testing.T) {
	ch := make(chan int)
	for i := 0; i < MAX_CLIENTS; i++ {
		conn, err := net.Dial("tcp", ":5000")
		defer conn.Close()
		if err != nil {
			t.Errorf("too many clients")
		}
		go runGetm(t, conn, i, ch)
	}
	num := 0
	for num < MAX_CLIENTS {
		num += <-ch
		//fmt.Println(num)
	}
}

func testConcurrentDelete(t *testing.T) {
	ch := make(chan int)
	for i := 0; i < MAX_CLIENTS; i++ {
		conn, err := net.Dial("tcp", ":5000")
		defer conn.Close()
		if err != nil {
			t.Errorf("too many clients")
		}
		go runDelete(t, conn, i, ch)
	}
	num := 0
	for num < MAX_CLIENTS {
		num += <-ch
		//fmt.Println(num)
	}
}

func runSet(t *testing.T, conn net.Conn, i int, ch chan int) {
	version := 1
	for j := 0; j < MAX_CMD_NUM; j++ {
		cases := TestCasePair{[]byte("set somekey" + strconv.Itoa(i) + " 0 5" + CRLF + "12345" + CRLF), []byte("OK " + strconv.Itoa(version) + CRLF)}
		doTest(t, cases, conn)
		//fmt.Println(i)
		version++
	}
	//fmt.Println(i)
	ch <- 1
}

func runGet(t *testing.T, conn net.Conn, i int, ch chan int) {
	for j := 0; j < MAX_CMD_NUM; j++ {
		cases := TestCasePair{[]byte("get somekey" + strconv.Itoa(i) + CRLF), []byte("VALUE 5" + CRLF + "12345" + CRLF)}
		doTest(t, cases, conn)
		//fmt.Println(i)
	}
	//fmt.Println(i)
	ch <- 1
}

func runCas(t *testing.T, conn net.Conn, i int, ch chan int) {
	version := MAX_CMD_NUM
	for j := 0; j < MAX_CMD_NUM; j++ {
		cases := TestCasePair{[]byte("cas somekey" + strconv.Itoa(i) + " 0 " + strconv.Itoa(version) + " 5" + CRLF + "12345" + CRLF), []byte("OK " + strconv.Itoa(version+1) + CRLF)}
		doTest(t, cases, conn)
		//fmt.Println(i)
		version++
	}
	//fmt.Println(i)
	ch <- 1
}

func runGetm(t *testing.T, conn net.Conn, i int, ch chan int) {
	version := MAX_CMD_NUM
	for j := 0; j < MAX_CMD_NUM; j++ {
		cases := TestCasePair{[]byte("getm somekey" + strconv.Itoa(i) + CRLF), []byte("VALUE " + strconv.Itoa(version) + " 0 5" + CRLF + "12345" + CRLF)}
		doTest(t, cases, conn)
		//fmt.Println(i)
	}
	//fmt.Println(i)
	ch <- 1
}

func runDelete(t *testing.T, conn net.Conn, i int, ch chan int) {
	cases := TestCasePair{[]byte("delete somekey" + strconv.Itoa(i) + CRLF), []byte("DELETED" + CRLF)}
	doTest(t, cases, conn)
	ch <- 1
}
