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

//this test function will start tests for various commands, one client at a time
func TestSerial(t *testing.T) {
	go main()
	//give some time for server to initialize
	time.Sleep(time.Second)
	testSetCommand(t)
}

func testSetCommand(t *testing.T) {
	testSetReplyExpected(t)
}

func testSetReplyExpected(t *testing.T) {
	conn, err := net.Dial("tcp", ":5000")
	defer conn.Close()
	time.Sleep(time.Millisecond)
	if err != nil {
		t.Errorf("Connection Error")
	}

	cases := []TestCasePair{
		{[]byte("set a 200 10\r\n1234567890\r\n"), []byte("OK 2\r\n")},                        //single length
		{[]byte("set b 200 10\r\n12345\r\n890\r\n"), []byte("OK 3\r\n")},                      //\r\n in middle
		{[]byte("set c 0 10\r\n12345\r\n890\r\n"), []byte("OK 4\r\n")},                        //perpetual key
		{[]byte("set \n 200 10\r\n1234567890\r\n"), []byte("ERR_CMD_ERR\r\nERR_CMD_ERR\r\n")}, //newline key (error)
		{[]byte("set d 200 10\r\n12345678901\r\n"), []byte("ERR_CMD_ERR\r\n")},                //value length greater (error)
		{[]byte("set e 200 10\r\n1234\r6789\r\n"), []byte("ERR_CMD_ERR\r\n")},                 //value length less (error)
		//key length high (250 bytes)
		{[]byte("set 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 200 10\r\n1234\r67890\r\n"), []byte("OK 5\r\n")},
		//key length high (251 bytes), error
		{[]byte("set 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901 200 10\r\n1234\r67890\r\n"), []byte("ERR_CMD_ERR\r\nERR_CMD_ERR\r\n")},
		{[]byte("set f -1 10\r\n1234\r6\r89\r\n"), []byte("ERR_CMD_ERR\r\nERR_CMD_ERR\r\n")}, //invalid expiry
		{[]byte("set f 200 0\r\n1234\r6\r89\r\n"), []byte("ERR_CMD_ERR\r\nERR_CMD_ERR\r\n")}, //invalid value size
	}

	for i, e := range cases {
		buf := make([]byte, 2048)
		conn.Write(e.command)
		n, _ := conn.Read(buf)
		if !bytes.Equal(buf[:n], e.expected) {
			fmt.Println(buf[:n], e.expected, string(buf[:n]), string(e.expected))
			t.Errorf("Error occured for case:" + strconv.Itoa(i))
		}
	}
}
