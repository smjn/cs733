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
	testIndividualCommands(t)
}
func testIndividualCommands(t *testing.T) {
	conn, err := net.Dial("tcp", ":5000")
	defer conn.Close()
	time.Sleep(time.Millisecond)
	if err != nil {
		t.Errorf("Connection Error")
	}

	cases := []TestCasePair{
		/*test set command*/
		{[]byte("set k 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 2" + CRLF)},                                  //fast expiring
		{[]byte("set l 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 3" + CRLF)},                                  //fast expiring
		{[]byte("set m 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 4" + CRLF)},                                  //fast expiring
		{[]byte("set n 1 10" + CRLF + "1234567890" + CRLF), []byte("OK 5" + CRLF)},                                  //fast expiring
		{[]byte("set a 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 6" + CRLF)},                                //single length
		{[]byte("set b 200 10" + CRLF + "12345" + CRLF + "890" + CRLF), []byte("OK 7" + CRLF)},                      //"+CRLF+" in middle
		{[]byte("set c 0 10" + CRLF + "12345" + CRLF + "890" + CRLF), []byte("OK 8" + CRLF)},                        //perpetual key
		{[]byte("set \n 200 10" + CRLF + "1234567890" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)}, //newline key (error)
		{[]byte("set d 200 10" + CRLF + "12345678901" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                        //value length greater (error)
		{[]byte("set e 200 10" + CRLF + "1234\r6789" + CRLF), []byte("ERR_CMD_ERR" + CRLF)},                         //value length less (error)
		//key length high (250 bytes)
		{[]byte("set 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 200 10" + CRLF + "1234\r67890" + CRLF), []byte("OK 9" + CRLF)},

		//version update
		{[]byte("set changing 200 2" + CRLF + "12" + CRLF + "set changing 200 2" + CRLF + "12" + CRLF + "set changing 200 2" + CRLF + "12" + CRLF), []byte("OK 10" + CRLF + "OK 11" + CRLF + "OK 12" + CRLF)},

		//key length high (251 bytes), error
		{[]byte("set 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901 200 10" + CRLF + "1234\r67890" + CRLF), []byte("ERR_CMD_ERR" + CRLF + "ERR_CMD_ERR" + CRLF)},

		{[]byte("set f -1 10" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},     //invalid expiry (number)
		{[]byte("set g xyz 10" + CRLF + "1234\r6\r890" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},   //invalid expiry (string)
		{[]byte("set h 200 -1" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},    //invalid value size (number)
		{[]byte("set i 200 xyz" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},   //invalid value size (string)
		{[]byte("set j 200 10 20" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)}, //invalid number of args >
		{[]byte("set j 200" + CRLF + "1234\r6\r89" + CRLF), []byte(ERR_CMD_ERR + CRLF + ERR_CMD_ERR + CRLF)},       //invalid number of args <

		/*test get command*/
		{[]byte("get a" + CRLF), []byte("VALUE 10" + CRLF + "1234567890" + CRLF)},           //fetch key set earlier
		{[]byte("get b" + CRLF), []byte("VALUE 10" + CRLF + "12345" + CRLF + "890" + CRLF)}, //fetch key with value having "+CRLF+" set earlier
		{[]byte("get non_existant" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},                   //fetch non existant key
		{[]byte("get k" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},                              //fetch expired key (k)
		{[]byte("get a 2" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                              //invalid number of args >
		{[]byte("get" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                                  //invalid number of args <

		//invalid key size
		{[]byte("get 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901" + CRLF), []byte(ERR_CMD_ERR + CRLF)},

		/*test delete command*/
		//set some keys first
		{[]byte("set d1 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 13" + CRLF)}, //some key will be deleted
		{[]byte("set d2 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 14" + CRLF)}, //some key will be deleted
		//deletion begins
		{[]byte("delete a" + CRLF), []byte(DELETED + CRLF)},                  //delete normal key
		{[]byte("delete l" + CRLF), []byte(ERR_NOT_FOUND + CRLF)},            //delete expired key
		{[]byte("delete non_existant" + CRLF), []byte(ERR_NOT_FOUND + CRLF)}, //delete non existant key
		{[]byte("delete d2 12" + CRLF), []byte(ERR_CMD_ERR + CRLF)},          //invalid arguments >
		{[]byte("delete" + CRLF), []byte(ERR_CMD_ERR + CRLF)},                //invalid arguments <

		/*test cas command*/
		//set some keys first
		{[]byte("set c1 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 15" + CRLF)}, //some key will be deleted
		{[]byte("set c2 200 10" + CRLF + "1234567890" + CRLF), []byte("OK 16" + CRLF)}, //some key will be deleted
		//set new value of version 15 key c1 and retrieve the result
		{[]byte("cas c1 300 15 9" + CRLF + "123456789" + CRLF), []byte("OK 17" + CRLF)},                              //cas key set earlier
		{[]byte("get c1" + CRLF), []byte("VALUE 9" + CRLF + "123456789" + CRLF)},                                     //verify cas'ed key
		{[]byte("cas m 2 4 5" + CRLF + "12345" + CRLF), []byte("ERR_NOT_FOUND" + CRLF)},                              //cas expired key
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

	for i, e := range cases {
		buf := make([]byte, 2048)
		conn.Write(e.command)
		n, _ := conn.Read(buf)
		if !bytes.Equal(buf[:n], e.expected) {
			fmt.Println(buf[:n], e.expected, "S:"+string(buf[:n]), "E:"+string(e.expected))
			t.Errorf("Error occured for case:" + strconv.Itoa(i))
		}
	}
}
