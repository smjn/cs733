package main

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestSet(t *testing.T) {
	go main()
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		t.Errorf("Error connecting to server")
	} else {
		time.Sleep(time.Second*2)
		conn.Write([]byte("set xyz 200 10\r\n"))
		time.Sleep(time.Millisecond)
		conn.Write([]byte("abcd\r\n"))
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		msg := string(buffer)
		if msg == ERR_CMD_ERR+"\r\n" {
			t.Errorf("Expected OK <version>")
		}
	}
}

func TestGet(t *testing.T) {
	//go main()
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		t.Errorf("Error connecting to server")
	} else {
		time.Sleep(time.Second)
		conn.Write([]byte("set xyz 200 10\r\n"))
		time.Sleep(time.Millisecond)
		conn.Write([]byte("abcdefg\r\n"))
		buffer := make([]byte, 1024)
		conn.Read(buffer)
		msg := string(buffer)
		if msg == ERR_CMD_ERR+"\r\n" {
			t.Errorf("Expected OK <version>")
		}

		conn.Write([]byte("get xyz\r\n"))
		time.Sleep(time.Millisecond)
		conn.Read(buffer)
		msg = string(buffer)
		if msg == ERR_CMD_ERR+"\r\n" {
			t.Errorf("Expected key value")
		}

		conn.Write([]byte("get tuv\r\n"))
		time.Sleep(time.Millisecond)
		buffer = make([]byte, 1024)
		conn.Read(buffer)
		n := bytes.Index(buffer, []byte{0})
		msg = string(buffer[:n])
		if msg != ERR_NOT_FOUND+"\r\n" {
			t.Errorf("Expected key value")
		}
	}
}
