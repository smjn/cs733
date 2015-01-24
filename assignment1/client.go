package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

var die bool

func doRead(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Server closed connection")
			die = true
			break
		}

		fmt.Println(string(buf))
	}
}

func tempmain() {
	conn, err := net.Dial("tcp", "localhost:5000")
	die = false
	go doRead(conn)

	if err != nil {
		fmt.Println("Err:", err)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		if die {
			break
		}
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Err: ", err)
		}
		fmt.Println(msg)
		buf := []byte(msg)[0 : len(msg)-1]
		buf = append(buf, []byte("\r\n")...)
		fmt.Println(len(buf))
		conn.Write(buf)
	}

	conn.Close()
}
