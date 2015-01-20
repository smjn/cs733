package main

import (
	"fmt"
	"net"
	"bytes"
)

func startServer() {
	fmt.Println("Server started")
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		panic("Could not start server!")
	}

	//infinite loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		
		if err != nil {
			fmt.Println("Err: ", err)
		}

		n := bytes.Index(buf, []byte{0})
		if n != 0 {
			msg := string(buf[:n-1])
			fmt.Println("Received: ", msg)
		}
	}
}

func main() {
	go startServer()
	var input string
	fmt.Scanln(&input)
}
