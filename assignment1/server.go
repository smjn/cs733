package main

import (
	"fmt"
	"encoding/gob"
	"net"
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
	//defer conn.Close()
	var msg string
	for {
		err := gob.NewDecoder(conn).Decode(&msg)
		if err != nil {
			//fmt.Println("Err: ", err)
		} else {
			fmt.Println("Received: ", msg)
		}
	}
}

func main() {
	go startServer()
	var input string
	fmt.Scanln(&input)
}
