package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func doRead(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		conn.Read(buf)
		fmt.Println(string(buf))
	}
}

func main() {
	conn, err := net.Dial("tcp", "localhost:5000")
	go doRead(conn)

	if err != nil {
		fmt.Println("Err:", err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Err: ", err)
		}
		fmt.Println(msg)
		conn.Write([]byte(msg))
	}

	conn.Close()
}
