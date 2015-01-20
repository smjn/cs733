package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"encoding/gob"
)

func client() {
	conn, err := net.Dial("tcp", "localhost:5000")
	//defer conn.Close()

	if err != nil {
		fmt.Println("Err:", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		msg, _ := reader.ReadString('\n')
		fmt.Println(msg)
		err = gob.NewEncoder(conn).Encode(msg)
		if err != nil {
			fmt.Println("Err:", err)
		}
	}
}

func main() {
	go client()
	var input string
	fmt.Scanln(&input)
}
