package main

import (
	//"bufio"
	"fmt"
	"net"
	//"os"
	"strconv"
	//"time"
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
	//go doRead(conn)
	//time.Sleep(time.Second*10)

	if err != nil {
		fmt.Println("Err:", err)
	}

	for i := 0; i < 100; i++ {
		data := make([]byte, 1024)
		conn.Write([]byte("set abc" + strconv.Itoa(i) + " 200 10\r\nabcd\nkefgh\r\n"))
		//time.Sleep(time.Millisecond)
		n, err := conn.Read(data)
		//fmt.Println(data[:n], err)
		if err == nil {
			fmt.Println(string(data[:n]))
		}
	}

	//reader := bufio.NewReader(os.Stdin)

	//for {
	//	if die {
	//		break
	//	}
	//	msg, err := reader.ReadString('\n')
	//	if err != nil {
	//		fmt.Println("Err: ", err)
	//	}
	//	fmt.Println(msg)
	//	buf := []byte(msg)[0 : len(msg)-1]
	//	buf = append(buf, []byte("\r\n")...)
	//	fmt.Println(len(buf))
	//	conn.Write(buf)
	//}

	conn.Close()
}
