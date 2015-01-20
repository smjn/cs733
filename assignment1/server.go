package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"strconv"
	"time"
)

const (
	//request
	SET = "set"
//	GET = "get"
//	GETM = "getm"
//	CAS = "cas"
//	DELETE = "delete"
	NOREPLY = "[noreply]"
//
//	//response
//	OK = "OK"
//	VALUE = "VALUE"
//	DELETED = "DELETED"
)

type Data struct {
	numBytes uint64
	version uint64
	expTime uint64
	value []byte
}

type KeyValueStore struct {
	dictionary map[string]*Data
	sync.RWMutex
}

var ver uint64

func startServer() {
	fmt.Println("Server started")
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		fmt.Println("Could not start server!")
	}

	//initialize key value store
	table := &KeyValueStore{dictionary:make(map[string]*Data)}

	//infinite loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleClient(&conn, table)
	}
}

func read(conn *net.Conn) (msg string, success bool){
	buf := make([]byte, 1024)
	_, err := (*conn).Read(buf)

	if err != nil {
		if err == io.EOF {
			fmt.Println("Client disconnected!")
			return "", false
		}
	}

	n := bytes.Index(buf, []byte{0})
	if n != 0 {
		msg := string(buf[:n-1])
		fmt.Println("Received: ", msg)
		return msg, true
	}

	return "", false
}

func handleClient(conn *net.Conn, table *KeyValueStore) {
	defer (*conn).Close()
	for {
		if msg, ok := read(conn); ok{
			parseInput(&msg, table)
		}
	}
}

func parseInput(msg *string, table *KeyValueStore) {
	tokens := strings.Fields(*msg)
	fmt.Println(tokens)
	switch tokens[0] {
		case SET:
			if v, ok := read(conn); ok{
				performSet(tokens[1:len(tokens)], v, table)
			}
		//case GET: performGet(tokens[1:len(tokens)])
		//case GETM: performGetm(tokens[1:len(tokens)])
		//case CAS: performCas(tokens[1:len(tokens)])
		//case DELETE: performDelete(tokens[1:len(tokens)])
		default: fmt.Println("Command not found")
	}
}

func performSet(tokens []string, val []byte, table *KeyValueStore){
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	n, _ := strconv.ParseUint(tokens[2], 10, 64)
	r := true
	
	if len(tokens) == 4 && tokens[3] == NOREPLY {
		r = false
	}

	fmt.Println(r)

	//read value 
	if v, ok := read(conn)
	(*table).Lock()
	//critical section start
	if ele, ok := (*table).dictionary[k]; ok {
		fmt.Println(ele)
	} else{
		ver++
		val := new(Data)
		(*val).numBytes = n
		(*val).version = ver
		(*val).expTime = e + uint64(time.Now().Unix())
		(*val).value = []byte{0}
		(*table).dictionary[k] = val
	}
	(*table).Unlock()
	debug(table)
}

func debug(table *KeyValueStore){
	fmt.Println("----start debug----")
	for key,val := range (*table).dictionary {
		fmt.Println(key, val)
	}
	fmt.Println("----end debug----")
}

func main() {
	ver = 1
	go startServer()
	var input string
	fmt.Scanln(&input)
}
