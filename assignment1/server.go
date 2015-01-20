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
	GET = "get"
	GETM = "getm"
	CAS = "cas"
	DELETE = "delete"
	NOREPLY = "[noreply]"
//
//	//response
//	OK = "OK"
//	VALUE = "VALUE"
//	DELETED = "DELETED"

	//errors
	ERR_CMD_ERR = "ERR_CMD_ERR"
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

func read(conn *net.Conn, toRead uint64) ([]byte, bool){
	buf := make([]byte, toRead)
	_, err := (*conn).Read(buf)

	if err != nil {
		if err == io.EOF {
			fmt.Println("Client disconnected!")
			return []byte{0}, false
		}
	}

	n := bytes.Index(buf, []byte{0})
	if n != 0 {
		fmt.Println("Received: ", string(buf[:n-1]))
		return buf[:n-1], true
	}

	return []byte{0}, false
}

func handleClient(conn *net.Conn, table *KeyValueStore) {
	defer (*conn).Close()
	for {
		if msg, ok := read(conn, 1024); ok{
			parseInput(conn, string(msg), table)
		}
	}
}

func isValid(cmd string, tokens []string, conn *net.Conn) int{
	var flag int
	switch cmd {
		case SET:
			if len(tokens) > 5 || len(tokens) < 4 {
				flag = 1
			}
			//other validations
		case GET:
			if len(tokens) != 2 {
				flag = 1
			}
			//other validations
		case GETM:
			if len(tokens) != 2 {
				flag = 1
			}
			//other validations
		case CAS:
			if len(tokens) > 6 || len(tokens) < 5 {
				flag = 1
			}
			//other validations
		case DELETE:
			if len(tokens) != 2 {
				flag = 1
			}
			//other validations
		default:
			return 0
	}
	
	switch flag {
		case 1: (*conn).Write([]byte(ERR_CMD_ERR))
	}

	return flag
}

func parseInput(conn *net.Conn, msg string, table *KeyValueStore) {
	tokens := strings.Fields(msg)
	//fmt.Println(tokens)
	switch tokens[0] {
		case SET:
			if isValid(SET, tokens, conn) != 0 {
				return
			}
			performSet(conn, tokens[1:len(tokens)], table)
		//case GET: performGet(tokens[1:len(tokens)])
		//case GETM: performGetm(tokens[1:len(tokens)])
		//case CAS: performCas(tokens[1:len(tokens)])
		//case DELETE: performDelete(tokens[1:len(tokens)])
		default: fmt.Println("Command not found")
	}
}

func performSet(conn *net.Conn, tokens []string, table *KeyValueStore){
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	n, _ := strconv.ParseUint(tokens[2], 10, 64)
	r := true
	
	if len(tokens) == 4 && tokens[3] == NOREPLY {
		r = false
	}

	fmt.Println(r)

	//read value
	v, ok := read(conn, n+2) 
	if !ok {
		//error here
		return
	}

	(*table).Lock()
	//critical section start
	var val *Data
	if _, ok := (*table).dictionary[k]; ok {
		val = (*table).dictionary[k]
	} else{
		val = new(Data)
		(*table).dictionary[k] = val
	}
	ver++
	(*val).numBytes = n
	(*val).version = ver
	(*val).expTime = e + uint64(time.Now().Unix())
	(*val).value = v

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
