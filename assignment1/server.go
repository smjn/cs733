package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	//request
	SET     = "set"
	GET     = "get"
	GETM    = "getm"
	CAS     = "cas"
	DELETE  = "delete"
	NOREPLY = "noreply"
	//
	//	//response
	OK    = "OK"
	CRLF  = "\r\n"
	VALUE = "VALUE"
	//	DELETED = "DELETED"

	//errors
	ERR_CMD_ERR = "ERR_CMD_ERR"
	ERR_NOT_FOUND = "ERR_NOT_FOUND"

	//logging
	LOG = true
)

type Data struct {
	numBytes uint64
	version  uint64
	expTime  uint64
	value    []byte
}

type KeyValueStore struct {
	dictionary map[string]*Data
	sync.RWMutex
}

var ver uint64
var logger *log.Logger

func startServer() {
	logger.Println("Server started")
	listener, err := net.Listen("tcp", ":5000")
	if err != nil {
		logger.Println("Could not start server!")
	}

	//initialize key value store
	table := &KeyValueStore{dictionary: make(map[string]*Data)}

	//infinite loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Println(err)
			continue
		}

		go handleClient(conn, table)
	}
}

func read(conn net.Conn, toRead uint64) ([]byte, bool) {
	buf := make([]byte, toRead)
	_, err := conn.Read(buf)

	if err != nil {
		if err == io.EOF {
			logger.Println("Client disconnected!")
			return []byte{0}, false
		}
	}

	n := bytes.Index(buf, []byte{0})
	if n != 0 {
		logger.Println("Received: ", buf[:n], string(buf[:n]))
		return buf[:n-2], true
	}

	return []byte{0}, false
}

func write(conn net.Conn, msg string) {
	buf := []byte(msg)
	buf = append(buf, []byte(CRLF)...)
	logger.Println(buf, len(buf))
	conn.Write(buf)
}

func handleClient(conn net.Conn, table *KeyValueStore) {
	defer conn.Close()
	for {
		if msg, ok := read(conn, 1024); ok {
			parseInput(conn, string(msg), table)
		} else {
			break
		}
	}
}

func isValid(cmd string, tokens []string, conn net.Conn) int {
	var flag int
	switch cmd {
	case SET:
		if len(tokens) > 5 || len(tokens) < 4 {
			flag = 1
			logger.Println(cmd, ":Invalid no. of tokens")
		}
		if len([]byte(tokens[1])) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid size of key")
		}
		if len(tokens) == 5 && tokens[4] != NOREPLY {
			logger.Println(cmd, ":optional arg incorrect")
			flag = 1
		}
		if _, err := strconv.ParseUint(tokens[2], 10, 64); err != nil {
			logger.Println(cmd, ":expiry time invalid")
			flag = 1
		}
		if _, err := strconv.ParseUint(tokens[3], 10, 64); err != nil {
			logger.Println(cmd, ":numBytes invalid")
			flag = 1
		}
		//other validations
	case GET:
		if len(tokens) != 2 {
			flag = 1
			logger.Println(cmd, ":Invalid number of arguments")
		}
		if len(tokens[1]) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid key size")
		}
		//other validations
	case GETM:
		if len(tokens) != 2 {
			flag = 1
		}
		if len(tokens[1]) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid key size")
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
	case 1:
		write(conn, ERR_CMD_ERR)
	}

	return flag
}

func parseInput(conn net.Conn, msg string, table *KeyValueStore) {
	tokens := strings.Fields(msg)
	var buffer bytes.Buffer
	//logger.Println(tokens)
	switch tokens[0] {
	case SET:
		if isValid(SET, tokens, conn) != 0 {
			return
		}
		if ver, ok, r := performSet(conn, tokens[1:len(tokens)], table); ok {
			logger.Println(ver)
			if r {
				buffer.Reset()
				buffer.WriteString(OK)
				buffer.WriteString(" ")
				buffer.WriteString(strconv.FormatUint(ver, 10))
				logger.Println(buffer.String())
				write(conn, buffer.String())
			}
		}
	case GET:
		if isValid(GET, tokens, conn) != 0 {
			return
		}
		if data, ok := performGet(conn, tokens[1:len(tokens)], table); ok {
			logger.Println("sending", tokens[1], "data")
			buffer.Reset()
			buffer.WriteString(VALUE)
			buffer.WriteString(" ")
			buffer.WriteString(strconv.FormatUint(data.numBytes, 10))
			write(conn, buffer.String())
			buffer.Reset()
			buffer.Write(data.value)
			write(conn, buffer.String())
		} else {
			buffer.Reset()
			buffer.WriteString(ERR_NOT_FOUND)
			write(conn, buffer.String())
		}

	case GETM:
		if isValid(GETM, tokens, conn) != 0 {
			return
		}
		if data, ok := performGetm(conn, tokens[1:len(tokens)], table); ok {
			logger.Println("sending", tokens[1], "metadata")
			buffer.Reset()
			buffer.WriteString(VALUE)
			buffer.WriteString(" ")
			buffer.WriteString(strconv.FormatUint(data.version, 10))
			buffer.WriteString(" ")
			buffer.WriteString(strconv.FormatUint(data.expTime - uint64(time.Now().Unix()), 10))
			buffer.WriteString(" ")
			buffer.WriteString(strconv.FormatUint(data.numBytes, 10))
			write(conn, buffer.String())
			buffer.Reset()
			buffer.Write(data.value)
			write(conn, buffer.String())
		} else {
			buffer.Reset()
			buffer.WriteString(ERR_NOT_FOUND)
			write(conn, buffer.String())
		}

	//case CAS: performCas(tokens[1:len(tokens)])
	//case DELETE: performDelete(tokens[1:len(tokens)])
	default:
		logger.Println("Command not found")
	}
}

func performSet(conn net.Conn, tokens []string, table *KeyValueStore) (uint64, bool, bool) {
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	n, _ := strconv.ParseUint(tokens[2], 10, 64)
	r := true

	if len(tokens) == 4 && tokens[3] == NOREPLY {
		r = false
	}

	logger.Println(r)

	//read value
	v, ok := read(conn, n+2)
	if !ok {
		//error here
		return 0, false, false
	}

	table.Lock()
	logger.Println("Table locked")
	//critical section start
	var val *Data
	if _, ok := table.dictionary[k]; ok {
		val = table.dictionary[k]
	} else {
		val = new(Data)
		table.dictionary[k] = val
	}
	ver++
	val.numBytes = n
	val.version = ver
	if e == 0 {
	val.expTime = e
	} else {
		val.expTime = e + uint64(time.Now().Unix())
	}
	val.value = v

	table.Unlock()
	logger.Println("Table unlocked")
	debug(table)
	return val.version, true, r
}

func performGet(conn net.Conn, tokens []string, table *KeyValueStore) (*Data, bool) {
	k := tokens[0]
	defer table.RUnlock()
	table.RLock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if v.expTime != 0 && v.expTime < uint64(time.Now().Unix()) {

			return nil, false
		}
		data := new(Data)
		data.numBytes = v.numBytes
		data.value = v.value[:]
		return data, true
	} else {
		return nil, false
	}
}

func performGetm(conn net.Conn, tokens []string, table *KeyValueStore) (*Data, bool) {
	k := tokens[0]
	defer table.RUnlock()
	table.RLock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if v.expTime != 0 && v.expTime < uint64(time.Now().Unix()) {
			return nil, false
		}
		data := new(Data)
		data.version = v.version
		data.expTime = v.expTime
		data.numBytes = v.numBytes
		data.value = v.value[:]
		return data, true
	} else {
		return nil, false
	}
}

func debug(table *KeyValueStore) {
	logger.Println("----start debug----")
	for key, val := range (*table).dictionary {
		logger.Println(key, val)
	}
	logger.Println("----end debug----")
}

func main() {
	ver = 1

	if LOG {
		logf, _ := os.OpenFile("serverlog.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		defer logf.Close()
		logger = log.New(logf, "SERVER: ", log.Ltime|log.Lshortfile)
	} else {
		logger = log.New(ioutil.Discard, "SERVER: ", log.Ldate)
	}

	go startServer()
	var input string
	fmt.Scanln(&input)
}
