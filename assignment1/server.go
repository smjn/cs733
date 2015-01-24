package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*Constants used throughout the program to identify commands, request, response, and error messages*/
const (
	//request
	SET     = "set"
	GET     = "get"
	GETM    = "getm"
	CAS     = "cas"
	DELETE  = "delete"
	NOREPLY = "noreply"

	//	//response
	OK      = "OK"
	CRLF    = "\r\n"
	VALUE   = "VALUE"
	DELETED = "DELETED"

	//errors
	ERR_CMD_ERR   = "ERR_CMD_ERR"
	ERR_NOT_FOUND = "ERR_NOT_FOUND"
	ERR_VERSION   = "ERR_VERSION"
	ERR_INTERNAL  = "ERR_INTERNAL"
)

//represents the value in the main hashtable (key, value) pair
type Data struct {
	numBytes    uint64 //number of bytes of the value bytes
	version     uint64 //current version of the key
	expTime     uint64 //time offset in seconds after which the key should expire
	value       []byte //bytes representing the actual content of the value
	isPerpetual bool   //specifies that the key does not expire
}

//represents the main hashtable where the dance actually happens
type KeyValueStore struct {
	dictionary   map[string]*Data //the hashtable that stores the (key, value) pairs
	sync.RWMutex                  //mutex for synchronization when reading or writing to the hashtable
}

//global version counter
var ver uint64

//pointer to custom logger
var logger *log.Logger

/*Function to start the server and accept connections.
 *arguments: none
 *return: none
 */
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

		go handleClient(conn, table) //client connection handler
	}
}

/*Function to read data from the connection and put it on the channel so it could be read in a systematic fashion.
 *arguments: channel shared between this go routine and other functions performing actions based on the commands given, client connection
 *return: none
 */
func myRead(ch chan string, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for {
		if ok := scanner.Scan(); !ok {
			break
		} else {
			temp := scanner.Text()
			ch <- temp
			logger.Println(temp, "$$")
		}
	}
}

/*Simple write function to send information to the client
 *arguments: client connection, msg to send to the client
 *return: none
 */
func write(conn net.Conn, msg string) {
	buf := []byte(msg)
	buf = append(buf, []byte(CRLF)...)
	logger.Println(buf, len(buf))
	conn.Write(buf)
}

/*After initial establishment of the connection with the client, this go routine handles further interaction
 *arguments: client connection, pointer to the hastable structure
 *return: none
 */
func handleClient(conn net.Conn, table *KeyValueStore) {
	defer conn.Close()
	//channel for every connection for every client
	ch := make(chan string)
	go myRead(ch, conn)

	for {
		msg := <-ch
		logger.Println("Channel: ", msg)
		if len(msg) == 0 {
			continue
		}
		parseInput(conn, string(msg), table, ch)
	}
}

/*Basic validations for various commands
 *arguments: command to check against, other parmameters sent with the command (excluding the value), client connection
 *return: integer representing error state
 */
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

	case GET:
		if len(tokens) != 2 {
			flag = 1
			logger.Println(cmd, ":Invalid number of arguments")
		}
		if len(tokens[1]) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid key size")
		}

	case GETM:
		if len(tokens) != 2 {
			flag = 1
		}
		if len(tokens[1]) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid key size")
		}

	case CAS:
		if len(tokens) > 6 || len(tokens) < 5 {
			flag = 1
		}
		if len([]byte(tokens[1])) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid size of key")
		}
		if len(tokens) == 6 && tokens[5] != NOREPLY {
			logger.Println(cmd, ":optional arg incorrect")
			flag = 1
		}
		if _, err := strconv.ParseUint(tokens[2], 10, 64); err != nil {
			logger.Println(cmd, ":expiry time invalid")
			flag = 1
		}
		if _, err := strconv.ParseUint(tokens[3], 10, 64); err != nil {
			logger.Println(cmd, ":version invalid")
			flag = 1
		}
		if _, err := strconv.ParseUint(tokens[4], 10, 64); err != nil {
			logger.Println(cmd, ":numbytes invalid")
			flag = 1
		}

	case DELETE:
		if len(tokens) != 2 {
			flag = 1
		}
		if len([]byte(tokens[1])) > 250 {
			flag = 1
			logger.Println(cmd, ":Invalid size of key")
		}

	default:
		return 0
	}

	switch flag {
	case 1:
		write(conn, ERR_CMD_ERR)
	}

	return flag
}

/*Function parses the command provided by the client and delegates further action to command specific functions.
 *Based on the return values of those functions, send appropriate messages to the client.
 *arguments: client connection, message from client, pointer to hashtable structure, channel shared with myRead function
 *return: none
 */
func parseInput(conn net.Conn, msg string, table *KeyValueStore, ch chan string) {
	tokens := strings.Fields(msg)
	//general error, don't check for commands, avoid the pain ;)
	if len(tokens) > 6 {
		write(conn, ERR_CMD_ERR)
		return
	}

	var buffer bytes.Buffer //for efficient string concatenation
	//logger.Println(tokens)
	switch tokens[0] {
	case SET:
		if isValid(SET, tokens, conn) != 0 {
			return
		}
		if ver, ok, r := performSet(conn, tokens[1:len(tokens)], table, ch); ok {
			debug(table)
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
		debug(table)

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
			if data.isPerpetual {
				buffer.WriteString("0")
			} else {
				buffer.WriteString(strconv.FormatUint(data.expTime-uint64(time.Now().Unix()), 10))
			}
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
		debug(table)

	case CAS:
		if isValid(CAS, tokens, conn) != 0 {
			return
		}
		if ver, ok, r := performCas(conn, tokens[1:len(tokens)], table, ch); r {
			if r {
				switch ok {
				case 0:
					buffer.Reset()
					buffer.WriteString(OK)
					buffer.WriteString(" ")
					buffer.WriteString(strconv.FormatUint(ver, 10))
					logger.Println(buffer.String())
					write(conn, buffer.String())
				case 1:
					buffer.Reset()
					buffer.WriteString(ERR_CMD_ERR)
					write(conn, buffer.String())
				case 2:
					buffer.Reset()
					buffer.WriteString(ERR_VERSION)
					write(conn, buffer.String())
				case 3:
					buffer.Reset()
					buffer.WriteString(ERR_NOT_FOUND)
					write(conn, buffer.String())
				}
			}
		}
		debug(table)

	case DELETE:
		if isValid(DELETE, tokens, conn) != 0 {
			return
		}
		if ok := performDelete(conn, tokens[1:len(tokens)], table); ok == 0 {
			write(conn, DELETED)
		} else {
			write(conn, ERR_NOT_FOUND)
		}
		debug(table)

	default:
		buffer.Reset()
		buffer.WriteString(ERR_CMD_ERR)
		write(conn, buffer.String())
	}
}

/*
 *Helper function to read value or cause timeout after 5 seconds
 *parameters: channel to read data from, threshold number of bytes to read
 *returns: the value string and error state
 */
func readValue(ch chan string, n uint64) ([]byte, bool) {
	//now we need to read the value which should have been sent
	valReadLength := uint64(0)
	var v string
	err := false
	up := make(chan bool, 1)
	//after 5 seconds passed reading value, we'll just send err to client
	go func() {
		time.Sleep(5 * time.Second)
		up <- true
	}()

	//use select for the data channel and the timeout channel
	for valReadLength < n {
		select {
		case temp := <-ch:
			logger.Println("Value chunk read!")
			valReadLength += uint64(len(temp))
			v += temp

		case <-up:
			err = true
			logger.Println("Oh, Oh timeout")
			//write(conn, ERR_INTERNAL)
			break
		}

		//will be true if timeout occurs
		if err {
			break
		}
	}

	if err {
		return []byte{0}, err
	}
	return []byte(v), err
}

/*Delegate function responsible for all parsing and hashtable interactions for the SET command sent by client
 *arguments: client connection, tokenized command sent by the client, pointer to hashtable structure, channel shared with myRead
 *return: version of inserted key (if successful, 0 otherwise), success or failure, whether to send reply to client
 */
func performSet(conn net.Conn, tokens []string, table *KeyValueStore, ch chan string) (uint64, bool, bool) {
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64) //expiry time offset
	n, _ := strconv.ParseUint(tokens[2], 10, 64) //numbytes
	r := true

	if len(tokens) == 4 && tokens[3] == NOREPLY {
		r = false
	}

	logger.Println(r)

	if v, err := readValue(ch, n); err {
		write(conn, ERR_INTERNAL)
		return 0, false, r
	} else {
		defer table.Unlock()
		table.Lock()
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
			val.isPerpetual = true
			val.expTime = 0
		} else {
			val.isPerpetual = false
			val.expTime = e + uint64(time.Now().Unix())
		}
		val.value = v
		return val.version, true, r
	}
}

/*Delegate function reponsible for activities related to the GET command sent by the client.
 *arguments: client connection, tokenized command sent by the client, pointer to hashtable structure
 *return: pointer to value corresponding to the key given by client, success or failure
 */
func performGet(conn net.Conn, tokens []string, table *KeyValueStore) (*Data, bool) {
	k := tokens[0]
	defer table.RUnlock()
	table.RLock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if !v.isPerpetual && v.expTime < uint64(time.Now().Unix()) {
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

/*Delegate function reponsible for activities related to the GETM command sent by the client.
 *arguments: client connection, tokenized command sent by the client, pointer to hashtable structure
 *return: pointer to value corresponding to the key given by client, success or failure
 */
func performGetm(conn net.Conn, tokens []string, table *KeyValueStore) (*Data, bool) {
	k := tokens[0]
	defer table.RUnlock()
	table.RLock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if !v.isPerpetual && v.expTime < uint64(time.Now().Unix()) {
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

/*Delegate function reponsible for activities related to the CAS command sent by the client.
 *arguments: client connection, tokenized command sent by the client, pointer to hashtable structure, channel shared with myRead
 *return: new version of updated key (if it is updated), error status {0: error while reading new value, 1: key found and changed,
 *2: version mismatch with key, 3: key not found}, whether to reply to client
 */
func performCas(conn net.Conn, tokens []string, table *KeyValueStore, ch chan string) (uint64, int, bool) {
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	ve, _ := strconv.ParseUint(tokens[2], 10, 64)
	n, _ := strconv.ParseUint(tokens[3], 10, 64)
	r := true

	logger.Println(k, e, ve, n, r)
	if len(tokens) == 5 && tokens[4] == NOREPLY {
		r = false
	}

	//read value
	if v, err := readValue(ch, n); err {
		return 0, 1, r
	} else {
		defer table.Unlock()
		table.Lock()
		if val, ok := table.dictionary[k]; ok {
			if val.version == ve {
				if val.isPerpetual || val.expTime >= uint64(time.Now().Unix()) {
					if e == 0 { //if expiry time is zero, key should not be deleted
						val.isPerpetual = true
						val.expTime = 0
					} else {
						val.isPerpetual = false
						val.expTime = e + uint64(time.Now().Unix())
					}
					val.numBytes = n
					ver++
					val.version = ver
					val.value = v
					return val.version, 0, r //key found and changed
				} else {
					logger.Println("expired key found!")
					//version found but key expired, can delete key safely and tell client that it does not exist
					delete(table.dictionary, k)
					return 0, 3, r
				}
			}
			return 0, 2, r //version mismatch
		}
		return 0, 3, r //key not found
	}
}

/*Delegate function reponsible for activities related to the DELETE command sent by the client.
 *arguments: client connection, tokenized command sent by the client, pointer to hashtable structure
 *return: integer secifying error state {0: found and deleted, 1: found but expired (deleted but client told non-existent, 2: key not found}
 */
func performDelete(conn net.Conn, tokens []string, table *KeyValueStore) int {
	k := tokens[0]
	logger.Println(tokens)
	flag := 1
	defer table.Unlock()
	table.Lock()
	//begin critical section
	if v, ok := table.dictionary[k]; ok {
		if v.isPerpetual || v.expTime >= uint64(time.Now().Unix()) {
			flag = 0 //found not expired
		}
		delete(table.dictionary, k) //delete anyway as expired or needs to be deleted
		return flag
	}
	return 2 //key not found
}

/*Simple function that dumps the contents of the hashtable
 *arguments: pointer to the hashtable structure
 *return: none
 */
func debug(table *KeyValueStore) {
	logger.Println("----start debug----")
	for key, val := range (*table).dictionary {
		logger.Println(key, val)
	}
	logger.Println("----end debug----")
}

/*Entry point of this program. Initializes the start of ther server and sets up the logger.
 *arguments: none
 *return: none
 */
func main() {
	ver = 1

	toLog := ""
	if len(os.Args) > 1 {
		toLog = os.Args[1]
	}

	if toLog != "" {
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
