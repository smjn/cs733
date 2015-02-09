package raft

import (
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"utils"
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

	//constant
	MAX_CMD_ARGS = 5
	MIN_CMD_ARGS = 2
	READ_TIMEOUT = 5
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

//pointer to custom logger
var logger *log.Logger

//cache
var table *KeyValueStore

/*Simple write function to send information to the client
 *arguments: client connection, msg to send to the client
 *return: none
 */
func write(conn net.Conn, msg string) {
	buf := []byte(msg)
	buf = append(buf, []byte(CRLF)...)
	conn.Write(buf)
}

/*Basic validations for various commands
 *arguments: command to check against, other parmameters sent with the command (excluding the value), client connection
 *return: integer representing error state
 */
func isValid(cmd string, tokens []string, conn net.Conn) int {
	switch cmd {
	case SET:
		if len(tokens) != 4 {
			logger.Println(cmd, ":Invalid no. of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len([]byte(tokens[1])) > 250 {
			logger.Println(cmd, ":Invalid size of key")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if _, err := strconv.ParseUint(tokens[2], 10, 64); err != nil {
			logger.Println(cmd, ":expiry time invalid")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if _, err := strconv.ParseUint(tokens[3], 10, 64); err != nil {
			logger.Println(cmd, ":numBytes invalid")
			write(conn, ERR_CMD_ERR)
			return 1
		}

	case GET:
		if len(tokens) != 2 {
			logger.Println(cmd, ":Invalid number of arguments")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len(tokens[1]) > 250 {
			logger.Println(cmd, ":Invalid key size")
			write(conn, ERR_CMD_ERR)
			return 1
		}

	case GETM:
		if len(tokens) != 2 {
			logger.Println(cmd, ":Invalid number of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len(tokens[1]) > 250 {
			logger.Println(cmd, ":Invalid key size")
			write(conn, ERR_CMD_ERR)
			return 1
		}

	case CAS:
		if len(tokens) != 5 {
			logger.Println(cmd, ":Invalid number of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len([]byte(tokens[1])) > 250 {
			logger.Println(cmd, ":Invalid size of key")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if _, err := strconv.ParseUint(tokens[2], 10, 64); err != nil {
			logger.Println(cmd, ":expiry time invalid")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if _, err := strconv.ParseUint(tokens[3], 10, 64); err != nil {
			logger.Println(cmd, ":version invalid")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if _, err := strconv.ParseUint(tokens[4], 10, 64); err != nil {
			logger.Println(cmd, ":numbytes invalid")
			write(conn, ERR_CMD_ERR)
			return 1
		}

	case DELETE:
		if len(tokens) != 2 {
			logger.Println(cmd, ":Invalid number of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len([]byte(tokens[1])) > 250 {
			logger.Println(cmd, ":Invalid size of key")
			write(conn, ERR_CMD_ERR)
			return 1
		}

	default:
		return 0
	}
	//compiler is happy
	return 0
}

func MonitorCommitChannel(ch chan LogEntry) {
	for {
		temp := <-ch
		conn := temp.(*LogEntryData).conn
		cmd := new(utils.Command)
		if err := cmd.GobDecode(temp.Data()); err != nil {
			log.Fatal("Error decoding command!")
		}
		ParseInput(conn, cmd)
	}
}

/*Function parses the command provided by the client and delegates further action to command specific functions.
 *Based on the return values of those functions, send appropriate messages to the client.
 *arguments: client connection, message from client, channel shared with myRead function
 *return: none
 */
func ParseInput(conn net.Conn, cmd *utils.Command) {
	msg := string(cmd.Cmd)
	tokens := strings.Fields(msg)
	//general error, don't check for commands, avoid the pain ;)
	if len(tokens) > MAX_CMD_ARGS || len(tokens) < MIN_CMD_ARGS {
		write(conn, ERR_CMD_ERR)
		return
	}

	//fmt.Println(tokens)

	//for efficient string concatenation
	var buffer bytes.Buffer
	switch tokens[0] {
	case SET:
		if isValid(SET, tokens, conn) != 0 {
			return
		}
		if ver, ok, r := performSet(conn, tokens[1:len(tokens)], cmd); ok {
			//debug(table)
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
		if data, ok := performGet(conn, tokens[1:len(tokens)]); ok {
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
		//debug(table)

	case GETM:
		if isValid(GETM, tokens, conn) != 0 {
			return
		}
		if data, ok := performGetm(conn, tokens[1:len(tokens)]); ok {
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
		//debug(table)

	case CAS:
		if isValid(CAS, tokens, conn) != 0 {
			return
		}
		if ver, ok, r := performCas(conn, tokens[1:len(tokens)], cmd); r {
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
		//debug(table)

	case DELETE:
		if isValid(DELETE, tokens, conn) != 0 {
			return
		}
		if ok := performDelete(conn, tokens[1:len(tokens)]); ok == 0 {
			write(conn, DELETED)
		} else {
			write(conn, ERR_NOT_FOUND)
		}
		//debug(table)

	default:
		buffer.Reset()
		buffer.WriteString(ERR_CMD_ERR)
		write(conn, buffer.String())
	}
}

/*Delegate function responsible for all parsing and hashtable interactions for the SET command sent by client
 *arguments: client connection, tokenized command sent by the client, command structure @utils.Command
 *return: version of inserted key (if successful, 0 otherwise), success or failure, whether to send reply to client
 */
func performSet(conn net.Conn, tokens []string, cmd *utils.Command) (uint64, bool, bool) {
	k := tokens[0]
	//expiry time offset
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	//numbytes
	n, _ := strconv.ParseUint(tokens[2], 10, 64)
	r := true

	logger.Println(r)

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
	val.numBytes = n
	val.version++
	if e == 0 {
		val.isPerpetual = true
		val.expTime = 0
	} else {
		val.isPerpetual = false
		val.expTime = e + uint64(time.Now().Unix())
	}
	val.value = cmd.Val
	return val.version, true, r
}

/*Delegate function reponsible for activities related to the GET command sent by the client.
 *arguments: client connection, tokenized command sent by the client
 *return: pointer to value corresponding to the key given by client, success or failure
 */
func performGet(conn net.Conn, tokens []string) (*Data, bool) {
	k := tokens[0]
	defer table.Unlock()
	//lock because if key is expired, we'll delete it
	table.Lock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if !v.isPerpetual && v.expTime < uint64(time.Now().Unix()) {
			//delete the key
			delete(table.dictionary, k)
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
 *arguments: client connection, tokenized command sent by the client
 *return: pointer to value corresponding to the key given by client, success or failure
 */
func performGetm(conn net.Conn, tokens []string) (*Data, bool) {
	k := tokens[0]
	defer table.Unlock()
	table.Lock()
	//critical section begin
	if v, ok := table.dictionary[k]; ok {
		if !v.isPerpetual && v.expTime < uint64(time.Now().Unix()) {
			//delete the key
			delete(table.dictionary, k)
			return nil, false
		}
		data := new(Data)
		data.version = v.version
		data.expTime = v.expTime
		data.numBytes = v.numBytes
		data.value = v.value[:]
		data.isPerpetual = v.isPerpetual

		return data, true
	} else {
		return nil, false
	}
}

/*Delegate function reponsible for activities related to the CAS command sent by the client.
 *arguments: client connection, tokenized command sent by the client, cmd pointer @utils.Command
 *return: new version of updated key (if it is updated), error status {0: error while reading new value, 1: key found and changed,
 *2: version mismatch with key, 3: key not found}, whether to reply to client
 */
func performCas(conn net.Conn, tokens []string, cmd *utils.Command) (uint64, int, bool) {
	k := tokens[0]
	e, _ := strconv.ParseUint(tokens[1], 10, 64)
	ve, _ := strconv.ParseUint(tokens[2], 10, 64)
	n, _ := strconv.ParseUint(tokens[3], 10, 64)
	r := true

	logger.Println(k, e, ve, n, r)

	defer table.Unlock()
	table.Lock()
	if val, ok := table.dictionary[k]; ok {
		if val.version == ve {
			if val.isPerpetual || val.expTime >= uint64(time.Now().Unix()) {
				//if expiry time is zero, key should not be deleted
				if e == 0 {
					val.isPerpetual = true
					val.expTime = 0
				} else {
					val.isPerpetual = false
					val.expTime = e + uint64(time.Now().Unix())
				}
				val.numBytes = n
				val.version++
				val.value = cmd.Val
				//key found and changed
				return val.version, 0, r
			} else {
				logger.Println("expired key found!")
				//version found but key expired, can delete key safely and tell client that it does not exist
				delete(table.dictionary, k)
				return 0, 3, r
			}
		}
		//version mismatch
		return 0, 2, r
	}
	//key not found
	return 0, 3, r
}

/*Delegate function reponsible for activities related to the DELETE command sent by the client.
 *arguments: client connection, tokenized command sent by the client
 *return: integer secifying error state {0: found and deleted, 1: found but expired (deleted but client told non-existent, 2: key not found}
 */
func performDelete(conn net.Conn, tokens []string) int {
	k := tokens[0]
	logger.Println(tokens)
	flag := 1
	defer table.Unlock()
	table.Lock()
	//begin critical section
	if v, ok := table.dictionary[k]; ok {
		if v.isPerpetual || v.expTime >= uint64(time.Now().Unix()) {
			//found not expired
			flag = 0
		}
		//delete anyway as expired or needs to be deleted
		delete(table.dictionary, k)
		return flag
	}
	//key not found
	return 2
}

/*Simple function that dumps the contents of the hashtable
 *arguments: none
 *return: none
 */
func debug() {
	logger.Println("----start debug----")
	for key, val := range (*table).dictionary {
		logger.Println(key, val)
	}
	logger.Println("----end debug----")
}

func InitKVStore(log *log.Logger) {
	logger = log

	//initialize key value store
	table = &KeyValueStore{dictionary: make(map[string]*Data)}
}

//server will not call this, we'll call it from test cases to clear the map
func ReInitServer() {
	defer table.Unlock()
	table.Lock()
	for key, _ := range table.dictionary {
		delete(table.dictionary, key)
	}
	//fmt.Println(table.dictionary)
}
