package kvstore

import (
	"bytes"
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

	//constant
	MAX_CMD_ARGS = 6
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

/*Function to start the server and accept connections.
 *arguments: none
 *return: none
 */
//func startServer() {
//	logger.Println("Server started")
//	listener, err := net.Listen("tcp", ":5000")
//	if err != nil {
//		logger.Println("Could not start server!")
//	}

//	//initialize key value store
//	table = &KeyValueStore{dictionary: make(map[string]*Data)}

//	//infinite loop
//	for {
//		conn, err := listener.Accept()
//		if err != nil {
//			logger.Println(err)
//			continue
//		}

//		go handleClient(conn) //client connection handler
//	}
//}

/*Function to read data from the connection and put it on the channel so it could be read in a systematic fashion.
 *arguments: channel shared between this go routine and other functions performing actions based on the commands given, client connection
 *return: none
 */
//func myRead(ch chan []byte, conn net.Conn) {
//	scanner := bufio.NewScanner(conn)
//	scanner.Split(CustomSplitter)
//	for {
//		if ok := scanner.Scan(); !ok {
//			break
//		} else {
//			temp := scanner.Bytes()
//			ch <- temp
//			logger.Println(temp, "$$")
//		}
//	}
//}

/*Simple write function to send information to the client
 *arguments: client connection, msg to send to the client
 *return: none
 */
func write(conn net.Conn, msg string) {
	buf := []byte(msg)
	buf = append(buf, []byte(CRLF)...)
	//logger.Println(buf, len(buf))
	conn.Write(buf)
}

/*After initial establishment of the connection with the client, this go routine handles further interaction
 *arguments: client connection
 *return: none
 */
//func handleClient(conn net.Conn) {
//	defer conn.Close()
//	//channel for every connection for every client
//	ch := make(chan []byte)
//	go myRead(ch, conn)

//	for {
//		msg := <-ch
//		logger.Println("Channel: ", msg, string(msg))
//		if len(msg) == 0 {
//			continue
//		}
//		parseInput(conn, string(msg), table, ch)
//	}
//}

/*Basic validations for various commands
 *arguments: command to check against, other parmameters sent with the command (excluding the value), client connection
 *return: integer representing error state
 */
func isValid(cmd string, tokens []string, conn net.Conn) int {
	switch cmd {
	case SET:
		if len(tokens) > 5 || len(tokens) < 4 {
			logger.Println(cmd, ":Invalid no. of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len([]byte(tokens[1])) > 250 {
			logger.Println(cmd, ":Invalid size of key")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len(tokens) == 5 && tokens[4] != NOREPLY {
			logger.Println(cmd, ":optional arg incorrect")
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
		if len(tokens) > 6 || len(tokens) < 5 {
			logger.Println(cmd, ":Invalid number of tokens")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len([]byte(tokens[1])) > 250 {
			logger.Println(cmd, ":Invalid size of key")
			write(conn, ERR_CMD_ERR)
			return 1
		}
		if len(tokens) == 6 && tokens[5] != NOREPLY {
			logger.Println(cmd, ":optional arg incorrect")
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

/*Function parses the command provided by the client and delegates further action to command specific functions.
 *Based on the return values of those functions, send appropriate messages to the client.
 *arguments: client connection, message from client, channel shared with myRead function
 *return: none
 */
func ParseInput(conn net.Conn, msg string, ch chan []byte) {
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
		if ver, ok, r := performSet(conn, tokens[1:len(tokens)], ch); ok {
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
		if ver, ok, r := performCas(conn, tokens[1:len(tokens)], ch); r {
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
 *arguments: client connection, tokenized command sent by the client, channel shared with myRead
 *return: version of inserted key (if successful, 0 otherwise), success or failure, whether to send reply to client
 */
func performSet(conn net.Conn, tokens []string, ch chan []byte) (uint64, bool, bool) {
	//-k := tokens[0]
	//expiry time offset
	//-e, _ := strconv.ParseUint(tokens[1], 10, 64)
	//numbytes
	//-n, _ := strconv.ParseUint(tokens[2], 10, 64)
	r := true

	if len(tokens) == 4 && tokens[3] == NOREPLY {
		r = false
	}

	logger.Println(r)

	//if v, err := readValue(ch, n); err {
	//	write(conn, ERR_CMD_ERR)
	//	return 0, false, r
	//} else {
	//	defer table.Unlock()
	//	table.Lock()
	//	//critical section start
	//	var val *Data
	//	if _, ok := table.dictionary[k]; ok {
	//		val = table.dictionary[k]
	//	} else {
	//		val = new(Data)
	//		table.dictionary[k] = val
	//	}
	//	val.numBytes = n
	//	val.version++
	//	if e == 0 {
	//		val.isPerpetual = true
	//		val.expTime = 0
	//	} else {
	//		val.isPerpetual = false
	//		val.expTime = e + uint64(time.Now().Unix())
	//	}
	//	val.value = v
	//	return val.version, true, r
	//}
	return 2, true, true
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
 *arguments: client connection, tokenized command sent by the client, channel shared with myRead
 *return: new version of updated key (if it is updated), error status {0: error while reading new value, 1: key found and changed,
 *2: version mismatch with key, 3: key not found}, whether to reply to client
 */
func performCas(conn net.Conn, tokens []string, ch chan []byte) (uint64, int, bool) {
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
	//if v, err := readValue(ch, n); err {
	//	return 0, 1, r
	//} else {
	//	defer table.Unlock()
	//	table.Lock()
	//	if val, ok := table.dictionary[k]; ok {
	//		if val.version == ve {
	//			if val.isPerpetual || val.expTime >= uint64(time.Now().Unix()) {
	//				//if expiry time is zero, key should not be deleted
	//				if e == 0 {
	//					val.isPerpetual = true
	//					val.expTime = 0
	//				} else {
	//					val.isPerpetual = false
	//					val.expTime = e + uint64(time.Now().Unix())
	//				}
	//				val.numBytes = n
	//				val.version++
	//				val.value = v
	//				//key found and changed
	//				return val.version, 0, r
	//			} else {
	//				logger.Println("expired key found!")
	//				//version found but key expired, can delete key safely and tell client that it does not exist
	//				delete(table.dictionary, k)
	//				return 0, 3, r
	//			}
	//		}
	//		//version mismatch
	//		return 0, 2, r
	//	}
	//	//key not found
	//	return 0, 3, r
	//}
	return 1, 1, true
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

/*Entry point of this program. Initializes the start of ther server and sets up the logger.
 *arguments: none
 *return: none
 */
//func main() {
//	toLog := ""
//	if len(os.Args) > 1 {
//		toLog = os.Args[1]
//	}

//	//toLog = "s"
//	if toLog != "" {
//		logf, _ := os.OpenFile("serverlog.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
//		defer logf.Close()
//		logger = log.New(logf, "SERVER: ", log.Ltime|log.Lshortfile)
//		//logger = log.New(os.Stdout, "SERVER: ", log.Ltime|log.Lshortfile)
//	} else {
//		logger = log.New(ioutil.Discard, "SERVER: ", log.Ldate)
//	}

//	go startServer()
//	var input string
//	fmt.Scanln(&input)
//}

func InitKVStore() {
	toLog := ""
	if len(os.Args) > 1 {
		toLog = os.Args[1]
	}

	//toLog = "s"
	if toLog != "" {
		logf, _ := os.OpenFile("serverlog.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		defer logf.Close()
		logger = log.New(logf, "SERVER: ", log.Ltime|log.Lshortfile)
		//logger = log.New(os.Stdout, "SERVER: ", log.Ltime|log.Lshortfile)
	} else {
		logger = log.New(ioutil.Discard, "SERVER: ", log.Ldate)
	}

	//initialize key value store
	table = &KeyValueStore{dictionary: make(map[string]*Data)}
}

func IsCas(msg string) bool {
	return msg == CAS
}

func IsSet(msg string) bool {
	return msg == SET
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
