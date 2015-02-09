package connhandler

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"log"
	"net"
	"raft"
	"strconv"
	"strings"
	"time"
	"utils"
)

/*
 *Helper function to read value or cause timeout after READ_TIMEOUT seconds
 *parameters: channel to read data from, threshold number of bytes to read, log pointer to write into
 *returns: the value string and error state
 */
func readValue(ch chan []byte, n uint64, logger *log.Logger) ([]byte, bool) {
	//now we need to read the value which should have been sent
	valReadLength := uint64(0)
	var v []byte
	err := false
	up := make(chan bool, 1)
	//after 5 seconds passed reading value, we'll just send err to client
	go func() {
		time.Sleep(5 * time.Second)
		up <- true
	}()

	//use select for the data channel and the timeout channel
	for valReadLength < n+2 {
		select {
		case temp := <-ch:
			valReadLength += uint64(len(temp))
			if valReadLength > n+2 {
				err = true
				break
			}
			v = append(v, temp...)

		case <-up:
			err = true
			break
		}

		//will be true if timeout occurs
		if err {
			logger.Println("Timeout")
			break
		}
	}

	if err {
		return []byte{0}, err
	}
	return v[:n], err
}

/*Copied from the bufio.Scanner (originally ScanLines).
 *By default it splits by '\n' but now we want it to split by '\r\n'
 *arguments: data in bytes, is eof reached
 *return: next sequence of bytes, chunk of data found, err state
 */
func CustomSplitter(data []byte, atEOF bool) (advance int, token []byte, err error) {
	omega := 0
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for {
		if i := bytes.IndexByte(data[omega:], '\n'); i >= 0 {
			//here we add omega as we are using the complete data array instead of the slice where we found '\n'
			if i > 0 && data[omega+i-1] == '\r' {
				//next byte begins at i+1 and data[0:i+1] returned
				return omega + i + 1, data[:omega+i+1], nil
			} else {
				//move the omega index to the byte after \n
				omega += i + 1
			}
		} else {
			//need to break free the chains
			break
		}
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

/*Function to read data from the connection and put it on the channel so it could be read in a systematic fashion.
 *arguments: channel shared between this go routine and other functions performing actions based on the commands given,
 *client connection
 *return: none
 */
func MyRead(ch chan []byte, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Split(CustomSplitter)
	for {
		if ok := scanner.Scan(); !ok {
			break
		} else {
			temp := scanner.Bytes()
			ch <- temp
		}
	}
}

/*Simple write function to send information to the client
 *arguments: client connection, msg to send to the client
 *return: none
 */
func Write(conn net.Conn, msg string) {
	buf := []byte(msg)
	buf = append(buf, []byte("\r\n")...)
	conn.Write(buf)
}

/*Will be invoked as go routine by server to every client connection. Will take care of all communication with the
 *client and the raft/kvstore
 *arguments: connection to client, pointer to raft, pointer to logger
 *return: none
 */
func HandleClient(conn net.Conn, rft *raft.Raft, logger *log.Logger) {
	defer conn.Close()
	//channel for every connection for every client
	ch := make(chan []byte)
	go MyRead(ch, conn)

	for {
		command := new(utils.Command)
		msg := <-ch
		logger.Println("got:", msg, string(msg))

		if len(msg) == 0 {
			continue
		}
		command.Cmd = msg
		flag := false
		nr := uint64(0)
		tokens := strings.Fields(string(msg))
		if tokens[0] == "cas" {
			n, _ := strconv.ParseUint(tokens[4], 10, 64)
			nr = n
			flag = true
		} else if tokens[0] == "set" {
			n, _ := strconv.ParseUint(tokens[3], 10, 64)
			nr = n
			flag = true
		}
		if flag {
			logger.Println("numbytes", nr)
			if v, err := readValue(ch, nr, logger); err {
				logger.Println("error reading value")
				Write(conn, "ERR_CMD_ERR")
				continue
			} else {
				command.Val = v
				//command.isVal = true
			}
		}

		buffer := new(bytes.Buffer)
		// writing
		enc := gob.NewEncoder(buffer)
		err := enc.Encode(command)
		if err != nil {
			//log.Fatal("encode error:", err)
		}

		if _, err := rft.Append(buffer.Bytes(), conn); err != nil {
			Write(conn, "ERR_REDIRECT 127.0.0.1 "+strconv.Itoa(raft.CLIENT_PORT+1))
			conn.Close()
			break
		}
	}
}
