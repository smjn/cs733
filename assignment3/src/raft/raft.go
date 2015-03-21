package raft

import (
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

//constant values used
const (
	CLIENT_PORT = 9000
	LOG_PORT    = 20000
	ACK_TIMEOUT = 5
	MIN_TIMEOUT = 300
	MAX_TIMEOUT = 500
	LEADER      = iota
	CANDIDATE
	FOLLOWER
)

// Global variable for generating unique log sequence numbers
var lsn Lsn

// Flag for enabling/disabling logging functionality
var DEBUG = true

// See Log.Append. Implements Error interface.
type ErrRedirect int

//Log sequence number, unique for all time.
type Lsn uint64

// Stores the server information
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

// Stores the replica information of the cluster
type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

type ClientAppend struct {
	logEntry LogEntry
}

type VoteRequest struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

type AppendRPC struct {
}

type Timeout struct {
}

type RaftEvent interface {
}

type SharedLog interface {
	Append(data []byte, conn net.Conn) (LogEntry, error)
	AddToChannel(entry LogEntry)
}

// Raft information
type Raft struct {
	LogArray      []*LogEntryData // In memory store for log entries
	commitCh      chan LogEntry   // Commit Channel
	clusterConfig *ClusterConfig  // Cluster
	id            int             // Server id
	sync.RWMutex
	Info        *log.Logger    //log for raft instance
	eventCh     chan RaftEvent //receive events related to various states
	votedFor    int
	currentTerm int
}

// Log entry interface
type LogEntry interface {
	GetLsn() Lsn              // Returns Lsn
	GetData() []byte          // Returns Data
	GetCommitted() bool       // Returns committed status
	SetCommitted(status bool) // Sets committed status
}

type LogEntryData struct {
	Id        Lsn      // Unique identifier for log entry
	Data      []byte   // Data bytes
	Committed bool     // Commit status
	conn      net.Conn // Connection for communicating with client
}

func getCurrentTerm(serverId int, info *log.Logger) int {
	if file, err := os.Open("currentTerm" + strconv.Itoa(serverId)); err != nil {
		ioutil.WriteFile("currentTerm"+strconv.Itoa(serverId), []byte("0"), 0666)
		info.Println("wrote in term file:0")
		return 0
	} else {
		if data, err := ioutil.ReadFile(file.Name()); err != nil {
			info.Println("error reading file")
			return -1
		} else {
			info.Println("read from file")
			if t, err2 := strconv.Atoi(string(data)); err2 != nil {
				info.Println("error converting")
				return -1
			} else {
				info.Println("Converted success", t)
				return t
			}
		}
		return -1
	}
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry, eventCh chan RaftEvent, toDebug bool) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.clusterConfig = config
	rft.id = thisServerId
	rft.eventCh = eventCh
	rft.Info = getLogger(thisServerId, toDebug)
	rft.currentTerm = getCurrentTerm(thisServerId, rft.Info)
	return rft, nil
}

// Creates a log entry. This implements the LogEntry interface
// data: data bytes, committed: commit status, conn: connection to client
// Returns the log entry
func NewLogEntry(data []byte, committed bool, conn net.Conn) *LogEntryData {
	entry := new(LogEntryData)
	entry.Id = lsn
	entry.Data = data
	entry.conn = conn
	entry.Committed = committed
	lsn++
	return entry
}

// Gets the Lsn
func (entry *LogEntryData) GetLsn() Lsn {
	return entry.Id
}

// Get data
func (entry *LogEntryData) GetData() []byte {
	return entry.Data
}

// Get committed status
func (entry *LogEntryData) GetCommitted() bool {
	return entry.Committed
}

// Sets the committed status
func (entry *LogEntryData) SetCommitted(committed bool) {
	entry.Committed = committed
}

//make raft implement the append function
func (rft *Raft) Append(data []byte, conn net.Conn) (LogEntry, error) {
	rft.Info.Println("Append Called")
	if rft.id != 1 {
		return nil, ErrRedirect(1)
	}
	defer rft.Unlock()
	rft.Lock()
	temp := NewLogEntry(data, false, conn)

	rft.LogArray = append(rft.LogArray, temp)

	return temp, nil
}

//AddToChannel
func (rft *Raft) AddToChannel(entry LogEntry) {
	rft.Info.Println("Adding to commit", entry)
	rft.commitCh <- entry
}

func NewServerConfig(serverId int) (*ServerConfig, error) {
	server := new(ServerConfig)
	server.Id = serverId
	server.Hostname = "127.0.0.1"
	server.ClientPort = CLIENT_PORT + serverId
	server.LogPort = LOG_PORT + serverId
	return server, nil
}

func NewClusterConfig(num_servers int) (*ClusterConfig, error) {
	config := new(ClusterConfig)
	config.Path = ""
	config.Servers = make([]ServerConfig, num_servers)

	for i := 1; i <= num_servers; i++ {
		curr_server, _ := NewServerConfig(i)
		config.Servers[i-1] = *(curr_server)
	}

	return config, nil
}

func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(0)
}

//entry loop to raft
func (rft *Raft) loop() {
	state := FOLLOWER
	for {
		rft.Info.Println("hello")
		switch state {
		case FOLLOWER:
			state = follower()
			//		case CANDIDATE:
			//			state = candidate()
			//		case LEADER:
			//			state = leader()
		default:
			return
		}
	}
}

func getTimer() *time.Timer {
	return time.NewTimer(time.Millisecond * time.Duration((rand.Intn(MAX_TIMEOUT)+MIN_TIMEOUT)%MAX_TIMEOUT))
}

func (rft *Raft) follower() int {
	//start candidate timeout
	candTimer := getTimer()
	for {
		//wrap in select
		select {
		case <-candTimer.C:
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				// Do not handle clients in follower mode. Send it back up the
				// pipe with committed = false
				event.(*ClientAppend).logEntry.SetCommitted(false)
				rft.commitCh <- event.(*ClientAppend).logEntry

			case *VoteRequest:
				req := event.(*VoteRequest)
				if req.term < rft.currentTerm {
					//reply as - not accepted as leader
				}
				if req.term > rft.currentTerm {
					//update currentTerm
				}
				//condition for - if not voted in current term
			}
		}
	}
}
