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
	CLIENT_PORT  = 9000
	LOG_PORT     = 20000
	ACK_TIMEOUT  = 5
	MIN_TIMEOUT  = 300
	MAX_TIMEOUT  = 500
	LEADER       = 10
	CANDIDATE    = 20
	FOLLOWER     = 30
	VOTED_FOR    = "votedFor"
	CURRENT_TERM = "currentTerm"
	FILE_WRITTEN = 0
	FILE_ERR     = -1
	NULL_VOTE    = 0
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
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	leaderCommit int
	entries      []*LogEntryData
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
	commitIndex int
	voters      int
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
	Term      int      //term number
	conn      net.Conn // Connection for communicating with client
}

func (rft *Raft) persistLog() {

}

func (rft *Raft) readLogFromDisk() {

}

func getSingleDataFromFile(name string, serverId int, info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)

	if file, err := os.Open(filename); err != nil {
		defer file.Close()
		ioutil.WriteFile(filename, []byte("0"), 0666)
		info.Println("wrote in " + filename + " file")
		return 0
	} else {
		if data, err := ioutil.ReadFile(file.Name()); err != nil {
			info.Println("error reading file " + filename)
			return FILE_ERR
		} else {
			info.Println("read from file " + filename)
			if t, err2 := strconv.Atoi(string(data)); err2 != nil {
				info.Println("error converting")
				return FILE_ERR
			} else {
				info.Println("Converted success "+filename, t)
				return t
			}
		}
	}
}

func writeFile(name string, serverId int, data int, info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)
	if file, err := os.Open(filename); err != nil {
		defer file.Close()
		return FILE_ERR
	} else {
		ioutil.WriteFile(filename, []byte(strconv.Itoa(data)), 0666)
		info.Println("wrote in " + filename + " file")
		return FILE_WRITTEN //file written
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
	rft.currentTerm = getSingleDataFromFile(CURRENT_TERM, thisServerId, rft.Info)
	getSingleDataFromFile(VOTED_FOR, thisServerId, rft.Info) //initialize the votedFor file.
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
			state = rft.follower()
		case CANDIDATE:
			state = rft.candidate()
		case LEADER:
			state = rft.leader()
		}
	}
}

func getTimer() *time.Timer {
	rand.Seed(time.Now().UnixNano())
	return time.NewTimer(time.Millisecond * time.Duration((rand.Intn(MAX_TIMEOUT)+MIN_TIMEOUT)%MAX_TIMEOUT))
}

func reInitializeTimer(t *time.Timer) *time.Timer {
	t.Stop()
	return getTimer()
}

func (rft *Raft) grantVote(reply bool, currentTerm int) {
	if reply {
		rft.voters++
	}
}

func (rft *Raft) replyAppendRPC(reply bool, currentTerm int) {
	//
}

func (rft *Raft) updateTermAndVote(term int) {
	writeFile(CURRENT_TERM, rft.id, term, rft.Info)
	rft.currentTerm = term
	writeFile(VOTED_FOR, rft.id, NULL_VOTE, rft.Info)
	rft.votedFor = NULL_VOTE
}

func (rft *Raft) follower() int {
	//start candidate timeout
	electionTimeout := getTimer()
	for {
		rft.Info.Println("xyz")
		//wrap in select
		select {
		case <-electionTimeout.C:
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				//Do not handle clients in follower mode.
				//Send it back up the pipeline.
				event.(*ClientAppend).logEntry.SetCommitted(false)
				rft.eventCh <- event.(*ClientAppend).logEntry

			case *VoteRequest:
				req := event.(*VoteRequest)
				reply := false
				if req.term < rft.currentTerm {
					reply = false
				}

				if req.term > rft.currentTerm ||
					req.lastLogTerm > rft.currentTerm ||
					(req.lastLogTerm == rft.currentTerm && req.lastLogIndex >= len(rft.LogArray)) {
					rft.updateTermAndVote(req.term)
					reply = true
				}

				if reply && rft.votedFor == NULL_VOTE {
					electionTimeout = reInitializeTimer(electionTimeout)
					writeFile(VOTED_FOR, rft.id, req.candidateId, rft.Info)
					rft.votedFor = req.candidateId
					rafts[req.candidateId].grantVote(reply, rft.currentTerm)
				}

			case *AppendRPC:
				electionTimeout = reInitializeTimer(electionTimeout)
				req := event.(*AppendRPC)
				reply := true
				if req.term < rft.currentTerm {
					reply = false
				}

				if req.term > rft.currentTerm {
					rft.updateTermAndVote(req.term)
					reply = true
				}

				//first condition to prevent out of bounds except
				if len(rft.LogArray) < req.prevLogIndex || rft.LogArray[req.prevLogIndex].Term != req.prevLogTerm {
					reply = false
				}

				if reply {
					i := req.prevLogIndex + 1
					for ; i < len(rft.LogArray); i++ {
						if req.entries[i-req.prevLogIndex-1].Term != rft.LogArray[i].Term {
							break
						}
					}
					rft.LogArray = append(rft.LogArray[0:i], req.entries[i-req.prevLogIndex-1:]...)
					//todo:also add to log

					if req.leaderCommit > rft.commitIndex {
						if req.leaderCommit > len(rft.LogArray)-1 {
							rft.commitIndex = len(rft.LogArray) - 1
						} else {
							rft.commitIndex = req.leaderCommit
						}
					}
				}
				rafts[req.leaderId].replyAppendRPC(reply, rft.currentTerm)
			}
		}
	}
}

func (rft *Raft) candidate() int {
	return 1
}

func (rft *Raft) leader() int {
	return 1
}
