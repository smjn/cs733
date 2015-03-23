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
	CLIENT_PORT       = 9000
	LOG_PORT          = 20000
	ACK_TIMEOUT       = 5
	MIN_TIMEOUT_ELEC  = 300
	MAX_TIMEOUT_ELEC  = 500
	HEARTBEAT_TIMEOUT = 100
	LEADER            = 10
	CANDIDATE         = 20
	FOLLOWER          = 30
	VOTED_FOR         = "votedFor"
	CURRENT_TERM      = "currentTerm"
	FILE_WRITTEN      = 0
	FILE_ERR          = -1
	NULL_VOTE         = 0
	LOG_INVALID_INDEX = -1
	LOG_INVALID_TERM  = -1
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
	logEntry *LogEntry
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
	Info           *log.Logger    //log for raft instance
	eventCh        chan RaftEvent //receive events related to various states
	votedFor       int
	currentTerm    int
	commitIndex    int
	voters         int
	monitorVotesCh chan bool
	et             *time.Timer
	isLeader       bool
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
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
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry, eventCh chan RaftEvent, monitorVotesCh chan bool, toDebug bool) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.clusterConfig = config
	rft.id = thisServerId
	rft.eventCh = eventCh
	rft.Info = getLogger(thisServerId, toDebug)
	if v := getSingleDataFromFile(CURRENT_TERM, thisServerId, rft.Info); v != FILE_ERR {
		rft.currentTerm = v
	} else {
		rft.currentTerm = 0
	}
	rft.monitorVotesCh = monitorVotesCh
	getSingleDataFromFile(VOTED_FOR, thisServerId, rft.Info) //initialize the votedFor file.
	rft.isLeader = false
	rft.nextIndex = make([]int, len(config.Servers))
	rft.matchIndex = make([]int, len(config.Servers))
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

func getRandTime(log *log.Logger) time.Duration {
	rand.Seed(time.Now().UnixNano())
	t := time.Millisecond * time.Duration(rand.Intn(MAX_TIMEOUT_ELEC-MIN_TIMEOUT_ELEC)+MIN_TIMEOUT_ELEC)
	log.Println("New rand time", t)
	return t
}

func (rft *Raft) grantVote(reply bool, currentTerm int) {
	if reply {
		rft.voters++
		rft.Info.Println(rft.id, "got vote")
		if !rft.isLeader && rft.voters > len(rafts)/2 {
			rft.LogC("got majority")
			rft.monitorVotesCh <- true
			rft.isLeader = true
		}
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

func (rft *Raft) LogF(msg string) {
	rft.Info.Println("F:", rft.id, msg)
}

func (rft *Raft) LogC(msg string) {
	rft.Info.Println("C:", rft.id, msg)
}

func (rft *Raft) LogL(msg string) {
	rft.Info.Println("L:", rft.id, msg)
}

func (rft *Raft) follower() int {
	//start candidate timeout
	rft.et = time.NewTimer(getRandTime(rft.Info))
	for {
		//wrap in select
		select {
		case <-rft.et.C:
			rft.LogF("election timeout")
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				rft.LogF("got client append")
				//Do not handle clients in follower mode.
				//Send it back up the pipeline.
				event.(*ClientAppend).logEntry.SetCommitted(false)
				rft.eventCh <- event.(*ClientAppend).logEntry

			case *VoteRequest:
				rft.LogF("got vote request")
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
					rft.et.Reset(getRandTime(rft.Info))
					rft.LogF("reset timer after voting")
					writeFile(VOTED_FOR, rft.id, req.candidateId, rft.Info)
					rft.LogF("voted for " + strconv.Itoa(req.candidateId))
					rft.votedFor = req.candidateId
				}
				//let leader know about the vote
				rafts[req.candidateId].grantVote(reply, rft.currentTerm)

			case *AppendRPC:
				rft.LogF("got append rpc")
				rft.et.Reset(getRandTime(rft.Info))
				rft.LogF("reset timer on appendRPC")
				req := event.(*AppendRPC)
				if len(req.entries) == 0 { //heartbeat
					rft.LogF("got hearbeat from " + strconv.Itoa(req.leaderId))
					continue
				}

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
				rft.LogF("AppendRPC")
				rafts[req.leaderId].replyAppendRPC(reply, rft.currentTerm)
			}
		}
	}
}

func (rft *Raft) candidate() int {
	//increment current term
	rft.LogC("became candidate")
	writeFile(CURRENT_TERM, rft.id, rft.currentTerm+1, rft.Info)
	rft.currentTerm++
	//vote for self
	rft.voters = 1
	writeFile(VOTED_FOR, rft.id, rft.id, rft.Info)
	rft.votedFor = rft.id
	//reset timer
	rft.et = time.NewTimer(getRandTime(rft.Info))
	rft.Info.Println(rft.id, "candidate got new timer")
	//create a vote request object
	req := &VoteRequest{
		term:        rft.currentTerm,
		candidateId: rft.id,
	}
	if len(rft.LogArray) == 0 {
		req.lastLogIndex = LOG_INVALID_INDEX
		req.lastLogTerm = LOG_INVALID_TERM
	} else {
		req.lastLogIndex = len(rft.LogArray) - 1
		req.lastLogTerm = rft.LogArray[req.lastLogIndex].Term
	}

	//send vote request to all servers
	for i := 1; i <= len(rafts); i++ {
		if i != rft.id {
			rft.LogC("sent vote request to " + strconv.Itoa(rafts[i].id))
			rafts[i].eventCh <- req
		}
	}

	for {
		select {
		case <-rft.monitorVotesCh:
			rft.LogC("C to L")
			return LEADER
		case <-rft.et.C:
			rft.LogC("C to C")
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case (*AppendRPC):
				rft.LogC("C to F")
				rft.et.Reset(getRandTime(rft.Info))
				return FOLLOWER
			}
		}
	}
}

func (rft *Raft) leader() int {
	rft.LogL("became leader")
	heartbeat := time.NewTimer(time.Millisecond * HEARTBEAT_TIMEOUT)
	heartbeatReq := new(AppendRPC)
	heartbeatReq.entries = []*LogEntryData{}
	heartbeatReq.leaderId = rft.id
	//reintialize nextIndex and matchIndex
	for i := 0; i < len(rft.nextIndex); i++ {
		rft.nextIndex[i] = len(rft.LogArray)
		rft.matchIndex[i] = 0
	}

	for {
		select {
		case <-heartbeat.C:
			for k, v := range rafts {
				if k != rft.id {
					rft.LogL("sending AppendRPC " + strconv.Itoa(k))
					v.eventCh <- heartbeatReq
				}
			}
			heartbeat.Reset(time.Millisecond * HEARTBEAT_TIMEOUT)

		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				//write data to log
				rft.LogL("got client data")
				entry := event.(*ClientAppend).logEntry
				rft.LogArray = append(rft.LogArray, entry)
				//todo:apply to state machine
				//todo:respond to client

			case *AppendRPC:

			case *VoteRequest:

			}
		}
	}
}
