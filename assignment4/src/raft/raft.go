package raft

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

//constant values used
const (
	CLIENT_PORT       = 9000
	LOG_PORT          = 20000
	MIN_TIMEOUT_ELEC  = 300
	MAX_TIMEOUT_ELEC  = 500
	HEARTBEAT_TIMEOUT = 100
	LEADER            = 10
	CANDIDATE         = 20
	FOLLOWER          = 30
	VOTED_FOR         = "votedFor"
	CURRENT_TERM      = "currentTerm"
	LOG_PERSIST       = "log"
	FILE_WRITTEN      = 0
	FILE_ERR          = -1
	NULL_VOTE         = -1
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

//structure used to identify a request from client
type ClientAppend struct {
	logEntry *LogEntryData
}

//structure used to identify a request from candidate for votes
type VoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//structure used to identify a reply from votes RPC
type VoteRequestReply struct {
	CurrentTerm int
	Reply       bool
}

//structure used to identify a reply from Append RPC
type AppendReply struct {
	CurrentTerm int
	Reply       bool
	Fid         int
	LogLength   int
}

//structure used to identify a req to perform Append Entries
type AppendRPC struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntryData
}

// Structure used for replying to the RPC calls
type Reply struct {
	X int
}

//interface used to encapsulate all structures composing the RPCs
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
	Info          *log.Logger    //log for raft instance
	eventCh       chan RaftEvent //receive events related to various states
	votedFor      int            //which server have you voted for
	currentTerm   int
	commitIndex   int
	voters        int            //number of servers who voted for you
	shiftStatusCh chan int       //used for switching candidate to follower or leader
	voteReplyCh   chan RaftEvent //used to receive votes from followers
	appendReplyCh chan RaftEvent //used to receive the reply fot append entries sent
	et            *time.Timer    //election timeout
	IsLeader      bool
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	LeaderId      int //so everyone knows who the leader is and can reply to the client
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

// Structure for calling commit RPC
type CommitData struct {
	Id Lsn
}

//persists log to the disk for later retrieval
func (rft *Raft) persistLog() {
	if file, err := os.OpenFile(LOG_PERSIST+strconv.Itoa(rft.id), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666); err != nil {
		rft.Info.Println("error opening log persist file", err.Error())
	} else {
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, e := range rft.LogArray {
			if err := enc.Encode(e); err != nil {
				rft.Info.Println("error persisting log entry", err.Error())
			}
		}
	}
}

//used for reading the entire log from disk if it exists
func (rft *Raft) readLogFromDisk() {
	rft.LogArray = []*LogEntryData{}
	if file, err := os.OpenFile(LOG_PERSIST+strconv.Itoa(rft.id), os.O_RDONLY, 0666); err != nil {
		rft.Info.Println("error reading log persist file")
	} else {
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			d := LogEntryData{}
			if err := dec.Decode(d); err != nil {
				rft.Info.Println("done reading log from file")
				break
			} else {
				rft.LogArray = append(rft.LogArray, &d)
			}
		}
	}
}

//for reading votedfor and currentterm from disk
func getSingleDataFromFile(name string, serverId int, info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)

	if file, err := os.Open(filename); err != nil {
		defer file.Close()
		ioutil.WriteFile(filename, []byte("-1"), 0666)
		//file.Sync()
		//info.Println("wrote in " + filename + " file")
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
				//info.Println("Converted success "+filename, t)
				return t
			}
		}
	}
}

//write data to some file, used to currentterm and votedfor
func writeFile(name string, serverId int, data int, Info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)
	ioutil.WriteFile(filename, []byte(strconv.Itoa(data)), 0666)
	Info.Println("wrote in "+filename, data)
	return FILE_WRITTEN //file written
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry, Info *log.Logger) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.clusterConfig = config
	rft.id = thisServerId
	rft.Info = Info
	if v := getSingleDataFromFile(CURRENT_TERM, thisServerId, rft.Info); v != FILE_ERR {
		rft.currentTerm = v
	} else {
		rft.currentTerm = 0
	}

	rft.eventCh = make(chan RaftEvent)
	rft.shiftStatusCh = make(chan int)
	rft.appendReplyCh = make(chan RaftEvent)
	rft.voteReplyCh = make(chan RaftEvent)
	getSingleDataFromFile(VOTED_FOR, thisServerId, rft.Info) //initialize the votedFor file.
	rft.IsLeader = false
	rft.nextIndex = make([]int, len(config.Servers))
	rft.matchIndex = make([]int, len(config.Servers))
	rft.commitIndex = -1
	rft.votedFor = -1
	rft.lastApplied = -1
	return rft, nil
}

// Creates a log entry. This implements the LogEntry interface
// data: data bytes, committed: commit status, conn: connection to client
// Returns the log entry
func (rft *Raft) NewLogEntry(data []byte, committed bool, conn net.Conn) *LogEntryData {
	entry := new(LogEntryData)
	entry.Id = lsn
	entry.Data = data
	entry.conn = conn
	entry.Committed = committed
	entry.Term = rft.currentTerm
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
	if !rft.IsLeader {
		return nil, ErrRedirect(rft.LeaderId)
	}
	temp := rft.NewLogEntry(data, false, conn)
	rft.AddToEventChannel(&ClientAppend{temp})
	return temp, nil
}

//AddToCommitChannel
func (rft *Raft) AddToChannel(entry LogEntry) {
	rft.Info.Println("Adding to commit", entry)
	rft.commitCh <- entry
}

//AddToEventChannel
func (rft *Raft) AddToEventChannel(entry RaftEvent) {
	rft.Info.Println("Adding to event channel", entry, reflect.TypeOf(entry))
	rft.eventCh <- entry
}

//to be executed by follower
func (rft *Raft) FetchVoteReply() RaftEvent {
	//follower puts the reply here after computations
	//we need to send this as reply back to candidate
	temp := <-rft.voteReplyCh
	return temp
}

//to be executed by follower
func (rft *Raft) FetchAppendReply() RaftEvent {
	//follower puts the reply here after computations
	//we need to send this as reply back to candidate
	temp := <-rft.appendReplyCh
	return temp
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

	for i := 0; i < num_servers; i++ {
		curr_server, _ := NewServerConfig(i)
		config.Servers[i] = *(curr_server)
	}

	return config, nil
}

func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(0)
}

//entry loop to raft
func (rft *Raft) Loop() {
	//start moninoring the commitIndex so that required entries are sent to the followers
	go rft.MonitorStateMachine()
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

//returns a new integer which is used to reset the timer
func getRandTime(log *log.Logger) int {
	rand.Seed(time.Now().UnixNano())
	t := rand.Intn(MAX_TIMEOUT_ELEC-MIN_TIMEOUT_ELEC) + MIN_TIMEOUT_ELEC
	log.Println("New rand time", t)
	return t
}

//check for majority for a candidate.
//if someone replies with higher currentterm, revert to follower status
func (rft *Raft) handleMajority(reply *VoteRequestReply) {
	majority := len(rft.clusterConfig.Servers) / 2
	rft.Info.Println("[C]: vote received", reply)
	if reply.Reply {
		rft.voters++
		rft.Info.Println("[C]: count", rft.voters)
		if !rft.IsLeader && rft.voters > majority {
			rft.shiftStatusCh <- LEADER
			rft.IsLeader = true
		}
	} else {
		if rft.currentTerm < reply.CurrentTerm {
			rft.Info.Println("Candidate to follower", rft.voters)
			rft.updateTermAndVote(reply.CurrentTerm)
			rft.shiftStatusCh <- FOLLOWER
		}
	}
}

//leader will call this
func (rft *Raft) handleAppendReply(temp *AppendReply) {
	//this is reply to heartbeat, ignore it
	if temp.Fid == -1 {
		return
	}
	if temp.Reply {
		rft.nextIndex[temp.Fid] = temp.LogLength
		rft.matchIndex[temp.Fid] = temp.LogLength
		//update commitindex
		for n := rft.commitIndex + 1; n < len(rft.LogArray); n++ {
			maj := 0
			for _, server := range rft.clusterConfig.Servers {
				if rft.matchIndex[server.Id] >= n {
					maj++
				}
			}
			if maj > len(rft.clusterConfig.Servers)/2 && rft.LogArray[n].Term == rft.currentTerm {
				rft.commitIndex = n
			}
		}
	} else {
		rft.nextIndex[temp.Fid]--
	}
}

func doVoteRequestRPC(hostname string, logPort int, temp *VoteRequest, rft *Raft) {
	rft.Info.Println("[C]:Vote request RPC")
	//rpc call to the caller
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	defer client.Close()
	if err != nil {
		rft.Info.Println("Dialing:", err, "returning")
		return
	}
	reply := new(VoteRequestReply)
	args := temp
	rft.Info.Println("Calling vote request RPC", logPort)
	voteReqCall := client.Go("RaftRPCService.VoteRequestRPC", args, reply, nil) //let go allocate done channel
	voteReqCall = <-voteReqCall.Done
	rft.handleMajority(reply)
}

//make append entries rpc call to followers
func doAppendRPCCall(hostname string, logPort int, temp *AppendRPC, rft *Raft) {
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	defer client.Close()
	if err != nil {
		rft.Info.Println("[L]: Dialing:", err, "returning")
		return
	}
	reply := new(AppendReply)
	args := temp
	rft.Info.Println("[L]: RPC Called", logPort)
	appendCall := client.Go("RaftRPCService.AppendRPC", args, reply, nil) //let go allocate done channel
	appendCall = <-appendCall.Done
	rft.handleAppendReply(reply)
}

//set currentterm to latest value and reinitialize votedfor
func (rft *Raft) updateTermAndVote(term int) {
	rft.currentTerm = term
	writeFile(CURRENT_TERM, rft.id, term, rft.Info)
	rft.votedFor = NULL_VOTE
	writeFile(VOTED_FOR, rft.id, NULL_VOTE, rft.Info)
}

func (rft *Raft) follower() int {
	//start candidate timeout
	rft.et = time.NewTimer(time.Millisecond * time.Duration(getRandTime(rft.Info)))
	SetIsLeader(false)
	for {
		//wrap in select
		select {
		case <-rft.et.C:
			rft.Info.Println("[F]: election timeout")
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				rft.Info.Println("[F]: got client append")
				//Do not handle clients in follower mode.
				//Send it back up the pipeline.
				event.(*ClientAppend).logEntry.SetCommitted(false)
				rft.eventCh <- event.(*ClientAppend).logEntry

			case *VoteRequest:
				req := event.(*VoteRequest)
				rft.Info.Println("[F]: got vote request", req)
				reply := false
				if req.Term < rft.currentTerm {
					rft.Info.Println("[F]: req.Term < rft.currentTerm")
					reply = false
				}

				if req.Term > rft.currentTerm ||
					req.LastLogTerm > rft.currentTerm ||
					(req.LastLogTerm == rft.currentTerm && req.LastLogIndex >= len(rft.LogArray)) {
					rft.Info.Println("[F]: updating term and vote", req.Term, NULL_VOTE)
					rft.updateTermAndVote(req.Term)
					reply = true
				}

				if reply && rft.votedFor == NULL_VOTE {
					rft.et.Reset(time.Millisecond * 300)
					rft.Info.Println("[F]: timer reset, after vote")
					rft.Info.Println("[F]: voted for ", req.CandidateId)
					rft.votedFor = req.CandidateId
					writeFile(VOTED_FOR, rft.id, req.CandidateId, rft.Info)
				}
				//let the asker know about the vote
				voteReply := &VoteRequestReply{rft.currentTerm, reply}
				rft.voteReplyCh <- voteReply

			case *AppendRPC:
				rft.et.Reset(time.Millisecond * time.Duration(getRandTime(rft.Info)))
				rft.Info.Println("[F]:", "Timer reset on AppendRPC")
				req := event.(*AppendRPC)
				if len(req.Entries) == 0 { //heartbeat
					rft.Info.Println("[F]: got hearbeat from " + strconv.Itoa(req.LeaderId))
					temp := &AppendReply{-1, true, -1, -1}
					rft.Info.Println("[F]: sending dummy reply to " + strconv.Itoa(req.LeaderId))
					rft.appendReplyCh <- temp
					rft.LeaderId = req.LeaderId
					continue
				}

				reply := true

				if req.PrevLogIndex == LOG_INVALID_INDEX || req.PrevLogIndex == LOG_INVALID_TERM {
					//rft.updateTermAndVote(req.Term)
					reply = true
				} else if req.Term < rft.currentTerm {
					reply = false
				} else if req.Term > rft.currentTerm {
					rft.updateTermAndVote(req.Term)
					reply = true
				}

				//first condition to prevent out of bounds except
				if !(req.PrevLogIndex == LOG_INVALID_INDEX) && rft.LogArray[req.PrevLogIndex].Term != req.PrevLogTerm {
					rft.Info.Println("[F]: terms unequal")
					reply = false
				}

				if reply {
					i := req.PrevLogIndex + 1
					for ; i < len(rft.LogArray); i++ {
						if req.PrevLogIndex == LOG_INVALID_INDEX || req.Entries[i-req.PrevLogIndex-1].Term != rft.LogArray[i].Term {
							break
						}
					}

					if req.PrevLogIndex == LOG_INVALID_INDEX {
						rft.LogArray = append(rft.LogArray, req.Entries...)
					} else {
						rft.LogArray = append(rft.LogArray[0:i], req.Entries[i-req.PrevLogIndex-1:]...)
					}

					if req.LeaderCommit > rft.commitIndex {
						if req.LeaderCommit > len(rft.LogArray)-1 {
							rft.commitIndex = len(rft.LogArray) - 1
						} else {
							rft.commitIndex = req.LeaderCommit
						}
					}
				}

				temp := &AppendReply{rft.currentTerm, reply, rft.id, len(rft.LogArray)}
				rft.appendReplyCh <- temp
				if reply {
					rft.persistLog()
				}
				rft.Info.Println("[F]: log is size", len(rft.LogArray))
			}
		}
	}
}

func (rft *Raft) candidate() int {
	//increment current term
	rft.Info.Println("[C]: became candidate")
	SetIsLeader(false)
	rft.currentTerm++
	writeFile(CURRENT_TERM, rft.id, rft.currentTerm, rft.Info)

	//vote for self
	rft.voters = 1
	rft.votedFor = rft.id
	writeFile(VOTED_FOR, rft.id, rft.id, rft.Info)

	//reset timer
	rft.et.Reset(time.Millisecond * time.Duration(getRandTime(rft.Info)))
	rft.Info.Println("[C]:", rft.id, "candidate got new timer")
	//create a vote request object
	req := &VoteRequest{
		Term:        rft.currentTerm,
		CandidateId: rft.id,
	}
	if len(rft.LogArray) == 0 {
		req.LastLogIndex = LOG_INVALID_INDEX
		req.LastLogTerm = LOG_INVALID_TERM
	} else {
		req.LastLogIndex = len(rft.LogArray) - 1
		req.LastLogTerm = rft.LogArray[req.LastLogIndex].Term
	}

	//send vote request to all servers
	rft.Info.Println("[C]:", "asking for votes from all servers")
	for _, server := range rft.clusterConfig.Servers {
		if server.Id != rft.id {
			go doVoteRequestRPC(server.Hostname, server.LogPort, req, rft)
		}
	}

	for {
		select {
		case status := <-rft.shiftStatusCh:
			if status == LEADER {
				rft.Info.Println("[Switch]: C to L")
				return LEADER
			} else {
				rft.Info.Println("[Switch]: C to F")
				return FOLLOWER
			}
		case <-rft.et.C:
			rft.Info.Println("[Switch]: C to C")
			return CANDIDATE
		case event := <-rft.eventCh:
			switch event.(type) {
			case (*AppendRPC):
				rft.Info.Println("[Switch]: C to F")
				rft.et.Reset(time.Millisecond * time.Duration(getRandTime(rft.Info)))
				return FOLLOWER
			}
		}
	}
}

//enforce the leaders log on all followers
func enforceLog(rft *Raft) {
	for {
		for _, server := range rft.clusterConfig.Servers {
			if rft.id != server.Id && len(rft.LogArray)-1 >= rft.nextIndex[server.Id] {
				req := &AppendRPC{}
				req.Term = rft.currentTerm
				req.LeaderId = rft.id
				req.LeaderCommit = rft.commitIndex
				req.Entries = rft.LogArray[rft.nextIndex[server.Id]:len(rft.LogArray)]
				req.PrevLogIndex = rft.nextIndex[server.Id] - 1
				if req.PrevLogIndex == -1 {
					req.PrevLogTerm = LOG_INVALID_TERM
				} else {
					req.PrevLogTerm = rft.LogArray[rft.nextIndex[server.Id]-1].Term
				}

				//appendRPC call
				//rft.Info.Println("[L]: XX append rpc enforce", req)
				doAppendRPCCall(server.Hostname, server.LogPort, req, rft)
				//rft.Info.Println("[L]: Sent append entries", strconv.Itoa(server.Id))
			}
		}
		time.Sleep(time.Millisecond)
	}
}

//simple function to execute send heartbeats to all servers
func sendHeartbeats(rft *Raft, heartbeatReq *AppendRPC) {
	for _, server := range rft.clusterConfig.Servers {
		if server.Id != rft.id {
			//doRPCCall for hearbeat
			go doAppendRPCCall(server.Hostname, server.LogPort, heartbeatReq, rft)
			//rft.Info.Println("[L]: Sent heartbeat", strconv.Itoa(server.Id))
		}
	}
}

func (rft *Raft) leader() int {
	rft.Info.Println("[L]: became leader")
	//update kvstore
	SetIsLeader(true)
	heartbeat := time.NewTimer(time.Millisecond * HEARTBEAT_TIMEOUT)
	heartbeatReq := new(AppendRPC)
	heartbeatReq.Entries = []*LogEntryData{}
	heartbeatReq.LeaderId = rft.id

	//build nextIndex and matchIndex
	for i := 0; i < len(rft.nextIndex); i++ {
		rft.nextIndex[i] = 0
		rft.matchIndex[i] = 0
	}

	//send first heartbeat
	sendHeartbeats(rft, heartbeatReq)
	go enforceLog(rft)

	for {
		select {
		case <-heartbeat.C:
			sendHeartbeats(rft, heartbeatReq)
			heartbeat.Reset(time.Millisecond * HEARTBEAT_TIMEOUT)

		case event := <-rft.eventCh:
			rft.Info.Println("[L]: got event", event, reflect.TypeOf(event))
			switch event.(type) {
			case *ClientAppend:
				//write data to log
				rft.Info.Println("[L]: got client data")
				entry := event.(*ClientAppend).logEntry
				rft.LogArray = append(rft.LogArray, entry)
				//will now be send to kvstore which'll decode and reply
				rft.persistLog()

			case *AppendRPC:

			case *VoteRequest:

			}
		}
	}
}

//run as go routine to monitor the commit index and execute on kvstore
func (rft *Raft) MonitorStateMachine() {
	rft.Info.Println("MonitorStateMachine initialized")
	for {
		if rft.commitIndex > rft.lastApplied {
			rft.lastApplied++
			rft.commitCh <- rft.LogArray[rft.lastApplied]
		}
		//to avoid cpu intensive go routine
		time.Sleep(time.Millisecond)
	}
}
