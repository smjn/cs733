package raft

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
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
	LOG_PERSIST       = "log"
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
	logEntry *LogEntryData
}

type VoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteRequestReply struct {
	CurrentTerm int
	Reply       bool
}

type AppendReply struct {
	CurrentTerm int
	Reply       bool
	Fid         int
	LogLength   int
}

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
	Info          *log.Logger    //log for raft instance
	eventCh       chan RaftEvent //receive events related to various states
	votedFor      int
	currentTerm   int
	commitIndex   int
	voters        int
	shiftStatusCh chan int
	voteReplyCh   chan RaftEvent
	appendReplyCh chan RaftEvent
	et            *time.Timer
	isLeader      bool
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
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
		if err := file.Sync(); err != nil {
			rft.Info.Println("error synching log persist file", err.Error())
		} else {
			rft.Info.Println("log persist success!")
		}
	}
}

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

func getSingleDataFromFile(name string, serverId int, info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)

	if file, err := os.Open(filename); err != nil {
		defer file.Close()
		ioutil.WriteFile(filename, []byte("0"), 0666)
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

func writeFile(name string, serverId int, data int, info *log.Logger) int {
	filename := name + strconv.Itoa(serverId)
	if file, err := os.Open(filename); err != nil {
		defer file.Close()
		return FILE_ERR
	} else {
		ioutil.WriteFile(filename, []byte(strconv.Itoa(data)), 0666)
		//info.Println("wrote in " + filename + " file")
		return FILE_WRITTEN //file written
	}
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

//AddToCommitChannel
func (rft *Raft) AddToChannel(entry LogEntry) {
	rft.Info.Println("Adding to commit", entry)
	rft.commitCh <- entry
}

//AddToEventChannel
func (rft *Raft) AddToEventChannel(entry RaftEvent) {
	rft.Info.Println("Adding to event channel", entry)
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

func (rft *Raft) handleMajority(reply *VoteRequestReply) {
	majority := len(rft.clusterConfig.Servers) / 2
	rft.Info.Println("[C]: favorable vote")
	if reply.Reply {
		rft.voters++
		rft.Info.Println("[C]: count", rft.voters)
		if !rft.isLeader && rft.voters > majority {
			rft.shiftStatusCh <- LEADER
			rft.isLeader = true
		}
	} else {
		if rft.currentTerm < reply.CurrentTerm {
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

func doAppendReplyRPC(hostname string, logPort int, temp *AppendReply, Info *log.Logger) {
	Info.Println("append reply RPC")
	//rpc call to the caller
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	if err != nil {
		Info.Fatal("Dialing:", err)
	}
	reply := new(Reply)
	args := temp
	Info.Println("[F]: Calling AppendReply RPC", logPort)
	appendReplyCall := client.Go("RaftRPCService.AppendReplyRPC", args, reply, nil) //let go allocate done channel
	appendReplyCall = <-appendReplyCall.Done
	Info.Println("Reply", appendReplyCall, reply.X)
}

func doVoteRequestRPC(hostname string, logPort int, temp *VoteRequest, rft *Raft) {
	rft.Info.Println("[C]:Vote request RPC")
	//rpc call to the caller
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	if err != nil {
		rft.Info.Fatal("Dialing:", err)
	}
	reply := new(VoteRequestReply)
	args := temp
	//rft.Info.Println("Calling vote request RPC", logPort)
	voteReqCall := client.Go("RaftRPCService.VoteRequestRPC", args, reply, nil) //let go allocate done channel
	voteReqCall = <-voteReqCall.Done
	rft.handleMajority(reply)
}

//make append entries rpc call to followers
func doAppendRPCCall(hostname string, logPort int, temp *AppendRPC, rft *Raft) {
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	if err != nil {
		rft.Info.Fatal("[L]: Dialing:", err)
	}
	reply := new(AppendReply)
	args := temp
	rft.Info.Println("[L]: RPC Called", logPort)
	appendCall := client.Go("RaftRPCService.AppendRPC", args, reply, nil) //let go allocate done channel
	appendCall = <-appendCall.Done
	rft.handleAppendReply(reply)
}

func (rft *Raft) updateTermAndVote(term int) {
	writeFile(CURRENT_TERM, rft.id, term, rft.Info)
	rft.currentTerm = term
	writeFile(VOTED_FOR, rft.id, NULL_VOTE, rft.Info)
	rft.votedFor = NULL_VOTE
}

func (rft *Raft) follower() int {
	//start candidate timeout
	rft.et = time.NewTimer(getRandTime(rft.Info))
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
				rft.Info.Println("[F]: got vote request")
				req := event.(*VoteRequest)
				reply := false
				if req.Term < rft.currentTerm {
					reply = false
				}

				if req.Term > rft.currentTerm ||
					req.LastLogTerm > rft.currentTerm ||
					(req.LastLogTerm == rft.currentTerm && req.LastLogIndex >= len(rft.LogArray)) {
					rft.updateTermAndVote(req.Term)
					reply = true
				}

				if reply && rft.votedFor == NULL_VOTE {
					rft.et.Reset(getRandTime(rft.Info))
					rft.Info.Println("[F]: timer reset, after vote")
					writeFile(VOTED_FOR, rft.id, req.CandidateId, rft.Info)
					rft.Info.Println("[F]: voted for ", strconv.Itoa(req.CandidateId))
					rft.votedFor = req.CandidateId
				}
				//let the asker know about the vote
				voteReply := &VoteRequestReply{rft.currentTerm, reply}
				//server := rft.clusterConfig.Servers[req.CandidateId]
				//doCastVoteRPC(server.Hostname, server.LogPort, voteReply, rft.Info)
				rft.voteReplyCh <- voteReply

			case *AppendRPC:
				//rft.LogF("got append rpc")
				rft.et.Reset(getRandTime(rft.Info))
				//rft.LogF("reset timer on appendRPC")
				req := event.(*AppendRPC)
				if len(req.Entries) == 0 { //heartbeat
					rft.Info.Println("[F]: got hearbeat from " + strconv.Itoa(req.LeaderId))
					temp := &AppendReply{-1, true, -1, -1}
					rft.Info.Println("[F]: sending dummy reply to " + strconv.Itoa(req.LeaderId))
					rft.appendReplyCh <- temp
					continue
				}

				reply := true

				if req.PrevLogIndex == LOG_INVALID_INDEX || req.PrevLogIndex == LOG_INVALID_TERM {
					rft.updateTermAndVote(req.Term)
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
					//todo:also add to log

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
				//doAppendReplyRPC(rft.clusterConfig.Servers[req.LeaderId].Hostname, rft.clusterConfig.Servers[req.LeaderId].LogPort, temp, rft.Info)
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
	writeFile(CURRENT_TERM, rft.id, rft.currentTerm+1, rft.Info)
	rft.currentTerm++
	//vote for self
	rft.voters = 1
	writeFile(VOTED_FOR, rft.id, rft.id, rft.Info)
	rft.votedFor = rft.id
	//reset timer
	rft.et = time.NewTimer(getRandTime(rft.Info))
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

	//reinitialize rft.monitorVotesCh
	//rft.monitorVotesCh = make(chan RaftEvent)
	//killCh := make(chan bool)
	//go monitorVotesChannelRoutine(rft, killCh)
	//time.Sleep(time.Millisecond * 10)

	//send vote request to all servers
	for _, server := range rft.clusterConfig.Servers {
		//rft.Info.Println(server.Id)
		if server.Id != rft.id {
			//rft.Info.Println("[C]: Vote request to " + strconv.Itoa(server.Id))
			go doVoteRequestRPC(server.Hostname, server.LogPort, req, rft)
			//rft.handleMajority(reply)
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
				rft.et.Reset(getRandTime(rft.Info))
				return FOLLOWER
			}
		}
	}
}

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
				if req.PrevLogIndex <= 0 {
					req.PrevLogTerm = LOG_INVALID_TERM
				} else {
					req.PrevLogTerm = rft.LogArray[rft.nextIndex[server.Id]-1].Term
				}

				//appendRPC call
				doAppendRPCCall(server.Hostname, server.LogPort, req, rft)
				rft.Info.Println("[L]: Sent append entries", strconv.Itoa(server.Id))
			}
			time.Sleep(time.Millisecond * 2)
		}
	}
}

func (rft *Raft) leader() int {
	rft.Info.Println("[L]: became leader")
	heartbeat := time.NewTimer(time.Millisecond * HEARTBEAT_TIMEOUT)
	heartbeatReq := new(AppendRPC)
	heartbeatReq.Entries = []*LogEntryData{}
	heartbeatReq.LeaderId = rft.id
	rft.currentTerm++

	rft.LogArray = append(
		rft.LogArray,
		&LogEntryData{
			Id:        1,
			Data:      []byte("hello"),
			Committed: false,
			Term:      rft.currentTerm,
		},
		&LogEntryData{
			Id:        2,
			Data:      []byte("world"),
			Committed: false,
			Term:      rft.currentTerm,
		})

	newEntry := &LogEntryData{
		Id:        3,
		Data:      []byte("goodbye"),
		Committed: false,
		Term:      rft.currentTerm,
	}

	//build nextIndex and matchIndex
	for i := 0; i < len(rft.nextIndex); i++ {
		rft.nextIndex[i] = 0
		rft.matchIndex[i] = 0
	}

	go enforceLog(rft)
	go func() {
		time.Sleep(time.Second * 2)
		rft.LogArray = append(rft.LogArray, newEntry)
	}()

	for {
		select {
		case <-heartbeat.C:
			for _, server := range rft.clusterConfig.Servers {
				if server.Id != rft.id {
					//doRPCCall for hearbeat
					go doAppendRPCCall(server.Hostname, server.LogPort, heartbeatReq, rft)
					rft.Info.Println("[L]: Sent heartbeat", strconv.Itoa(server.Id))
				}
			}
			heartbeat.Reset(time.Millisecond * HEARTBEAT_TIMEOUT)

		case event := <-rft.eventCh:
			switch event.(type) {
			case *ClientAppend:
				//write data to log
				rft.Info.Println("[L]: got client data")
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

func StartRaft(rft *Raft) {
	rft.loop()
}
