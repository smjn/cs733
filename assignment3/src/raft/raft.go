package raft

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
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
	CANDIDATE   = iota
	FOLLOWER    = iota
)

// Logger
var Info *log.Logger

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

type ClientAppend struct{
	
}

type VoteRequest struct{
	
}

type AppendRPC struct{
	
}

type Timeout struct{
	
}

type RaftEvent interface{
	
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
	eventCh chan RaftEvent	//receive events related to various states
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

// Structure for calling commit RPC
type CommitData struct {
	Id Lsn
}

// Structure used for replying to the RPC calls
type Reply struct {
	X int
}

// Structure for registering RPC methods
type AppendEntries struct{}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry, eventCh, chan RaftEvent, logger *log.Logger) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.clusterConfig = config
	rft.id = thisServerId
	Info = logger
	lsn = 0
	rft.eventCh = eventCh
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

// Goroutine that monitors channel to check if the majority of servers have replied
func monitorAckChannel(rft *Raft, ack_ch <-chan int, log_entry LogEntry, majCh chan bool) {
	acks_received := 0
	num_servers := len(rft.clusterConfig.Servers)
	required_acks := num_servers / 2
	up := make(chan bool, 1)
	err := false

	go func() {
		time.Sleep(ACK_TIMEOUT * time.Second)
		up <- true
	}()

	for {
		select {
		case temp := <-ack_ch:
			Info.Println("Ack Received:", temp)
			acks_received += temp
			if acks_received == required_acks {
				Info.Println("Majority Achieved", log_entry.(*LogEntryData).Id)
				rft.LogArray[log_entry.(*LogEntryData).Id].Committed = true
				//Info.Println(rft.LogArray)
				rft.commitCh <- log_entry

				temp := new(CommitData)
				temp.Id = log_entry.(*LogEntryData).Id
				for _, server := range rft.clusterConfig.Servers[1:] {
					go doCommitRPCCall(server.Hostname, server.LogPort, temp)
				}

				majCh <- true
				err = true
				break
			}

		case <-up:
			Info.Println("Error")
			err = true
			break
		}
		if err {
			break
		}
	}
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

// Call CommitRPC to inform the followers of newly committed log entry
func doCommitRPCCall(hostname string, logPort int, temp *CommitData) {
	Info.Println("Commit RPC")
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	if err != nil {
		Info.Fatal("Dialing:", err)
	}
	reply := new(Reply)
	args := temp
	Info.Println("Calling Commit RPC", logPort)
	commitCall := client.Go("AppendEntries.CommitRPC", args, reply, nil) //let go allocate done channel
	commitCall = <-commitCall.Done
	Info.Println("Reply", commitCall, reply.X)
}

//make rpc call to followers
func doRPCCall(ackChan chan int, hostname string, logPort int, temp *LogEntryData) {
	client, err := rpc.Dial("tcp", hostname+":"+strconv.Itoa(logPort))
	if err != nil {
		Info.Fatal("Dialing:", err)
	}
	reply := new(Reply)
	args := temp
	Info.Println("RPC Called", logPort)
	appendCall := client.Go("AppendEntries.AppendEntriesRPC", args, reply, nil) //let go allocate done channel
	appendCall = <-appendCall.Done
	Info.Println("Reply", appendCall, reply.X)
	ackChan <- reply.X
}

//make raft implement the append function
func (rft *Raft) Append(data []byte, conn net.Conn) (LogEntry, error) {
	Info.Println("Append Called")
	if rft.id != 1 {
		return nil, ErrRedirect(1)
	}
	defer rft.Unlock()
	rft.Lock()
	temp := NewLogEntry(data, false, conn)

	rft.LogArray = append(rft.LogArray, temp)

	ackChan := make(chan int)
	majChan := make(chan bool)
	go monitorAckChannel(rft, ackChan, temp, majChan)

	for _, server := range rft.clusterConfig.Servers[1:] {
		go doRPCCall(ackChan, server.Hostname, server.LogPort, temp)
	}

	if <-majChan {
		//
	}

	return temp, nil
}

//AddToChannel
func (rft *Raft) AddToChannel(entry LogEntry) {
	Info.Println("Adding to commit", entry)
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
func (raft *Raft) loop() {
	state := FOLLOWER
	for {
		switch state {
		case FOLLOWER:
			state = follower()
		case CANDIDATE:
			state = candidate()
		case LEADER:
			state = leader()
		default:
			return
		}
	}
}

func (raft *Raft) follower() {
	//start candidate timeout
	canTimeout = time.After((randGen.Intn(MAX_TIMEOUT) + MIN_TIMEOUT) % MAX_TIMEOUT)
	for {
		//wrap in select
        event := <- raft.eventCh
        switch event.(type) {
        case ClientAppend:
            // Do not handle clients in follower mode. Send it back up the
            // pipe with committed = false
            ev.logEntry.commited = false
            commitCh <- ev.logentry
        case VoteRequest:
            msg = event.msg
            if msg.term < currentterm, respond with 
            if msg.term > currentterm, upgrade currentterm
            if not already voted in my term
                reset timer
                reply ok to event.msg.serverid
                remember term, leader id (either in log or in separate file)
        case AppendRPC:
            reset timer
            if msg.term < currentterm, ignore
            reset heartbeat timer
            upgrade to event.msg.term if necessary
            if prev entries of my log and event.msg match
               add to disk log
               flush disk log
               respond ok to event.msg.serverid
            else
               respond err.
        case Timeout : return candidate  // new state back to loop()
    }
}