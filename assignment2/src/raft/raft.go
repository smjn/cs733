package raft

import (
	"log"
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
)

// Logger
var Info *log.Logger

// Global variable for generating unique log sequence numbers
var lsn Lsn

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

// Structure used for replying to the RPC calls
type Reply struct {
	X int
}

// Structure for registering RPC methods
type AppendEntries struct{}

var cluster_config *ClusterConfig

func GetClusterConfig() *ClusterConfig {
	return cluster_config
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry, logger *log.Logger) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.clusterConfig = config
	cluster_config = config
	rft.id = thisServerId
	Info = logger
	lsn = 0
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
		doRPCCall(ackChan, server.Hostname, server.LogPort, temp)
	}

	if <-majChan {
		//
	}

	return temp, nil
}

//AddToChannel
func (rft *Raft) AddToChannel(entry LogEntry) {
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
