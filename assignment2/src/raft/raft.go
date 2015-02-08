package raft

import (
	"log"
	"net/rpc"
	"strconv"
	"time"
)

//constant values used
const (
	CLIENT_PORT = 9000
	ACK_TIMEOUT = 5
)

// Logger
var Info *log.Logger

// Flag for enabling/disabling logging functionality
var DEBUG = true

type Lsn uint64 //Log sequence number, unique for all time.

type ErrRedirect int // See Log.Append. Implements Error interface.

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}

type LogEntryData struct {
	id        Lsn
	data      []byte
	committed bool
}

type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

type SharedLog interface {
	Append(data []byte) (LogEntry, error)
}

type Raft struct {
	log_array      []*LogEntryData
	commitCh       chan LogEntry
	cluster_config *ClusterConfig //cluster
	id             int            //this server id
}

type Args struct {
	X int
}

type Reply struct {
	X int
}

type AppendEntries struct{}

var cluster_config *ClusterConfig

func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.cluster_config = config
	rft.id = thisServerId
	return rft, nil
}

//goroutine that monitors channel for commiting log entry
func monitorCommitChannel(raft *Raft) { //unidirectional -- can only read from the channel
	for {
		//var temp LogEntry
		temp := <-raft.commitCh                                  //receive from the channel
		raft.log_array[temp.(*LogEntryData).id].committed = true //commit the value
		//update the kv store here
	}
}

//goroutine that monitors channel to check if the majority of servers have replied
func monitorAckChannel(rft *Raft, ack_ch <-chan int, log_entry LogEntry, majEventCh chan int) {
	acks_received := 0
	num_servers := len(rft.cluster_config.Servers)
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
			acks_received += temp
			if acks_received == required_acks {
				rft.commitCh <- log_entry
				majEventCh <- 1
				err = true
				break
			}

		case <-up:
			err = true
			break
		}
		if err {
			break
		}
	}
}

//make LogEntryData implement the
func (entry *LogEntryData) Lsn() Lsn {
	return entry.id
}

func (entry *LogEntryData) Data() []byte {
	return entry.data
}

func (entry *LogEntryData) Committed() bool {
	return entry.committed
}

//make raft implement the append function
func (raft *Raft) Append(data []byte) (LogEntry, error) {
	if raft.id != 1 {
		return nil, ErrRedirect(1)
	}
	temp := new(LogEntryData)
	temp.id = 1
	temp.committed = false
	temp.data = data
	raft.log_array = append(raft.log_array, temp)

	ackChan := make(chan int)
	majEventCh := make(chan int)
	go monitorAckChannel(raft, ackChan, temp, majEventCh)
	go monitorCommitChannel(raft)

	for _, server := range cluster_config.Servers[1:] {
		go func(ackChan chan int) {
			client, err := rpc.Dial("tcp", server.Hostname+":"+strconv.Itoa(server.LogPort))
			if err != nil {
				Info.Fatal("Dialing:", err)
			}
			reply := new(Reply)
			args := &Args{7}
			appendCall := client.Go("AppendEntries.AppendEntriesRPC", args, reply, nil) //let go allocate done channel
			appendCall = <-appendCall.Done
			ackChan <- reply.X
		}(ackChan)
	}
	//channel will return 1 if majority
	if <-majEventCh == 1 {
		raft.commitCh <- temp
	}
	return temp, nil
}

func NewServerConfig(server_id int) (*ServerConfig, error) {
	server := new(ServerConfig)
	server.Id = server_id
	server.Hostname = "127.0.0.1"
	server.ClientPort = CLIENT_PORT
	server.LogPort = CLIENT_PORT + server_id
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
