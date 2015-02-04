package main

import (
	//"log"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strconv"
)

//constant values used
const (
	CLIENT_PORT = 9000
)

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
	log_array      []LogEntryData
	commitCh       chan *LogEntryData
	cluster_config *ClusterConfig //cluster
	id             uint64         //this server
}

var cluster_config *ClusterConfig

func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (*Raft, error) {
	rft := new(Raft)
	rft.commitCh = commitCh
	rft.cluster_config = config
	rft.id = thisServerId
	return rft, nil
}

//goroutine that monitors channel for commiting log entry
func monitor_commitCh(c <-chan *LogEntryData) { //unidirectional -- can only read from the channel
	for {
		var temp *LogEntryData
		temp = <-c //receive from the channel
		temp.committed = true

		//now update key value store here
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
	if raft.server.Id != 0 {
		return nil, raft.Id
	}
	temp := LogEntry{1, data, false}
	raft.log_array = append(raft.log_array, temp)
	//broadcast to other servers
	//wait for acks
	//send commit on channel
	raft.commitCh <- temp
}

type RPChandle struct {
}

func (r *RPChandle) AppendEntriesRPC(log_entry LogEntryData) bool {

	return true
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
	return "Redirect to server " + strconv.Itoa(cluster_config.Servers[0].Id)
}

func start_rpc(this_server *ServerConfig) {
	rpc.Register()
}

func main() {
	server_id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("argument ", os.Args[1], "is not string")
	}
	this_server, _ := NewServerConfig(server_id)

	num_servers, err2 := strconv.Atoi((os.Args[2]))
	if err2 != nil {
		fmt.Println("argument ", os.Args[2], "is not string")
	}
	cluster_config, _ := NewClusterConfig(num_servers)

	fmt.Println(reflect.TypeOf(this_server))
	fmt.Println(reflect.TypeOf(cluster_config))

	go start_rpc(this_server)

	var dummy_input string
	fmt.Scanln(&dummy_input)
}
