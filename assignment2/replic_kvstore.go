package main

import (
	//"log"
	//"net"
	//"net/rpc"
	"fmt"
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
	id        uint64
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
	//some good stuff needs to go here
}

var cluster_config *ClusterConfig

//make LogEntryData implement the
func (entry *LogEntryData) Lsn() uint64 {
	return entry.id
}

func (entry *LogEntryData) Data() []byte {
	return entry.data
}

func (entry *LogEntryData) Committed() bool {
	return entry.committed
}

//make raft implement the append function
//func (raft *Raft) Append(data []byte) (LogEntry, error) {
//}

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
}
