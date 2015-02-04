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

func NewServerConfig(server_id int) (*ServerConfig, error) {
	this_server := new(ServerConfig)
	this_server.Id = server_id
	this_server.Hostname = "127.0.0.1"
	this_server.ClientPort = CLIENT_PORT
	this_server.LogPort = CLIENT_PORT + server_id
	return this_server, nil
}

func main() {
	server_id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("argument ", os.Args[1], "is not string")
	}
	this_server, _ := NewServerConfig(server_id)
	fmt.Println(reflect.TypeOf(this_server))

}
