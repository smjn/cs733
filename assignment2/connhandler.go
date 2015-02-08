package main

import (
	"kvstore"
	"raft"
)

func main() {
	server_id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		Info.Println("argument ", os.Args[1], "is not string")
	}

	initializeLogger(server_id)
	Info.Println("Start")

	this_server, _ := NewServerConfig(server_id)

	num_servers, err2 := strconv.Atoi((os.Args[2]))
	if err2 != nil {
		Info.Println("argument ", os.Args[2], "is not string")
	}
	cluster_config, _ := NewClusterConfig(num_servers)

	Info.Println(reflect.TypeOf(this_server))
	Info.Println(reflect.TypeOf(cluster_config))

	initializeInterServerCommunication(this_server)

	var dummy_input string
	fmt.Scanln(&dummy_input)
}
