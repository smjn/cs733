package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

//constant values used
const (
	NUM_SERVERS int = 5
)

func TestServersCommunic(i int) {
	cmd := exec.Command("go", "run", "server.go", strconv.Itoa(i+1), strconv.Itoa(NUM_SERVERS))
	f, err := os.OpenFile(strconv.Itoa(i), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("error opening file: %v", err)
	}

	defer f.Close()
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Run()
}

func main() {
	for i := 0; i < NUM_SERVERS; i++ {
		go TestServersCommunic(i)
	}

	var dummy_input string
	fmt.Scanln(&dummy_input)
}
