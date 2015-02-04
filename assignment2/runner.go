package main

import (
	//"os"
	"fmt"
	//"log"
	"os/exec"
	"strconv"
	//"syscall"
)

//constant values used
const (
	NUM_SERVERS int = 5
)

func TestServersCommunic(i int) {
	cmd := exec.Command("go", "run", "replic_kvstore.go", strconv.Itoa(i+1), strconv.Itoa(NUM_SERVERS))
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(out))
}

func main() {
	for i := 0; i < NUM_SERVERS; i++ {
		go TestServersCommunic(i)
	}

	var dummy_input string
	fmt.Scanln(&dummy_input)
}
