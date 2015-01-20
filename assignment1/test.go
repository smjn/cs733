package main

import (
	"fmt"
)

func main() {
	var n int
	fmt.Scanf("%d", &n)
	buf := make([]byte, n)
	buf[0] = 1
}
