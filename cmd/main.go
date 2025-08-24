package main

import (
	"os"
)

var (
	ExitCodeDone = 0
	ExitCodeErr  = 0
)

func main() {
	os.Exit(ExitCodeDone)
}
