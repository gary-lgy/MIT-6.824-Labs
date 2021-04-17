package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func serverDPrint(id int, format string, args ...interface{}) {
	DPrintf(fmt.Sprintf("[%d] %s", id, format), args...)
}

func Assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}
