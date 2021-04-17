package raft

import (
	"fmt"
	"log"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func serverDPrint(id int, source string, format string, args ...interface{}) {
	DPrintf(fmt.Sprintf("[%d] (%s) %s", id, source, format), args...)
}

func Assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}
