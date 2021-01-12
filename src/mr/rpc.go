package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType string

const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"
)

type RequestForTaskArgs struct {
	CompletedTaskId   int
	CompletedTaskType TaskType
}

type RequestForTaskReply struct {
	Map    *MapTask
	Reduce *ReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
