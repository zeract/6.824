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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// alloc args
type Args struct {
}

// alloc reply
type Reply struct {
	Filename   string // map task read the file
	TaskType   int    // task type, 0 is map, 1 reduce
	TaskNumber int    // the number of map or reduce task
	NReduce    int    // the num of reduce tasks
	NMap       int    // the num of map tasks
}

// finish args
type FinishArgs struct {
	Number     int // the  task number
	FinishType int // Finsih type, 0 is map task, 1 is reduce task
}

// finish relpy
type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
