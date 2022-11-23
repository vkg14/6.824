package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskType uint8

const (
	Map TaskType = iota
	Reduce
	Exit
)

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	Type       TaskType
	TaskNumber int
	InputFiles []string
	NReduce    int
}

type MarkCompleteArgs struct {
	Type        TaskType
	TaskNumber  int
	OutputFiles []string
}

type MarkCompleteReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
