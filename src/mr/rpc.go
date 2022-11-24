package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType uint8

const (
	Map TaskType = iota
	Reduce
	Exit
)

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Type       TaskType
	TaskNumber int
	InputFiles []string
	NReduce    int
}

type ReportTaskResultArgs struct {
	Type       TaskType
	TaskNumber int
	WorkerId   int
}

type ReportTaskResultReply struct {
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
