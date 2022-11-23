package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState uint8

const (
	Idle TaskState = iota
	Running
	Completed
)

type TaskInfo struct {
	taskNumber  int
	state       TaskState
	inputFiles  []string
	outputFiles []string
}

func (t *TaskInfo) IsComplete() bool {
	return t.state == Completed
}

func (t *TaskInfo) IsIdle() bool {
	return t.state == Idle
}

func (t *TaskInfo) IsRunning() bool {
	return t.state == Running
}

func (t *TaskInfo) SetState(state TaskState) {
	t.state = state
}

type ConcurrentTaskInfo struct {
	m        sync.Mutex
	tasks    []*TaskInfo
	complete bool
}

func (c *ConcurrentTaskInfo) AssignIdleTask() *TaskInfo {
	c.m.Lock()
	// ensure to unlock after
	defer c.m.Unlock()
	for _, t := range c.tasks {
		if !t.IsIdle() {
			continue
		}
		t.SetState(Running)
		return t
	}
	return nil
}

func (c *ConcurrentTaskInfo) SetState(task int, state TaskState) {
	c.m.Lock()
	// ensure to unlock after
	defer c.m.Unlock()
	if task >= len(c.tasks) {
		return
	}
	c.tasks[task].SetState(state)
}

func (c *ConcurrentTaskInfo) AllComplete() bool {
	if c.complete {
		// Can touch this without incurring dangerous race condition
		return true
	}
	c.m.Lock()
	// ensure to unlock after
	defer c.m.Unlock()
	for _, t := range c.tasks {
		if !t.IsComplete() {
			return false
		}
	}
	c.complete = true
	return true
}

type Coordinator struct {
	nReduce     int
	inputFiles  []string
	mapTasks    *ConcurrentTaskInfo
	reduceTasks *ConcurrentTaskInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	t := c.mapTasks.AssignIdleTask()
	fmt.Printf("Found task %v to assign with input file %v\n", t.taskNumber, t.inputFiles[0])
	reply.InputFiles = t.inputFiles
	reply.TaskNumber = t.taskNumber
	reply.Type = Map
	reply.NReduce = c.nReduce
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	// Initialize map tasks
	mapTasks := make([]*TaskInfo, nMap)
	for i := range mapTasks {
		mapTasks[i] = &TaskInfo{taskNumber: i, state: Idle, inputFiles: []string{files[i]}}
	}
	c := Coordinator{nReduce: nReduce, inputFiles: files, mapTasks: &ConcurrentTaskInfo{tasks: mapTasks}}

	// Your code here.

	c.server()
	return &c
}
