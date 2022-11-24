package mr

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus uint8

const (
	Unscheduled TaskStatus = iota
	Running
	Done
)

const UseAtomics = true

type Coordinator struct {
	nReduce              int
	inputFiles           []string
	unscheduledTaskQueue chan int // a channel "queue" with tasks ready for execution

	/*
		nTasksLeft starts as (nReduce+nMap) and will be decremented as tasks complete.
		It is stored as int32 to make atomic operations possible with the atomic lib.
	*/
	nTasksLeft int32

	/*
		taskStage starts as 0 (Map) and will transition to 1 (Reduce) and 2 (Exit)
		when the requisite number of tasks have been completed.  It is stored as
		int32 to make atomic operations possible with the atomic lib.
	*/
	taskStage int32

	/*
		Status of tasks is stored in the int32 arrays below
		Status is initialized at 0 (Unscheduled) and can be
		atomically swapped to 1 (Running) and eventually to
		2 (Done).  These are used when UseAtomics = true
	*/
	mapTaskStatus    []int32
	reduceTaskStatus []int32

	/*
		Channels to communicate success from RPC handler to task-tracking goroutine
		These are used when UseAtomics = false
	*/
	mapTaskChannels    []chan int
	reduceTaskChannels []chan int
}

func (c *Coordinator) generateInputFiles(taskType TaskType, taskNumber int) []string {
	var inputFiles []string
	switch taskType {
	case Map:
		inputFiles = append(inputFiles, c.inputFiles[taskNumber])
	case Reduce:
		for i := range c.inputFiles {
			inputFiles = append(inputFiles, fmt.Sprintf("mr-%v-%v", i, taskNumber))
		}
	}
	return inputFiles
}

func (c *Coordinator) enqueueReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		c.unscheduledTaskQueue <- i
	}
}

func (c *Coordinator) adjustTaskStage(taskType TaskType, nTasksLeft int) {
	switch taskType {
	case Map:
		if nTasksLeft == c.nReduce {
			// Before the system stages reduce tasks, it is impossible for the system
			// to mark more than nMap tasks as complete.  Therefore, we can only enter
			// this block *once* when no waitForTaskResult
			// goroutines are pending. This increment will move taskStage from 0 -> 1
			fmt.Printf("Map stage complete. Reduce stage starting.\n")
			atomic.AddInt32(&c.taskStage, 1)
			c.enqueueReduceTasks()
		}
	case Reduce:
		if nTasksLeft == 0 {
			// After the system stages reduce tasks, it is impossible for the system
			// to mark more than nMap+nReduce tasks as complete.
			// Therefore, we can only enter this block *once* when
			// no waitForTaskResult goroutines are pending.
			// This increment will move taskStage from 1 -> 2
			fmt.Printf("MapReduce has completed.\n")
			atomic.AddInt32(&c.taskStage, 1)
			close(c.unscheduledTaskQueue)
		}

	}
}

func (c *Coordinator) getTaskChannel(taskType TaskType, taskNumber int) chan int {
	switch taskType {
	case Map:
		return c.mapTaskChannels[taskNumber]
	case Reduce:
		return c.reduceTaskChannels[taskNumber]
	default:
		// Should never get here - maybe error or log?
		return nil
	}
}

func (c *Coordinator) waitForTaskChannels(taskType TaskType, taskNumber int, assignedWorker int) {
	// This is the crux of our logic.  The goroutine for waitForTaskResult(t,k) will
	// either decrement the remaining counter on completion OR reschedule the task
	// on timeout.  If the latter happens, the counter does not get decremented until
	// the task is reassigned to a worker by RequestTask RPC Handler and a new
	// waitForTaskResult(t,k) is run (with the current one having exited on timeout).
	// This maintains the invariant that each completed task only decrements the counter
	// exactly once.
	timeout := time.After(10 * time.Second)
	select {
	case workerId := <-c.getTaskChannel(taskType, taskNumber):
		if workerId != assignedWorker {
			fmt.Printf(
				"Received a different worker %v instead of %v. Continue anyway (idempotent!)...\n",
				workerId,
				assignedWorker,
			)
		}
		fmt.Printf("Task %v completion notified on worker %v\n", taskNumber, assignedWorker)
		tasksLeft := atomic.AddInt32(&c.nTasksLeft, -1)
		c.adjustTaskStage(taskType, int(tasksLeft))
	case <-timeout:
		// Re-schedule task.  To keep our invariant, we can only decrement our counter for this
		// task *after* another worker has been assigned the rescheduled task.
		fmt.Printf("Timeout. Re-scheduling task: %v, stage: %v\n", taskNumber, taskType)
		c.unscheduledTaskQueue <- taskNumber
	}
}

func (c *Coordinator) waitForTaskAtomics(taskType TaskType, taskNumber int) {
	<-time.After(10 * time.Second)
	// What can happen here:
	// Changed status from Running to Unsched: task timed out; reschedule
	// Failed status change: task has been already marked done.
	if c.changeTaskStatus(taskType, taskNumber, Running, Unscheduled) {
		fmt.Printf("Timeout. Re-scheduling task: %v, stage: %v\n", taskNumber, taskType)
		c.unscheduledTaskQueue <- taskNumber
	}
}

func (c *Coordinator) changeTaskStatus(taskType TaskType, taskNumber int, oldState TaskStatus, newState TaskStatus) bool {
	// Valid state transitions are 0->1 (via RequestTask), 1->2 (via ReportTaskResult), and 1->0 (via waitForTaskResult)
	if oldState == Done || (oldState == Unscheduled && newState == Done) {
		fmt.Printf("Invalid state change attempted from %v to %v.\n", oldState, newState)
		return false
	}
	var status []int32
	switch taskType {
	case Map:
		status = c.mapTaskStatus
	case Reduce:
		status = c.reduceTaskStatus
	default:
		fmt.Printf("Received unknown type %v in changeTaskStatus.\n", taskType)
		return false
	}
	return atomic.CompareAndSwapInt32(&status[taskNumber], int32(oldState), int32(newState))
}

// RequestTask RPC handler
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	val, ok := <-c.unscheduledTaskQueue
	if !ok {
		// Channel closed so we should tell worker to exit!
		reply.TaskNumber = -1
		reply.Type = Exit
		return nil
	}
	taskType := TaskType(atomic.LoadInt32(&c.taskStage))

	// Assign reply fields
	reply.Type = taskType
	reply.TaskNumber = val
	reply.InputFiles = c.generateInputFiles(reply.Type, reply.TaskNumber)
	reply.NReduce = c.nReduce
	fmt.Printf(
		"Coordinator found task %v in stage %v to assign with input files %v\n",
		reply.TaskNumber,
		reply.Type,
		reply.InputFiles,
	)
	// Track completion with a go-routine
	if !UseAtomics {
		go c.waitForTaskChannels(reply.Type, reply.TaskNumber, args.WorkerId)
	} else {
		// We atomically change the status and then track timeouts only
		if c.changeTaskStatus(reply.Type, reply.TaskNumber, Unscheduled, Running) {
			go c.waitForTaskAtomics(reply.Type, reply.TaskNumber)
		} else {
			fmt.Printf("Invariant broken. Failed state change in RequestTask.")
		}
	}
	return nil
}

func (c *Coordinator) ReportTaskResult(args *ReportTaskResultArgs, reply *ReportTaskResultReply) error {
	if !UseAtomics {
		// The value sent below is received by goroutine waitForTaskResult(type, taskNumber) and
		// will mark the task as completed if the goroutine has not timed out (and thus
		// scheduled a back-up).
		fmt.Printf("Marking task %v for stage %v as completed\n", args.TaskNumber, args.Type)
		c.getTaskChannel(args.Type, args.TaskNumber) <- args.WorkerId
	} else {
		// What can happen here:
		// Atomically transition from Running -> Done: this task is now complete.
		// - adjust tasksLeft counter
		// - modify stage of coordinator if necessary
		// Failed atomic transition: this task has timed out via waitForTaskAtomics.
		// - the task has been rescheduled, so we need not do anything.
		if c.changeTaskStatus(args.Type, args.TaskNumber, Running, Done) {
			fmt.Printf("Finished task %v for stage %v.\n", args.TaskNumber, args.Type)
			tasksLeft := atomic.AddInt32(&c.nTasksLeft, -1)
			c.adjustTaskStage(args.Type, int(tasksLeft))
		}
	}
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

// Done simply checks that the taskStage is Exit and returns.
// This member is atomically mutated and once state has shifted
// to Exit, we can always safely move on.
func (c *Coordinator) Done() bool {
	return TaskType(atomic.LoadInt32(&c.taskStage)) == Exit
}

// MakeCoordinator creates a coordinator and starts the HTTP server
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	// Initialize unscheduled task queue with enough buffer for all tasks.
	unscheduledTaskQueue := make(chan int, nMap+nReduce)

	mapTaskChannels := make([]chan int, nMap)
	mapTaskStatus := make([]int32, nMap)
	for i := 0; i < nMap; i++ {
		// Push all *map* tasks as available initially
		unscheduledTaskQueue <- i
		// Make buffer 2 to handle potential backups
		mapTaskChannels[i] = make(chan int, 2)
	}

	reduceTaskChannels := make([]chan int, nReduce)
	reduceTaskStatus := make([]int32, nReduce)
	for i := 0; i < nReduce; i++ {
		// Make buffer 2 to handle potential backups
		reduceTaskChannels[i] = make(chan int, 2)
	}
	c := Coordinator{
		nReduce:              nReduce,
		inputFiles:           files,
		taskStage:            int32(0),
		nTasksLeft:           int32(nMap + nReduce),
		unscheduledTaskQueue: unscheduledTaskQueue,
		mapTaskStatus:        mapTaskStatus,
		reduceTaskStatus:     reduceTaskStatus,
		mapTaskChannels:      mapTaskChannels,
		reduceTaskChannels:   reduceTaskChannels,
	}

	c.server()
	return &c
}
