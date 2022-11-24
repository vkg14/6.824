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

type Coordinator struct {
	// Constants (only read)
	nReduce    int
	inputFiles []string
	// Channels which are thread-safe
	unscheduledTaskQueue chan int
	mapTaskChannels      []chan int
	reduceTaskChannels   []chan int
	// Members to be atomically loaded and incremented/decremented
	taskStage  int32
	nTasksLeft int32
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

func (c *Coordinator) waitForTaskResult(taskType TaskType, taskNumber int, assignedWorker int) {
	// This is the crux of our logic.  The goroutine for waitForTaskResult(t,k) will
	// either decrement the remaining counter on completion OR reschedule the task
	// on timeout.  If the latter happens, the counter does not get decremented until
	// the task is reassigned to a worker by RequestTask RPC Handler and a new
	// waitForTaskResult(t,k) is run (with the current one having exited on timeout).
	// This maintains the invariant that each completed task only decrements the counter
	// exactly once.
	timeout := time.After(10 * time.Second)
	breakLoop := false
	for !breakLoop {
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
			breakLoop = true
		case <-timeout:
			// Re-schedule task.  To keep our invariant, we can only decrement our counter for this
			// task *after* another worker has been assigned the rescheduled task.
			fmt.Printf("Timeout. Re-scheduling task: %v, stage: %v\n", taskNumber, taskType)
			c.unscheduledTaskQueue <- taskNumber
			breakLoop = true
		}
	}
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
	// Track completion
	go c.waitForTaskResult(reply.Type, reply.TaskNumber, args.WorkerId)
	return nil
}

func (c *Coordinator) ReportTaskComplete(args *ReportTaskCompleteArgs, reply *ReportTaskCompleteReply) error {
	// The value sent below is received by goroutine waitForTaskResult(type, taskNumber) and
	// will mark the task as completed if the goroutine has not timed out (and thus
	// scheduled a back-up).
	fmt.Printf("Marking task %v for stage %v as completed\n", args.TaskNumber, args.Type)
	c.getTaskChannel(args.Type, args.TaskNumber) <- args.WorkerId
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
	for i := 0; i < nMap; i++ {
		// Push all *map* tasks as available initially
		unscheduledTaskQueue <- i
		// Make buffer 2 to handle potential backups
		mapTaskChannels[i] = make(chan int, 2)
	}

	reduceTaskChannels := make([]chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		// Make buffer 2 to handle potential backups
		reduceTaskChannels[i] = make(chan int, 2)
	}
	c := Coordinator{
		nReduce:              nReduce,
		inputFiles:           files,
		unscheduledTaskQueue: unscheduledTaskQueue,
		mapTaskChannels:      mapTaskChannels,
		reduceTaskChannels:   reduceTaskChannels,
		taskStage:            int32(0),
		nTasksLeft:           int32(nMap + nReduce),
	}

	c.server()
	return &c
}
