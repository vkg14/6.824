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
	nMap       int
	inputFiles []string
	// Channels which are thread-safe
	availableTaskChan  chan int
	completionChannels []chan int
	// Members to be atomically loaded and incremented
	taskStage         uint32
	numCompletedTasks uint32
}

func (c *Coordinator) GenerateInputFiles(taskType TaskType, taskNumber int) []string {
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

func (c *Coordinator) TaskNumberOffset(taskType TaskType) int {
	switch taskType {
	case Map:
		return 0
	case Reduce:
		return c.nMap
	default:
		// error?
		return -1
	}
}

func (c *Coordinator) StageReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		c.availableTaskChan <- i
	}
}

func (c *Coordinator) CheckStageChange(taskType TaskType, numCompletedTasks int) {
	switch taskType {
	case Map:
		if numCompletedTasks == c.nMap {
			// Before the system stages reduce tasks, it is impossible for the system
			// to mark more than nMap tasks as complete.  Therefore, we can only enter
			// this block *once* when no TrackTaskProgress
			// goroutines are pending. This increment will move taskStage from 0 -> 1
			fmt.Printf("Map stage complete. Reduce stage starting.\n")
			atomic.AddUint32(&c.taskStage, 1)
			c.StageReduceTasks()
		}
	case Reduce:
		if numCompletedTasks == c.nMap+c.nReduce {
			// After the system stages reduce tasks, it is impossible for the system
			// to mark more than nMap+nReduce tasks as complete.
			// Therefore, we can only enter this block *once* when
			// no TrackTaskProgress goroutines are pending.
			// This increment will move taskStage from 1 -> 2
			fmt.Printf("MapReduce has completed.\n")
			atomic.AddUint32(&c.taskStage, 1)
			close(c.availableTaskChan)
		}

	}
}

func (c *Coordinator) TrackTaskProgress(taskType TaskType, taskNumber int) {
	// This is the crux of our logic.  The goroutine for TrackTaskProgress(t,k) will
	// either increment the completed counter on completion OR reschedule the task
	// on timeout.  If the latter happens, the counter does not get incremented until
	// the task is reassigned to a worker by AssignTask RPC Handler and a new
	// TrackTaskProgress(t,k) is run (with the current one having exited on timeout).
	// This maintains the invariant that each completed task only increments the counter
	// exactly once.
	select {
	case <-c.completionChannels[c.TaskNumberOffset(taskType)+taskNumber]:
		fmt.Printf("Task completion notified: %v \n", taskNumber)
		completed := atomic.AddUint32(&c.numCompletedTasks, 1)
		// Change stage based on fixed values seen here.
		// Ex. If two separate threads, hit this code,
		// CheckStageChange should be called with *different* values of completed.
		c.CheckStageChange(taskType, int(completed))
	case <-time.After(10 * time.Second):
		// Re-schedule task.  To keep our invariant, we can only increment our counter for this
		// task *after* another worker has been assigned the rescheduled task.
		fmt.Printf("Timeout. Re-scheduling task: %v, stage: %v\n", taskNumber, taskType)
		c.availableTaskChan <- taskNumber
	}
}

// AssignTask RPC handler
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	val, ok := <-c.availableTaskChan
	if !ok {
		// Channel closed so we should tell worker to exit!
		reply.TaskNumber = -1
		reply.Type = Exit
		return nil
	}
	taskType := TaskType(atomic.LoadUint32(&c.taskStage))

	// Assign reply fields
	reply.Type = taskType
	reply.TaskNumber = val
	reply.InputFiles = c.GenerateInputFiles(reply.Type, reply.TaskNumber)
	reply.NReduce = c.nReduce
	fmt.Printf(
		"Coordinator found task %v in stage %v to assign with input files %v\n",
		reply.TaskNumber,
		reply.Type,
		reply.InputFiles,
	)
	// Track completion
	go c.TrackTaskProgress(reply.Type, reply.TaskNumber)
	return nil
}

func (c *Coordinator) MarkComplete(args *MarkCompleteArgs, reply *MarkCompleteReply) error {
	channelIndex := c.TaskNumberOffset(args.Type) + args.TaskNumber
	// The value sent below is received by goroutine TrackTaskProgress(type, taskNumber) and
	// will mark the task as completed if the goroutine has not timed out (and thus
	// scheduled a back-up).
	fmt.Printf("Marking task %v for stage %v as completed\n", args.TaskNumber, args.Type)
	c.completionChannels[channelIndex] <- 1
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
	return TaskType(atomic.LoadUint32(&c.taskStage)) == Exit
}

// MakeCoordinator creates a coordinator and starts the HTTP server
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	// Initialize channels
	availableTaskChannel := make(chan int, nMap)
	completionChannels := make([]chan int, nMap+nReduce)
	for i := 0; i < nMap; i++ {
		// Push all *map* tasks as available initially
		availableTaskChannel <- i
		// Make buffer 2 to handle potential backups
		completionChannels[i] = make(chan int, 2)
	}
	for i := 0; i < nReduce; i++ {
		completionChannels[nMap+i] = make(chan int, 2)
	}
	c := Coordinator{
		nReduce:            nReduce,
		nMap:               nMap,
		inputFiles:         files,
		availableTaskChan:  availableTaskChannel,
		completionChannels: completionChannels,
	}

	c.server()
	return &c
}
