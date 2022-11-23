package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Constants that need not be locked (only read)
	nReduce    int
	nMap       int
	inputFiles []string
	// Channels which are thread-safe
	availableTaskChan  chan int
	completionChannels []chan int
	m                  sync.RWMutex
	// Members to be guarded by mutex
	stage             TaskType
	numCompletedTasks int
}

// Example RPC handler
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GenerateInputFiles(taskNumber int) []string {
	var inputFiles []string
	switch c.stage {
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

func (c *Coordinator) CheckStageChange() {
	c.m.Lock()
	defer c.m.Unlock()
	if c.stage == Map && c.numCompletedTasks == c.nMap {
		fmt.Printf("Map stage complete. Reduce stage starting.\n")
		c.stage = Reduce
		go c.StageReduceTasks()
	} else if c.stage == Reduce && c.numCompletedTasks == c.nMap+c.nReduce {
		fmt.Printf("MapReduce has completed.\n")
		c.stage = Exit
		close(c.availableTaskChan)
	}
}

func (c *Coordinator) TrackTaskProgress(taskType TaskType, taskNumber int) {
	// This is the crux of our logic.  This method
	// will increment total completed tasks if completed within the time limit.
	// As long as this running for a particular task, no other worker will be
	// assigned this task.
	select {
	case <-c.completionChannels[c.TaskNumberOffset(taskType)+taskNumber]:
		fmt.Printf("Task completion notified: %v \n", taskNumber)
		c.m.Lock()
		defer c.m.Unlock()
		c.numCompletedTasks++
		go c.CheckStageChange()
	case <-time.After(10 * time.Second):
		// Re-schedule task
		fmt.Printf("Timeout. Re-scheduling task: %v, stage: %v\n", taskNumber, taskType)
		c.availableTaskChan <- taskNumber
	}
}

// AssignTask RPC handler
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	val, ok := <-c.availableTaskChan
	if !ok {
		// Channel closed so we must exit!
		reply.TaskNumber = -1
		reply.Type = Exit
		return nil
	}
	// Need to check RW Mutex here for stage
	c.m.RLock()
	defer c.m.RUnlock()
	reply.Type = c.stage
	reply.TaskNumber = val
	reply.InputFiles = c.GenerateInputFiles(reply.TaskNumber)
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
	c.completionChannels[channelIndex] <- 1
	fmt.Printf("Marking task %v for stage %v as completed\n", args.TaskNumber, args.Type)
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
	c.m.RLock()
	defer c.m.RUnlock()
	return c.stage == Exit
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
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
		stage:              Map,
	}

	c.server()
	return &c
}
