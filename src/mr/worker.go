package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Convenience type for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	breakLoop := false
	for !breakLoop {
		reply := CallRequestTask()
		fmt.Printf("Worker assigned task %v for stage %v.\n", reply.TaskNumber, reply.Type)
		switch reply.Type {
		case Map:
			ApplyMap(mapf, reply.InputFiles[0], reply.NReduce, reply.TaskNumber)
			CallReportTaskResult(reply.Type, reply.TaskNumber)
		case Reduce:
			ApplyReduce(reducef, reply.InputFiles, reply.TaskNumber)
			CallReportTaskResult(reply.Type, reply.TaskNumber)
		default:
			fmt.Printf("Breaking worker loop...\n")
			breakLoop = true
		}
	}
}

func ApplyMap(mapf func(string, string) []KeyValue, filename string, nReduce int, taskNumber int) []string {
	var intermediate []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// Create temp files and json encoders
	outputFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := range outputFiles {
		outputFiles[i], _ = os.CreateTemp("", "tmp")
		encoders[i] = json.NewEncoder(outputFiles[i])
	}

	// Write these to files based on hash of key
	for _, kv := range intermediate {
		hashBucket := ihash(kv.Key) % nReduce
		encoders[hashBucket].Encode(&kv)
	}

	// Rename files
	var result []string
	for i := range outputFiles {
		newName := fmt.Sprintf("mr-%v-%v", taskNumber, i)
		os.Rename(outputFiles[i].Name(), newName)
		result = append(result, newName)
		fmt.Printf("Renamed file to %v\n", newName)
		outputFiles[i].Close() // Clean up open files
	}

	return result
}

func ApplyReduce(reducef func(string, []string) string, files []string, taskNumber int) string {
	// Read in key-values
	var kva []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Error while reading file %v\n", filename)
			// Should likely kill execution of this worker here and let coordinator reschedule
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// Sort by key so keys are grouped together
	sort.Sort(ByKey(kva))

	// Create temporary file while reducing, buffering and writing elements
	outputFile, _ := os.CreateTemp("", fmt.Sprintf("task-%v-temp", taskNumber))
	defer outputFile.Close()

	// Loop over sorted keys, aggregating matching keys and reducing them
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// Rename to correct format and close file
	outputName := fmt.Sprintf("mr-out-%v", taskNumber)
	os.Rename(outputFile.Name(), outputName)
	fmt.Printf("Renamed file to %v\n", outputName)

	return outputName
}

func CallRequestTask() *RequestTaskReply {
	args := RequestTaskArgs{WorkerId: os.Getpid()}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		// Exit assuming that coordinator can't be contacted
		reply.Type = Exit
	}
	// Should return by ref?
	return &reply
}

func CallReportTaskResult(taskType TaskType, number int) ReportTaskResultReply {
	args := ReportTaskResultArgs{Type: taskType, TaskNumber: number, WorkerId: os.Getpid()}
	reply := ReportTaskResultReply{}
	ok := call("Coordinator.ReportTaskResult", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	// Should return by ref?
	return reply

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
