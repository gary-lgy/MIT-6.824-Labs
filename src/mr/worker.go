package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	rpcArgs := &RequestForTaskArgs{
		// use -1 to indicate the first run
		CompletedTaskId:   -1,
		CompletedTaskType: TaskTypeMap,
	}

	for {
		reply := requestForTask(rpcArgs)
		if mapTask := reply.Map; mapTask != nil {
			doMap(mapTask, mapf)
			rpcArgs.CompletedTaskId = mapTask.Id
			rpcArgs.CompletedTaskType = TaskTypeMap
		} else if reduceTask := reply.Reduce; reduceTask != nil {
			// TODO
			//doReduce(reduceTask, reducef)
			//rpcArgs.CompletedTaskId = reduceTask.Id
			//rpcArgs.CompletedTaskType = TaskTypeReduce
		} else {
			// TODO: is this request to terminate?
			panic("received invalid reply from master")
		}
	}
}

func requestForTask(args *RequestForTaskArgs) *RequestForTaskReply {
	reply := RequestForTaskReply{}
	call("Master.RequestForTask", args, &reply)
	return &reply
}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) {
	fmt.Printf("Performing map id=%v inputFilename=%v\n", task.Id, task.InputFile)

	// Produce the intermediate kv pairs
	inputFile, err := os.Open(task.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFile)
	}

	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFile)
	}

	inputFile.Close()

	intermediateKv := mapf(task.InputFile, string(content))

	// write the results to intermediate files
	outputEncoders := make([]*json.Encoder, task.NReduce)
	for _, kv := range intermediateKv {
		partition := ihash(kv.Key) % task.NReduce

		// TODO: use temp files?

		// lazily create files for each partition
		if outputEncoders[partition] == nil {
			file, err := os.Create(intermediateFilename(task.Id, partition))
			if err != nil {
				log.Fatalf("failed to create intermediate file %v\n", err)
			}
			outputEncoders[partition] = json.NewEncoder(file)
		}

		err = outputEncoders[partition].Encode(&kv)
		if err != nil {
			log.Fatalf("failed to encode %v\n", err)
		}
	}
}

func intermediateFilename(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
