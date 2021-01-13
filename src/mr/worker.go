package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func getWorkerId() string {
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", b)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	workerId := getWorkerId()
	log.SetPrefix("[Worker " + workerId + "] ")
	rpcArgs := &RequestForTaskArgs{
		CompletedTaskId:   -1,
		CompletedTaskType: TaskTypeMap,
		WorkerId: workerId,
	}

	for {
		reply := requestForTask(rpcArgs)
		if reply == nil {
			// master terminated
			log.Printf("Master terminated\n")
			break
		}
		if mapTask := reply.Map; mapTask != nil {
			doMap(mapTask, mapf)
			rpcArgs.CompletedTaskId = mapTask.Id
			rpcArgs.CompletedTaskType = TaskTypeMap
		} else if reduceTask := reply.Reduce; reduceTask != nil {
			doReduce(reduceTask, reducef)
			rpcArgs.CompletedTaskId = reduceTask.Id
			rpcArgs.CompletedTaskType = TaskTypeReduce
		} else {
			// job done, terminate
			log.Printf("Received finish signal from master\n")
			break
		}
	}
}

func requestForTask(args *RequestForTaskArgs) *RequestForTaskReply {
	reply := RequestForTaskReply{}
	if !call("Master.RequestForTask", args, &reply) {
		return nil
	}
	return &reply
}

func intermediateFilename(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) {
	log.Printf("Performing map id=%v inputFilename=%v\n", task.Id, task.InputFile)

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

	// Create an encoder for each output file
	outputFiles := make([]*os.File, task.NReduce)
	outputEncoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		file, err := ioutil.TempFile("", intermediateFilename(task.Id, i))
		if err != nil {
			log.Fatalf("failed to create intermediate file %v\n", err)
		}
		outputFiles[i] = file
		outputEncoders[i] = json.NewEncoder(file)
	}

	// write the results to intermediate files
	for _, kv := range intermediateKv {
		partition := ihash(kv.Key) % task.NReduce
		err = outputEncoders[partition].Encode(&kv)
		if err != nil {
			log.Fatalf("failed to encode %v\n", err)
		}
	}

	// rename the temp files
	for i, outputFile := range outputFiles {
		outputFile.Close()
		err = os.Rename(outputFile.Name(), intermediateFilename(task.Id, i))
		if err != nil {
			log.Fatalf("failed to rename temp output file of map: %v\n", err)
		}
	}
}

func shuffle(task *ReduceTask) []KeyValue {
	kva := make([]KeyValue, 0)

	for i := 0; i < task.NMap; i++ {
		inputFilename := intermediateFilename(i, task.Id)

		inputFile, err := os.Open(inputFilename)
		if err != nil {
			log.Fatalf("cannot open %v", inputFilename)
		}

		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		inputFile.Close()
	}

	return kva
}

func doReduce(task *ReduceTask, reducef func(string, []string) string) {
	log.Printf("Performing reduce id=%v", task.Id)

	kva := shuffle(task)
	sort.Sort(ByKey(kva))

	outputFilename := fmt.Sprintf("mr-out-%d", task.Id)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("failed to create output file %v\n", err)
	}

	i := 0
	for i < len(kva) {
		key := kva[i].Key
		values := make([]string, 0)
		j := i
		for j < len(kva) && kva[j].Key == key {
			values = append(values, kva[j].Value)
			j++
		}
		output := reducef(key, values)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)

		i = j
	}

	outputFile.Close()
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

	log.Printf("rpc error: %v\n", err)
	return false
}
