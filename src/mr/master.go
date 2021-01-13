package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTask struct {
	Id        int
	InputFile string
	NReduce   int
}

func NewMapTask(id int, inputFile string, nReduce int) *MapTask {
	return &MapTask{
		Id:        id,
		InputFile: inputFile,
		NReduce:   nReduce,
	}
}

type ReduceTask struct {
	Id   int
	NMap int
}

func NewReduceTask(id int, nMap int) *ReduceTask {
	return &ReduceTask{
		Id:   id,
		NMap: nMap,
	}
}

type Master struct {
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask

	runnableMapTaskQueue    []int
	runnableReduceTaskQueue []int

	completedMapTasks    map[int]struct{}
	completedReduceTasks map[int]struct{}

	sync.Mutex
}

func (m *Master) processCompletedTask(id int, taskType TaskType) {
	if taskType == TaskTypeMap {
		log.Printf("Worker reported map task %v complete\n", id)

		if _, alreadyCompleted := m.completedMapTasks[id]; alreadyCompleted {
			log.Printf("Map task %v already completed. Re-execution?\n", id)
		} else {
			m.completedMapTasks[id] = struct{}{}
			log.Printf("Map task %v completed normally\n", id)
		}

		if len(m.completedMapTasks) == len(m.mapTasks) {
			log.Printf("===============================================\n")
			log.Printf("All map tasks completed, can start reduce phase\n")
			log.Printf("===============================================\n")

			for i, _ := range m.reduceTasks {
				m.runnableReduceTaskQueue = append(m.runnableReduceTaskQueue, i)
			}
		}
	} else if taskType == TaskTypeReduce {
		log.Printf("Worker reported reduce task %v complete\n", id)

		if _, alreadyCompleted := m.completedReduceTasks[id]; alreadyCompleted {
			log.Printf("Reduce task %v already completed. Re-execution?\n", id)
		} else {
			m.completedReduceTasks[id] = struct{}{}
			log.Printf("Reduce task %v completed normally\n", id)
		}
	} else {
		panic("received invalid task type from worker")
	}
}

func (m *Master) RequestForTask(completedTask *RequestForTaskArgs, reply *RequestForTaskReply) error {
	m.Lock()
	defer m.Unlock()

	// If the worker has completed a task, mark it as completed
	if completedTask != nil && completedTask.CompletedTaskId >= 0 {
		m.processCompletedTask(completedTask.CompletedTaskId, completedTask.CompletedTaskType)
	}

	// Give the worker another task

	if len(m.runnableMapTaskQueue) > 0 {
		// map task available, give map task
		task := m.mapTasks[m.runnableMapTaskQueue[0]]
		m.runnableMapTaskQueue = m.runnableMapTaskQueue[1:]
		reply.Map = task
		log.Printf("Assigned map task %v to worker\n", task.Id)
		return nil
	}

	if len(m.runnableReduceTaskQueue) > 0 {
		// reduce task available, give reduce task
		task := m.reduceTasks[m.runnableReduceTaskQueue[0]]
		m.runnableReduceTaskQueue = m.runnableReduceTaskQueue[1:]
		reply.Reduce = task
		log.Printf("Assigned reduce task %v to worker\n", task.Id)
		return nil
	}

	// no runnable tasks, either we have finished, or we are waiting for map tasks to finish

	// if all tasks are done, tell all workers to terminate
	if m.done() {
		return nil
	}

	// not done but no runnable task, sleep and wait
	// TODO: sleep and wait cond var

	// TODO: anticipate worker failure

	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) done() bool {
	return len(m.completedMapTasks) == len(m.mapTasks) &&
		len(m.completedReduceTasks) == len(m.reduceTasks)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock()
	defer m.Unlock()

	return m.done()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)

	mapTasks := make([]*MapTask, 0, nMap)
	runnableMapTaskQueue := make([]int, 0, nMap)
	for i, inputFile := range files {
		mapTasks = append(mapTasks, NewMapTask(i, inputFile, nReduce))
		// Map tasks are immediately runnable
		runnableMapTaskQueue = append(runnableMapTaskQueue, i)
	}

	reduceTasks := make([]*ReduceTask, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, NewReduceTask(i, nMap))
	}

	m := Master{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,

		runnableMapTaskQueue:    runnableMapTaskQueue,
		runnableReduceTaskQueue: make([]int, 0, nReduce),

		completedMapTasks:    make(map[int]struct{}),
		completedReduceTasks: make(map[int]struct{}),
	}

	m.server()
	return &m
}
