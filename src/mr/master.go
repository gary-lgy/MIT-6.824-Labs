package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

	hasTask *sync.Cond
	sync.Mutex
}

func (m *Master) processCompletedTask(id int, taskType TaskType, workerId string) {
	if taskType == TaskTypeMap {
		log.Printf("Worker %v reported map task %v complete\n", workerId, id)

		if _, alreadyCompleted := m.completedMapTasks[id]; alreadyCompleted {
			log.Printf("Map task %v already completed. Re-execution?\n", id)
		} else {
			m.completedMapTasks[id] = struct{}{}
			log.Printf("Map task %v completed normally\n", id)
		}

		if len(m.completedMapTasks) == len(m.mapTasks) {
			log.Printf("===============================================\n")
			log.Printf("All map tasks completed, start reduce phase\n")
			log.Printf("===============================================\n")

			// Tell blocked workers to start working on reduce tasks
			m.hasTask.Broadcast()

			for i, _ := range m.reduceTasks {
				m.runnableReduceTaskQueue = append(m.runnableReduceTaskQueue, i)
			}
		}
	} else if taskType == TaskTypeReduce {
		log.Printf("Worker %v reported reduce task %v complete\n", workerId, id)

		if _, alreadyCompleted := m.completedReduceTasks[id]; alreadyCompleted {
			log.Printf("Reduce task %v already completed. Re-execution?\n", id)
		} else {
			m.completedReduceTasks[id] = struct{}{}
			log.Printf("Reduce task %v completed normally\n", id)
		}
	} else {
		panic("received invalid task type from worker " + workerId)
	}
}

func (m *Master) popRunnableMapTask() *MapTask {
	if len(m.runnableMapTaskQueue) <= 0 {
		return nil
	}
	task := m.mapTasks[m.runnableMapTaskQueue[0]]
	m.runnableMapTaskQueue = m.runnableMapTaskQueue[1:]
	return task
}

func (m *Master) popRunnableReduceTask() *ReduceTask {
	if len(m.runnableReduceTaskQueue) <= 0 {
		return nil
	}
	task := m.reduceTasks[m.runnableReduceTaskQueue[0]]
	m.runnableReduceTaskQueue = m.runnableReduceTaskQueue[1:]
	return task
}

// If the task is not done after 10 seconds, assume the worker has died
func (m *Master) prepareForWorkerFailure(taskId int, taskType TaskType, workerId string) {
	time.Sleep(10 * time.Second)

	m.Lock()
	defer m.Unlock()

	if taskType == TaskTypeMap {
		if _, ok := m.completedMapTasks[taskId]; !ok {
			m.runnableMapTaskQueue = append(m.runnableMapTaskQueue, taskId)
			log.Printf("Worker %v did not complete map task %d within 10 seconds. Retry.\n", workerId, taskId)
			m.hasTask.Signal()
		}
	} else if taskType == TaskTypeReduce {
		if _, ok := m.completedReduceTasks[taskId]; !ok {
			m.runnableReduceTaskQueue = append(m.runnableReduceTaskQueue, taskId)
			log.Printf("Worker %v did not complete reduce task %d within 10 seconds. Retry.\n", workerId, taskId)
			m.hasTask.Signal()
		}
	}
}

func (m *Master) RequestForTask(completedTask *RequestForTaskArgs, reply *RequestForTaskReply) error {
	m.Lock()
	defer m.Unlock()

	// If the worker has completed a task, mark it as completed
	if completedTask != nil && completedTask.CompletedTaskId >= 0 {
		m.processCompletedTask(completedTask.CompletedTaskId, completedTask.CompletedTaskType, completedTask.WorkerId)
	}

	// Try to give the worker another task
	for {
		if m.done() {
			// tell all workers to terminate
			m.hasTask.Broadcast()
			return nil
		}

		if mapTask := m.popRunnableMapTask(); mapTask != nil {
			reply.Map = mapTask
			log.Printf("Assigned map task %v to worker %v\n", mapTask.Id, completedTask.WorkerId)
			go m.prepareForWorkerFailure(mapTask.Id, TaskTypeMap, completedTask.WorkerId)
			return nil
		}
		if reduceTask := m.popRunnableReduceTask(); reduceTask != nil {
			reply.Reduce = reduceTask
			log.Printf("Assigned reduce task %v to worker %v\n", reduceTask.Id, completedTask.WorkerId)
			go m.prepareForWorkerFailure(reduceTask.Id, TaskTypeReduce, completedTask.WorkerId)
			return nil
		}

		// Not done but no runnable task.
		// We are either waiting for all map tasks to finish before starting the reduce phase
		// or waiting for dispatched reduce tasks to finish.
		// In either case, we wait until we are clear what to do.
		log.Printf("No runnable task available for worker %v\n", completedTask.WorkerId)
		m.hasTask.Wait()
	}
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
	log.SetPrefix("[Master] ")

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
	m.hasTask = sync.NewCond(&m.Mutex)

	m.server()
	return &m
}
