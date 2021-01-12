package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	TaskStatusIdle TaskStatus = iota
	TaskStatusInProgress
	TaskStatusCompleted
)

type TaskInfo struct {
	Id     int
	Status TaskStatus
}

type MapTask struct {
	TaskInfo
	InputFile string
	NReduce   int
}

func NewMapTask(id int, inputFile string, nReduce int) *MapTask {
	return &MapTask{
		TaskInfo:  TaskInfo{Id: id, Status: TaskStatusIdle},
		InputFile: inputFile,
		NReduce:   nReduce,
	}
}

type ReduceTask struct {
	TaskInfo
	NMap int
}

func NewReduceTask(id int, nMap int) *ReduceTask {
	return &ReduceTask{
		TaskInfo: TaskInfo{Id: id, Status: TaskStatusIdle},
		NMap:     nMap,
	}
}

type Master struct {
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask

	mapTasksRemaining    int
	reduceTasksRemaining int

	sync.Mutex
}

func (m *Master) processCompletedTask(id int, taskType TaskType) {
	if taskType == TaskTypeMap {
		log.Printf("Worker reported map task %v complete\n", id)
		task := m.mapTasks[id]
		switch task.Status {
		case TaskStatusIdle:
			panic("cannot go from idle to completed")
		case TaskStatusInProgress:
			log.Printf("Map task %v completed normally\n", id)
			task.Status = TaskStatusCompleted
			m.mapTasksRemaining--
		case TaskStatusCompleted:
			log.Printf("Map task %v already completed. Re-execution?\n", id)
		}
	} else if taskType == TaskTypeReduce {
		log.Printf("Worker reported reduce task %v complete\n", id)
		task := m.reduceTasks[id]
		switch task.Status {
		case TaskStatusIdle:
			panic("cannot go from idle to completed")
		case TaskStatusInProgress:
			log.Printf("Reduce task %v completed normally\n", id)
			task.Status = TaskStatusCompleted
			m.reduceTasksRemaining--
		case TaskStatusCompleted:
			log.Printf("Reduce task %v already completed. Re-execution?\n", id)
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

	// First try looking for a map task
	for _, mapTask := range m.mapTasks {
		if mapTask.Status == TaskStatusIdle {
			mapTask.Status = TaskStatusInProgress
			reply.Map = mapTask
			log.Printf("Assigned map task %v to worker\n", mapTask.Id)
			return nil
		}
	}

	// If no more map tasks, try reduce task
	// TODO: need to determine whether we can start to give out reduce tasks
	//for _, reduceTask := range m.reduceTasks {
	//	if reduceTask.Status == TaskStatusIdle {
	//		reduceTask.Status = TaskStatusInProgress
	//		reply.Reduce = reduceTask
	//		return nil
	//	}
	//}

	// TODO: anticipate worker failure

	// TODO: handle all tasks completed

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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock()
	defer m.Unlock()

	return m.mapTasksRemaining == 0 && m.reduceTasksRemaining == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)

	mapTasks := make([]*MapTask, 0, nMap)
	for i, inputFile := range files {
		mapTasks = append(mapTasks, NewMapTask(i, inputFile, nReduce))
	}

	reduceTasks := make([]*ReduceTask, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, NewReduceTask(i, nMap))
	}

	m := Master{
		mapTasks:             mapTasks,
		reduceTasks:          reduceTasks,
		mapTasksRemaining:    nMap,
		reduceTasksRemaining: nReduce,
	}

	m.server()
	return &m
}
