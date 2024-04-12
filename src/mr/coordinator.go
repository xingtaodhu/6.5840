package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	TaskID    int
	TaskType  string // "Map" or "Reduce"
	FileName  string
	StartTime time.Time
	NReduce   int
	NMap      int
	WorkerID  int
}

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	phase         string
	Files         []string
	idleTasks     []Task
	runningTasks  []Task
	finishedTasks []Task
	failedTasks   []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	log.Printf("RequestTask: %v %v %v\n", args.WorkerID, args.HasFinishedTask, args.FinishedTask)
	log.Printf("Coordinator: %v\n", len(c.idleTasks))
	if args.HasFinishedTask {
		task := args.FinishedTask
		c.finishedTasks = append(c.finishedTasks, task)
		for i, t := range c.runningTasks {
			if t.TaskID == task.TaskID {
				c.runningTasks = append(c.runningTasks[:i], c.runningTasks[i+1:]...)
				break
			}
		}
	}
	if len(c.finishedTasks) == len(c.Files) {
		reply.AllTaskDone = true
		return nil
	}
	if len(c.idleTasks) > 0 {
		log.Printf("idleTasks size: %v\n", len(c.idleTasks))
		task := c.idleTasks[0]
		task.StartTime = time.Now()
		task.WorkerID = args.WorkerID
		c.runningTasks = append(c.runningTasks, task)
		c.idleTasks = c.idleTasks[1:]
		reply.Task = task
		reply.WorkerID = args.WorkerID
		reply.haveNewTask = true
	} else if len(c.failedTasks) > 0 {
		log.Printf("failedTasks size: %v\n", len(c.failedTasks))
		task := c.failedTasks[0]
		task.StartTime = time.Now()
		task.WorkerID = args.WorkerID
		c.runningTasks = append(c.runningTasks, task)
		c.failedTasks = c.failedTasks[1:]
		reply.Task = task
		reply.WorkerID = args.WorkerID
		reply.haveNewTask = true
	} else {
		log.Printf("No new task")
		reply.WorkerID = args.WorkerID
		reply.haveNewTask = false
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	if len(c.finishedTasks) == len(c.Files) {
		ret = true
	}
	log.Printf("coordinator done value: %v\n", ret)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		phase:   "Map",
		Files:   files,
		nReduce: nReduce,
	}
	for id, file := range files {
		log.Printf("file: %v\n", file)
		task := Task{
			TaskID:   id,
			FileName: file,
			TaskType: "Map",
			NReduce:  nReduce,
		}
		c.idleTasks = append(c.idleTasks, task)
	}
	log.Printf("idleTasks size: %v\n", len(c.idleTasks))
	c.server()
	return &c
}
