package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2
)

type Task struct {
	Status    int
	WorkerId  string
	StartedAt time.Time
}

type MapTask struct {
	FileName string
	Task
}

type ReduceTask struct {
	Region    int
	Locations []string
	Task
}

type Coordinator struct {
	MapTasks             []*MapTask
	MapTasksRemaining    int
	ReduceTasks          []*ReduceTask
	ReduceTasksRemaining int
	Mu                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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
	if c.MapTasksRemaining == 0 && c.ReduceTasksRemaining == 0 {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]*MapTask, len(files))
	c.MapTasksRemaining = len(files)
	c.ReduceTasks = make([]*ReduceTask, nReduce)
	c.ReduceTasksRemaining = nReduce
	c.Mu = sync.Mutex{}

	//Initialize map tasks
	for i, file := range files {
		c.MapTasks[i] = &MapTask{
			FileName: file,
			Task:     Task{Status: IDLE},
		}
	}

	//Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &ReduceTask{
			Region: nReduce + 1,
			Task:   Task{Status: IDLE},
		}
	}

	fmt.Printf("Coordinator initialized with %v Map Tasks\n", len(files))
	fmt.Printf("Coordinator initialized with %v Reduce Tasks\n", nReduce)
	c.server()
	return &c
}
