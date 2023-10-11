package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	workers      []int
	map_tasks    []int      // 0 for idle, 1 for in-progress, 2 for completed
	reduce_tasks []int      // 0 for idle, 1 for in-progress, 2 for completed
	filenames    []string   // store the filenames of input files
	mutex        sync.Mutex // mutex for coordinator and rpc
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// alloc map or reduce task for worker and change the state of master data structure
//
func (c *Coordinator) Alloc(args *Args, reply *Reply) error {
	c.mutex.Lock()
	for i := 0; i < len(c.map_tasks); i++ {
		if c.map_tasks[i] == 0 {
			// the map task i is allocated to worker
			c.map_tasks[i] = 1

			// the worker state is changed to in-progress
			c.workers[args.number] = 1

			// pass the filename to reply data structure
			reply.filename = c.filenames[i]
		}
	}
	defer c.mutex.Unlock()
	return nil
}

func (c *Coordinator) Finish(args *Args, reply *Reply) error {
	c.mutex.Lock()
	c.map_tasks[args.number] = 2

	defer c.mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	c.mutex.Lock()
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
	c.mutex.Unlock()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	ret := true

	// Your code here.
	for i := 0; i < len(c.map_tasks); i++ {
		if c.map_tasks[i] == 2 {
			ret = ret || true
		} else {
			ret = ret || false
		}
	}
	for i := 0; i < len(c.reduce_tasks); i++ {
		if c.reduce_tasks[i] == 2 {
			ret = ret || true
		} else {
			ret = ret || false
		}
	}
	defer c.mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// initialize map, reduce and filenames
	c.map_tasks = make([]int, len(files))
	c.filenames = make([]string, len(files))
	c.reduce_tasks = make([]int, nReduce)
	for i := 0; i < len(files); i++ {
		c.map_tasks[i] = 0
		c.filenames[i] = files[i]
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] = 0
	}
	// Your code here.

	c.server()
	return &c
}
