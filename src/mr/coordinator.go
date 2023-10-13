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
	NReduce      int
	NMap         int
	map_tasks    []int      // 0 for idle, 1 for in-progress, 2 for completed
	reduce_tasks []int      // 0 for idle, 1 for in-progress, 2 for completed
	filenames    []string   // store the filenames of input files
	mutex        sync.Mutex // mutex for coordinator and rpc
	cond         sync.Cond  // conditional variable for issue request
	isDone       bool       // all tasks is done
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
	defer c.mutex.Unlock()
	// look for map task
	for i := 0; i < c.NMap; i++ {
		if c.map_tasks[i] == 0 {
			// the map task i is allocated to worker
			c.map_tasks[i] = 1

			// pass the filename to reply data structure
			reply.Filename = c.filenames[i]

			// pass the task number
			reply.TaskNumber = i

			// pass the task type, 0 for map task, 1 for reduce task
			reply.TaskType = 0

			// pass the map num
			reply.NMap = c.NMap

			// pass the reduce num
			reply.NReduce = c.NReduce
			return nil
		}
	}
	// if all map tasks not all done, the request for reduce need issue
	for {
		map_done := true
		for i := 0; i < c.NMap; i++ {
			if c.map_tasks[i] != 2 {
				map_done = false
			}
		}
		if !map_done {
			c.cond.Wait()
		} else {
			break
		}
	}

	// look for reduce task
	for i := 0; i < c.NReduce; i++ {
		if c.reduce_tasks[i] == 0 {
			// the reduce task i is allocated to worker
			c.reduce_tasks[i] = 1

			// pass the task type
			reply.TaskType = 1

			// pass the task number
			reply.TaskNumber = i

			// pass the map num
			reply.NMap = c.NMap

			// pass the reduce num
			reply.NReduce = c.NReduce
			return nil
		}
	}
	for {
		reduce_done := true
		for i := 0; i < c.NReduce; i++ {
			if c.reduce_tasks[i] != 2 {
				reduce_done = false
			}
		}
		if !reduce_done {
			c.cond.Wait()
		} else {
			break
		}
	}
	reply.TaskType = 2
	c.isDone = true

	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// 0 is map task finish
	if args.FinishType == 0 {
		// change the map task state to finish
		c.map_tasks[args.Number] = 2

	} else {
		// 1 is reduce task finish

		// change the reduce task state to finish
		c.reduce_tasks[args.Number] = 2

	}

	// Broadcast for all issue requests
	c.cond.Broadcast()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// initialize map, reduce and filenames
	c.map_tasks = make([]int, len(files))
	c.filenames = make([]string, len(files))
	c.reduce_tasks = make([]int, nReduce)
	c.NReduce = nReduce
	c.NMap = len(files)
	c.isDone = false
	for i := 0; i < len(files); i++ {
		c.map_tasks[i] = 0
		c.filenames[i] = files[i]
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] = 0
	}

	c.server()
	return &c
}
