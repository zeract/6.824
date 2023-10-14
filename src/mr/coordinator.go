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

type Coordinator struct {
	// Your definitions here.
	NReduce           int
	NMap              int
	map_tasks         []int       // 0 for idle, 1 for in-progress, 2 for completed
	map_tasks_time    []time.Time // record the map task start time
	reduce_tasks      []int       // 0 for idle, 1 for in-progress, 2 for completed
	reduce_tasks_time []time.Time // record the reduce task start time
	filenames         []string    // store the filenames of input files
	mutex             sync.Mutex  // mutex for coordinator and rpc
	cond              sync.Cond   // conditional variable for issue request
	isDone            bool        // all tasks is done
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

	// if all map tasks not all done, the request for reduce need issue
	for {
		// look for map task
		for i := 0; i < c.NMap; i++ {

			if c.map_tasks[i] == 0 || (c.map_tasks[i] == 1 && time.Since(c.map_tasks_time[i]).Seconds() > 10) {
				// the map task i is allocated to worker
				c.map_tasks[i] = 1

				// record the start time
				c.map_tasks_time[i] = time.Now()

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
				// fmt.Printf("Alloc map task %d\n", i)
				return nil
			}

		}
		map_done := true
		for _, m := range c.map_tasks {
			if m != 2 {
				map_done = false
				break
			}
		}
		if !map_done {
			// fmt.Printf("worker wait!\n")
			c.cond.Wait()
		} else {
			break
		}
		// fmt.Printf("Next loop!\n")
	}
	// fmt.Printf("Got in reduce phase\n")
	for {
		// look for reduce task
		for i := 0; i < c.NReduce; i++ {
			if c.reduce_tasks[i] == 0 || (c.reduce_tasks[i] == 1 && time.Since(c.reduce_tasks_time[i]).Seconds() > 10) {
				// the reduce task i is allocated to worker
				c.reduce_tasks[i] = 1

				// record the start time
				c.reduce_tasks_time[i] = time.Now()

				// pass the task type
				reply.TaskType = 1

				// pass the task number
				reply.TaskNumber = i

				// pass the map num
				reply.NMap = c.NMap

				// pass the reduce num
				reply.NReduce = c.NReduce
				// fmt.Printf("Alloc reduce task %d\n", i)
				return nil
			}
		}
		reduce_done := true
		for _, r := range c.reduce_tasks {
			if r != 2 {
				reduce_done = false
				break
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
	c.map_tasks_time = make([]time.Time, len(files))
	c.filenames = make([]string, len(files))
	c.reduce_tasks = make([]int, nReduce)
	c.reduce_tasks_time = make([]time.Time, nReduce)
	c.NReduce = nReduce
	c.NMap = len(files)
	c.isDone = false
	c.cond = *sync.NewCond(&c.mutex) // forget to initialize
	c.filenames = files

	go func() {
		for {
			c.mutex.Lock()
			c.cond.Broadcast()
			// fmt.Printf("Second Broadcast!\n")
			c.mutex.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
