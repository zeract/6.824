package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := Args{}

		// declare a reply structure.
		reply := Reply{}

		call("Coordinator.Alloc", &args, &reply)

		// alloc the map task
		if reply.TaskType == 0 {

			finish_arg := FinishArgs{}
			finish_reply := FinishReply{}

			// open the map file
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("can't open %v", reply.Filename)
			}

			//	read the file content
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			// call map function to get key/value pair
			kva := mapf(reply.Filename, string(content))
			var files []*os.File
			var encs []*json.Encoder
			for i := 0; i < reply.NReduce; i++ {
				// filename := "mr-" + strconv.Itoa(reply.taskNumber) + "-" + strconv.Itoa(i)
				temp_file, err := ioutil.TempFile("", "")
				if err != nil {
					log.Fatalf("Create temp File fail")
				}
				files = append(files, temp_file)

				enc := json.NewEncoder(temp_file)
				encs = append(encs, enc)
			}

			// partition the key/value pair to R region
			for _, kv := range kva {
				// the reduce task number
				key := ihash(kv.Key) % reply.NReduce

				encode_err := encs[key].Encode(kv)
				if encode_err != nil {
					log.Fatalf("Write file fail!")
				}
			}

			// atomic Rename the file and Close the file
			for i := 0; i < reply.NReduce; i++ {
				filename := "mr-" + strconv.Itoa(reply.TaskNumber) + "-" + strconv.Itoa(i)
				os.Rename(files[i].Name(), filename)
				files[i].Close()
			}

			// finish the map task
			finish_arg.FinishType = 0
			finish_arg.Number = reply.TaskNumber
			call("Coordinator.Finish", &finish_arg, &finish_reply)

		} else if reply.TaskType == 1 {
			// alloc the reduce task
			finish_arg := FinishArgs{}
			finish_reply := FinishReply{}

			m := reply.NMap

			var kva []KeyValue
			// read the intermediate files from map
			for i := 0; i < m; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNumber)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Open the file failed!")
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}

			}
			// sort the keys
			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(reply.TaskNumber)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in kva[] and print
			// the result in mr-out-X
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
			// finish the reduce task
			finish_arg.FinishType = 1
			finish_arg.Number = reply.TaskNumber
			call("Coordinator.Finish", &finish_arg, &finish_reply)

		} else {
			break
		}

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
