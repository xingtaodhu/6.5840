package mr

import (
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var WorkerId int = 1

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := TaskArgs{
		// WorkerId is global incrementing variable
		WorkerID:        WorkerId,
		HasFinishedTask: false,
	}
	WorkerId++
	reply := TaskReply{}
	// 无限循环，直到没有新的任务
	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok == false {
			fmt.Printf("Coordinator.RequestTask failed.")
			break
		}
		if reply.AllTaskDone {
			log.Printf("All tasks done")
			break
		}
		if !reply.haveNewTask {
			// sleep 300ms
			log.Printf("No new task, sleep 300ms")
			time.Sleep(300 * time.Millisecond)
			args.HasFinishedTask = false
			continue
		}
		task := reply.Task
		log.Printf("Get new task: %v", task)
		if task.TaskType == "Map" {
			// map task
			filename := task.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content := make([]byte, 0)
			_, err = file.Read(content)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate := make([][]KeyValue, task.NReduce)
			for _, kv := range kva {
				reduceTask := ihash(kv.Key) % task.NReduce
				intermediate[reduceTask] = append(intermediate[reduceTask], kv)
			}
			for i, kva := range intermediate {
				oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
				ofile, _ := os.Create(oname)
				for _, kv := range kva {
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}
		}
		args.HasFinishedTask = true
		args.FinishedTask = task

		/*if task.TaskType == "Reduce" {
			// reduce task
			oname := fmt.Sprintf("mr-out-%d", task.TaskID)
			ofile, _ := os.Create(oname)
			intermediate := make([]KeyValue, 0)
			for i := 0; i < task.NMap; i++ {
				iname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
				ifile, _ := os.Open(iname)
				content := make([]byte, 0)
				_, err := ifile.Read(content)
				if err != nil {
					continue
				}
				ifile.Close()

			}

		}*/
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	/*args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok == false {
		fmt.Printf("Coordinator.RequestTask failed.")
		return
	}
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)*/

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
