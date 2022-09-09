package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func DispKeyValue(kv *KeyValue) {
	fmt.Printf("%+v", kv)
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

	args := MapRequestRPCArg{RPCArg{GetMapTask}}
	reply := MapTaskRequestReply{RPCReply{RPCReplyInit}, ""}
	intermediate := make([]KeyValue, 0)
	for reply.Status != RPCNoMoreFile && reply.Status != RPCStatusFailed {
		fmt.Printf("%+v \n", args)
		fmt.Printf("%+v \n", reply)
		if reply.Status == RPCReplyInit {
			res := questTask(&args, &reply)
			if !res {
				break
			}
			continue
		}
		execMapTask(reply.Filename, mapf, intermediate)
		res := questTask(&args, &reply)
		if !res {
			break
		}
	}
	if reply.Status == RPCStatusFailed {
		fmt.Printf("worker rpc request failed")
	}
	if reply.Status == RPCNoMoreFile {
		fmt.Printf("finish assigned task")
		for _, v := range intermediate {
			fmt.Printf("%+v", v)
		}
	}

}

func questTask(args *MapRequestRPCArg, reply *MapTaskRequestReply) bool {
	res := call("Coordinator.QuestMapTaskService", &args, &reply)
	if !res {
		fmt.Printf("quest failed\n")
		return false
	}
	return true
}

func execMapTask(filename string, mapf func(string, string) []KeyValue, intermediate []KeyValue) {
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
	intermediate = append(intermediate, kva...)
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
