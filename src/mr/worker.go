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

func LogError(ErrorMessage string, code int) {
	fmt.Printf(ErrorMessage+", code:%d \n", code)
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
	res := false
	for reply.Status != RPCNoMoreFile && reply.Status != RPCStatusFailed {
		intermediate := make([]KeyValue, 0)
		if reply.Status == RPCReplyInit {
			res = call("Coordinator.QuestMapTaskService", &args, &reply)
			if !res {
				LogError("quest Task Failed", reply.Status)
				return
			}
			continue
		}
		execMapTask(reply.Filename, mapf, &intermediate)
		// set intermediate back to coordinator
		initReply := MapTaskResultReply{RPCReply{RPCReplyInit}}
		if !sendIntermediateToCoordinator(&intermediate, &initReply) {
			LogError("worker set coordinator intermediate failed", initReply.Status)
			return
		}
		res = call("Coordinator.QuestMapTaskService", &args, &reply)
		if !res {
			LogError("quest Task Failed", reply.Status)
			return
		}
	}
	if reply.Status != RPCNoMoreFile {
		LogError("worker rpc request failed", reply.Status)
		return
	}
	fmt.Printf("worker finished\n")

}

func execMapTask(filename string, mapf func(string, string) []KeyValue, intermediate *[]KeyValue) {
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
	*intermediate = append(*intermediate, kva...)
}

func sendIntermediateToCoordinator(intermediate *[]KeyValue, initReply *MapTaskResultReply) bool {
	args := MapResultRPCArg{}
	args.RPCArg = RPCArg{MapTaskFinished}
	args.Intermediate = *intermediate

	res := call("Coordinator.MapTaskSetIntermediateService", &args, &initReply)
	return res
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
