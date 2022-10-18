package mr

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

func (kv KeyValue) KVToString() {
	return
}

func DispKeyValue(kv *KeyValue) {
	fmt.Printf("%+v", kv)
}

func LogError(ErrorMessage string, code int) {
	fmt.Printf(ErrorMessage+", code:%d \n", code)
}

func ToBytes(kvPairs []KeyValue) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(kvPairs)
	if err != nil {
		fmt.Println("failed gob Encode", err)
	}
	return b.Bytes()
}

func FromBytes(byteArr []byte) []KeyValue {
	m := []KeyValue{}
	b := bytes.Buffer{}
	b.Write(byteArr)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		fmt.Println("failed gob decode", err)
	}
	return m
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

	// args := TaskRequestRPCArg{RPCArg{GetMapTask}}
	// reply := TaskRequestReply{RPCReply{RPCReplyInit}, ""}
	// res := false
	// for reply.Status != RPCNoMoreFile && reply.Status != RPCStatusFailed {
	// 	intermediate := make([]KeyValue, 0)
	// 	if reply.Status == RPCReplyInit {
	// 		res = call("Coordinator.QuestMapTaskService", &args, &reply)
	// 		if !res {
	// 			LogError("quest Task Failed", reply.Status)
	// 			return
	// 		}
	// 		continue
	// 	}
	// 	execMapTask(reply.Filename, mapf, &intermediate)
	// 	// set intermediate back to coordinator
	// 	initReply := MapTaskResultReply{RPCReply{RPCReplyInit}}
	// 	if !sendIntermediateToCoordinator(&intermediate, &initReply) {
	// 		LogError("worker set coordinator intermediate failed", initReply.Status)
	// 		return
	// 	}
	// 	res = call("Coordinator.QuestMapTaskService", &args, &reply)
	// 	if !res {
	// 		LogError("quest Task Failed", reply.Status)
	// 		return
	// 	}
	// }
	// if reply.Status != RPCNoMoreFile {
	// 	LogError("worker rpc request failed", reply.Status)
	// 	return
	// }
	// fmt.Printf("worker finished\n")
	workerId := InitWorkerId
	mapTaskTimes := 0
	args := TaskQuestRPCArgs{}
	args.WorkerId = workerId
	reply := TaskQuestRPCReply{}

	for i := 0; i < 15; i++ {
		fmt.Printf("current workerId in args is:" + strconv.Itoa(args.WorkerId) + "\n")
		call("Coordinator.QuestTaskService", &args, &reply)
		fmt.Printf("replyTaskType:" + strconv.Itoa(reply.TaskType) + "\n")
		switch reply.TaskType {
		case WorkerIdAssignmentTask:
			workerId = reply.workerId
			args.WorkerId = workerId
		case MapTask:
			mapTaskTimes += 1
			execMapTask(reply.MapReply.Filename, mapf, reply.MapReply.NReduce, workerId, mapTaskTimes)
		case ReduceTask:
			execReduceTask(reply.ReduceReply.SrcFilename, reply.ReduceReply.TargetFilename)
		}
	}

}

func execReduceTask(sourceFilename string, targetFilename string) {
	sourceFile, err := os.Open(sourceFilename)
	if err != nil {
		log.Fatalf("cannot open source file:%v", sourceFilename)
	}
	targetFile, err := os.Open(targetFilename)
	if err != nil {
		log.Fatalf("cannot open target file:%v", targetFilename)
	}
	sourceFileBytes, _ := ioutil.ReadAll(sourceFile)
	targetFileBytes, _ := ioutil.ReadAll(targetFile)
	sourceFile.Close()
	targetFile.Close()

	sourceFileContent := string(sourceFileBytes)
	targetFileContent := string(targetFileBytes)
	// TODO: reduce sourceFileContent with targetFileContent
}

func writeToFile(intermediate *map[int][]KeyValue, workerId int, mapTaskTimes int) {
	for i := 0; i < len(*intermediate); i++ {
		filepath := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(mapTaskTimes)
		file, _ := os.Create(filepath)
		for j := 0; j < len((*intermediate)[i]); j++ {
			fmt.Fprintf(file, "%v %v\n", (*intermediate)[i][j].Key, (*intermediate)[i][j].Value)
		}
		file.Close()
	}
}

func execMapTask(filename string, mapf func(string, string) []KeyValue, nReduce int, workerId int, mapTaskTimes int) {

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
	intermediate := map[int][]KeyValue{}
	// fmt.Printf("nReduce is" + strconv.Itoa(nReduce) + "\n")
	for _, v := range kva {
		target := ihash(v.Key) % nReduce
		intermediate[target] = append(intermediate[target], v)
	}
	writeToFile(&intermediate, workerId, mapTaskTimes)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
