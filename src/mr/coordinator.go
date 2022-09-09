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
	taskList     []string
	curIdx       int
	intermediate []KeyValue
	mux          sync.Mutex
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

func (c *Coordinator) QuestMapTaskService(args *MapRequestRPCArg, reply *MapTaskRequestReply) error {
	if c.curIdx == len(c.taskList) {
		// no more task to assign
		reply.Status = RPCNoMoreFile
		reply.Filename = ""
		return nil
	}
	c.mux.Lock()
	reply.Filename = c.taskList[c.curIdx]
	c.curIdx++
	c.mux.Unlock()
	reply.Status = RPCStatusOK

	return nil
}

func (c *Coordinator) MapTaskSetIntermediateService(args *MapResultRPCArg) error {
	c.mux.Lock()
	c.intermediate = append(c.intermediate, args.Intermediate...)
	c.mux.Unlock()
	return nil
}

func (c *Coordinator) GetIntermediateService(args *RPCArg, reply *intermediateQuestReply) error {
	if args.CommandType != QuestIntermediate {
		reply.Status = RPCInterfaceMissUsed
		return nil
	}

	reply.Intermediate = c.intermediate
	reply.Status = RPCStatusOK
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{make([]string, 0), 0, make([]KeyValue, 0), sync.Mutex{}}

	// Your code here.

	for _, v := range files {
		c.taskList = append(c.taskList, v)
	}
	c.server()
	return &c
}
