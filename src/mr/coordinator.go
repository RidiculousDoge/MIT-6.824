package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskList  []string
	curMapIdx    int

	reduceTaskList []string // mr-m-n.txt
	curReduceIdx   int
	mux            sync.Mutex
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

func (c *Coordinator) QuestMapTaskService(args *TaskRequestRPCArg, reply *TaskRequestReply) error {
	if c.curMapIdx == len(c.mapTaskList) {
		// no more task to assign
		fmt.Printf("no more task to assign\n")
		reply.Status = RPCNoMoreFile
		reply.Filename = ""
		return nil
	}
	c.mux.Lock()
	reply.Filename = c.mapTaskList[c.curMapIdx]
	c.curMapIdx++
	c.mux.Unlock()
	reply.Status = RPCStatusOK

	return nil
}



func (c *Coordinator) assignMapTask(reply *TaskRequestReply){
	// dont judge task availability in this function
	// directly assign map task
	c.mux.Lock()
	reply.Filename = c.mapTaskList[c.curMapIdx]
	c.curMapIdx++
	c.mux.Unlock()
	reply.Status = RPCStatusOK
	reply.TaskType = MapTask
}

func (c *Coordinator) assignReduceTask(reply *TaskRequestReply){
	
}

func (c *Coordinator) QuestTaskService(args *RPCArg, reply *RPCReply) error {
	switch args.

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
	c := Coordinator{}

	// Your code here.
	c.mapTaskList = make([]string, 0)
	c.curMapIdx = 0
	c.reduceTaskList = make([]string, 0)
	c.curReduceIdx = 0

	for _, v := range files {
		c.mapTaskList = append(c.mapTaskList, v)
	}
	c.server()
	return &c
}
