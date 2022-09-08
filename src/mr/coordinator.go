package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/liyue201/gostl/ds/queue"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskQueue    queue.Queue
	reduceTaskQueue queue.Queue
	intermediate    []KeyValue
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
	if c.mapTaskQueue.Empty() {
		reply.status = RPCNoMoreFile
		reply.filename = ""
		return nil
	}

	// filepath := c.mapTaskQueue.Pop()
	// reply.filename = filepath

	return nil
}

func (c *Coordinator) MapTaskSetIntermediateService(args *RPCArg) error {
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
	c := Coordinator{*queue.New(), *queue.New(), make([]KeyValue, 0)}

	// Your code here.

	for _, v := range files {
		c.mapTaskQueue.Push(v)
	}
	c.server()
	return &c
}
