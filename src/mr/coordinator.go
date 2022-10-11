package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"

	llq "github.com/emirpasic/gods/queues/linkedlistqueue"
)

type Coordinator struct {
	// Your definitions here.
	workerCnt int

	mapTaskList []string
	curMapIdx   int

	reduceTaskList []llq.Queue // [{mr-x-1,mr-y-1,mr-z-1},{mr-x-2,mr-y-2,mr-z-2}]

	nReduce int
	mux     sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) assignMapTask(reply *TaskQuestRPCReply) {
	// dont judge task availability in this function
	// directly assign map task
	c.mux.Lock()
	reply.MapReply.Filename = c.mapTaskList[c.curMapIdx]
	c.curMapIdx++
	reply.MapReply.NReduce = c.nReduce
	reply.TaskType = MapTask
	c.mux.Unlock()
}

func getTargetFilename(Srcfilename string) string {
	tmp := strings.Split(Srcfilename, "-")
	targetFilename := tmp[0] + "-" + tmp[2]
	return targetFilename
}

func (c *Coordinator) processMapTaskResult(workerId int) {
	for i := 0; i < c.nReduce; i++ {
		filename := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i)
		c.mux.Lock()
		c.reduceTaskList[i].Enqueue(filename)
		c.mux.Unlock()
	}
}

func (c *Coordinator) assignReduceTask(reply *TaskQuestRPCReply) {
	c.mux.Lock()
	flag := true // mark if all queues are empty
	for _, q := range c.reduceTaskList {
		if !q.Empty() {
			flag = false
			// assign reduce task
			filenameIf, ok := q.Dequeue()
			if !ok {
				reply.TaskType = ServerErr
				break
			}
			filename := filenameIf.(string)
			reply.TaskType = ReduceTask
			reply.ReduceReply.NReduce = c.nReduce
			reply.ReduceReply.SrcFilename = filename
			reply.ReduceReply.TargetFilename = getTargetFilename(filename)
			break
		}
	}
	if flag {
		reply.TaskType = WaitTask
	}
	c.mux.Unlock()
}

func (c *Coordinator) QuestTaskService(args *TaskQuestRPCArgs, reply *TaskQuestRPCReply) error {
	// always assign map task firstly
	if args.workerId == InitWorkerId {
		//assign workerId
		reply.workerId = c.workerCnt
	} else {
		reply.workerId = args.workerId
	}

	if c.curMapIdx < len(c.mapTaskList) {
		c.assignMapTask(reply)
		return nil
	}
	c.assignReduceTask(reply)
	return nil
}

func (c *Coordinator) ProcessTaskResultService(args *TaskResultRPCArgs, reply *TaskResultRPCReply) error {
	if args.TaskType == MapTask {
		c.processMapTaskResult(args.workerId)
	}
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
	c.workerCnt++
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
	c.workerCnt = 0

	for _, v := range files {
		c.mapTaskList = append(c.mapTaskList, v)
	}
	c.server()
	return &c
}
