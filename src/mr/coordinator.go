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

	workerTasks map[int][]int //{"workerId":[TaskType,TaskType]}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) assignMapTask(reply *TaskQuestRPCReply, workerId int) {
	// dont judge task availability in this function
	// directly assign map task
	reply.MapReply.Filename = c.mapTaskList[c.curMapIdx]
	c.curMapIdx++
	reply.MapReply.NReduce = c.nReduce
	reply.TaskType = MapTask
	reply.workerId = workerId
}

func getTargetFilename(Srcfilename string) string {
	tmp := strings.Split(Srcfilename, "-")
	targetFilename := tmp[0] + "-" + tmp[2]
	return targetFilename
}

func (c *Coordinator) processMapTaskResult(workerId int) {
	for i := 0; i < c.nReduce; i++ {
		filename := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i)
		c.reduceTaskList[i].Enqueue(filename)
	}
}

// TODO: check assignReduceTask func
func (c *Coordinator) assignReduceTask(reply *TaskQuestRPCReply, workerId int) {
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
			reply.workerId = workerId
			break
		}
	}
	if flag {
		reply.TaskType = WaitTask
	}
}

func (c *Coordinator) isReduceTaskListEmpty() bool {
	// return true if all empty
	res := true
	for _, q := range c.reduceTaskList {
		if !q.Empty() {
			res = false
			break
		}
	}
	return res
}

// TODO: finish response processor
// TODO: get filename according to workerId
func (c *Coordinator) processMapTaskResponse(workerId int) {
	vec := c.workerTasks[workerId]

}

func (c *Coordinator) assignTask(workerId int, reply *TaskQuestRPCReply) {
	if c.curMapIdx >= len(c.mapTaskList) && c.isReduceTaskListEmpty() {
		// all empty
		reply.workerId = workerId
		reply.TaskType = WaitTask
		return
	}

	if c.curMapIdx >= len(c.mapTaskList) {
		// still have reduce task to assign
		c.assignReduceTask(reply, workerId)
		return
	}

	c.assignMapTask(reply, workerId)
}

func (c *Coordinator) QuestTaskService(args *TaskQuestRPCArgs, reply *TaskQuestRPCReply) error {
	// always assign map task firstly
	c.mux.Lock()
	if args.workerId == InitWorkerId {
		//assign workerId
		c.workerCnt++
		reply.workerId = c.workerCnt
		reply.TaskType = WorkerIdAssignmentTask
		c.workerTasks[c.workerCnt] = make([]int, 0)
		c.workerTasks[c.workerCnt] = append(c.workerTasks[c.workerCnt], WorkerIdAssignmentTask)
		c.mux.Unlock()
		return nil
	}
	// process Response
	sliceLength := len(c.workerTasks[c.workerCnt]) - 1
	lastTask := c.workerTasks[c.workerCnt][sliceLength]
	if lastTask == MapTask {
		c.processMapTaskResponse(args.workerId)
	}

	// assign task

	// unlock coordinator
	c.mux.Unlock()
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
