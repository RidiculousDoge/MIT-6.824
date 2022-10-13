package mr

import (
	"fmt"
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
	reply.MapReply.NReduce = c.nReduce
	c.workerTasks[workerId] = append(c.workerTasks[workerId], MapTask)
}

// TODO: update file format, modify this func
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
	fmt.Printf("assigning reduce task for worker:" + strconv.Itoa(workerId) + "\n")
	for _, q := range c.reduceTaskList {
		if !q.Empty() {
			// assign reduce task
			filenameIf, ok := q.Dequeue()
			if !ok {
				reply.TaskType = ServerErr
				c.workerTasks[workerId] = append(c.workerTasks[workerId], ServerErr)
				break
			}
			filename := filenameIf.(string)
			reply.TaskType = ReduceTask
			reply.ReduceReply.NReduce = c.nReduce
			reply.ReduceReply.SrcFilename = filename                       // mr-x-y-i
			reply.ReduceReply.TargetFilename = getTargetFilename(filename) // mr-y
			reply.workerId = workerId
			c.workerTasks[workerId] = append(c.workerTasks[workerId], ReduceTask)
			break
		}
	}
	reply.TaskType = ServerErr
	c.workerTasks[workerId] = append(c.workerTasks[workerId], ServerErr)
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
	fmt.Print("processing map task response for worker:" + strconv.Itoa(workerId) + "\n")
	vec := c.workerTasks[workerId]
	MapTaskcnt := -1
	for _, v := range vec {
		if v == MapTask {
			MapTaskcnt++
		}
	}
	for i := 0; i < c.nReduce; i++ {
		filename := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(MapTaskcnt)
		c.reduceTaskList[i].Enqueue(filename)
	}
}

func (c *Coordinator) assignTask(workerId int, reply *TaskQuestRPCReply) {
	fmt.Printf("assigning task for worker:" + strconv.Itoa(workerId) + "\n")
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
	if args.WorkerId == InitWorkerId {
		//assign workerId
		c.workerCnt++
		reply.workerId = c.workerCnt - 1
		reply.TaskType = WorkerIdAssignmentTask
		c.workerTasks[c.workerCnt-1] = make([]int, 0)
		c.workerTasks[c.workerCnt-1] = append(c.workerTasks[c.workerCnt-1], WorkerIdAssignmentTask)
		fmt.Printf("assigning workerIdAssignmentTask with id " + strconv.Itoa(c.workerCnt-1) + "\n")
		c.mux.Unlock()
		return nil
	}
	// process Response
	sliceLength := len(c.workerTasks[args.WorkerId]) - 1
	fmt.Printf("sliceLength:" + strconv.Itoa(sliceLength) + "\n")
	lastTask := c.workerTasks[args.WorkerId][sliceLength]
	fmt.Printf("lastTask:" + strconv.Itoa(lastTask) + "\n")
	fmt.Printf("slice:")
	for i := 0; i <= sliceLength; i++ {
		fmt.Printf(strconv.Itoa(c.workerTasks[args.WorkerId][i]) + " ")
	}
	fmt.Printf("\n")
	if lastTask == MapTask {
		c.processMapTaskResponse(args.WorkerId)
	}

	// assign task
	c.assignTask(args.WorkerId, reply)

	// unlock coordinator
	c.mux.Unlock()
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
	c.nReduce = nReduce
	c.mapTaskList = make([]string, 0)
	c.curMapIdx = 0
	c.workerCnt = 0
	c.workerTasks = make(map[int][]int)
	c.reduceTaskList = make([]llq.Queue, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskList[i] = *llq.New()
	}

	for _, v := range files {
		c.mapTaskList = append(c.mapTaskList, v)
	}
	c.server()
	return &c
}
