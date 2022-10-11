package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// init workerid
const InitWorkerId = -1

// Task Type
const MapTask = 100
const ReduceTask = 101
const WaitTask = 102
const ServerErr = 103
const ExitTask = 104
const WorkerIdAssignmentTask = 105

// Task Result Type
const TaskFinished = 200
const TaskFailed = 201

// Add your RPC definitions here.

// RPC Args
type RPCArg struct {
	CommandType int
}

type TaskQuestRPCArgs struct {
	workerId int
}

type TaskResultRPCArgs struct {
	workerId   int
	TaskType   int
	TaskResult int
}

type TaskQuestRPCReply struct {
	workerId    int
	TaskType    int
	MapReply    MapReply
	ReduceReply ReduceReply
}
type MapReply struct {
	Filename string
	NReduce  int
}

type ReduceReply struct {
	TargetFilename string // mr-y
	SrcFilename    string // mr-x-y
	NReduce        int
}

type TaskResultRPCReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
