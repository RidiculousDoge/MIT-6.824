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

// command type
const GetMapTask = 100
const GetReduceStatus = 101
const GetReduceTask = 102
const MapTaskFinished = 103
const ReduceTaskFinished = 104
const QuestIntermediate = 105

// Task reply info
const MapTaskAssigned = 200
const NoMapTaskAssigned = 201
const ReduceTaskAssigned = 202
const NoReduceTaskAssigned = 203

//  RPC Status
const RPCStatusOK = 300
const RPCStatusFailed = 301
const RPCNoMoreFile = 302
const RPCInterfaceMissUsed = 303
const RPCReplyInit = 304

//Task Type
const MapTask = 400
const ReduceTask = 401
const WaitTask = 402

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// RPC Args
type RPCArg struct {
	CommandType int
}

type TaskRequestRPCArg struct {
	RPCArg
}

type MapResultRPCArg struct {
	RPCArg
	Intermediate []KeyValue
}

//RPC Replies
type RPCReply struct {
	TaskType int
	Status   int
}

type TaskRequestReply struct {
	RPCReply
	Filename string
}

type MapTaskResultReply struct {
	RPCReply
}

type IntermediateQuestReply struct {
	RPCReply
	Intermediate []KeyValue
}

type ReduceTaskResultReply struct {
	RPCReply
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
