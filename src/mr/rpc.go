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

// Task type
const GetMapTask = 100
const GetReduceStatus = 101
const GetReduceTask = 102
const MapTaskFinished = 103
const ReduceTaskFinished = 104

// Task reply info
const MapTaskAssigned = 200
const NoMapTaskAssigned = 201
const ReduceTaskAssigned = 202
const NoReduceTaskAssigned = 203

//  RPC Status
const RPCStatusOK = 300
const RPCStatusFailed = 301
const RPCNoMoreFile = 302

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
	commandType int
}

type MapRequestRPCArg struct {
	RPCArg
}

type MapResultRPCArg struct {
	RPCArg
	intermediate []KeyValue
}

//RPC Replies
type RPCReply struct {
	status int
}

type MapTaskRequestReply struct {
	RPCReply
	filename string
}

type MapTaskResultReply struct {
	RPCReply
}

type ReduceReply struct {
	filename1 string
	filename2 string
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
