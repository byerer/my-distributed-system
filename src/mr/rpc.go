package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

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
type GetTaskRequest struct {
}

type GetTaskResponse struct {
	Kind    int
	Flag    bool
	AllDone bool
	//map resp
	Filename string
	NReduce  int
	MapID    int
	//reduce resp
	ReduceID int
	MapCnt   int
}

type FinishTaskRequest struct {
	Kind     int
	MapID    int
	ReduceID int
}

type FinishTaskResponse struct {
}

type UpdateTimeReq struct {
	Kind     int
	MapID    int
	ReduceID int
}

type UpdateTimeResp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
