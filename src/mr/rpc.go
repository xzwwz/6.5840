package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

type TaskRequestArgs struct {
	Wid int
}

type TaskRequestReply struct {
	TaskType int //0:wait 1:map 2:reduce -1:done
	TaskId int
	Task Task
	Worker []int
	NReduce int
}

type TaskReportArgs struct {
	TaskType int //0:map 1:reduce
	TaskId int
	Wid int
}

type TaskReportReply struct {
}

type TaskDataArgs struct {
	TaskId int
}

type TaskDataReply struct{
	Data []byte
	Size int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}




