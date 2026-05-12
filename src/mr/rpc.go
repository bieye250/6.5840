package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type GetTask struct {
	FileName string
	Version int
	ReduceId int
}
type TaskReply struct {
	MapFileName string
	Status TaskArgs
	ReduceFileNames []string
	Version int
	ReduceId int
}

type TaskArgs int

const (
	Finish TaskArgs = iota
	MapTask
	ReduceTask
	Wait
)