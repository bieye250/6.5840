package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

type TaskState int

const (
	TaskIdle TaskState = iota
	TaskInProgress
	TaskCompleted
)

type taskInfo struct {
	state   TaskState
	start   time.Time
	version int
}

type Coordinator struct {
	nReduce int

	mu sync.Mutex

	mapTasks map[string]*taskInfo

	leftMapTask int

	leftReduceTask int

	completedFiles []string

	reduceTasks map[int]*taskInfo
}

func (c *Coordinator) GetTask(args *GetTask, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fileName := args.FileName
	version := args.Version

	// fmt.Printf("receive request: %v\n", args)

	if c.leftMapTask > 0 {
		task, ok := c.mapTasks[fileName]
		if ok && task.state == TaskInProgress && task.version == args.Version {
			delete(c.mapTasks, fileName)
			c.leftMapTask--
			c.completedFiles = append(c.completedFiles, fmt.Sprintf("%s-%d", fileName, version))
		}

		// try find an idle map task
		for file, task := range c.mapTasks {
			if task.state == TaskIdle || (task.state == TaskInProgress && time.Since(task.start) > taskTimeout) {
				c.mapTasks[file].state = TaskInProgress
				c.mapTasks[file].start = time.Now()
				task.version++

				reply.Status = MapTask
				reply.MapFileName = file
				reply.Version = task.version
				return nil
			}
		}
		if c.leftMapTask > 0 {
			reply.Status = Wait
			return nil
		}
	}
	// try find an idle reduce task
	if c.leftMapTask == 0 && c.leftReduceTask > 0 {
		idx := args.ReduceId
		task, ok := c.reduceTasks[idx]
		if ok && task.state == TaskInProgress && task.version == args.Version {
			delete(c.reduceTasks, idx)
			c.leftReduceTask--
		}

		for reduceId, task := range c.reduceTasks {
			if task.state == TaskIdle || (task.state == TaskInProgress && time.Since(task.start) > taskTimeout) {
				task.state = TaskInProgress
				task.start = time.Now()
				task.version++

				reply.ReduceId = reduceId
				reply.ReduceFileNames = c.completedFiles
				reply.Status = ReduceTask
				reply.Version = task.version
				return nil
			}
		}
		reply.Status = Wait
		return nil
	}

	if c.leftMapTask == 0 && c.leftReduceTask == 0 {
		reply.Status = Finish
		go func() {
			files, err := filepath.Glob("../main/*.txt-*")
			if err != nil {
				log.Fatalf("glob error: %v", err)
			}
			for _, file := range files {
				// fmt.Println("??" + file)
				os.Remove(file)
			}
		}()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leftMapTask == 0 && c.leftReduceTask == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:        nReduce,
		mapTasks:       make(map[string]*taskInfo),
		leftMapTask:    len(files),
		leftReduceTask: nReduce,
		reduceTasks:    make(map[int]*taskInfo),
	}

	for _, file := range files {
		c.mapTasks[file] = &taskInfo{
			state: TaskIdle,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &taskInfo{
			state: TaskIdle,
		}
	}
	c.server(sockname)
	return &c
}
