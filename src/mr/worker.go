package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

const nReduce = 10

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	var fileName string
	var reduceId int
	var version int

	for {
		taskReply := CallTask(fileName, reduceId, version)
		if taskReply == nil || taskReply.Status == Finish {
			return
		}
		fileName = taskReply.MapFileName
		reduceId = taskReply.ReduceId
		version = taskReply.Version

		switch taskReply.Status {
		// map task
		case MapTask:
			mapTask(fileName, mapf, version)

		// reduce task
		case ReduceTask:
			reducetask(reducef, reduceId, taskReply.ReduceFileNames)
		case Wait:
			time.Sleep(time.Second)
		}
	}

}

func CallTask(fileName string, reduceId int, version int) *TaskReply {
	args := GetTask{
		FileName: fileName,
		ReduceId: reduceId,
		Version:  version,
	}

	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		return &reply
	}
	return nil
}

func mapTask(filename string, mapf func(string, string) []KeyValue, version int) {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	files := make(map[int]*os.File)
	encoders := make(map[int]*json.Encoder)

	for _, kv := range kva {
		hash := ihash(kv.Key) % nReduce
		outFile, ok := files[hash]
		if !ok {
			outFile, err = os.OpenFile(fmt.Sprintf("%s-%d-%d", filename, version, hash), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				log.Fatalf("cannot open temp file: %v", err)
			}
			files[hash] = outFile
			encoders[hash] = json.NewEncoder(outFile)
		}
		if err := encoders[hash].Encode(&kv); err != nil {
			log.Fatalf("cannot write to temp file: %v", err)
		}
	}

	for _, f := range files {
		f.Close()
	}
}

func reducetask(reducef func(string, []string) string, reducerId int, reduceFileNames []string) {
	intermediate := []KeyValue{}

	for _, str := range reduceFileNames {
		fileName := fmt.Sprintf("%s-%d", str, reducerId)
		file, err := os.Open(fileName)
		if err != nil {
			// log.Fatalf("cannot open %v", fileName)
			// log.Printf("cannot open %v", err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()

	}

	if len(intermediate) == 0 {
		return
	}

	// 排序中间结果
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	oname := "mr-out-" + strconv.Itoa(reducerId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create output file %v", oname)
	}
	defer ofile.Close()

	// 对每个不同的 key 调用 reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 写入输出
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
