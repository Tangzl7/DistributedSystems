package mr

import "os"
import "fmt"
import "log"
import "time"
import "sort"
import "strconv"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
func (b ByKey) Len() int {return len(b)}
func (b ByKey) Less(i int, j int) bool {return b[i].Key < b[j].Key}
func (b ByKey) Swap(i int, j int) {b[i], b[j] = b[j], b[i]}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func workMap(taskInfo TaskInfo, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := taskInfo.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for idx:=0; idx<nReduce; idx++ {
		outFiles[idx], _ = ioutil.TempFile("./", "mr-tmp-*")
		fileEncs[idx] = json.NewEncoder(outFiles[idx])
	}
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		enc := fileEncs[idx]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	for idx, file := range outFiles {
		outPath := fmt.Sprintf("./mr-%v-%v", taskInfo.FileIndex, idx)
		err := os.Rename(file.Name(), outPath)
		if err != nil {
			fmt.Printf("Rename tempfile failed for %v\n", outPath)
			panic("Rename tempfile failed")
		}
		file.Close()
	}
	CallTaskDone(&taskInfo)
}

func workReduce(taskInfo TaskInfo, reducef func(string, []string) string) {
	prefix := "./mr-"
	suffix := "-" + strconv.Itoa(taskInfo.PartIndex)
	intermediate := []KeyValue{}
	for idx:=0; idx<taskInfo.NFile; idx++ {
		filePath := prefix + strconv.Itoa(idx) + suffix
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", filePath, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	outFile, _ := ioutil.TempFile(".", "mr-*")
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k:=i; k<j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outPath := "./mr-out-" + strconv.Itoa(taskInfo.PartIndex)
	os.Rename(outFile.Name(), outPath)
	outFile.Close()
	CallTaskDone(&taskInfo)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		taskInfo := CallScheduleTask()
		switch taskInfo.State {
		case TaskMap:
			// 执行map操作
			fmt.Println(taskInfo.FileName)
			workMap(*taskInfo, mapf)
			break
		case TaskReduce:
			// 执行reduce操作
			workReduce(*taskInfo, reducef)
			break
		case TaskWait:
			fmt.Println("Wait for task")
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskAllDone:
			fmt.Println("All tasks completed")
			return
		default:
			fmt.Println("Task information error")
		}
	}
}

// 申请分配任务
func CallScheduleTask() *TaskInfo {
	args := TaskArgs{}
	reply := TaskInfo{}
	call("Coordinator.Schedule", &args, &reply)
	return &reply
}

// 完成工作后汇报
func CallTaskDone(taskInfo *TaskInfo) {
	reply := TaskInfo{}
	call("Coordinator.TaskDone", taskInfo, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
