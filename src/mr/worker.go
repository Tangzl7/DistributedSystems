package mr

import "fmt"
import "log"
import "time"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
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
			fmt.Println("map operation start")
			break
		case TaskReduce:
			// 执行reduce操作
			fmt.Println("reduce operation start")
			break
		case TaskWait:
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
	call("Coordinator.TaskDone", taskInfo)
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
