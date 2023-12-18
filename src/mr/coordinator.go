package mr

import "log"
import "net"
import "os"
import "fmt"
import "time"
import "net/rpc"
import "net/http"

const TIMEOUT = 30

type Coordinator struct {
	// Your definitions here.
	mapWaiting TaskInfoArray
	reduceWaiting TaskInfoArray
	mapRunning TaskInfoArray
	reduceRunning TaskInfoArray

	isDone bool
	nFile int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Schedule(args *TaskArgs, reply *TaskInfo) error {
	// 是否有未分配的map任务
	mapTask := c.mapWaiting.Pop()
	if mapTask != nil {
		mapTask.EndTime = time.Now().Unix() + TIMEOUT
		c.mapRunning.Push(mapTask)
		*reply = *mapTask
		fmt.Println("map task schedule")
		return nil
	}
	// 是否有未分配的reduce任务
	reduceTask := c.reduceWaiting.Pop()
	if reduceTask != nil {
		reduceTask.EndTime = time.Now().Unix() + TIMEOUT
		c.reduceRunning.Push(reduceTask)
		*reply = *reduceTask
		fmt.Println("reduce task schedule")
		return nil
	}
	// 任务已经分配完，但未执行完
	if c.mapRunning.Size() > 0 || c.reduceRunning.Size() > 0 {
		reply.State = TaskWait
		return nil
	}
	// 任务全部执行完
	reply.State = TaskAllDone
	c.isDone = true
	return nil
}


func (c *Coordinator) TaskDone(args *TaskInfo, reply *TaskInfo) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		c.mapRunning.Remove(args.FileIndex, -1)
		if c.mapRunning.Size() == 0 && c.mapWaiting.Size() == 0 {
			c.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		c.reduceRunning.Remove(-1, args.PartIndex)
		break
	default:
		fmt.Println("Task Done error")
	}
	return nil
}

func (c *Coordinator) distributeReduce() {
	for i:=0; i<c.nReduce; i++ {
		taskInfo := &TaskInfo {
			State: TaskReduce,
			NFile: c.nFile,
			PartIndex: i,
		}
		c.reduceWaiting.Push(taskInfo)
	}
}

func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(2 * time.Second)
		if c.mapRunning.Size() > 0 {
			taskArray := c.mapRunning.TimeOut()
			for _, taskInfo := range taskArray {
				c.mapWaiting.Push(&taskInfo)
			}
		}
		if c.reduceRunning.Size() > 0 {
			taskArray := c.reduceRunning.TimeOut()
			for _, taskInfo := range taskArray {
				c.reduceWaiting.Push(&taskInfo)
			}
		}
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func (c *Coordinator) InitCoordinator(files []string, nReduce int) {
	c.nFile = len(files)
	c.nReduce = nReduce
	for i:=0; i<len(files); i++ {
		taskInfo := &TaskInfo {
			State: TaskMap,
			FileName: files[i],
			FileIndex: i,
			NReduce: nReduce,
		}
		c.mapWaiting.Push(taskInfo)
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.isDone = false
	c.InitCoordinator(files, nReduce)
	go c.checkTimeOut()

	c.server()
	return &c
}
