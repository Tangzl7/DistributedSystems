package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mapWaiting TaskInfoArray
	reduceWaiting TaskInfoArray
	mapRunning TaskInfoArray
	reduceRunning TaskInfoArray

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Schedule(args *TaskArgs, reply *TaskInfo) {
	// 是否有未分配的map任务
	mapTask := c.mapWaiting.Pop()
	if mapTask != nil {
		c.mapRunning.Push(mapTask)
		reply = &mapTask
		fmt.Println("map task schedule")
		return
	}
	// 是否有未分配的reduce任务
	reduceTask := c.reduceWaiting.Pop()
	if reduceTask != nil {
		c.reduceRunning.Push(reduceTask)
		reply = &reduceTask
		fmt.Println("reduce task schedule")
		return
	}
	// 任务已经分配完，但未执行完
	if this.mapRunning.Size() > 0 || this.reduceRunning.Size() > 0 {
		reply.State = TaskWait
		return
	}
	// 任务全部执行完
	reply.Start = TaskAllDone
	c.isDone = true
	return
}


func (c *Coordinator) TaskDone(args *TaskInfo) {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", are.FileIndex, args.FileName)
		c.mapRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete", args.PartIndex)
		c.reduceRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		fmt.Println("Task Done error")
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
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.isDone = false


	c.server()
	return &c
}
