package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "sync"
import "strconv"

const (
	TaskMap = 0
	TaskReduce = 1
	TaskWait = 2
	TaskAllDone = 3
)
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {

}

// worker和coordinator之间交换的任务信息
type TaskInfo struct {
	State int
	FileName string
	FileIndex int
	PartIndex int
}

// 任务信息的数组
type TaskInfoArray struct {
	taskArray []TaskInfo
	mutex sync.Mutex
}
func (this *TaskInfoArray) lock() {
	this.mutex.Lock()
}
func (this *TaskInfoArray) unlock() {
	this.mutex.Unlock()
}
func (this *TaskInfoArray) Size() int {
	return len(this.taskArray)
}
func (this *TaskInfoArray) Pop() TaskInfo {
	this.lock()
	arrayLen := len(this.taskArray)
	if arrayLen == 0 {
		this.unlock()
		return nil
	}
	ret := this.taskArray[arrayLen - 1]
	this.taskArray = this.taskArray[:arrayLen-1]
	this.unlock()
	return ret
}
func (this *TaskInfoArray) Push(taskInfo TaskInfo) {
	this.lock()
	if taskInfo == nil {
		this.unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskInfo)
	this.unlock()
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
