package raft

import "log"
import "time"
import "math/rand"

// Debugging
const Debug = false

func RandTimeOut() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := time.Duration(r.Int63()) % TimeOut
	return t + TimeOut
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) LastLogIdx() {
	return rf.logs[len(rf.logs) - 1].Index
}