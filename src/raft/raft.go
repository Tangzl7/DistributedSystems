package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"time"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	NoBody = -1
	Follower = 0
	Candidate = 1
	Leader = 2
	TimeOut = 250 * time.Millisecond
	HeartBeatTime = 25 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term int
	Index int
	Cmd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state int
	term int
	voteCnt int
	votedFor int

	heartBeatCh chan struct{}
	grantVoteCh chan struct{}
	leaderCh chan struct{}

	commitIdx int
	appliedIdx int
	logs []Log
	nextIdxs []int
	matchIdxs []int

	commitCh chan struct{}
	applyCh chan ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm int
}

func (rf *Raft) GetMe() int {
	return rf.me
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) GetPersisterSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, voterFor int
	var logs []Log
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&term) != nil || d.Decode(&voterFor) != nil || d.Decode(&logs) != nil ||
		 d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("read persist fail\n")
	} else {
		rf.term = term
		rf.votedFor = voterFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.appliedIdx = lastIncludedIndex
		rf.commitIdx = lastIncludedIndex
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) initRaft(applyCh chan ApplyMsg) {
	// 这里也必须加锁，因为自己可能重启，但之前开启的协程还在运行
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Follower
	rf.term = 0
	rf.voteCnt = 0
	rf.votedFor = NoBody

	rf.heartBeatCh = make(chan struct{}, 100)
	rf.grantVoteCh = make(chan struct{}, 100)
	rf.leaderCh = make(chan struct{}, 100)
	
	rf.commitIdx = 0
	rf.appliedIdx = 0
	rf.commitCh = make(chan struct{}, 100)
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, Log{Term:0, Index:0})

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIdxs = make([]int, len(rf.peers))
	rf.matchIdxs = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIdxs[i] = rf.LastLogIdx() + 1
		rf.matchIdxs[i] = 0
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	
	index := -1
	term := rf.term
	isLeader := rf.state == Leader
	if isLeader {
		index = rf.LastLogIdx() + 1
		term = rf.term
		log := Log {
			Term: term,
			Index: index,
			Cmd: command,
		}
		rf.logs = append(rf.logs, log)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.state {
		case Follower:
			select {
			case <- rf.heartBeatCh:
			case <- rf.grantVoteCh:
			case <- time.After(RandTimeOut()):
				rf.state = Candidate
			}
			break
		case Candidate:
			rf.mu.Lock()
			rf.term ++
			rf.voteCnt = 1
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()

			go rf.boatcastRV()
			select {
			case <- rf.heartBeatCh:
				rf.state = Follower
			case <- rf.leaderCh:
				rf.mu.Lock() // 忘记加lock了
				rf.state = Leader
				rf.nextIdxs = make([]int, len(rf.peers))
				rf.matchIdxs = make([]int, len(rf.peers))
				for i:= range rf.peers {
					rf.nextIdxs[i] = rf.LastLogIdx() + 1
					rf.matchIdxs[i] = 0
				}
				rf.mu.Unlock()
			case <- time.After(RandTimeOut()):
			}
			break
		case Leader:
			rf.boatcastHB()
			time.Sleep(HeartBeatTime)
			break
		}
		// time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	for !rf.killed() {
		select {
		case <- rf.commitCh:
			rf.mu.Lock()
			for i:=rf.appliedIdx+1; i<=rf.commitIdx; i++ {
				msg := ApplyMsg {
					CommandIndex: rf.logs[i - rf.lastIncludedIndex].Index,
					CommandValid: true,
					Command: rf.logs[i - rf.lastIncludedIndex].Cmd,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.appliedIdx = i
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.initRaft(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commit()

	return rf
}
