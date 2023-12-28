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
//	"bytes"
	"sync"
	"time"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	NoBody = -1
	Follower = 0
	Candidate = 1
	Leader = 2
	TimeOut = 250 * time.Millisecond
	HeartBeatTime = 100 * time.Millisecond
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogTerm int
	LastLogIdx int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type HeartBeatArgs struct {
	Term int
	LeaderId int
	PrevLogTerm int
	PrevLogIdx int
	Logs []Log
	LeaderCommit int
}

type HeartBeatReply struct {
	Term int
	Sucess bool
	XTerm int // 冲突点日志的term
	XIdx int  // XTerm冲突日志位置
}


func (rf *Raft) initRaft(applyCh chan ApplyMsg) {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isLeader = rf.state == Leader
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
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	for !rf.killed() {
		select {
		case <- rf.commitCh:
			rf.mu.Lock()
			for i:=rf.appliedIdx; i<=rf.commitIdx; i++ {
				msg := ApplyMsg {
					CommandIndex: i,
					CommandValid: true,
					Command: rf.logs[i].Cmd,
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
