package raft

// import "fmt"
// import "log"

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
	Sucess bool
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.logs = rf.logs[index-rf.lastIncludedIndex:]
	rf.lastIncludedTerm = rf.logs[0].Term
	rf.lastIncludedIndex = rf.logs[0].Index
	data := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Sucess = false
	if args.Term < rf.term {
		return
	}

	if rf.term < args.Term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = NoBody
	}

	rf.heartBeatCh <- struct{}{}
	rf.votedFor = args.LeaderId

	// follwer的快照更新
	if rf.lastIncludedTerm > args.LastIncludedTerm || (rf.lastIncludedTerm == args.LastIncludedTerm &&
		  rf.lastIncludedIndex > args.LastIncludedIndex) {
		rf.persist()
		return
	}
	// 快照相同，但nextIdxs可能落后，需要更新
	if (rf.lastIncludedTerm == args.LastIncludedTerm && rf.lastIncludedIndex == args.LastIncludedIndex) {
		reply.Sucess = true
		rf.persist()
		return
	}

	// leader的快照更新
	reply.Sucess = true
	snaps := args.LastIncludedIndex - rf.lastIncludedIndex
	if snaps >= len(rf.logs) {
		rf.logs = make([]Log, 1)
	} else {
		rf.logs = rf.logs[snaps:]
	}
	rf.logs[0].Term = args.LastIncludedTerm
	rf.logs[0].Index = args.LastIncludedIndex

	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	if rf.appliedIdx < rf.lastIncludedIndex {
		rf.appliedIdx = rf.lastIncludedIndex
	}
	if rf.commitIdx < rf.lastIncludedIndex {
		rf.commitIdx = rf.lastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	snapApplyMsg := ApplyMsg {
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm: args.LastIncludedTerm,
		Snapshot: args.Data,
	}
	rf.applyCh <- snapApplyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if reply.Sucess {
		rf.nextIdxs[server] = args.LastIncludedIndex + 1
		rf.matchIdxs[server] = rf.nextIdxs[server] - 1
	}
	return ok
}

