package raft

// import "log"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogTerm int
	LastLogIdx int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) boatcastRV() {
	rf.mu.Lock()
	args := RequestVoteArgs {
		Term: rf.term,
		CandidateId: rf.me,
		LastLogTerm: rf.LastLogTerm(),
		LastLogIdx: rf.LastLogIdx(),
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go func(idx int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(idx, &args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}

	if rf.state != Candidate || args.Term != rf.term {
		return ok
	}

	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.state = Follower
		rf.votedFor = NoBody
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.voteCnt ++
		if rf.state == Candidate && rf.voteCnt > len(rf.peers) / 2 {
			rf.state = Leader
			rf.leaderCh <- struct{}{}
		}
	}

	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	reply.Term = rf.term

	if rf.term > args.Term {
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = NoBody
	}

	log_flag := false
	if args.LastLogTerm > rf.logs[rf.LastLogIdx() - rf.lastIncludedIndex].Term {
		log_flag = true
	}
	if args.LastLogTerm == rf.logs[rf.LastLogIdx() - rf.lastIncludedIndex].Term && args.LastLogIdx >= rf.LastLogIdx() {
		log_flag = true
	}

	if (rf.votedFor == NoBody || rf.votedFor == args.CandidateId) && log_flag == true {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.grantVoteCh <- struct{}{}
	}
}
