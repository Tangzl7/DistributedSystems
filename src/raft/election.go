package raft

func (rf *Raft) boatcastRV() {
	rf.mu.Lock()
	args := RequestVoteArgs {
		Term: rf.term,
		CandidateId: rf.me,
		LastLogTerm: rf.logs[len(rf.logs) - 1].Term,
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

func (rf *Raft) boatcastHB() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.state == Leader {
			go func(idx int) {
				args := HeartBeatArgs {
					Term: rf.term,
					LeaderId: rf.me,
					LeaderCommit: rf.commitIdx,
				}

				if rf.nextIdxs[idx] < 1 {
					args.PrevLogIdx = 0
				} else if rf.nextIdxs[idx]-1 <= rf.LastLogIdx() { // 这种情况是可能存在的
					args.PrevLogIdx = rf.nextIdxs[idx]-1
				} else {
					args.PrevLogIdx = rf.LastLogIdx()
				}
				args.PrevLogTerm = rf.logs[args.PrevLogIdx].Term
				if rf.nextIdxs[idx] <= rf.LastLogIdx() {
					args.Logs = append(args.Logs, rf.logs[rf.nextIdxs[idx]:]...)
				}
				reply := HeartBeatReply{}
				rf.sendHeartBeat(idx, &args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.term {
		return ok
	}

	if reply.Term > rf.term {
		rf.state = Follower
		rf.term = reply.Term
		rf.votedFor = NoBody
		rf.persist()
		return ok
	}

	if reply.Sucess {
		rf.nextIdxs[server] = args.PrevLogIdx + len(args.Logs) + 1
		rf.matchIdxs[server] = rf.nextIdxs[server] - 1

		commit := rf.commitIdx
		// 之前想直接找到第一个不匹配的位置，它之前的就全是匹配的了，但这样存在错误，应为cnt++的条件包含了
		// term的判断，即不匹配的位置可能是term不同，这样的不匹配时可以接受的
		for i:=commit+1; i<len(rf.logs); i++ {
			cnt := 1
			for j, _ := range rf.peers {
				if j != rf.me  && rf.matchIdxs[j]>=i && rf.logs[i].Term == rf.term {
					cnt ++
				}
			}
			if cnt > len(rf.peers) / 2 {
				commit = i
				break
			}
		}
		if commit > rf.commitIdx {
			rf.commitIdx = commit
			rf.commitCh <- struct{}{}
		}
	} else {
		if reply.XTerm == -1 || reply.XTerm == -2 {
			rf.nextIdxs[server] = reply.XIdx + 1
		} else  {
			rf.nextIdxs[server] = reply.XIdx
			for i:=args.PrevLogIdx; i>=0; i-- {
				if rf.logs[i].Term == reply.XTerm {
					rf.nextIdxs[server] = i + 1
					break
				}
				if rf.logs[i].Term < reply.XTerm {
					break
				}
			}
		}
	}

	return ok
}

func (rf *Raft) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

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

	// 要覆盖该Follower已经提交了的日志
	if args.PrevLogIdx + 1 <= rf.commitIdx {
		reply.XTerm = -1
		reply.XIdx = rf.commitIdx
		return
	}
	// 越界，要覆盖的位置大于已有日志长度
	if args.PrevLogIdx > rf.LastLogIdx() {
		reply.XTerm = -2
		reply.XIdx = len(rf.logs) - 1
		return
	}
	// term不同
	if args.PrevLogTerm != rf.logs[args.PrevLogIdx].Term {
		reply.XTerm = rf.logs[args.PrevLogIdx].Term
		reply.XIdx = args.PrevLogIdx
		for i:=args.PrevLogIdx; i>=0; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIdx = i + 1
				break
			}
		}
		return
	}

	// 没有异常
	reply.Sucess = true
	if args.Logs != nil { // 必须加这个，防止加入空
		rf.logs = append(rf.logs[:args.PrevLogIdx+1], args.Logs...)
	}
	if rf.commitIdx < args.LeaderCommit {
		rf.commitIdx = args.LeaderCommit
		if rf.commitIdx > rf.LastLogIdx() {
			rf.commitIdx = rf.LastLogIdx()
		}
		rf.commitCh <- struct{}{}
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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
	if args.LastLogTerm > rf.logs[rf.LastLogIdx()].Term {
		log_flag = true
	}
	if args.LastLogTerm == rf.logs[rf.LastLogIdx()].Term && args.LastLogIdx >= rf.LastLogIdx() {
		log_flag = true
	}

	if (rf.votedFor == NoBody || rf.votedFor == args.CandidateId) && log_flag == true {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.grantVoteCh <- struct{}{}
	}
}
