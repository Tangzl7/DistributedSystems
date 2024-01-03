package raft

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
				} else if rf.nextIdxs[idx]-1 <= rf.LastLogIdx() {
					args.PrevLogIdx = rf.nextIdxs[idx]-1
				} else { // 这种情况是可能存在的
					args.PrevLogIdx = rf.LastLogIdx()
				}
				args.PrevLogTerm = rf.logs[args.PrevLogIdx-rf.lastIncludeIndex].Term
				if rf.nextIdxs[idx] <= rf.LastLogIdx() {
					args.Logs = append(args.Logs, rf.logs[rf.nextIdxs[idx]-rf.lastIncludeIndex:]...)
				}
				reply := HeartBeatReply{}
				rf.sendAppendEntries(idx, &args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

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
		for i:=commit+1; i<len(rf.logs) + rf.lastIncludeIndex; i++ {
			cnt := 1
			for j, _ := range rf.peers {
				if j != rf.me  && rf.matchIdxs[j]>=i && rf.logs[i-rf.lastIncludeIndex].Term == rf.term {
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
			for i:=args.PrevLogIdx-rf.lastIncludeIndex; i>=0; i-- {
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

func (rf *Raft) AppendEntries(args *HeartBeatArgs, reply *HeartBeatReply) {
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
		reply.XIdx = len(rf.logs) + rf.lastIncludeIndex - 1
		return
	}
	// term不同
	if args.PrevLogTerm != rf.logs[args.PrevLogIdx - rf.lastIncludeIndex].Term {
		reply.XTerm = rf.logs[args.PrevLogIdx - rf.lastIncludeIndex].Term
		reply.XIdx = args.PrevLogIdx - rf.lastIncludeIndex
		for i:=reply.XIdx; i>=rf.lastIncludeIndex; i-- {
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
		rf.logs = append(rf.logs[:args.PrevLogIdx+1-rf.lastIncludeIndex], args.Logs...)
	}
	if rf.commitIdx < args.LeaderCommit {
		rf.commitIdx = args.LeaderCommit
		if rf.commitIdx > rf.LastLogIdx() {
			rf.commitIdx = rf.LastLogIdx()
		}
		rf.commitCh <- struct{}{}
	}
}