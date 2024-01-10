package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

import "fmt"
import "time"
import "sort"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	appliedIdx map[int64]int
	results map[int]chan Op
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	ClientId int64
	CmdId int
	Type string
	Args interface{}
}

func (sc *ShardCtrler) appendEntry(cmd Op) bool {
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		return false
	}

	sc.mu.Lock()
	ch, ok := sc.results[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.results[index] = ch
	}
	sc.mu.Unlock()

	select {
	case op := <- sc.results[index]:
		return op == cmd
	case <- time.After(time.Millisecond * TimeOut):
		return false
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Type: "Join",
		Args: args,
	}
	
	ok := sc.appendEntry(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Type: "Leave",
		Args: args,
	}
	
	ok := sc.appendEntry(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Type: "Move",
		Args: args,
	}
	
	ok := sc.appendEntry(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Type: "Query",
		Args: args,
	}
	
	ok := sc.appendEntry(cmd)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		if args.Num >= 0 && args.Num < len(sc.configs) {
			reply.Config = sc.configs[args.Num].DeepCopy()
		} else {
			reply.Config = sc.configs[len(sc.configs)-1].DeepCopy()
		}
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) serve() {
	for {
		applyMsg := <-sc.applyCh
		index := applyMsg.CommandIndex
		cmd := applyMsg.Command.(Op)
		
		sc.mu.Lock()
		lastCmdId, exist := sc.appliedIdx[cmd.ClientId]
		if !exist || cmd.CmdId > lastCmdId {

			if cmd.Type == "Join" {
				if args, ok := cmd.Args.(JoinArgs); ok {
					sc.doJoin(args)
				} else {
					sc.doJoin(*(cmd.Args.(*JoinArgs)))
				}
			} else if cmd.Type == "Leave" {
				if args, ok := cmd.Args.(LeaveArgs); ok {
					sc.doLeave(args)
				} else {
					sc.doLeave(*(cmd.Args.(*LeaveArgs)))
				}
			} else if cmd.Type == "Move" {
				if args, ok := cmd.Args.(MoveArgs); ok {
					sc.doMove(args)
				} else {
					sc.doMove(*(cmd.Args.(*MoveArgs)))
				}
			} else if cmd.Type == "Query" {
				// noting to do
			} else {
				fmt.Printf("Unknown fault in server.go:serve\n")
			}

			if cmd.Type != "Query" {
				sc.appliedIdx[cmd.ClientId] = cmd.CmdId
			}
		}

		ch, exist := sc.results[index]
		if exist {
			select {
			case <- sc.results[index]:
			default:
			}
			ch <- cmd
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) doJoin(args JoinArgs) {
	cfg := sc.configs[len(sc.configs)-1].DeepCopy()
	cfg.Num ++

	// map的遍历顺序不固定，因此必须先对key进行排序
	gids := make([]int, 0)
	for gid := range args.Servers {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		cfg.Groups[gid] = args.Servers[gid]
		avg := NShards / len(cfg.Groups)
		cnts := make(map[int]int)

		if len(cfg.Groups) == 1 {
			for i:=0; i<len(cfg.Shards); i++ {
				cfg.Shards[i] = gid
			}
			continue
		}

		for i := range cfg.Groups {
			cnts[i] = 0
		}

		for i:=0; i<len(cfg.Shards); i++ {
			cnts[cfg.Shards[i]] ++
		}

		for i:=0; i<avg; i++ {
			maxGroupGid, minGroupGid := 0, 0
			maxGroupShs, minGroupShs := -1, NShards+1

			for j, cnt := range cnts {
				if cnt > maxGroupShs {
					maxGroupGid = j
					maxGroupShs = cnt
				} else if cnt == maxGroupShs && maxGroupGid < j {
					maxGroupGid = j
				}

				if cnt < minGroupShs {
					minGroupGid = j
					minGroupShs = cnt
				} else if cnt == minGroupShs && minGroupGid > j {
					minGroupGid = j
				}
			}

			if maxGroupShs - minGroupShs <= 1 {
				break
			}

			for j:=0; j<len(cfg.Shards); j++ {
				if cfg.Shards[j] == maxGroupGid {
					cfg.Shards[j] = minGroupGid
					cnts[maxGroupGid] --
					cnts[minGroupGid] ++
					break
				}
			}
		}
	}
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) doLeave(args LeaveArgs) {
	cfg := sc.configs[len(sc.configs)-1].DeepCopy()
	cfg.Num ++

	for i:=0; i<len(args.GIDs); i++ {
		delete(cfg.Groups, args.GIDs[i])
	}

	cnts := make(map[int]int)
	for i := range cfg.Groups {
		cnts[i] = 0
	}
	for i:=0; i<len(cfg.Shards); i++ {
		_, ok := cnts[cfg.Shards[i]]
		if ok {
			cnts[cfg.Shards[i]] ++
		}
	}

	for i:=0; i<len(args.GIDs); i++ {
		for j:=0; j<len(cfg.Shards); j++ {
			if cfg.Shards[j] == args.GIDs[i] {
				minGroupGid, minGroupShs := 0, NShards+1

				for k, cnt := range cnts {
					if cnt < minGroupShs {
						minGroupGid = k
						minGroupShs = cnt
					} else if cnt == minGroupShs && minGroupGid > k {
						minGroupGid = k
					}
				}

				cfg.Shards[j] = minGroupGid
				cnts[minGroupGid] ++
			}
		}
	}

	sc.configs = append(sc.configs, cfg)
}


func (sc *ShardCtrler) doMove(args MoveArgs) {
	cfg := sc.configs[len(sc.configs)-1].DeepCopy()
	cfg.Num ++
	cfg.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, cfg)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.appliedIdx = make(map[int64]int)
	sc.results = make(map[int]chan Op)

	go sc.serve()

	return sc
}
