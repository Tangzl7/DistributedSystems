package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"time"
	"bytes"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


// 对应于raft中的cmd
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdId int
	Key string
	Value string
	Type string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB map[string]string
	appliedIdx map[int64]int
	results map[int]chan Op
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Key: args.Key,
		Type: "Get",
	}

	ok := kv.appendEntry(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	val, exist := kv.kvDB[args.Key]
	kv.mu.Unlock()

	if !exist {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = val
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Key: args.Key,
		Value: args.Value,
		Type: args.Op,
	}

	ok := kv.appendEntry(cmd)
	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) appendEntry(cmd Op) bool {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.results[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.results[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <- kv.results[index]:
		return op == cmd
	case <- time.After(time.Millisecond * TimeOut):
		return false
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) serve() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			cmd := applyMsg.Command.(Op)
	
			kv.mu.Lock()
			lastCmdId, exist := kv.appliedIdx[cmd.ClientId]
			if !exist || cmd.CmdId > lastCmdId {
				kv.appliedIdx[cmd.ClientId] = cmd.CmdId
	
				if cmd.Type == "Put" {
					kv.kvDB[cmd.Key] = cmd.Value
				} else if cmd.Type == "Append" {
					kv.kvDB[cmd.Key] += cmd.Value
				}
			}

			if kv.maxraftstate != -1 && kv.rf.GetPersisterSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvDB)
				e.Encode(kv.appliedIdx)
				data := w.Bytes()
				kv.rf.Snapshot(index, data)
			}
	
			ch, exist := kv.results[index]
			if exist {
				select {
				case <- kv.results[index]:
				default:
				}
				ch <- cmd
			}
			kv.mu.Unlock()
		} else {
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)

			kv.mu.Lock()
			kv.kvDB = make(map[string]string)
			kv.appliedIdx = make(map[int64]int)
			d.Decode(&kv.kvDB)
			d.Decode(&kv.appliedIdx)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var appliedIdx map[int64]int

	if d.Decode(&db) != nil || d.Decode(&appliedIdx) != nil {
		DPrintf("readSnapshot err\n")
	} else {
		kv.kvDB = db
		kv.appliedIdx = appliedIdx
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.appliedIdx = make(map[int64]int)
	kv.results = make(map[int]chan Op)

	kv.readSnapshot(kv.rf.GetPersister().ReadSnapshot())

	go kv.serve()

	return kv
}
