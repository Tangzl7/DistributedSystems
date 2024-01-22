package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "log"
import "time"
import "bytes"
import "sync/atomic"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Shard struct {
	KvDB map[string]string
	ConfigIdx int
}

// 既作为Cmd，也作为rpc参数内容
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdId int
	Key string
	Value string
	Type string

	ConfigIdx int
	UpConfig shardctrler.Config
	ShardId int
	Shard Shard
	AppliedIdx map[int64]int
}

type OpReply struct {
	ClientId int64
	CmdId int
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dead int32
	sck *shardctrler.Clerk

	config shardctrler.Config
	lastConfig shardctrler.Config

	shardsKvDB []Shard
	appliedIdx map[int64]int
	results map[int]chan OpReply
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) serve() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			index := applyMsg.CommandIndex
			cmd := applyMsg.Command.(Op)
			reply := OpReply {
				ClientId: cmd.ClientId,
				CmdId: cmd.CmdId,
				Err: OK,
			}
	
			kv.mu.Lock()
			lastCmdId, exist := kv.appliedIdx[cmd.ClientId]
			DPrintf("me %v, type %v, cmdid %v applied %v\n", kv.me, cmd.Type, cmd.CmdId, lastCmdId)
			if cmd.Type == "Put" || cmd.Type == "Append" || cmd.Type == "Get" {
				shard := key2shard(cmd.Key)
				if kv.config.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if kv.shardsKvDB[shard].KvDB == nil {
					reply.Err = ErrShardNotArrived
				} else if !exist || cmd.CmdId > lastCmdId {
					kv.appliedIdx[cmd.ClientId] = cmd.CmdId
		
					if cmd.Type == "Put" {
						kv.shardsKvDB[shard].KvDB[cmd.Key] = cmd.Value
					} else if cmd.Type == "Append" {
						kv.shardsKvDB[shard].KvDB[cmd.Key] += cmd.Value
					}
				}
			} else {
				switch cmd.Type {
				case "AddShard":
					if kv.config.Num < cmd.CmdId {
						reply.Err = ErrConfigNotArrived
						break
					}
					kv.addShardHandler(cmd)
				case "RemoveShard":
					kv.removeShardHandler(cmd)
				case "UpConfig":
					kv.upConfigHandler(cmd)
				}
			}

			if kv.maxraftstate != -1 && kv.rf.GetPersisterSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.shardsKvDB)
				e.Encode(kv.appliedIdx)
				e.Encode(kv.lastConfig)
				e.Encode(kv.config)
				data := w.Bytes()
				kv.rf.Snapshot(index, data)
			}
	
			ch, ok := kv.results[index]
			if !ok {
				ch = make(chan OpReply, 1)
				kv.results[index] = ch
			}
			ch <- reply
			kv.mu.Unlock()
		} else {
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)

			kv.mu.Lock()
			kv.shardsKvDB = make([]Shard, shardctrler.NShards)
			kv.appliedIdx = make(map[int64]int)
			var config, lastConfig shardctrler.Config
			d.Decode(&kv.shardsKvDB)
			d.Decode(&kv.appliedIdx)
			d.Decode(&lastConfig)
			d.Decode(&config)
			kv.config = config
			kv.lastConfig = lastConfig
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) updataConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			time.Sleep(UpConfigInterval)
			continue
		}
		kv.mu.Unlock()

		kv.mu.Lock()
		// 把自己的分给别人
		if !kv.allSent() {
			appliedIdx := make(map[int64]int)
			for k, v := range kv.appliedIdx {
				appliedIdx[k] = v
			}
			for shard, gid := range kv.lastConfig.Shards {
				if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.shardsKvDB[shard].ConfigIdx < kv.config.Num {
					shardData := kv.cloneShard(kv.config.Num, kv.shardsKvDB[shard].KvDB)
					args := ShardArgs {
						AppliedIdx: appliedIdx,
						ShardId: shard,
						Shard: shardData,
						ClientId: int64(gid),
						CmdId: kv.config.Num,
					}

					serversList := kv.config.Groups[kv.config.Shards[shard]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}
					go kv.sendAddShard(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigInterval)
			continue
		}
		config := kv.config
		sck := kv.sck
		kv.mu.Unlock()

		upConfig := sck.Query(config.Num + 1)
		if upConfig.Num != config.Num + 1 {
			time.Sleep(UpConfigInterval)
			continue
		}

		cmd := Op {
			Type: "UpConfig",
			ClientId: int64(kv.gid),
			CmdId: upConfig.Num,
			UpConfig: upConfig,
		}
		kv.appendEntry(cmd)
		// kv.rf.Start(cmd)
	}
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.shardsKvDB[shard].ConfigIdx < kv.config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid != kv.gid && kv.config.Shards[shard] == kv.gid && kv.shardsKvDB[shard].ConfigIdx < kv.config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db []Shard
	var appliedIdx map[int64]int
	var lastConfig shardctrler.Config
	var config shardctrler.Config

	if d.Decode(&db) != nil || d.Decode(&appliedIdx) != nil || d.Decode(&lastConfig) != nil || d.Decode(&config) != nil {
		DPrintf("readSnapshot err\n")
	} else {
		kv.shardsKvDB = db
		kv.appliedIdx = appliedIdx
		kv.lastConfig = lastConfig
		kv.config = config
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.sck = shardctrler.MakeClerk(kv.ctrlers)

	kv.shardsKvDB = make([]Shard, shardctrler.NShards)
	kv.appliedIdx = make(map[int64]int)
	kv.results = make(map[int]chan OpReply)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	
	kv.readSnapshot(kv.rf.GetPersister().ReadSnapshot())

	go kv.serve()
	go kv.updataConfig()

	return kv
}
