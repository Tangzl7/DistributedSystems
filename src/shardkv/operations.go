package shardkv

import "time"
import "6.824/labrpc"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsKvDB[shard].KvDB == nil {
		reply.Err = ErrShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrShardNotArrived {
		return
	}
	
	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Key: args.Key,
		Type: "Get",
	}

	err := kv.appendEntry(cmd)
	if err != OK {
		reply.Err = err
		return
	}

	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsKvDB[shard].KvDB == nil {
		reply.Err = ErrShardNotArrived
	} else {
		val, exist := kv.shardsKvDB[shard].KvDB[args.Key]
		if !exist {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = val
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	DPrintf("%v %v %v %v %v", args.ClientId, args.CmdId, kv.config.Shards[shard], kv.gid, kv.me)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsKvDB[shard].KvDB == nil {
		reply.Err = ErrShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrShardNotArrived {
		return
	}

	cmd := Op {
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Key: args.Key,
		Value: args.Value,
		Type: args.Op,
	}

	reply.Err = kv.appendEntry(cmd)
}

func (kv *ShardKV) AddShard(args *ShardArgs, reply *ShardReply) {
	cmd := Op {
		AppliedIdx: args.AppliedIdx,
		ShardId: args.ShardId,
		Shard: args.Shard,
		ClientId: args.ClientId,
		CmdId: args.CmdId,
		Type: "AddShard",
	}
	reply.Err = kv.appendEntry(cmd)
}

func (kv *ShardKV) appendEntry(cmd Op) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch, ok := kv.results[index]
	if !ok {
		ch = make(chan OpReply, 1)
		kv.results[index] = ch
	}
	kv.mu.Unlock()

	// DPrintf("append %v", cmd.Type)

	select {
	case opReply := <- ch:
		kv.mu.Lock()
		delete(kv.results, index)
		if opReply.ClientId != cmd.ClientId || opReply.CmdId != cmd.CmdId {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return opReply.Err
	case <- time.After(time.Millisecond * TimeOut):
		return ErrTimeOut
	}
}

func (kv *ShardKV) sendAddShard(servers []*labrpc.ClientEnd, args *ShardArgs) {
	index, start := 0, time.Now()
	for {
		reply := ShardReply{}
		ok := servers[index].Call("ShardKV.AddShard", args, &reply)

		if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
			kv.mu.Lock()
			cmd := Op {
				Type: "RemoveShard",
				ClientId: int64(kv.gid),
				CmdId: kv.config.Num,
				ShardId: args.ShardId,
			}
			kv.mu.Unlock()
			kv.appendEntry(cmd)
			break
		}
		index = (index + 1) % len(servers)
		if index == 0 {
			time.Sleep(UpConfigInterval)
		}
	}
}

func (kv *ShardKV) addShardHandler(cmd Op) {
	if cmd.Shard.ConfigIdx < kv.config.Num || kv.shardsKvDB[cmd.ShardId].KvDB != nil {
		return
	}

	kv.shardsKvDB[cmd.ShardId] = kv.cloneShard(cmd.Shard.ConfigIdx, cmd.Shard.KvDB)

	for clientId, appliedId := range cmd.AppliedIdx {
		if id, ok := kv.appliedIdx[clientId]; !ok || id < appliedId {
			kv.appliedIdx[clientId] = appliedId
		}
	}
	DPrintf("add successful\n")
}

func (kv *ShardKV) removeShardHandler(cmd Op) {
	if cmd.CmdId < kv.config.Num {
		return
	}
	kv.shardsKvDB[cmd.ShardId].KvDB = nil
	kv.shardsKvDB[cmd.ShardId].ConfigIdx = cmd.CmdId
	DPrintf("remove successful\n")
}

func (kv *ShardKV) upConfigHandler(cmd Op) {
	DPrintf("Update Config 1\n")
	config := kv.config
	upConfig := cmd.UpConfig
	if config.Num >= upConfig.Num {
		return
	}

	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && config.Shards[shard] == 0 {
			kv.shardsKvDB[shard].KvDB = make(map[string]string)
			kv.shardsKvDB[shard].ConfigIdx = upConfig.Num
		}
	}
	kv.lastConfig = config
	kv.config = upConfig
	DPrintf("Update Config 2\n")
}

func (kv *ShardKV) cloneShard(configIdx int, KvDB map[string]string) Shard{
	shard := Shard {
		KvDB: make(map[string]string),
		ConfigIdx: configIdx,
	}

	for k, v := range KvDB {
		shard.KvDB[k] = v
	}
	return shard
}