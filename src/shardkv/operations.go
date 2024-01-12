package shardkv

import "time"

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
	}
	val, exist := kv.shardsKvDB[shard].KvDB[args.Key]
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrShardNotArrived {
		return
	}

	if !exist {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = val
	}
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

	err := kv.appendEntry(cmd)
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
	reply.Err = err
}

func (kv *ShardKV) appendEntry(cmd Op) Err {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	ch, ok := kv.results[index]
	if !ok {
		ch = make(chan OpReply, 1)
		kv.results[index] = ch
	}
	kv.mu.Unlock()

	select {
	case opReply := <- kv.results[index]:
		if opReply.ClientId != cmd.ClientId || opReply.CmdId != cmd.CmdId {
			return ErrInconsistentData
		}
		return opReply.Err
	case <- time.After(time.Millisecond * TimeOut):
		return ErrTimeOut
	}
}