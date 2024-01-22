package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrShardNotArrived = "ErrShardNotArrived"
	ErrConfigNotArrived = "ErrConfigNotArrived"
	ErrInconsistentData = "ErrInconsistentData"
	ErrTimeOut = "ErrTimeOut"
	TimeOut = 500 * time.Millisecond
	UpConfigInterval = 100 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CmdId int
}

type GetReply struct {
	Err   Err
	Value string
}


type ShardArgs struct {
	AppliedIdx map[int64]int
	ShardId int
	Shard Shard
	ClientId int64
	CmdId int
}

type ShardReply struct {
	Err Err
}

