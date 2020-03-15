package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"log"
	"6.824/src/raft"
	"sync"
	"sync/atomic"
    "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Command string
    ClientId int64
    Seq     int
    Key     string
    Value   string
}

func (op *Op) equals(cmd Op) bool {
    if op.Command==cmd.Command && op.ClientId==cmd.ClientId && op.Seq==cmd.Seq && op.Key==cmd.Key && op.Value==cmd.Value {
        return true
    }
    return false
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here
    database    map[string]string
    cliSeq      map[int64]int
    commandCh   map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    command := Op{
        ClientId:   args.ClientId,
        Seq:        args.Seq,
        Key:        args.Key,
        Value:      "",
    }
    kv.mu.Lock()
    seq,ok := kv.cliSeq[args.ClientId]
    //该命令已经被执行过
    if ok && args.Seq<=seq {
        reply.Err = OK
        reply.Value = kv.database[args.Key]
        kv.mu.Unlock()
        return
    }
    index,_,isleader := kv.rf.Start(command)
    if isleader==false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }
    ch := kv.getCommandCh(index)
    defer delete(kv.commandCh,index)
    kv.mu.Unlock()
    op := Op{}
    select {
    case op = <- ch:
        reply.Err = OK
        reply.Value = kv.database[args.Key]
    case <- time.After(1000*time.Millisecond):
        reply.Err = ErrWrongLeader
        return
    }
    //该命令可能没有被提交，因此接收到的该index下的命令可能不正确
    if !op.equals(command) {
        reply.Err = ErrWrongLeader
        reply.Value = ""
    }
    return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    command := Op{
        Command:    args.Op,
        ClientId:   args.ClientId,
        Seq:        args.Seq,
        Key:        args.Key,
        Value:      args.Value,
    }
    kv.mu.Lock()
    seq,ok := kv.cliSeq[args.ClientId]
    if ok && args.Seq<=seq {
        reply.Err = OK
        kv.mu.Unlock()
        return
    }
    index,_,isleader := kv.rf.Start(command)
    if isleader==false {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }
    ch := kv.getCommandCh(index)
    kv.mu.Unlock()
    op := Op{}
    select {
    case op = <- ch:
        reply.Err = OK
    case <- time.After(1000*time.Millisecond):
        reply.Err = ErrWrongLeader
    }
    if !op.equals(command) {
        reply.Err = ErrWrongLeader
    }
    return
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
    kv.database = make(map[string]string,0)
    kv.cliSeq = make(map[int64]int,0)
    kv.commandCh = make(map[int]chan Op,0)

    //若通道容量为0会发生阻塞
	kv.applyCh = make(chan raft.ApplyMsg,100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
    go kv.startReceive()

	return kv
}

func (kv *KVServer) getCommandCh(index int) chan Op {
    ch,ok := kv.commandCh[index]
    if !ok {
        kv.commandCh[index] = make(chan Op,5)
        ch,_ = kv.commandCh[index]
    }
    return ch
}

func (kv *KVServer)startReceive() {
    for {
        if kv.killed() {
            return
        }
        msg := <- kv.applyCh
        commandIndex := msg.CommandIndex
        op := msg.Command.(Op)
      
        //多个线程读写的变量需要加锁
         kv.mu.Lock()
        seq, ok := kv.cliSeq[op.ClientId]
        if ok && op.Seq<=seq {
            DPrintf("duplicated seq %d",seq)
            kv.mu.Unlock()
            continue
        }
        kv.cliSeq[op.ClientId] = op.Seq
        kv.mu.Unlock()
        //kv.database不会发生竞争?
        switch op.Command {
        case "Get":
            DPrintf("server %d apply GET, key:%s",kv.me,op.Key)
        case "Put":
            DPrintf("server %d apply PUT, key:%s, value:%s",kv.me,op.Key,op.Value)
            kv.database[op.Key] = op.Value
        case "Append":
            DPrintf("server %d apply APPEND, key:%s,value:%s",kv.me,op.Key,op.Value)
            value,_ := kv.database[op.Key]
            kv.database[op.Key] = value + op.Value
        default:
            DPrintf("server %d: %s not support",kv.me, op.Command)
        }
        kv.mu.Lock()
        kv.getCommandCh(commandIndex)<-op
        kv.mu.Unlock()
    }
}


