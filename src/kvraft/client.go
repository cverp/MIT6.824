package kvraft

import "6.824/src/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"


type Clerk struct {
    mu          sync.Mutex
	servers     []*labrpc.ClientEnd
	// You will have to modify this struct.
    leader      int
    clientId    int64
    seq         int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
    ck.leader = 0
    ck.clientId = nrand()
    ck.seq = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    ck.mu.Lock()
    args := GetArgs{}
    args.Key = key
    args.ClientId = ck.clientId
    args.Seq = ck.seq
    ck.seq++
    reply := GetReply{}

    lastleader := ck.leader
    ck.mu.Unlock()
    for i:=0;;i++{
        serverid := (lastleader + i)%len(ck.servers)
        ok := ck.servers[serverid].Call("KVServer.Get",&args,&reply)
        if ok && reply.Err==OK {
            ck.mu.Lock()
            ck.leader = serverid
            ck.mu.Unlock()
            return reply.Value
        } else {
        }
    }
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    ck.mu.Lock()
    args := PutAppendArgs{
        Key:        key,
        Value:      value,
        ClientId:   ck.clientId,
        Seq:        ck.seq,
        Op:         op,
    }
    ck.seq++
    reply := PutAppendReply{}

    lastleader := ck.leader
    ck.mu.Unlock()
    <-time.After(time.Second)
    for i:=0;;i++ {
        serverid := (i+lastleader)%len(ck.servers)
        ok := ck.servers[serverid].Call("KVServer.PutAppend",&args,&reply)
        if ok && reply.Err==OK {
            ck.leader = serverid
            DPrintf("%s key %s,value %s succeed",op,key,value)
            break
        } else {
            DPrintf("serverid %d, err: %s",serverid,reply.Err)
        }
    }
    return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
