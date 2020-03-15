package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "6.824/src/labrpc"
import "6.824/src/labgob"
import "time"
import "math/rand"
import "sort"
import "bytes"
//import "fmt"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
    HBINTERVAL = 100 * time.Millisecond 
)

//选举超时: 300-500ms
func getRandomElectionTime() time.Duration {
    return (time.Duration(rand.Intn(3))+3)*100*time.Millisecond
}

type LogEntry struct {
    Term    int
    Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32                 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    currentTerm int
    votedFor    int
    log         []LogEntry
    commitIndex int
    lastApplied int
    nextIndex   []int
    matchIndex  []int

    state   int
    applyCh chan ApplyMsg
    //获得票数
    votes   int

    electionTimer *time.Timer
    pingTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state==LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
    err := e.Encode(rf.currentTerm)
    if err!=nil {
        DPrintf("raft %d, encode currentTerm err: %v",rf.currentTerm,err)
    }
    err = e.Encode(rf.votedFor)
     if err!=nil {
        DPrintf("raft %d, encode votedFor err: %v",rf.currentTerm,err)
    }

    err = e.Encode(rf.log)
    if err!=nil {
        DPrintf("raft %d, encode log err: %v",rf.currentTerm,err)
    }

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
    var currentTerm int
	var votedFor    int
    var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
	    d.Decode(&votedFor) != nil || d.Decode(&log)!=nil {
	    DPrintf("decode error")
	} else {
	    rf.currentTerm = currentTerm
	    rf.votedFor = votedFor
        rf.log = log
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term    int
    VoteGranted bool
}

//日志是否比当前的新
func upToDate(currTerm int, currIndex int, dstTerm int, dstIndex int) bool {
    if currTerm!=dstTerm {
        return dstTerm>currTerm
    }
    return dstIndex>=currIndex
}

func getMajoritySameIndex(matchIndex []int) int {
	tmp := make([]int, len(matchIndex))
	copy(tmp, matchIndex)

	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))

	idx := len(tmp) / 2
	return tmp[idx]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm>args.Term {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }
    //fmt.Println("here??")
    //无论什么状态，立即变成FOLLOWER
    if rf.currentTerm<args.Term {
        rf.becomeFollower(args.Term)
        rf.persist()
    }

    //fmt.Printf("raft:%d, votedFor:%d\n",rf.me, rf.votedFor)
    //fmt.Printf("currTerm:%d, currIndex:%d, argsTerm:%d, argsIndex:%d\n",rf.currentTerm,len(rf.log)-1,args.LastLogTerm,args.LastLogIndex)
    if (rf.votedFor==-1 || rf.votedFor==args.CandidateId) && upToDate(rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex){
        reply.Term = rf.currentTerm
        reply.VoteGranted = true
        rf.votedFor = args.CandidateId
        rf.persist()
    } else {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
    }
    return
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogTerm     int
    PrevLogIndex    int
    Entries         []LogEntry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term        int
    Success     bool
}

func minInt(a, b int) int {
    if a<b {
        return a
    }
    return b
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if args.Term<rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    //收到来自LEADER的信息，有两种情况：
    //1.当前是CANDIDATE，变成FOLLOWER
    //2.当前是FOLLOWER，重置计时
    rf.becomeFollower(args.Term)
    rf.persist()
    //与PrevLogIndex,PrevLogTerm不匹配，删除PrevLogIndex之后的log
    if args.PrevLogIndex>=len(rf.log) || rf.log[args.PrevLogIndex].Term!=args.PrevLogTerm {
        if args.PrevLogIndex < len(rf.log) {
            rf.log = rf.log[0:args.PrevLogIndex]
            rf.persist()
        }
        return
    }
    rf.log = append(rf.log[0:args.PrevLogIndex+1],args.Entries...)
    rf.persist()
    if args.LeaderCommit>rf.commitIndex {
        //prevIndex := rf.commitIndex
        rf.commitIndex = minInt(args.LeaderCommit,len(rf.log)-1)
    }
    DPrintf("raft %d, log len:%d, commitIndex:%d,lastAppliedIndex:%d",rf.me,len(rf.log),rf.commitIndex,rf.lastApplied)
    reply.Term = rf.currentTerm
    reply.Success = true
    return
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
	ok := rf.peers[server].Call("Raft.RequestVote", *args, reply)
    //fmt.Printf("server %d, voteReply: %t\n", server,reply.VoteGranted)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if reply.Term>rf.currentTerm {
        rf.becomeFollower(reply.Term)
        rf.persist()
    }
    //状态已经发生变化，直接返回
    if rf.state != CANDIDATE || rf.currentTerm!=args.Term {
        return ok
    }
    if ok==false {
        return ok
    }
    if reply.VoteGranted==true {
        rf.votes++
        if rf.votes>len(rf.peers)/2 {
            rf.becomeLeader()
        }
    }
	return ok
}

func (rf *Raft) sendLogEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries",*args,reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if reply.Term > rf.currentTerm {
        rf.becomeFollower(reply.Term)
        rf.persist()
    }
    //状态发生变化，直接返回
    if rf.state != LEADER || rf.currentTerm != args.Term {
        return ok
    }

    id := server
	if reply.Success {
  		rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries) // do not depend on len(rf.log)
  		rf.nextIndex[id] = rf.matchIndex[id] + 1
        DPrintf("raft %d, matchIndex %+v",rf.me,rf.matchIndex)

  		majorityIndex := getMajoritySameIndex(rf.matchIndex)
  		if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
    		rf.commitIndex = majorityIndex
    		DPrintf("raft %d advance commit index to %d\n", rf.me, rf.commitIndex)
  		}
	} else {
  		prevIndex := args.PrevLogIndex
  		for prevIndex > 0 && rf.log[prevIndex].Term == args.PrevLogTerm {
    		prevIndex--
  		}
  		rf.nextIndex[id] = prevIndex + 1
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

    rf.mu.Lock()
    defer rf.mu.Unlock()
    state := ""
    if rf.state==LEADER {
        state = "LEADER"
    } else if rf.state==CANDIDATE {
        state = "CANDIDATE"
    } else {
        state = "FOLLOWER"
    }
    DPrintf("raft %d, currentTerm:%d, state:%s, get commmand:%+v",rf.me,rf.currentTerm,state,command)
    if rf.state!=LEADER {
        return 0,0,false
    }
    rf.log = append(rf.log,LogEntry{Term:rf.currentTerm, Command:command})
    rf.persist()
    index = len(rf.log)-1
    term = rf.log[index].Term
    isLeader = true
    DPrintf("raft %d get command, log len %d, commitIndex %d",rf.me,len(rf.log),rf.commitIndex)
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
    rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.commitIndex = 0
    rf.lastApplied = 0
    serversNum := len(rf.peers)
    rf.nextIndex = make([]int,serversNum)
    rf.matchIndex = make([]int,serversNum)
    rf.state = FOLLOWER

    rf.log = append(rf.log, LogEntry{Term:0,Command:nil})
	rf.electionTimer = time.NewTimer(getRandomElectionTime())
	rf.pingTimer = time.NewTimer(HBINTERVAL)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
	go rf.electionLoop()
    go rf.applyLoop()

	return rf
}

func (rf *Raft) becomeCandidate() {
    rf.state = CANDIDATE
    rf.votedFor = rf.me
    rf.currentTerm++
    rf.votes = 1
    DPrintf("raft: %d, term: %d become candidate\n", rf.me,rf.currentTerm)
}

func (rf *Raft) becomeFollower(term int) {
    rf.state = FOLLOWER
    rf.votedFor = -1
    rf.currentTerm = term
    rf.electionTimer.Reset(getRandomElectionTime())
    DPrintf("raft: %d, term: %d become follower\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
    rf.state = LEADER
    for i:=0; i<len(rf.peers); i++ {
        rf.matchIndex[i] = 0
        rf.nextIndex[i] = len(rf.log)
    }
    rf.pingTimer.Reset(HBINTERVAL)
    go rf.pingLoop()
    DPrintf("raft: %d, term: %d become leader\n", rf.me,rf.currentTerm)
}


func (rf *Raft) electionLoop() {    
    for {
        <-rf.electionTimer.C
        rf.electionTimer.Reset(getRandomElectionTime())
        rf.mu.Lock()
        if rf.state==LEADER {
            rf.mu.Unlock()
            continue
        }

        rf.becomeCandidate()
        rf.persist()
        for i:=0;i<len(rf.peers);i++ {
            if i==rf.me {
                continue
            }
            args := &RequestVoteArgs {
                Term:           rf.currentTerm,
                CandidateId:    rf.me,
                LastLogIndex:   len(rf.log)-1,
                LastLogTerm:    rf.log[len(rf.log)-1].Term,
            }

           // fmt.Printf("here1 %d\n",args.Term)
            reply := &RequestVoteReply {
                Term:           0,
                VoteGranted:    false,
            }
            go rf.sendRequestVote(i,args,reply)
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) pingLoop() {
    for {
        rf.mu.Lock()
        if rf.state != LEADER {
            rf.mu.Unlock()
            return
        }
        //append entries to each peer
        for peerId := range rf.peers {
            if peerId==rf.me {
                rf.nextIndex[peerId] = len(rf.log)
                rf.matchIndex[peerId] = len(rf.log)-1
                continue
            }
            prevLogIndex := rf.nextIndex[peerId]-1
            appendEntriesArgs := &AppendEntriesArgs {
                Term:               rf.currentTerm,
                LeaderId:           rf.me,
                PrevLogIndex:       prevLogIndex,
                PrevLogTerm:        rf.log[prevLogIndex].Term,
                Entries:            rf.log[rf.nextIndex[peerId]:],
                LeaderCommit:       rf.commitIndex,
            }
			appendEntriesReply := &AppendEntriesReply {
				Term:		0,
				Success:	false,			
			}
            go rf.sendLogEntries(peerId,appendEntriesArgs,appendEntriesReply)
        }
        rf.mu.Unlock()
        <-rf.pingTimer.C
        rf.pingTimer.Reset(HBINTERVAL)
        DPrintf("raft %d start next ping round\n",rf.me)
    }
}

func (rf *Raft) applyLoop() {
    for {
        time.Sleep(time.Duration(rand.Intn(5)+3)*10*time.Millisecond)
        //rf.mu.Lock()
        DPrintf("raft %d start apply, commitIndex %d, lastApplied %d",rf.me,rf.commitIndex,rf.lastApplied)
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            rf.apply(rf.lastApplied,rf.log[rf.lastApplied])
        }
        //rf.mu.Unlock()
    }
}

func (rf *Raft) apply(applyIndex int, entry LogEntry) {
    rf.applyCh <- ApplyMsg{
        CommandValid:   true,
        Command:        entry.Command,
        CommandIndex:   applyIndex,
    }
    DPrintf("raft %d, apply cammand: %d", rf.me, applyIndex)
}
