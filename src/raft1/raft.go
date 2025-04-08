package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int // 0 follower 1 candidate 2 leader

	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat   time.Time
	electionTimeout time.Duration

	// tickerCancel context.CancelFunc
	// runCancel    context.CancelFunc
	stateCh chan struct{}
}

const (
	//心跳包发送间隔
	HEARTBEATINTERVAL = 120 * time.Millisecond

	// 选举超时时间为 3倍心跳间隔-5倍心跳间隔
	MINELECTIONTIMEOUT = 3 * HEARTBEATINTERVAL //75 * time.Millisecond
	MAXELECTIONTIMEOUT = 5 * HEARTBEATINTERVAL //125 * time.Millisecond

	LEADERINTERVAL = 150 * time.Millisecond

	REQUESTVOTEINTERVAL = 120 * time.Millisecond
)

type Log struct {
	Command string
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
// 获取当前状态 加读锁
// 返回当前的 term 和 是否为 leader
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	// DPrintf("node: %v read lock\n", rf.me)
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term := rf.currentTerm
	isleader := rf.state == 2
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	// DPrintf("node: %v lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// 如果请求的term 小于 自身的 term ，拒绝投票
	// 返回 this 的 term ，告知 对面更新的 term
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// DPrintf("node: %v lock\n", rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// 如果请求的term 大于 自身的 term ，更新自己的term，将状态转为 follower
	if args.Term > rf.currentTerm {
		// DPrintf("node: %v lock\n", rf.me)
		rf.votedFor = -1
		if rf.state != 0 {
			rf.convertToFollower(args.Term)
		} else {
			rf.currentTerm = args.Term
		}
	}
	// 如果已经投过票且投票的对象不是该请求的发起者，拒绝投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// // 判断请求投票的candidate的log是否落后于自身 ，
	// // Term Priority > Index Priority
	// index := len(rf.logs) - 1
	// // 如果 candidate 的 LastLogTerm < 自身的 LastLogTerm = rf.logs[index].term
	// // 说明 candidate 的 log 没有 this 的新
	// if args.LastLogTerm < rf.logs[index].term {
	// 	reply.VoteGranted = false
	// } else if args.LastLogTerm == rf.logs[index].term {
	// 	// 如果 candidate 的 LastLogTerm == this 的 LastLogTerm，
	// 	// 则比较 candidate 的 LastLogIndex 是否 大于等于 this 的 LastLogIndex
	// 	if args.LastLogIndex < index {
	// 		reply.VoteGranted = false
	// 	}
	// }

	reply.Term = rf.currentTerm // 回复 currentTerm
	reply.VoteGranted = true    // 投票成功
	// DPrintf("node: %v lock\n", rf.me)
	rf.votedFor = args.CandidateId // 将 this 的投票对象设置为当前请求的发起者
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) convertToFollower(term int) {
	// rf.mu.Lock()
	DPrintf("node: %v convert to follower\n", rf.me)
	rf.state = 0
	rf.currentTerm = term
	rf.lastHeartbeat = time.Now()
	rf.stateCh <- struct{}{}
	// rf.mu.Unlock()
	// DPrintf("node: %v unlock in convert to follower\n", rf.me)
}

func (rf *Raft) runAsFollower() {
	DPrintf("node: %v run as follower in term %v\n", rf.me, rf.currentTerm)
	<-rf.stateCh
}

func (rf *Raft) convertToCanditate() {
	// rf.mu.Lock()
	DPrintf("node: %v convert to candidate\n", rf.me)
	rf.state = 1
	rf.currentTerm += 1
	rf.electionTimeout = GetRandomElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.stateCh <- struct{}{}
	// rf.mu.Unlock()
	// DPrintf("node: %v unlock in convert to candidate\n", rf.me)
}

func (rf *Raft) runAsCandidate() {
	DPrintf("node: %v run as candidate in term %v\n", rf.me, rf.currentTerm)
	go rf.startElection()
	<-rf.stateCh

}

func (rf *Raft) convertToLeader() {
	// rf.mu.Lock()
	DPrintf("node: %v convert to leader\n", rf.me)
	rf.state = 2
	rf.stateCh <- struct{}{}
	// rf.mu.Unlock()
	// DPrintf("node: %v unlock in convert to leader\n", rf.me)
}

func (rf *Raft) runAsLeader() {
	DPrintf("node: %v run as leader in term %v\n", rf.me, rf.currentTerm)
	// 定时发送 心跳包
	ctx, cancel := context.WithCancel(context.Background())
	go rf.sendHeartbeat(ctx)
	<-rf.stateCh
	cancel()
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	// DPrintf("node: %v read lock in start election in term %v\n", rf.me, rf.currentTerm)
	term := rf.currentTerm
	candidateId := rf.me
	// lastLogIndex := rf.commitIndex
	// lastLogTerm := rf.logs[lastLogIndex].term
	rf.votedFor = rf.me
	rf.mu.Unlock()
	// DPrintf("node: %v read unlock in start election in term %v\n", rf.me, rf.currentTerm)

	var numOfVote int32 = 1
	cond := sync.NewCond(&rf.mu)

	for p := range rf.peers {
		if p == candidateId {
			continue
		}
		// DPrintf("node: %v read lock\n", rf.me)
		rf.mu.RLock()
		if rf.state != 1 {
			rf.mu.RUnlock()
			return
		}
		rf.mu.RUnlock()

		go func(server int) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateId: candidateId,
				// LastLogIndex: lastLogIndex,
				// LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			for {
				rf.mu.RLock()
				if rf.state != 1 {
					rf.mu.RUnlock()
					return
				}
				rf.mu.RUnlock()
				DPrintf("node: %v send request vote to node  %v in term %v\n", rf.me, server, term)
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					break
				}
				<-time.After(REQUESTVOTEINTERVAL)
			}
			// DPrintf("node: %v send request vote to node  %v\n", rf.me, server)

			// DPrintf("node: %v read lock\n", rf.me)
			rf.mu.Lock()
			// defer rf.mu.Unlock()
			DPrintf("node: %v receive request vote reply from node  %v : VoteGranted : %v , reply term : %v\n", rf.me, server, reply.VoteGranted, reply.Term)
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == 1 {
				// DPrintf("node: %v vote to %v \n", server, rf.me)
				atomic.AddInt32(&numOfVote, 1)
				if int(numOfVote) > len(rf.peers)/2 {
					rf.convertToLeader()
					cond.Broadcast()
				}
			}
			rf.mu.Unlock()
		}(p)

	}
	// DPrintf("node: %v lock\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.state == 1 && int(numOfVote) <= len(rf.peers)/2 {
		cond.Wait()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// 如果prevLogIndex 的Log 的Term 不等于 prevLogTerm，return false
	// DPrintf("node: %v read lock\n", rf.me)
	// DPrintf("node %v recive append from node %v , term : %v \n", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.state != 0 {
		rf.convertToFollower(args.Term)
		// DPrintf("node: %v read lock\n", rf.me)
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	// DPrintf("node: %v lock\n", rf.me)
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			rf.mu.RLock()
			state := rf.state
			if state != 2 {
				return
			}
			term := rf.currentTerm
			leaderId := rf.me
			rf.mu.RUnlock()

			// var wg sync.WaitGroup
			for p := range rf.peers {
				if p == rf.me {
					continue
				}
				// DPrintf("node: %v read lock\n", rf.me)

				// wg.Add(1)
				go func(server int, term int, leaderId int) {
					// defer wg.Done()
					args := AppendEntriesArgs{
						Term:     term,
						LeaderId: leaderId,
					}
					reply := AppendEntriesReply{}

					rf.mu.RLock()
					if rf.state != 2 {
						rf.mu.RUnlock()
						return
					}
					rf.mu.RUnlock()
					// DPrintf("node %v send append to node %v with term %v\n", rf.me, server, term)
					ok := rf.sendAppendEntries(server, &args, &reply)
					if !ok {
						return
					}
					// DPrintf("node: %v read lock\n", rf.me)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					}
					rf.mu.Unlock()
				}(p, term, leaderId)
			}
			// wg.Wait()
			time.Sleep(HEARTBEATINTERVAL)
		}

	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 获取一段随机的选举超时时间
func GetRandomElectionTimeout() time.Duration {
	// return time.Duration(
	// 	MINELECTIONTIMEOUT.Microseconds()+
	// 		rand.Int63n(
	// 			MAXELECTIONTIMEOUT.Microseconds()-MINELECTIONTIMEOUT.Microseconds(),
	// 		),
	// ) * time.Millisecond
	return MINELECTIONTIMEOUT + time.Duration(rand.Int63n(int64(MAXELECTIONTIMEOUT-MINELECTIONTIMEOUT)))
}

func (rf *Raft) ticker() {
	// DPrintf("node: %v start ticker\n", rf.me)
	ms := 50 + (rand.Int63() % 300)
	sleepTime := time.Duration(ms) * time.Millisecond
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		<-time.After(sleepTime)
		// DPrintf("node: %v read lock in tricker\n", rf.me)
		rf.mu.Lock()
		if rf.state == 2 {
			// DPrintf("node: %v read unlock in tricker\n", rf.me)
			sleepTime = LEADERINTERVAL
			rf.mu.Unlock()
			continue
		}
		elapsed := time.Since(rf.lastHeartbeat)
		timeout := rf.electionTimeout
		// DPrintf("node: %v elapsed: %v timeout: %v\n", rf.me, elapsed, timeout)

		if elapsed > timeout {
			// DPrintf("node: %v read lock in tricker timeout\n", rf.me)
			// 发起选举
			DPrintf("node: %v timeout\n", rf.me)
			// 变成 candidate
			rf.convertToCanditate()
		}
		ms = 50 + (rand.Int63() % 300)
		sleepTime = time.Duration(ms) * time.Millisecond
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// rf.mu = sync.Mutex{}

	// for p := range peers {
	// 	DPrintf(" perr  %v\n", p)
	// }

	// DPrintf("node: %v start\n", me)

	// Your initialization code here (3A, 3B, 3C).
	rf.state = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.electionTimeout = GetRandomElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.stateCh = make(chan struct{})
	rf.lastHeartbeat = time.Now()
	// start ticker goroutine to start elections

	// DPrintf("node %v start\n", rf.me)
	go rf.run()

	go rf.ticker()

	return rf
}

func (rf *Raft) run() {
	for !rf.killed() {
		// rf.mu.Lock()
		// ctx, cancel := context.WithCancel(context.Background())
		// rf.runCancel = cancel
		// rf.mu.Unlock()
		rf.mu.RLock()
		// DPrintf("node: %v read lock in run\n", rf.me)
		currentState := rf.state

		switch currentState {
		case 0:
			{
				rf.mu.RUnlock()
				// DPrintf("node: %v read unlock in run follower\n", rf.me)
				// rf.runAsFollower(ctx)
				// DPrintf("node %v run as follower\n", rf.me)
				rf.runAsFollower()
			}
		case 1:
			{
				rf.mu.RUnlock()
				// DPrintf("node: %v read unlock in run candidate\n", rf.me)
				// rf.runAsCandidate(ctx)
				// DPrintf("node %v run as candidate\n", rf.me)
				rf.runAsCandidate()
			}
		case 2:
			{
				rf.mu.RUnlock()
				// DPrintf("node: %v read unlock in run leader\n", rf.me)
				// rf.runAsLeader(ctx)
				// DPrintf("node %v run as leader\n", rf.me)
				rf.runAsLeader()
			}
		}
	}
}
