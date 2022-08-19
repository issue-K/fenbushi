package raft

import (
	"6.824/log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role string

const (
	follower  = "follower"
	candidate = "candidate"
	leader    = "leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       Role // 当前状态
	votes       int  // 作为候选人已获得的票数
	curTerm     int  // 当前任期
	hasVote     bool // 是否有票
	leaderIndex int  // leader的下标, 不为-1表示当前有leader
	stateChange chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()

	var term int = rf.curTerm
	var isleader bool = (rf.leaderIndex == rf.me)

	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 投票请求
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From     int // 该候选人id
	FromTerm int // 候选人的任期号
}

// 投票响应
type RequestVoteReply struct {
	// Your data here (2A).
	Vote bool // 是否获得选票
	Term int
}

// 接收方: 有人在请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.FromTerm > rf.curTerm && rf.hasVote {
		log.Printf("[Request Vote] %d 节点(任期%d) 投票给 %d 节点(任期%d)\n", rf.me, rf.curTerm, args.From, args.FromTerm)
		rf.mu.Lock()
		rf.mu.Unlock()
		reply.Vote = true
	} else {
		log.Printf("[Request Vote] %d 节点(任期%d) 拒绝投票给 %d 节点(任期%d)\n", rf.me, rf.curTerm, args.From, args.FromTerm)
		reply.Vote = false
		reply.Term = rf.curTerm
	}
}

type AppendEntriesArgs struct {
	From     int
	FromTerm int
}
type AppendEntriesReply struct {
	Ack  bool // 是否成功接收
	Term int
}

// 接收方: 有人在推送日志/心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 只要发送者的任期更新,就接收
	if args.FromTerm >= rf.curTerm {
		log.Printf("[ AppendEntries ] %d 节点(任期%d) 接收 %d leader(任期%d)的心跳包\n", rf.me, rf.curTerm, args.From, args.FromTerm)
		rf.FollowerInit(args.From, args.FromTerm)
		rf.stateChange <- 1
		reply.Ack = true
	} else {
		log.Printf("[ AppendEntries ] %d 节点(任期%d) 拒绝 %d leader(任期%d)的心跳包\n", rf.me, rf.curTerm, args.From, args.FromTerm)
		reply.Ack = false
		reply.Term = args.FromTerm
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) CandidateStart() {
	rf.votes = 1
	log.Printf("[ request vote ] %d raft 开始请求投票\n", rf.me)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		if rf.state != candidate {
			break
		}
		go func(id int) {
			args := &RequestVoteArgs{
				From:     rf.me,
				FromTerm: rf.curTerm,
			}
			reply := &RequestVoteReply{
				Vote: false,
			}
			if rf.state != candidate {
				return
			}
			rf.sendRequestVote(id, args, reply)
			if rf.state != candidate {
				return
			}
			if reply.Vote { // 获得选票
				rf.mu.Lock()
				rf.votes++
				rf.mu.Unlock()
				// 若选票多于一半, 那么自己已经成为ld, 需要周期的向其他节点发送心跳包
				if rf.state != candidate {
					return
				}
				if rf.votes >= len(rf.peers)/2+1 {
					rf.LeaderInit()
					rf.stateChange <- 1
				}
			} else {
				rf.FollowerInit(-1, reply.Term)
			}
		}(index)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendAllAppendEntries() {
	log.Printf("[ LeaderInfo ] %d leader 开始发送心跳包\n", rf.me)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		if rf.state != leader {
			break
		}
		//	log.Printf("[leaderInfo] %d leader 发心跳包给 %d节点", rf.me, index)
		go func(id int) {
			args := &AppendEntriesArgs{From: rf.me, FromTerm: rf.curTerm}
			reply := &AppendEntriesReply{}
			if rf.state != leader {
				return
			}
			ok := rf.sendAppendEntries(id, args, reply)
			if rf.state != leader {
				return
			}
			// todo: 是否需要根据reply.Ack判断,本次消息再次发送?
			if reply.Ack == false && ok {
				log.Printf("[ refuse ] %d raft 被拒绝接收心跳/日志\n", rf.me)
				rf.FollowerInit(-1, reply.Term)
			}
		}(index)
	}
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
	term := rf.curTerm
	isLeader := (rf.me == rf.leaderIndex)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func randTime(l int, r int) int {
	return l + rand.Intn(r-l)
}
func (rf *Raft) LeaderInit() {
	rf.mu.Lock()

	rf.state = leader
	rf.leaderIndex = rf.me

	rf.mu.Unlock()
	log.Printf("[ leader ]%d raft,term = %v\n", rf.me, rf.curTerm)
}

// 当超时, 自己就需要开展新的任期, 开始获取选票
func (rf *Raft) CandidateInit() {
	rf.mu.Lock()

	rf.state = candidate
	rf.votes = 1 // 自己先投自己一票
	rf.hasVote = false
	rf.curTerm++ // 任期+1
	rf.leaderIndex = -1

	rf.mu.Unlock()
	log.Printf("[ candidate ]%d raft, term = %v\n", rf.me, rf.curTerm)
}
func (rf *Raft) FollowerInit(leaderId int, leaderTerm int) {
	rf.mu.Lock()

	rf.state = follower
	rf.votes = 0
	rf.hasVote = true
	if leaderId != -1 {
		rf.leaderIndex = leaderId
	}
	rf.curTerm = leaderTerm
	rf.mu.Unlock()

	log.Printf("[ follower ]%d raft, term = %v\n", rf.me, rf.curTerm)
}

func (rf *Raft) LeaderStart() {
	// 周期性的向其他节点发心跳包
	tick := time.NewTicker(time.Millisecond * 50)
	for {
		<-tick.C
		rf.sendAllAppendEntries()
		if rf.state != leader {
			break
		}
	}
}

func (rf *Raft) ticker() {
	log.Printf("[ ticker ] %d raft 开始ticker.......\n", rf.me)
	rf.FollowerInit(-1, 1)
	for rf.killed() == false {

		x := randTime(600, 1000)
		tick := time.NewTimer(time.Millisecond * time.Duration(x))
		// 三者其一
		for {
			var flag bool = false
			select {
			case <-tick.C: // 定时器到时间, 自己需要发起选举
				rf.CandidateInit()
				flag = true
			case <-rf.stateChange:
				flag = true
			}
			if flag {
				break
			}
		}
		if rf.state == candidate { // 没有大多数票
			go rf.CandidateStart() // 请求投票
		} else if rf.state == leader {
			rf.LeaderStart()
		} else if rf.state == follower {
			continue
		}
	}
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
	rf.leaderIndex = -1
	rf.curTerm = 1
	rf.stateChange = make(chan int, 1)
	log.Printf("%d raft start.........\n", me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
