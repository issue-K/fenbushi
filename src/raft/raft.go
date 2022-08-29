package raft

import (
	"6.824/log"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a int, b int) int {
	return -max(-a, -b)
}

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

type logEntry struct {
	Command interface{} // 命令
	Term    int
}
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	mu1       sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 2A
	state       Role // 当前状态
	votes       int  // 作为候选人已获得的票数
	curTerm     int  // 当前任期
	votedFor    int  // 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	connectable bool // 本次计时器内,是否有收到leader的心跳包或是投票给别人过
	leaderIndex int  // leader的下标, 不为-1表示当前有leader
	stateChange chan int
	// 2B
	log         []logEntry
	commitIndex int           //已提交的最高索引条目
	lastApplied int           //已被应用到状态机上的最高索引条目
	nextIndex   []int         // 对于每台服务器, 应发送的下一个日志条目
	matchIndex  []int         // 对于每台服务器, 已知的复制到该节点的最高日志条目
	applyCh     chan ApplyMsg // 应用到状态机
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()

	var term int = rf.curTerm
	var isleader bool = (rf.state == leader)
	if rf.killed() == true {
		isleader = false
	}
	if isleader {
		log.Printf("[ think ] %d raft ( term = %d ) is leader\n", rf.me, rf.curTerm)
	}
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
	CandidateId  int // 该候选人id
	Term         int // 候选人的任期号
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// 投票响应
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool // 是否获得选票
	Term        int
}

// 接收方: 有人在请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	curTerm := rf.curTerm
	reply.Term = curTerm

	if args.Term > curTerm { // 任期号更大,更新自己的任期
		rf.FollowerInit(-1, args.Term)
	}

	var index int = len(rf.log) - 1
	var newLog bool = args.LastLogTerm > rf.log[index].Term || (args.LastLogTerm == rf.log[index].Term && args.LastLogIndex >= index)
	if args.Term < rf.curTerm { // 任期号更小,拒绝投票
		log.Printf("[Request Vote] %d 节点(任期%d) 拒绝投票给 %d 节点(任期%d)[任期号更小]\n", rf.me, rf.curTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
	} else if !newLog { // 日志比自己旧
		log.Printf("[Request Vote] %d 节点(任期%d) 拒绝投票给 %d 节点(任期%d)[日志更旧]\n", rf.me, rf.curTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId { // 已经给别人投票过了
		log.Printf("[Request Vote] %d 节点(任期%d) 拒绝投票给 %d 节点(任期%d)[已投票过]\n", rf.me, rf.curTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
	} else { // 成功
		rf.connectable = true
		log.Printf("[Request Vote] %d 节点(任期%d) 投票给 %d 节点(任期%d)\n", rf.me, rf.curTerm, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []logEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人已知的已提交最高日志条目
}
type AppendEntriesReply struct {
	Ack  bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Term int  // 当前任期，对于领导人而言 它会更新自己的任期
}

// 接收方: 有人在推送日志/心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.curTerm
	reply.Ack = false

	var index int = len(rf.log) - 1
	if args.Term < rf.curTerm { // 任期号都没自己新, 拒绝同步日志
		reply.Ack = false
	} else if index < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // 在对应位置上没有该条日志
		reply.Ack = false
	} else {
		log.Printf("[ appendEntries ] %d raft(任期%d) 接收 %d raft(任期%d) 的心跳包", rf.me, rf.curTerm, args.LeaderId, args.Term)
		rf.connectable = true
		// 开始同步日志
		reply.Ack = true
		//log.Printf("]\n", args.PrevLogIndex, len(rf.log))
		rf.log = rf.log[:args.PrevLogIndex+1] // 需要舍弃之后的日志
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		if args.LeaderCommit > rf.commitIndex {
			for j := rf.commitIndex + 1; j <= args.LeaderCommit && j < len(rf.log); j++ {
				//应用到状态机
				applyMsg := ApplyMsg{
					Command:      rf.log[j].Command,
					CommandValid: true,
					CommandIndex: j,
				}
				//	log.Printf("[ APPLYCH-----------------------] %d raft 提交 %d 日志\n", rf.me, j)
				rf.applyCh <- applyMsg
				rf.lastApplied = j
			}
			rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit) // 更新已提交日志的索引
		}
		rf.FollowerInit(args.LeaderId, args.Term)
	}

	if rf.curTerm < args.Term {
		rf.FollowerInit(-1, args.Term)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) CandidateStart() {
	rf.mu.Lock()
	rf.votes = 1
	peers := rf.peers
	me := rf.me
	curTerm := rf.curTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	log.Printf("[ request vote ] %d raft 开始请求投票\n", rf.me)
	var wg sync.WaitGroup
	wg.Add(len(peers) - 1)
	for index, _ := range peers {
		if index == me {
			continue
		}

		go func(id int) {
			defer wg.Done()
			args := &RequestVoteArgs{
				CandidateId:  me,
				Term:         curTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if rf.state != candidate {
				return
			}
			ok := rf.sendRequestVote(id, args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.state != candidate {
				return
			}
			if reply.VoteGranted { // 获得选票
				rf.votes++
				// 若选票多于一半, 那么自己已经成为ld, 需要周期的向其他节点发送心跳包
				if rf.votes >= len(rf.peers)/2+1 {
					rf.LeaderInit()
					rf.stateChange <- 1
				}
			} else {
				// 失败的情况下, 如果任期号更小, 直接回到follower状态
				if reply.Term > rf.curTerm {
					rf.FollowerInit(-1, reply.Term)
				}
			}
		}(index)
	}
	wg.Wait()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendAllAppendEntries() {
	log.Printf("[ LeaderInfo ] %d leader(任期%d) 开始发送心跳包\n", rf.me, rf.curTerm)

	rf.mu.Lock()
	peers := rf.peers
	curTerm := rf.curTerm
	me := rf.me
	nextIndex := rf.nextIndex
	logs := rf.log
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for index, _ := range peers {
		if index == me {
			continue
		}

		//	log.Printf("[leaderInfo] %d leader 发心跳包给 %d节点", rf.me, index)
		go func(id int) {
			entries := make([]logEntry, 0)
			if nextIndex[id] <= len(logs)-1 { // 只有当存在这条日志时才需要发送
				entries = append(entries, logs[nextIndex[id]])
			}
			//	log.Printf("[ DEBUG ] index = %d,nextIndex[id] = %d\n", rf.nextIndex[id]-1, rf.nextIndex[id])
			args := &AppendEntriesArgs{
				LeaderId:     me,
				Term:         curTerm,
				PrevLogIndex: nextIndex[id] - 1,          // 紧邻新日志条目之前的那个日志条目的索引
				PrevLogTerm:  logs[nextIndex[id]-1].Term, // 紧邻新日志条目之前的那个日志条目的任期
				Entries:      entries,                    // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
				LeaderCommit: commitIndex,                // 领导人已知的已提交最高日志条目
			}
			reply := &AppendEntriesReply{}
			if rf.state != leader {
				return
			}
			ok := rf.sendAppendEntries(id, args, reply)
			if !ok || rf.state != leader {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Ack == false {
				log.Printf("[ appendEntriesm reply ] %d raft(任期%d) 被 %d raft(任期%d) 拒绝", rf.me, rf.curTerm, id, reply.Term)
				if rf.curTerm < reply.Term { // 任期号旧
					rf.FollowerInit(-1, reply.Term) // 回到跟随者状态并更新任期号
				} else { // 日志冲突
					rf.nextIndex[id]--
				}
			} else {
				if len(entries) != 0 {
					rf.matchIndex[id] = rf.nextIndex[id]
					rf.nextIndex[id]++
				}
			}
		}(index)
	}
}

/*
the service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log. if this server isn't the leader, returns false. otherwise start the agreement and return immediately. there is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election. even if the Raft instance has been killed, this function should return gracefully.

 the first return value is the index that the command will appear at if it's ever committed.the second return value is the current term. the third return value is true if this server believes it is the leader.
使用Raft的服务(例如k/v服务器)想要对添加到Raft日志中的下一个命令启动协议。
如果这个服务器不是leader，返回false。否则启动协议并立即返回。
不能保证这个命令会被提交到Raft日志，因为leader可能会失败或输掉选举。
即使Raft实例已经被杀死，该函数也应该优雅地返回。
第一个返回值是命令提交后出现的索引。第二个返回值是当前项。如果服务器认为它是leader，则第三个返回值为true。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := (rf.me == rf.leaderIndex)
	if isLeader == true {
		log.Printf("[写入日志......................] %v\n", command)
		logf := logEntry{Command: command, Term: rf.curTerm}
		rf.log = append(rf.log, logf)
		rf.matchIndex[rf.me]++
		rf.nextIndex[rf.me]++
		index = len(rf.log) - 1
		term = rf.curTerm
	}
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
	//rf.mu1.Lock()
	//rf.mu1.Unlock()

	rf.state = leader
	rf.votes = 0
	rf.leaderIndex = rf.me
	le := len(rf.peers)
	logLen := len(rf.log)
	for i := 0; i < le; i++ {
		rf.nextIndex[i] = logLen
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)

	log.Printf("[ leader ]%d raft,term = %v\n", rf.me, rf.curTerm)
}

// 当超时, 自己就需要开展新的任期, 开始获取选票
func (rf *Raft) CandidateInit() {
	//rf.mu1.Lock()
	//defer rf.mu1.Unlock()

	rf.state = candidate
	rf.votes = 1 // 自己先投自己一票
	rf.votedFor = rf.me
	rf.curTerm++ // 任期+1
	rf.leaderIndex = -1

	log.Printf("[ candidate ]%d raft, term = %v\n", rf.me, rf.curTerm)
}
func (rf *Raft) FollowerInit(leaderId int, leaderTerm int) {
	//rf.mu1.Lock()
	//defer rf.mu1.Unlock()

	rf.state = follower
	rf.votes = 0
	if leaderTerm > rf.curTerm {
		rf.curTerm = leaderTerm
		rf.votedFor = -1
	}
	rf.leaderIndex = leaderId

	log.Printf("[ follower ]%d raft, term = %v\n", rf.me, rf.curTerm)
}

func (rf *Raft) LeaderStart() {
	// 周期性的向其他节点发心跳包
	tick := time.NewTicker(time.Millisecond * 100)
	for {
		<-tick.C

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != leader {
			go rf.ticker()
			return
		}
		rf.sendAllAppendEntries()
		// 开始检查自己的日志是否已被提交

		rf.mu.Lock()
		temp := make([]int, 0)
		for j, _ := range rf.matchIndex {
			temp = append(temp, rf.matchIndex[j])
		}
		sort.Ints(temp)
		ed := len(rf.peers) - 1     // 末尾索引
		half := len(rf.peers)/2 + 1 // 多于一半
		rf.commitIndex = temp[ed-half+1]

		for j := rf.lastApplied + 1; j <= rf.commitIndex; j++ {
			//应用到状态机
			applyMsg := ApplyMsg{
				Command:      rf.log[j].Command,
				CommandValid: true,
				CommandIndex: j,
			}
			//	log.Printf("[ APPLYCH-----------------------] %d raft 提交 %d 日志\n", rf.me, j)
			rf.applyCh <- applyMsg
			rf.lastApplied = j
		}
		rf.mu.Unlock()
	}
}

// todo: 在计时器超时时, 如果已经给别人投票过, 自己是否需要变成新的candidate?
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.connectable = false
		x := randTime(600, 1000)
		tick := time.NewTimer(time.Millisecond * time.Duration(x))
		// 三者其一
		for {
			var flag bool = false
			select {
			case <-tick.C: // 定时器到时间, 自己需要发起选举
				if rf.connectable == false { // 没收到任何消息(心跳包/投票给别人)
					rf.mu.Lock()
					rf.CandidateInit()
					rf.mu.Unlock()
				}
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
			go rf.LeaderStart()
			break
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
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		leaderIndex: -1,
		curTerm:     1,
		stateChange: make(chan int, 1),
		// 2B
		log:         make([]logEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}
	rf.FollowerInit(-1, 1)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.log[0] = logEntry{Term: 0, Command: struct{}{}} // 默认携带一条空日志
	log.Printf("%d raft start.........\n", me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
