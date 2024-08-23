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

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type Entry struct {
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有都有的
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int

	status  int
	applyCh chan ApplyMsg
	// 仅leader有的
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.status == LEADER {
		isleader = true
	} else {
		isleader = false
	}
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
	CandidateID  int
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm || (args.Term == currentTerm && rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// 更新任期号和重新开始选举计时
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 重置已投票的候选人
	}

	// 2. 检查是否已经投票给其他候选人或者当前日志是否比候选人的新
	isLogUpToDate := (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && isLogUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// for 3A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm { //leader的term比自己小，假的leader，不处理
		reply.Success = false
		reply.Term = currentTerm
		return
	}
	rf.status = FOLLOWER        //收到leader的消息，自己变成follower
	rf.votedFor = args.LeaderID //有争议
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex == 0 {
			fmt.Println("fuck")
		}
		return
	}
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	rf.currentTerm = args.Term
	//更新commitIndex
	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1
	term = rf.currentTerm
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		currentStatus := rf.status
		rf.mu.Unlock()

		switch currentStatus {
		case FOLLOWER:
			rf.mu.Lock()
			rf.votedFor = -1
			rf.status = CANDIDATE
			rf.mu.Unlock()
			time.Sleep(time.Duration(200) * time.Millisecond)

		case CANDIDATE:
			rf.election()
		case LEADER:
			//初始化
			rf.mu.Lock()
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyCh <- msg
			}
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				entries := append([]Entry{}, rf.log[rf.nextIndex[i]:]...)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				go rf.handleAppendEntries(i, args)
				rf.mu.Unlock()
			}
			time.Sleep(110 * time.Millisecond) //每秒心跳不超过十次
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	replyChan := make(chan RequestVoteReply, len(rf.peers)-1)
	voteCount := 1
	finished := 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			replyChan <- reply
		}(i)
	}
	flag := false
	for voteCount < len(rf.peers)/2+1 && finished < len(rf.peers)-1 { //
		select {
		case reply := <-replyChan:
			if reply.VoteGranted {
				voteCount++
			} else {
				rf.mu.Lock()
				tempTerm := reply.Term
				if rf.currentTerm < tempTerm {
					rf.currentTerm = tempTerm
					rf.status = FOLLOWER
				}
				rf.mu.Unlock()
			}
			finished++
		case <-time.After(time.Duration(200+rand.Int63()%150) * time.Millisecond):
			flag = true
			break
		}
		if flag {
			break
		}
	}
	rf.mu.Lock()
	if voteCount >= len(rf.peers)/2+1 {
		rf.status = LEADER
		fmt.Println(fmt.Sprintf("Server %d 选举成功 in term %d", rf.me, rf.currentTerm))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 1
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleAppendEntries(server int, args AppendEntriesArgs) {
	rf.mu.Lock()
	if rf.status != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	for {
		for !rf.sendAppendEntries(server, &args, &reply) {
		} //一直发送直到成功
		if reply.Term > args.Term || reply.Success == true {
			break
		}
		rf.mu.Lock()
		args.PrevLogIndex--
		if args.PrevLogIndex >= len(rf.log) {
			fmt.Println(rf.log, rf.nextIndex)
		}
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		rf.nextIndex[server]--
		args.Entries = append([]Entry{}, rf.log[rf.nextIndex[server]:]...)
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if reply.Term > args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = FOLLOWER
		}
	} else if reply.Success {
		rf.matchIndex[server] = len(rf.log) - 1
		rf.nextIndex[server] = len(rf.log)
		//检查，更新commitIndex
		matchCount := 1
		for iter := 0; iter < len(rf.peers); iter++ {
			if iter == rf.me {
				continue
			}
			if rf.matchIndex[iter] == len(rf.log)-1 {
				matchCount++
			}
		}
		if matchCount >= len(rf.peers)/2+1 {
			rf.commitIndex = len(rf.log) - 1
		}
	} else {
		fmt.Println("error")
	}
	rf.mu.Unlock()
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.status = FOLLOWER
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []Entry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//time.Sleep(100 * time.Millisecond)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
