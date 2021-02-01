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

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

// The 3 possible states a server can be in
type raftServerState int

const (
	Leader raftServerState = iota
	Candidate
	Follower
)

func (s raftServerState) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	default:
		panic("invalid state")
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex                     // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state raftServerState

	currentTerm      int
	votedFor         int
	numVotesGathered int

	leaderId               int
	lastElectionCheckpoint time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()

	return rf.currentTerm, rf.state == Leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		serverDPrint(rf.me, "rejected RequestVote RPC from %d with lower term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, "received RequestVote RPC from %d with higher term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	reply.Term = rf.currentTerm

	// (2B): Check for up-to-date ness

	if rf.votedFor < 0 || args.CandidateId == rf.votedFor {
		serverDPrint(rf.me, "voted for %d for term %d\n", args.CandidateId, rf.currentTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastElectionCheckpoint = time.Now()
	} else {
		serverDPrint(rf.me, "did not vote for %d for term %d because I've already voted for %d\n", args.CandidateId, rf.currentTerm, rf.votedFor)
		reply.VoteGranted = false
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		serverDPrint(rf.me, "rejected AppendEntries RPC from %d with lower term (%d) than me (%d)\n",
			args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastElectionCheckpoint = time.Now()

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, "received AppendEntries RPC from %d with higher term (%d) than me (%d)\n",
			args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if rf.leaderId != args.LeaderId {
		serverDPrint(rf.me, "recognize server %d as new leader for term %d\n", args.LeaderId, args.Term)
		rf.state = Follower
		rf.leaderId = args.LeaderId
	}

	// Process log entries

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

const (
	MinElectionTimeout   = 800 * time.Millisecond
	MaxElectionTimeout   = 1600 * time.Millisecond
	ElectionCheckTimeout = 100 * time.Millisecond
	HeartbeatTimeout     = 100 * time.Millisecond
)

func getRandomTimeout(lowerBound, upperBound time.Duration) time.Duration {
	timeRange := upperBound - lowerBound
	variance := rand.Int63n(timeRange.Milliseconds())
	return lowerBound + time.Duration(variance)*time.Millisecond
}

func (rf *Raft) electionLoop() {
	electionTimeout := getRandomTimeout(MinElectionTimeout, MaxElectionTimeout)
	for {
		time.Sleep(ElectionCheckTimeout)

		if rf.killed() {
			return
		}

		rf.Lock()
		if rf.state != Leader && time.Since(rf.lastElectionCheckpoint) >= electionTimeout {
			serverDPrint(rf.me, "election timeout after %d milliseconds", electionTimeout.Milliseconds())
			rf.startElection()
			rf.lastElectionCheckpoint = time.Now()
			// Get new randomised election timeout
			electionTimeout = getRandomTimeout(MinElectionTimeout, MaxElectionTimeout)
		}
		rf.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.leaderId = -1
	rf.state = Candidate

	serverDPrint(rf.me, "start new election for term %d\n", rf.currentTerm)

	// Vote for self
	rf.votedFor = rf.me
	rf.numVotesGathered = 1

	// save a copy in case rf.currentTerm changes by the time we process the replies
	termForElection := rf.currentTerm
	args := RequestVoteArgs{Term: termForElection, CandidateId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for !rf.killed() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.processVoteResponse(i, termForElection, &reply)
					return
				}
				serverDPrint(rf.me, "RequestVote RPC to %d for term %d failed\n", i, termForElection)
			}
		}(i)
	}
}

func (rf *Raft) processVoteResponse(serverId int, termForElection int, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	Assert(reply.Term >= termForElection)
	Assert(rf.currentTerm >= termForElection)

	// To process a vote, all of the following must be true:
	// 1. The response's term must be equal to the current term
	// 2. I must be at the same term as when I initiated the election
	// 3. I am still a candidate

	if reply.Term > rf.currentTerm {
		// reply.Term > rf.currentTerm >= termForElection
		serverDPrint(rf.me, "voter %d has higher term (%d) than me (%d)\n",
			serverId, reply.Term, termForElection)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		return
	} else if reply.Term < rf.currentTerm {
		serverDPrint(rf.me, "received RequestVote response from peer %d with term %d, but I'm already at term %d\n",
			serverId, reply.Term, rf.currentTerm)
		return
	}

	if termForElection != rf.currentTerm {
		Assert(termForElection < rf.currentTerm)
		serverDPrint(rf.me, "received RequestVote response for term %d election from peer %d, but I'm already at term %d\n",
			termForElection, serverId, rf.currentTerm)
		return
	}
	Assert(reply.Term == termForElection && termForElection == rf.currentTerm)

	if rf.state != Candidate {
		serverDPrint(rf.me, "received RequestVote response for term %d from peer %d, but I'm a %v now\n",
			termForElection, serverId, rf.state)
		return
	}

	serverDPrint(rf.me, "received %v vote from %d for term %d\n", reply.VoteGranted, serverId, termForElection)
	if reply.VoteGranted {
		rf.numVotesGathered++
		if rf.numVotesGathered >= (len(rf.peers)+1)/2 {
			serverDPrint(rf.me, "received majority vote (%d) for term %d, become leader\n", rf.numVotesGathered, rf.currentTerm)
			rf.state = Leader
			rf.leaderId = rf.me
			rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	Assert(rf.state == Leader)

	heartbeatId := randstring(4)

	termAsLeader := rf.currentTerm
	serverDPrint(rf.me, "sending heartbeat %s for term %d\n", heartbeatId, termAsLeader)

	args := AppendEntriesArgs{Term: termAsLeader, LeaderId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for !rf.killed() {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.processHeartbeatResponse(i, heartbeatId, &reply)
					return
				}
				serverDPrint(rf.me, "AppendEntries to %d for term %d id = %s failed\n", i, termAsLeader, heartbeatId)
			}
		}(i)
	}
}

func (rf *Raft) processHeartbeatResponse(serverId int, heartbeatId string, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if reply.Term > rf.currentTerm {
		serverDPrint(rf.me, "received AppendEntries response id = %s from peer %d with higher term (%d) than me (%d)\n",
			heartbeatId, serverId, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
	}
}

func (rf *Raft) heartbeatLoop() {
	for {
		time.Sleep(HeartbeatTimeout)

		if rf.killed() {
			return
		}

		rf.Lock()
		if rf.state == Leader {
			rf.sendHeartbeat()
		}
		rf.Unlock()
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
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.numVotesGathered = 0

	rf.leaderId = -1
	rf.lastElectionCheckpoint = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop()
	go rf.heartbeatLoop()

	return rf
}

func serverDPrint(id int, format string, args ...interface{}) {
	DPrintf(fmt.Sprintf("[%d] %s", id, format), args...)
}
