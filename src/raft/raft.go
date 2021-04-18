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
	// Lock to protect shared access to this peer's state
	sync.Mutex

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// ------------ Constants -----------------

	// RPC end points of all peers
	peers []*labrpc.ClientEnd
	// Object to hold this peer's persisted state
	persister *Persister
	// This peer's index into peers[]
	me int

	// ------------ Misc -----------------

	// set by Kill()
	dead int32

	// -------------- Persistent state ----------------

	// Current term number.
	currentTerm int
	// The server this server voted for in the current term.
	votedFor int
	// Log entries
	log []interface{}

	// -------------- Volatile state ----------------

	// Whether this server is a follower, candidate, or leader.
	state raftServerState
	// Index of highest log entry known to be committed
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int

	// Id of the server in the current term.
	leaderId int

	// Indices of the next log entry to be sent to each server.
	// Only valid when the server is a leader.
	nextIndex []int
	// Indices of highest log entry known to be replicated on each server.
	// Only valid when the server is a leader.
	matchIndex []int

	// Number of votes this server gathered in the current term.
	// Only valid when the server is a candidate.
	numVotesGathered int

	// The last time this server received heartbeat from a leader or voted for a candidate.
	// Only valid when the server is not a leader.
	lastTimeToReceiveHeartbeatOrGrantVote time.Time
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

func (rf *Raft) convertToFollower() {
	rf.state = Follower
	rf.votedFor = -1
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
		serverDPrint(rf.me, "RequestVote", "rejected RequestVote RPC from %d with lower term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, "RequestVote", "received RequestVote RPC from %d with higher term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		serverDPrint(rf.me, "RequestVote", "stale, convert to follower\n")
		rf.convertToFollower()
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

	// TODO: (2B): Check for up-to-date ness

	if rf.votedFor < 0 || args.CandidateId == rf.votedFor {
		serverDPrint(rf.me, "RequestVote", "voted for %d for term %d\n", args.CandidateId, rf.currentTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()
	} else {
		serverDPrint(rf.me, "RequestVote", "did not vote for %d for term %d because I've already voted for %d\n", args.CandidateId, rf.currentTerm, rf.votedFor)
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
		serverDPrint(rf.me, "AppendEntries", "rejected AppendEntries RPC from %d with lower term (%d) than me (%d)\n",
			args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()
	serverDPrint(rf.me, "AppendEntries", "received AppendEntries RPC from %d term = (%d) my term = (%d)\n",
		args.LeaderId, args.Term, rf.currentTerm)

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, "AppendEntries", "stale, convert to follower")
		rf.convertToFollower()
		rf.currentTerm = args.Term
	}

	if rf.leaderId != args.LeaderId {
		serverDPrint(rf.me, "AppendEntries", "recognize server %d as new leader for term %d\n", args.LeaderId, args.Term)
		rf.convertToFollower()
		rf.leaderId = args.LeaderId
	}

	// TODO: Process log entries

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
	MinElectionTimeout = 800 * time.Millisecond
	MaxElectionTimeout = 1600 * time.Millisecond
	// The interval at which a server will check for election timeout.
	ElectionTimeoutCheckInterval = 100 * time.Millisecond
	// The interval at which a leader will send heartbeats.
	HeartbeatInterval = 100 * time.Millisecond
)

func getRandomTimeout(lowerBound, upperBound time.Duration) time.Duration {
	timeRange := upperBound - lowerBound
	variance := rand.Int63n(timeRange.Milliseconds())
	return lowerBound + time.Duration(variance)*time.Millisecond
}

// Periodically check for election timeout.
func (rf *Raft) electionTimeoutLoop() {
	electionTimeout := getRandomTimeout(MinElectionTimeout, MaxElectionTimeout)
	for {
		time.Sleep(ElectionTimeoutCheckInterval)

		if rf.killed() {
			return
		}

		rf.Lock()
		if rf.state != Leader && time.Since(rf.lastTimeToReceiveHeartbeatOrGrantVote) >= electionTimeout {
			serverDPrint(rf.me, "RequestVote", "election timeout after %d milliseconds", electionTimeout.Milliseconds())
			rf.startElection()
			// Get new randomised election timeout
			electionTimeout = getRandomTimeout(MinElectionTimeout, MaxElectionTimeout)
		}
		rf.Unlock()
	}
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.leaderId = -1
	// Vote for self
	rf.votedFor = rf.me
	rf.numVotesGathered = 1
	rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()
}

func (rf *Raft) startElection() {
	rf.convertToCandidate()

	serverDPrint(rf.me, "RequestVote", "start new election for term %d\n", rf.currentTerm)

	// save a copy in case rf.currentTerm changes by the time we process the replies
	termForElection := rf.currentTerm
	args := RequestVoteArgs{Term: termForElection, CandidateId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.processVoteResponse(i, termForElection, &reply)
					return
				}

				serverDPrint(rf.me, "RequestVote", "RequestVote RPC to %d for term %d failed\n", i, termForElection)
				// Check if we need to retry

				// If already killed, don't retry
				if rf.killed() {
					return
				}

				// If no longer anticipating votes, don't retry
				rf.Lock()
				shouldRetry := rf.currentTerm == termForElection && rf.state == Candidate
				rf.Unlock()

				if !shouldRetry {
					return
				} else {
					serverDPrint(rf.me, "RequestVote", "retry RequestVote to %d for term %d\n", i, termForElection)
				}
			}
		}(i)
	}
}

func (rf *Raft) processVoteResponse(serverId int, requestForVoteTerm int, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	Assert(reply.Term >= requestForVoteTerm)
	Assert(rf.currentTerm >= requestForVoteTerm)

	// To process a vote, all of the following must be true:
	// 1. The response's term must be equal to the current term
	// 2. I must be at the same term as when I initiated the election
	// 3. I am still a candidate

	if reply.Term > rf.currentTerm {
		// reply.Term > rf.currentTerm >= requestForVoteTerm
		serverDPrint(rf.me, "RequestVote", "voter %d has higher term (%d) than me (%d)\n",
			serverId, reply.Term, requestForVoteTerm)
		serverDPrint(rf.me, "RequestVote", "stale, convert to follower\n")
		rf.convertToFollower()
		rf.currentTerm = reply.Term
		return
	} else if reply.Term < rf.currentTerm {
		serverDPrint(rf.me, "RequestVote", "received RequestVote response from peer %d with term %d, but I'm already at term %d\n",
			serverId, reply.Term, rf.currentTerm)
		return
	}

	if requestForVoteTerm != rf.currentTerm {
		Assert(requestForVoteTerm < rf.currentTerm)
		serverDPrint(rf.me, "RequestVote", "received RequestVote response for term %d election from peer %d, but I'm already at term %d\n",
			requestForVoteTerm, serverId, rf.currentTerm)
		return
	}
	Assert(reply.Term == requestForVoteTerm && requestForVoteTerm == rf.currentTerm)

	if rf.state != Candidate {
		serverDPrint(rf.me, "RequestVote", "received RequestVote response for term %d from peer %d, but I'm a %v now\n",
			requestForVoteTerm, serverId, rf.state)
		return
	}

	serverDPrint(rf.me, "RequestVote", "received %v vote from %d for term %d\n", reply.VoteGranted, serverId, requestForVoteTerm)
	if reply.VoteGranted {
		rf.numVotesGathered++
		if rf.gotMajorityVote() {
			serverDPrint(rf.me, "RequestVote", "received majority vote (%d) for term %d, become leader\n", rf.numVotesGathered, rf.currentTerm)
			rf.convertToLeader()
			rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) gotMajorityVote() bool {
	return rf.numVotesGathered >= (len(rf.peers)+1)/2
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.leaderId = rf.me
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) sendHeartbeat() {
	Assert(rf.state == Leader)

	heartbeatId := randstring(4)

	termAsLeader := rf.currentTerm
	serverDPrint(rf.me, "AppendEntries", "sending heartbeat %s for term %d\n", heartbeatId, termAsLeader)

	args := AppendEntriesArgs{Term: termAsLeader, LeaderId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok {
				rf.processHeartbeatResponse(i, heartbeatId, &reply)
				return
			} else {
				serverDPrint(rf.me, "AppendEntries", "heartbeat to %d for term %d id = %s failed\n", i, termAsLeader, heartbeatId)
			}
		}(i)
	}
}

func (rf *Raft) processHeartbeatResponse(serverId int, heartbeatId string, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if reply.Term > rf.currentTerm {
		serverDPrint(rf.me, "AppendEntries", "received AppendEntries response id = %s from peer %d with higher term (%d) than me (%d)\n",
			heartbeatId, serverId, reply.Term, rf.currentTerm)
		serverDPrint(rf.me, "AppendEntries", "stale, convert to follower")
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
	}
}

func (rf *Raft) heartbeatLoop() {
	for {
		time.Sleep(HeartbeatInterval)

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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]interface{}, 0)

	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.leaderId = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.numVotesGathered = 0
	rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeoutLoop()
	go rf.heartbeatLoop()

	return rf
}
