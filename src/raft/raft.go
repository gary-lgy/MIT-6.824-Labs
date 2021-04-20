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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

// An entry in a Raft server's log
type LogEntry struct {
	Command interface{}
	Term    int
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
	// The channel to send ApplyMsg
	applyCh chan<- ApplyMsg
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
	log []*LogEntry

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
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if err := encoder.Encode(rf.currentTerm); err != nil {
		panic(err)
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := encoder.Encode(rf.log); err != nil {
		panic(err)
	}
	data := buffer.Bytes()
	serverDPrint(rf.me, rf.state, "Persist",
		"persisting state, currentTerm = %d, votedFor = %d, log length = %d\n",
		rf.currentTerm, rf.votedFor, len(rf.log))
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
	serverDPrint(rf.me, rf.state, "Persist", "reading persisted state\n")
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var log []*LogEntry
	if err := decoder.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&log); err != nil {
		panic(err)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	serverDPrint(rf.me, rf.state, "Persist",
		"read persisted state currentTerm = %d, votedFor = %d, log length = %d\n",
		currentTerm, votedFor, len(log))
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

	serverDPrint(rf.me, rf.state, "RequestVote",
		"received RequestVote RPC %+v, my state = %+v\n",
		args, stringifyState(rf))
	defer func() {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"finished processing RequestVote RPC, reply = %+v, my state = %+v\n",
			reply, stringifyState(rf))
	}()

	if args.Term < rf.currentTerm {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"rejected RequestVote RPC from %d with lower term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	needPersistState := false
	defer func() {
		if needPersistState {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"received RequestVote RPC from %d with higher term (%d) than me (%d)\n",
			args.CandidateId, args.Term, rf.currentTerm)
		serverDPrint(rf.me, rf.state, "RequestVote", "stale, convert to follower\n")
		rf.convertToFollower()
		rf.currentTerm = args.Term
		needPersistState = true
	}

	reply.Term = rf.currentTerm

	if rf.votedFor >= 0 && args.CandidateId != rf.votedFor {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"did not vote for %d for term %d because I've already voted for %d\n",
			args.CandidateId, rf.currentTerm, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	lastLogEntryTerm := 0
	if len(rf.log) > 0 {
		lastLogEntryTerm = rf.log[len(rf.log)-1].Term
	}

	if isMoreUpToDate(lastLogEntryTerm, len(rf.log), args.LastLogTerm, args.LastLogIndex) {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"did not vote for %d for term %d because my log is more up-to-date\n",
			args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	serverDPrint(rf.me, rf.state, "RequestVote",
		"voted for %d for term %d\n",
		args.CandidateId, rf.currentTerm)
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()
	needPersistState = true
}

func isMoreUpToDate(lastEntryTerm1, logLength1, lastEntryTerm2, logLength2 int) bool {
	if lastEntryTerm1 != lastEntryTerm2 {
		return lastEntryTerm1 > lastEntryTerm2
	} else {
		return logLength1 > logLength2
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
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	serverDPrint(rf.me, rf.state, "AppendEntries",
		"received AppendEntries RPC %+v, my state = %+v\n",
		args, stringifyState(rf))
	defer func() {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"finished processing AppendEntries RPC, reply = %+v, my state = %+v\n",
			reply, stringifyState(rf))
	}()

	if args.Term < rf.currentTerm {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"rejected AppendEntries RPC from %d with lower term (%d) than me (%d)\n",
			args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastTimeToReceiveHeartbeatOrGrantVote = time.Now()
	needPersistState := false
	defer func() {
		if needPersistState {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		serverDPrint(rf.me, rf.state, "AppendEntries", "stale, convert to follower")
		rf.convertToFollower()
		rf.currentTerm = args.Term
		needPersistState = true
	}

	if rf.leaderId != args.LeaderId {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"recognize server %d as new leader for term %d\n",
			args.LeaderId, args.Term)
		rf.convertToFollower()
		rf.leaderId = args.LeaderId
		// TODO: need to persist here?
		needPersistState = true
	}

	reply.Term = rf.currentTerm

	// Process log entries

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 &&
		(args.PrevLogIndex > len(rf.log) ||
			rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"reject because prevLogEntry index = %d term = %d does not exist in my log\n",
			args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		return
	}

	reply.Success = true

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	numMatchingEntries := 0
	for i, newEntry := range args.Entries {
		entryIndex := args.PrevLogIndex + i + 1
		if entryIndex > len(rf.log) {
			break
		}

		existingEntry := rf.log[entryIndex-1]
		if existingEntry.Term == newEntry.Term {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"new entry term = %d already exists in log\n",
				newEntry.Term)
			numMatchingEntries++
			continue
		}

		serverDPrint(rf.me, rf.state, "AppendEntries",
			"conflicting entry found, truncating log, newTerm = %d, existingTerm = %d\n",
			newEntry.Term, existingEntry.Term)
		rf.log = rf.log[:entryIndex-1]
		needPersistState = true
		break
	}

	// 4. Append any new entries not already in the log
	Assert(numMatchingEntries >= 0)
	Assert(numMatchingEntries <= len(args.Entries))
	entriesToAppend := args.Entries[numMatchingEntries:]
	serverDPrint(rf.me, rf.state, "AppendEntries",
		"appending %d new entries\n",
		len(entriesToAppend))
	rf.log = append(rf.log, entriesToAppend...)
	if len(entriesToAppend) > 0 {
		needPersistState = true
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := args.LeaderCommit
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if newCommitIndex > lastNewEntryIndex {
			newCommitIndex = lastNewEntryIndex
		}
		serverDPrint(rf.me, rf.state, "Commit",
			"updating commitIndex from %d to %d\n",
			rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.checkForCommandsToApply()
	}
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
	// Your code here (2B).

	rf.Lock()
	defer rf.Unlock()

	serverDPrint(rf.me, rf.state, "Start",
		"received command %s, my state = %+v\n",
		stringifyCommand(command), stringifyState(rf))
	defer func() {
		serverDPrint(rf.me, rf.state, "Start",
			"finished processing command %s, my state = %+v\n",
			stringifyCommand(command), stringifyState(rf))
	}()

	if rf.state != Leader {
		return -1, -1, false
	}

	logIndex := len(rf.log) + 1
	serverDPrint(rf.me, rf.state, "Start",
		"starting agreement for entry index = (%d) term = (%d)\n",
		logIndex, rf.currentTerm)
	rf.log = append(rf.log, &LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	return logIndex, rf.currentTerm, true
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
	ElectionTimeoutCheckInterval = 50 * time.Millisecond
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
			serverDPrint(rf.me, rf.state, "RequestVote",
				"election timeout after %d milliseconds",
				electionTimeout.Milliseconds())
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
	rf.persist()

	serverDPrint(rf.me, rf.state, "Election",
		"Start election, my state = %+v\n",
		stringifyState(rf))

	// save a copy in case rf.currentTerm changes by the time we process the replies
	termForElection := rf.currentTerm

	lastLogEntryTerm := 0
	if len(rf.log) > 0 {
		lastLogEntryTerm = rf.log[len(rf.log)-1].Term
	}

	args := RequestVoteArgs{
		Term:         termForElection,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  lastLogEntryTerm,
	}
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

				// Check if we need to retry

				// If already killed, don't retry
				if rf.killed() {
					return
				}

				serverDPrint(rf.me, Candidate, "RequestVote",
					"RPC to %d for term %d failed\n",
					i, termForElection)

				// If no longer anticipating votes, don't retry
				rf.Lock()
				shouldRetry := rf.currentTerm == termForElection && rf.state == Candidate
				rf.Unlock()

				if !shouldRetry {
					return
				} else {
					serverDPrint(rf.me, Candidate, "RequestVote",
						"retry RPC to %d for term %d\n",
						i, termForElection)
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

	serverDPrint(rf.me, rf.state, "RequestVote",
		"received RequestVote response from %d, reply = %+v, my state = %+v\n",
		serverId, reply, stringifyState(rf))
	defer func() {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"finished processing RequestVote response from %d, reply = %+v, my state = %+v\n",
			serverId, reply, stringifyState(rf))
	}()

	// To process a vote, all of the following must be true:
	// 1. The response's term must be equal to the current term
	// 2. I must be at the same term as when I initiated the election
	// 3. I am still a candidate

	if reply.Term > rf.currentTerm {
		// reply.Term > rf.currentTerm >= requestForVoteTerm
		serverDPrint(rf.me, rf.state, "RequestVote",
			"voter %d has higher term (%d) than me (%d)\n",
			serverId, reply.Term, requestForVoteTerm)
		serverDPrint(rf.me, rf.state, "RequestVote",
			"stale, convert to follower\n")
		rf.convertToFollower()
		rf.currentTerm = reply.Term
		rf.persist()
		return
	} else if reply.Term < rf.currentTerm {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"reject response because I'm already at term %d\n",
			rf.currentTerm)
		return
	}

	if requestForVoteTerm != rf.currentTerm {
		Assert(requestForVoteTerm < rf.currentTerm)
		serverDPrint(rf.me, rf.state, "RequestVote",
			"reject response because I'm already at term %d\n",
			rf.currentTerm)
		return
	}
	Assert(reply.Term == requestForVoteTerm && requestForVoteTerm == rf.currentTerm)

	if rf.state != Candidate {
		serverDPrint(rf.me, rf.state, "RequestVote",
			"reject response because I'm a %v now\n",
			rf.state)
		return
	}

	serverDPrint(rf.me, rf.state, "RequestVote",
		"received %v vote from %d for term %d\n",
		reply.VoteGranted, serverId, requestForVoteTerm)
	if reply.VoteGranted {
		rf.numVotesGathered++
		if isMajority(rf.numVotesGathered, len(rf.peers)) {
			serverDPrint(rf.me, rf.state, "RequestVote",
				"received majority vote (%d) for term %d, become leader\n",
				rf.numVotesGathered, rf.currentTerm)
			rf.convertToLeader()
		}
	}
}

func majority(totalNum int) int {
	return (totalNum)/2 + 1
}

func isMajority(num int, totalNum int) bool {
	return num >= majority(totalNum)
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.leaderId = rf.me
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
		if i == rf.me {
			continue
		}
		// First send empty AppendEntries to assert dominance
		rf.sendAppendEntriesToFollower(i)
		// Then start loop to send heartbeat and actually append entries
		go rf.appendEntriesLoop(i, rf.currentTerm)
	}
}

// If there are log entries to send to the follower, send them.
// Otherwise, send an empty AppendEntries for heartbeat.
func (rf *Raft) sendAppendEntriesToFollower(peer int) {
	Assert(peer != rf.me)
	Assert(rf.state == Leader)

	termAsLeader := rf.currentTerm

	// By default, send the RPC as a heartbeat with no entries
	args := AppendEntriesArgs{
		Term:         termAsLeader,
		LeaderId:     rf.me,
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	// If there are entries to send, put them in the args
	nextLogIndex := rf.nextIndex[peer]
	lastLogIndex := len(rf.log)
	if lastLogIndex >= nextLogIndex {
		entriesToSend := rf.log[nextLogIndex-1:]
		args.Entries = make([]*LogEntry, len(entriesToSend))
		Assert(len(args.Entries) == len(entriesToSend))
		// Make a copy to avoid data race
		copy(args.Entries, entriesToSend)
	}
	// If the first entry to send is not the first entry in the log
	// attach info about the previous entry for consistency check
	if nextLogIndex > 1 {
		args.PrevLogIndex = nextLogIndex - 1
		prevLogEntry := rf.log[nextLogIndex-2]
		args.PrevLogTerm = prevLogEntry.Term
	}

	serverDPrint(rf.me, rf.state, "AppendEntries",
		"sending AppendEntries to %d %+v\n",
		peer, args)

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.processAppendEntriesResponse(peer, &args, &reply)
			return
		}

		if rf.killed() {
			return
		}

		serverDPrint(rf.me, Leader, "AppendEntries",
			"RPC with %d entries to %d for term %d failed\n",
			len(args.Entries), peer, termAsLeader)

		// Don't need to retry here
		// New RPC will be send in the next iteration of AppendEntries loop
	}()
}

func (rf *Raft) processAppendEntriesResponse(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	serverDPrint(rf.me, rf.state, "AppendEntries",
		"received AppendEntries response from %d reply = %+v for request = %+v, my state = %+v\n",
		serverId, reply, args, stringifyState(rf))
	defer func() {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"finished processing AppendEntries response from %d reply = %+v for request = %+v, my state = %+v\n",
			serverId, reply, args, stringifyState(rf))
	}()

	if reply.Term > rf.currentTerm {
		serverDPrint(rf.me, rf.state, "AppendEntries", "stale, convert to follower")
		rf.convertToFollower()
		rf.currentTerm = reply.Term
		rf.persist()
		return
	} else if reply.Term < rf.currentTerm {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"reject response because I'm already at term %d\n",
			rf.currentTerm)
		return
	}

	if args.Term != rf.currentTerm {
		Assert(args.Term < rf.currentTerm)
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"reject response because I'm already at term %d\n",
			rf.currentTerm)
		return
	}

	Assert(reply.Term == args.Term && args.Term == rf.currentTerm)

	if rf.state != Leader {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"reject response because I'm a %v now\n",
			rf.state)
		return
	}

	if reply.Success {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"request sent in term = %d to peer %d was accepted\n",
			args.Term, serverId)

		// update nextIndex and matchIndex

		newNextLogIndex := args.PrevLogIndex + len(args.Entries) + 1
		if rf.nextIndex[serverId] < newNextLogIndex {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"update nextIndex for follower %d from %d to %d\n",
				serverId, rf.nextIndex[serverId], newNextLogIndex)
			rf.nextIndex[serverId] = newNextLogIndex
		} else {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"not updating nextIndex for follower %d from %d to %d\n",
				serverId, rf.nextIndex[serverId], newNextLogIndex)
		}

		newMatchIndex := newNextLogIndex - 1
		if rf.matchIndex[serverId] < newMatchIndex {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"update matchIndex for follower %d from %d to %d\n",
				serverId, rf.matchIndex[serverId], newMatchIndex)
			rf.matchIndex[serverId] = newMatchIndex
			rf.checkForCommandsToCommit()
		} else {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"not updating matchIndex for follower %d from %d to %d\n",
				serverId, rf.matchIndex[serverId], newMatchIndex)
		}
	} else {
		serverDPrint(rf.me, rf.state, "AppendEntries",
			"request sent in term = %d to peer %d was rejected\n",
			args.Term, serverId)

		// decrement nextIndex and retry
		// retry is handled by the next iteration of the AppendEntries loop

		// don't accidentally increment the nextIndex
		newNextIndex := args.PrevLogIndex
		if rf.nextIndex[serverId] > newNextIndex {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"update nextIndex for follower %d from %d to %d\n",
				serverId, rf.nextIndex[serverId], newNextIndex)
			rf.nextIndex[serverId] = newNextIndex
		} else {
			serverDPrint(rf.me, rf.state, "AppendEntries",
				"not updating nextIndex for follower %d from %d to %d\n",
				serverId, rf.nextIndex[serverId], newNextIndex)
		}
	}
}

func (rf *Raft) checkForCommandsToCommit() {
	Assert(rf.state == Leader)

	// Find what's replicated on a majority
	// Sort the matchIndex and take the middle
	// Make a copy so as not to mutate the original copy
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	Assert(len(matchIndexCopy) == len(rf.matchIndex))
	// The leader has all the logs
	matchIndexCopy[rf.me] = len(rf.log)

	sort.Ints(matchIndexCopy)
	newCommitIndex := matchIndexCopy[len(rf.matchIndex)-majority(len(rf.matchIndex))]

	// Nothing is replicated yet
	if newCommitIndex < 1 {
		return
	}

	replicatedEntry := rf.log[newCommitIndex-1]
	if newCommitIndex <= rf.commitIndex || replicatedEntry.Term != rf.currentTerm {
		return
	}

	serverDPrint(rf.me, rf.state, "Commit",
		"committing entry term = %d, index = %d, my state = %+v\n",
		replicatedEntry.Term, newCommitIndex, stringifyState(rf))
	rf.commitIndex = newCommitIndex
	rf.checkForCommandsToApply()
}

func (rf *Raft) checkForCommandsToApply() {
	go func() {
		rf.Lock()
		defer rf.Unlock()

		for rf.commitIndex > rf.lastApplied {
			entryIndex := rf.lastApplied + 1
			entry := rf.log[entryIndex-1]

			applyMsg := ApplyMsg{
				Command:      entry.Command,
				CommandValid: true,
				CommandIndex: entryIndex,
			}
			rf.applyCh <- applyMsg
			rf.lastApplied = entryIndex

			serverDPrint(rf.me, rf.state, "Apply",
				"applied command index = %d, term = %d, my state = %+v\n",
				entryIndex, entry.Term, stringifyState(rf))
		}
	}()
}

func (rf *Raft) appendEntriesLoop(peer int, termAsLeader int) {
	for {
		time.Sleep(HeartbeatInterval)

		if rf.killed() {
			return
		}

		rf.Lock()
		if rf.state != Leader || termAsLeader != rf.currentTerm {
			rf.Unlock()
			return
		}
		rf.sendAppendEntriesToFollower(peer)
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
	rf.applyCh = applyCh
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]*LogEntry, 0)

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

	return rf
}
