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
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 550
)

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

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// LogEntry stores a single command and the term when it was received by the leader.
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Persistent state on all servers (Figure 2 + Figure 13)
	currentTerm       int
	votedFor          int
	lastIncludedIndex int
	lastIncludedTerm  int
	log               []LogEntry // log[0] is a sentinel at lastIncludedIndex with Term == lastIncludedTerm
	snapshot          []byte

	// Volatile state on all servers
	state            RaftState
	electionDeadline time.Time
	commitIndex      int
	lastApplied      int

	// Pending snapshot to deliver to applyCh via the dedicated applier goroutine
	hasPendingSnapshot   bool
	pendingSnapshot      []byte
	pendingSnapshotTerm  int
	pendingSnapshotIndex int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) logTerm(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) resetElectionTimer() {
	ms := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	rf.electionDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var entries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&entries) != nil {
		Debug(dError, "S%d Error decoding persisted state", rf.me)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	if len(entries) == 0 {
		rf.log = []LogEntry{{Term: lastIncludedTerm}}
	} else {
		rf.log = entries
	}
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastLogIndex() {
		return
	}

	snapTerm := rf.logTerm(index)
	tail := rf.log[index-rf.lastIncludedIndex+1:]
	newLog := make([]LogEntry, 1+len(tail))
	newLog[0] = LogEntry{Term: snapTerm}
	copy(newLog[1:], tail)

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = snapTerm
	rf.snapshot = clone(snapshot)

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
	Debug(dSnap, "S%d Snapshot at index=%d term=%d remainingLog=%d", rf.me, index, snapTerm, len(rf.log)-1)
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast Backup fields
	XTerm  int // Term of the conflicting entry (-1 if no entry at PrevLogIndex)
	XIndex int // First index of XTerm in follower's log
	XLen   int // Total length of follower's log (lastLogIndex + 1)
}

// InstallSnapshot RPC arguments structure (Figure 13).
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// Election restriction (§5.4.1): candidate's log must be at least as up-to-date.
	myLastTerm := rf.lastLogTerm()
	myLastIdx := rf.lastLogIndex()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		rf.persist()
		reply.VoteGranted = true
		Debug(dVote, "S%d granted vote to S%d for term %d", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.state != Follower {
		rf.state = Follower
	}
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	entries := args.Entries

	// Handle delayed/overlapping RPC whose PrevLogIndex is behind our snapshot boundary.
	if prevLogIndex < rf.lastIncludedIndex {
		if prevLogIndex+len(entries) <= rf.lastIncludedIndex {
			reply.Success = true
			return
		}
		offset := rf.lastIncludedIndex - prevLogIndex
		entries = entries[offset:]
		prevLogIndex = rf.lastIncludedIndex
		prevLogTerm = rf.lastIncludedTerm
	}

	// Follower's log is too short.
	if prevLogIndex > rf.lastLogIndex() {
		reply.XTerm = -1
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// Conflicting term at prevLogIndex: return fast-backup hints.
	if rf.logTerm(prevLogIndex) != prevLogTerm {
		xTerm := rf.logTerm(prevLogIndex)
		firstIdx := prevLogIndex
		for firstIdx > rf.lastIncludedIndex+1 && rf.logTerm(firstIdx-1) == xTerm {
			firstIdx--
		}
		reply.XTerm = xTerm
		reply.XIndex = firstIdx
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// Append new entries, truncating on any conflict (§5.3).
	modified := false
	for i, entry := range entries {
		idx := prevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.logTerm(idx) != entry.Term {
				rf.log = append([]LogEntry(nil), rf.log[:idx-rf.lastIncludedIndex]...)
				rf.log = append(rf.log, entries[i:]...)
				modified = true
				break
			}
		} else {
			rf.log = append(rf.log, entries[i:]...)
			modified = true
			break
		}
	}
	if modified {
		rf.persist()
	}

	// Advance commitIndex if leaderCommit > commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		lastNew := prevLogIndex + len(entries)
		newCommit := min(args.LeaderCommit, lastNew)
		if newCommit > rf.lastLogIndex() {
			newCommit = rf.lastLogIndex()
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// InstallSnapshot RPC handler (Figure 13).
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.state != Follower {
		rf.state = Follower
	}
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Ignore stale snapshot that does not advance past our current snapshot or commitIndex.
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// Retain suffix if our log already contains an entry matching LastIncludedIndex & LastIncludedTerm.
	var newLog []LogEntry
	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.logTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		tail := rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]
		newLog = make([]LogEntry, 1+len(tail))
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		copy(newLog[1:], tail)
	} else {
		newLog = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = clone(args.Data)

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.hasPendingSnapshot = true
	rf.pendingSnapshot = clone(args.Data)
	rf.pendingSnapshotTerm = args.LastIncludedTerm
	rf.pendingSnapshotIndex = args.LastIncludedIndex

	rf.persist()
	rf.applyCond.Signal()
	Debug(dSnap, "S%d installed snapshot index=%d term=%d from S%d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// Start agreement on a new command.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	idx := rf.lastLogIndex()
	term := rf.currentTerm
	rf.matchIndex[rf.me] = idx
	rf.nextIndex[rf.me] = idx + 1
	rf.persist()

	rf.broadcastReplicationLocked()
	return idx, term, true
}

func (rf *Raft) broadcastReplicationLocked() {
	if rf.state != Leader {
		return
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateToPeer(peer, rf.currentTerm)
	}
}

func (rf *Raft) replicateToPeer(peer int, term int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peer]
	if nextIdx <= rf.lastIncludedIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              clone(rf.snapshot),
		}
		rf.mu.Unlock()

		var reply InstallSnapshotReply
		if !rf.sendInstallSnapshot(peer, &args, &reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.logTerm(prevLogIndex)
	src := rf.log[nextIdx-rf.lastIncludedIndex:]
	entries := make([]LogEntry, len(src))
	copy(entries, src)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		if newMatch+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.advanceCommitIndexLocked()
		return
	}

	// Fast backup on conflict
	if reply.XTerm == -1 {
		rf.nextIndex[peer] = reply.XLen
	} else {
		foundIdx := -1
		for idx := rf.lastLogIndex(); idx > rf.lastIncludedIndex; idx-- {
			if rf.logTerm(idx) == reply.XTerm {
				foundIdx = idx
				break
			}
		}
		if foundIdx != -1 {
			rf.nextIndex[peer] = foundIdx + 1
		} else {
			rf.nextIndex[peer] = reply.XIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// advanceCommitIndexLocked updates commitIndex according to Figure 2 (§5.4.2):
// only commit entries from the current term once replicated on a majority.
func (rf *Raft) advanceCommitIndexLocked() {
	majority := len(rf.peers)/2 + 1
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.logTerm(n) != rf.currentTerm {
			continue
		}
		count := 1 // self
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

func (rf *Raft) startElectionLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	votes := 1
	majority := len(rf.peers)/2 + 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(p, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= majority {
				rf.state = Leader
				nextIdx := rf.lastLogIndex() + 1
				for i := range rf.peers {
					rf.nextIndex[i] = nextIdx
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.lastLogIndex()
				Debug(dLeader, "S%d won election for term %d", rf.me, term)
				rf.broadcastReplicationLocked()
			}
		}(peer)
	}
}

// Kill marks this peer as dead and wakes up the applier goroutine so it exits cleanly.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) ticker() {
	lastHeartbeat := time.Time{}
	for !rf.killed() {
		rf.mu.Lock()
		now := time.Now()
		if rf.state == Leader {
			if now.Sub(lastHeartbeat) >= HeartbeatInterval {
				rf.broadcastReplicationLocked()
				lastHeartbeat = now
			}
		} else if now.After(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
		time.Sleep(15 * time.Millisecond)
	}
}

// applier is the single goroutine that delivers committed entries and snapshots to applyCh.
// It never holds rf.mu while sending on applyCh, preventing deadlocks when the service calls Snapshot().
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && !rf.hasPendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.hasPendingSnapshot {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshot,
				SnapshotTerm:  rf.pendingSnapshotTerm,
				SnapshotIndex: rf.pendingSnapshotIndex,
			}
			rf.hasPendingSnapshot = false
			rf.pendingSnapshot = nil
			if rf.lastApplied < msg.SnapshotIndex {
				rf.lastApplied = msg.SnapshotIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		rf.lastApplied++
		idx := rf.lastApplied
		cmd := rf.log[idx-rf.lastIncludedIndex].Command
		msg := ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: idx,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}

// Make creates a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		applyCh:    applyCh,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		state:      Follower,
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	go rf.ticker()
	go rf.applier()

	return rf
}
