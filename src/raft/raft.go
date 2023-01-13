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
	"io"
	"log"
	"math"
	"math/rand"
	"os"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// Notification - Empty struct for notification
type Notification struct{}

type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type PersistedState struct {
	Term     int
	VotedFor int
	Log      []LogEntry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// The following are (thread-safe) channels used for event notifications
	validHeartbeatCh       chan Notification
	voteGrantedCh          chan Notification
	majorityAchievedCh     chan Notification
	demotionNotificationCh chan Notification
	logsBroadcastedCh      chan Notification
	killCh                 chan Notification

	// The following need to be locked by a mutex
	// Election
	currentTerm int
	state       State
	votedFor    int
	nVotes      int

	// Replication
	commitIndex int
	lastApplied int
	log         []LogEntry
	nextIndex   []int
	matchIndex  []int
}

// GetState - server's belief of the current term and its leader status
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Needs to be called WITH lock
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(PersistedState{
		Term:     rf.currentTerm,
		VotedFor: rf.votedFor,
		Log:      rf.log,
	})
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Lock and unlock is likely unnecessary since this is called before ticker()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistedState
	if d.Decode(&state) != nil {
		// error...
	} else {
		rf.currentTerm = state.Term
		rf.votedFor = state.VotedFor
		rf.log = state.Log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// notifyEvent sends an event notification to a channel if any thread is waiting on that notification.
func notifyEvent(eventCh chan Notification, eventType string) {
	// From: https://stackoverflow.com/questions/25657207/how-to-know-a-buffered-channel-is-full
	select {
	case eventCh <- Notification{}:
		log.Printf("Notified a %v event!\n", eventType)
	default:
		// Do nothing; ticker() is NOT waiting on this event
	}
}

// *** METHODS THAT ARE NOT THREAD-SAFE AND REQUIRE CALLER TO HAVE ACQUIRED LOCK ***

// demoteToFollower - requires caller to have lock held
func (rf *Raft) demoteToFollower(updatedTerm int) {
	// General premise is that this Raft server is out of date.
	// Invariant - caller verified that updatedTerm > rf.currentTerm or (updatedTerm == rf.currentTerm and state != F)
	// Within AE and RV responses and RV handler, updatedTerm > rf.currentTerm guaranteed
	// Within AE handler, updatedTerm == rf.currentTerm only reaches here when state != Follower
	// (ie, to tell another candidate server that it has lost the current term election).
	rf.state = Follower
	rf.currentTerm = updatedTerm
	rf.votedFor = -1
	rf.nVotes = 0
	// Send this notification to the main loop, which will refrain from any leader/candidate behavior once received.
	notifyEvent(rf.demotionNotificationCh, fmt.Sprintf("(DemotionToFollower, %v)", rf.me))
	// In 3 out of 4 cases -> demotion results in a term change
	// In 4th case, candidate is demoted to follower by a new leader who is elected in said term
	// Since a call to demote to follower will likely change term -> persist!
	rf.persist()
}

// lastLogIndex - assumes lock is held by caller
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// lastLogTerm - assumes lock is held by caller
func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

// adjustFollowerCommit - requires lock to be held by caller
func (rf *Raft) adjustFollowerCommit(leaderCommit, lastNewIndex int) {
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = min(leaderCommit, lastNewIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.applyToStateMachine()
		}
	}
}

// adjustLeaderCommit - caller needs to hold lock
func (rf *Raft) adjustLeaderCommit() {
	// Find max N such that majority of matchIndex[i] >= N and log[N].term == currentTerm
	// We can rely on the fact that log[0].term = 0 to eventually stop our loop (dummy LogEntry)
	for i := rf.lastLogIndex(); rf.log[i].Term == rf.currentTerm; i-- {
		nSatisfied := 0
		for j := range rf.matchIndex {
			if rf.matchIndex[j] >= i {
				nSatisfied++
			}
		}
		if nSatisfied > len(rf.peers)/2 {
			// Majority consensus achieved for log @ commit index
			rf.commitIndex = i
			if rf.commitIndex > rf.lastApplied {
				rf.applyToStateMachine()
			}
			break
		}
	}
}

// applyToStateMachine - ensures that any committed log command is only applied once via mutex guarding.
// *** requires lock to be held by caller
func (rf *Raft) applyToStateMachine() {
	for nextCommit := rf.lastApplied + 1; nextCommit <= rf.commitIndex; nextCommit++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[nextCommit].Command,
			CommandIndex: nextCommit,
		}
	}
	rf.lastApplied = rf.commitIndex
}

// *** END ***

// RequestVoteArgs RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Candidate is outdated, ignore and get them to update
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		// If this server is already on this term, it got here one of two ways:
		// 1) it voted for a candidate already in the current election (in which case its votedFor = someCandidate)
		// 2) it got an AppendEntries from a leader whose term was this (in which case its votedFor = -1)
		// In either case, we don't want to explicitly demote and only grant vote if we voted for candidate already.
		// We also DO NOT want to reset the election timer since a vote has already been cast.
		reply.Term = args.Term
		reply.VoteGranted = rf.votedFor == args.Candidate
	} else {
		// T > currentTerm -> Become a follower
		rf.demoteToFollower(args.Term)
		reply.Term = args.Term
		reply.VoteGranted = false
		// Vote for candidate IF up-to-date condition passes:
		// 1) candidate last log entry term is later than this server OR
		// 2) same last log term and candidate has same or greater length log
		if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.Candidate
			// Reset election timer since vote has been granted
			notifyEvent(rf.voteGrantedCh, fmt.Sprintf("(RequestVote, %v)", rf.me))
			// We voted -> persist!
			rf.persist()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// AppendEntries is an RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// "Leader" is outdated, ignore and get them to update / demote
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// T >= currentTerm -> this message is from the presumed leader and thus a valid "heartbeat" to reset timer
	notifyEvent(rf.validHeartbeatCh, fmt.Sprintf("(ValidHeartbeat, %v)", rf.me))

	if args.Term > rf.currentTerm || rf.state != Follower {
		// 1) if T > currentTerm, we need to explicitly update our currentTerm
		// 2) if T == currentTerm but state != Follower, we need to demote since this request is from the new leader.
		rf.demoteToFollower(args.Term)
	}

	reply.Term = args.Term
	reply.Success = false
	// Checkpoint #2: Log Consistency check
	if len(rf.log) <= args.PrevLogIndex {
		// Tell leader to set nextIndex to end of our log
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = math.MaxInt
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Find first index with term == ConflictTerm -> if leader does not have conflict term in log,
		// all indices *this index* onwards need to be replaced.
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for rf.log[idx].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx + 1
		if rf.log[reply.ConflictIndex].Term != reply.ConflictTerm {
			log.Panicf("Logic to derive conflict indices is wrong.")
		}
		return
	}

	reply.Success = true

	// Starting at prevLogIndex + 1 (one after the last guaranteed match), we check for the first mismatching index.
	startIndex := args.PrevLogIndex + 1
	i := 0
	for startIndex+i <= rf.lastLogIndex() && i < len(args.Entries) {
		if rf.log[startIndex+i].Term != args.Entries[i].Term {
			break
		}
		i++
	}

	if i == len(args.Entries) {
		// No new entries -> return success but DO NOT adjust/truncate log
		// We will need a modified version of Checkpoint #5 with the last new log == startIndex + i - 1.
		rf.adjustFollowerCommit(args.LeaderCommit, startIndex+i-1)
		return
	}

	// We have new entries and potentially mismatches starting at rf.log[startIndex+i]
	// 1) delete rf.log[startIndex+i:] (if in-bounds)
	// 2) append Entries[i:] to end of rf.log
	if startIndex+i <= rf.lastLogIndex() {
		// Checkpoint #3 in AE handler: Delete all mismatches; ie, rf.log[startIndex+i:]
		rf.log = rf.log[:startIndex+i]
	}
	// Checkpoint #4 in AE handler: Append new entries from the mismatch point.
	rf.log = append(rf.log, args.Entries[i:]...)
	rf.persist() // log successfully changed

	// Checkpoint #5 in AE Handler: Adjust commitIndex with the last log as the newest
	rf.adjustFollowerCommit(args.LeaderCommit, rf.lastLogIndex())
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// We are called synchronously from ticker() if Follower or Candidate hit election timeout
	// Based on figure 2, move state to Candidate, vote for self, adjust term
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.nVotes = 1 // important: vote for self
	// New election -> term and votedFor modified -> persist!
	rf.persist()
	log.Printf("Server %v starting an election in term %v!\n", rf.me, rf.currentTerm)
	// Send Request Votes
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		Candidate:    rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	for i := range rf.peers {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

// sendRequestVote - called by candidate to garner votes from peers.  Generally will be called
// in goroutine since labrpc's simulated network can have delays or losses, which we do not want to
// block any unrelated function.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Convert to follower
		rf.demoteToFollower(reply.Term)
		return
	}

	if rf.currentTerm > args.Term || !reply.VoteGranted || rf.killed() {
		// We are in a later term, the vote is not for us, or we are dead
		return
	}

	// If we are here, reply.Term == args.Term == rf.currentTerm
	rf.nVotes++

	if rf.nVotes > len(rf.peers)/2 {
		// Majority achieved
		// Note: we can hit this block multiple times for a (leader, term) since it may get an
		// overwhelming majority of votes.  As such, we must take care to swap state variables only once.
		notifyEvent(rf.majorityAchievedCh, fmt.Sprintf("(MajorityAchieved, %v)", rf.me))
	}
}

// shouldRetryAppendEntries - adjusts state given reply and determines whether to retry the AE
func (rf *Raft) shouldRetryAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Failed because we are out of date -> convert to follower
		rf.demoteToFollower(reply.Term)
		return false
	}

	// If we were demoted elsewhere, term has passed, or we died, stop retrying
	if rf.state != Leader || rf.currentTerm > args.Term || rf.killed() {
		return false
	}

	if reply.Success {
		// We are leader and got success from follower -> we can adjust nextIndex and matchIndex
		// Since rf.log length may have changed, matchIndex should be precisely set according to the new entries.
		// We can set nextIndex to matchIndex + 1 -> might increase num entries sent in message but reduce total RPCs
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.adjustLeaderCommit()
		return false
	}

	// We did not succeed because of Log Matching condition -> push back nextIndex, modify args, and retry
	// The algorithm below is to find the index after the last instance of conflict term
	// If conflict term isn't present in this leader's log, then we use conflict index directly.
	// These heuristics speed up the backtracking of next index to more quickly find a common point
	// between the leader and follower.  NextIndex *may* overshoot, but then we simply end up sending more
	// entries in the message - the follower figures out which entries are "new".
	idx := args.PrevLogIndex
	for rf.log[idx].Term > reply.ConflictTerm {
		idx--
	}
	if rf.log[idx].Term == reply.ConflictTerm {
		rf.nextIndex[server] = idx + 1
	} else {
		rf.nextIndex[server] = reply.ConflictIndex
	}
	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex >= args.PrevLogIndex {
		log.Panicf(
			"New prevlogidx %v did not decrease from old one %v for leader %v in term %v",
			prevLogIndex,
			args.PrevLogIndex,
			rf.me,
			rf.currentTerm,
		)
	}
	// LeaderID and Term should be left unchanged.
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex].Term
	args.Entries = rf.log[rf.nextIndex[server]:]
	// Commit idx could have changed since we last populated this RPC args struct.
	args.LeaderCommit = rf.commitIndex
	return true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	for {
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			return
		}
		// Check if we should retry and adjust args and state accordingly
		if !rf.shouldRetryAppendEntries(server, args, reply) {
			break
		}
	}
}

// broadcastLogUpdates is called when this Raft server thinks it is leader.
func (rf *Raft) broadcastLogUpdates() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Should not be sending AEs if we have been demoted
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				Leader:       rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
			}
			go rf.sendAppendEntries(i, &args)
		}
	}
}

// promoteToLeader - performs state changes to Leader state
func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// When ticker() calls promoteToLeader, we are a Candidate with a majority vote; however,
	// an RPC handler can demote us to Follower before we can grab the lock here.
	if rf.state == Follower {
		return
	}
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex() // We fully match our own log
	log.Printf("Server %v is now the leader in term %v.\n", rf.me, rf.currentTerm)
}

// Start - the client API for providing new commands to push to the log and apply
// to the state machine.  Only the leader should do anything with this.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist() // Log changes when new command comes in -> persist

	rf.matchIndex[rf.me] = rf.lastLogIndex() // We always match our own last log index

	// Send log updates via AEs to all peers
	go rf.broadcastLogUpdates()
	notifyEvent(rf.logsBroadcastedCh, fmt.Sprintf("(LogsBroadcasted, %v)", rf.me))

	return rf.lastLogIndex(), rf.lastLogTerm(), true
}

// Kill "kills" this server from perspective of labrpc framework.
// We notify the main loop (ticker) of the kill so it can exit.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	notifyEvent(rf.killCh, fmt.Sprintf("(Kill, %v)", rf.me))
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func getElectionTimeout() time.Duration {
	// Paper says [150, 300) but lab assignment says to pick higher, so shifted up by 200 ms
	min := 350
	max := 500
	return time.Duration(rand.Intn(max-min) + min)
}

// ticker represents the main loop for the server.  It outlines waiting behavior for
// each possible state of the server.  While waiting on these notifications (triggered by RPC requests and replies),
// it does not hold the mutex so that all other functionality is unblocked.  Another thread / goroutine will tell
// ticker of a notification by pushing to the relevant channel IF ticker is awaiting a notification from that channel.
// Each of the blocked select statements also has a time element (either the election timeout or the idle-time
// heartbeat) to push forward the state when no other notifications have been received.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			// Follower waits on election timeout, heartbeat, granted vote or kill notification
			electionTimeout := time.After(getElectionTimeout() * time.Millisecond)
			select {
			case <-electionTimeout:
				rf.startNewElection()
			case <-rf.validHeartbeatCh:
				// reset timeout; ie, do nothing
			case <-rf.voteGrantedCh:
				// reset timeout; ie, do nothing
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		case Candidate:
			// Candidate waits on election timeout, a vote majority, a demotion, or kill notification
			electionTimeout := time.After(getElectionTimeout() * time.Millisecond)
			select {
			case <-electionTimeout:
				// Start another election
				rf.startNewElection()
			case <-rf.majorityAchievedCh:
				// Transition to leader and broadcast heartbeats
				rf.promoteToLeader()
				rf.broadcastLogUpdates()
			case <-rf.demotionNotificationCh:
				// do nothing, another leader has been established, re-enter loop with new follower state
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		case Leader:
			// Use select to wait on heartbeat timeout, a demotion, or a kill notification
			select {
			case <-time.After(110 * time.Millisecond):
				// Max 10 heartbeats per second (enforced by tester)
				rf.broadcastLogUpdates()
			case <-rf.logsBroadcastedCh:
				// do nothing, this is to prevent excessive heartbeats by resetting the heartbeat timer
			case <-rf.demotionNotificationCh:
				// do nothing, re-enter loop with new follower state
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Mutex protected members
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nVotes = 0
	rf.log = append(rf.log, LogEntry{}) // Fills in with term = 0, cmd = nil for dummy index 0
	rf.commitIndex = -1
	rf.lastApplied = 0 // Ensure that dummy LogEntry does not get applied
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Notification channels
	rf.validHeartbeatCh = make(chan Notification)
	rf.voteGrantedCh = make(chan Notification)
	rf.demotionNotificationCh = make(chan Notification)
	rf.majorityAchievedCh = make(chan Notification)
	rf.logsBroadcastedCh = make(chan Notification)
	rf.killCh = make(chan Notification)

	debug := false
	if !debug {
		log.SetOutput(io.Discard)
	} else {
		log.SetOutput(os.Stdout) // To debug
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
