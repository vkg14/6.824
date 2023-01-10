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

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

type State uint8

const (
	Follower State = iota
	Leader
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// The following are channels used for event notifications
	validHeartbeatCh       chan bool
	validRequestVoteCh     chan bool
	majorityAchievedCh     chan bool
	demotionNotificationCh chan bool
	killCh                 chan bool

	// The following need to be locked by a mutex
	// TODO: consider making a few of these atomic?
	currentTerm int
	state       State
	votedFor    int
	nVotes      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term      int
	Candidate int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Candidate is outdated, ignore and get them to update
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		// Already voted in this term
		reply.Term = args.Term
		reply.VoteGranted = rf.votedFor == args.Candidate
	} else {
		// T > currentTerm -> vote for candidate, become a follower
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.demoteToFollower(args.Term)
		rf.votedFor = args.Candidate
		// Reset election timer since vote has been granted
		notifyEvent(rf.validRequestVoteCh, fmt.Sprintf("(RequestVote, %v)", rf.me))
	}
}

// AppendEntries is an RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// For part 2a, just treat as heartbeat
	if args.Term < rf.currentTerm {
		// Candidate is outdated, ignore and get them to update
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// T >= currentTerm -> this message is from the presumed leader
	reply.Term = args.Term
	reply.Success = true
	if args.Term > rf.currentTerm || rf.state != Follower {
		// 1) if T > currentTerm, we need to explicitly update our currentTerm
		// 2) if T == currentTerm but state != Follower, we need to demote since this request is from the new leader.
		rf.demoteToFollower(args.Term)
	}
	notifyEvent(rf.validHeartbeatCh, fmt.Sprintf("(ValidHeartbeat, %v)", rf.me))
}

// notifyEvent sends an event notification to a bool channel if any thread is waiting on that notification.
func notifyEvent(eventCh chan bool, eventType string) {
	// From: https://stackoverflow.com/questions/25657207/how-to-know-a-buffered-channel-is-full
	select {
	case eventCh <- true:
		fmt.Printf("Notified a %v event!\n", eventType)
	default:
		// Do nothing; nothing was waiting on an event
	}
}

func (rf *Raft) demoteToFollower(updatedTerm int) {
	// IMPORTANT: This method requires that the calling method has the lock.
	// General premise is that this Raft server is out of date.
	// Invariant - caller verified that updatedTerm > rf.currentTerm or (updatedTerm == rf.currentTerm and state != F)
	// Within AE and RV responses and RV handler, updatedTerm > rf.currentTerm guaranteed
	// Within AE handler, updatedTerm == rf.currentTerm only reaches here when state != Follower
	// (ie, to tell another candidate server that it has lost the current term election).
	rf.state = Follower
	rf.currentTerm = updatedTerm
	rf.votedFor = -1
	rf.nVotes = 0
	// Send this notification so that a server that believed it was leader/candidate can behave like a follower
	notifyEvent(rf.demotionNotificationCh, fmt.Sprintf("(DemotionToFollower, %v)", rf.me))
}

func (rf *Raft) startNewElection() {
	// Based on figure 2, move state to Candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// We are called synchronously from ticker() if Follower or Candidate hit election timeout
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.nVotes = 1 // important: vote for self
	fmt.Printf("Server %v starting an election in term %v!\n", rf.me, rf.currentTerm)
	// Send Request Votes
	args := RequestVoteArgs{
		Term:      rf.currentTerm,
		Candidate: rf.me,
	}
	for i := range rf.peers {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	args := AppendEntriesArgs{
		Term:   rf.currentTerm,
		Leader: rf.me,
	}
	for i := range rf.peers {
		if i != rf.me {
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		// TODO: Should we re-send this request to vote?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Convert to follower
		rf.demoteToFollower(reply.Term)
		return
	}

	if rf.currentTerm > reply.Term || !reply.VoteGranted {
		// We are in a later term or the vote is not for us
		return
	}

	// If we are here, reply.Term == args.Term == rf.currentTerm
	rf.nVotes++

	if rf.nVotes > len(rf.peers)/2 {
		// Majority achieved
		rf.state = Leader
		notifyEvent(rf.majorityAchievedCh, fmt.Sprintf("(MajorityAchieved, %v)", rf.me))
		fmt.Printf("Server %v is now the leader in term %v.\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		// TODO: Should we re-send this heartbeat?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// Convert to follower
		rf.demoteToFollower(reply.Term)
		return
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

	// Your code here (2B).

	return index, term, isLeader
}

// Kill "kills" this server from perspective of labrpc framework.
// We notify the main loop (ticker) of the kill so it can exit.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			// Use select to wait on election timeout, heartbeat, granted vote or kill notification
			electionTimeout := time.After(getElectionTimeout() * time.Millisecond)
			select {
			case <-electionTimeout:
				fmt.Printf("Election timeout - server %v starting an election!\n", rf.me)
				rf.startNewElection()
			case <-rf.validHeartbeatCh:
				// reset timeout; ie, do nothing
			case <-rf.validRequestVoteCh:
				// reset timeout; ie, do nothing
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		case Candidate:
			// Use select to wait on election timeout, a vote majority, a demotion, or kill notification
			electionTimeout := time.After(getElectionTimeout() * time.Millisecond)
			select {
			case <-electionTimeout:
				// Start another election
				fmt.Printf("Election timeout - server %v starting an election!\n", rf.me)
				rf.startNewElection()
			case <-rf.majorityAchievedCh:
				// Send heartbeats immediately here
				rf.sendHeartbeats()
			case <-rf.demotionNotificationCh:
				// do nothing, re-enter loop with new follower state
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		case Leader:
			// Use select to wait on heartbeat timeout, a demotion, or a kill notification
			select {
			case <-time.After(110 * time.Millisecond):
				// Max 10 heartbeats per second (enforced by tester)
				rf.sendHeartbeats()
			case <-rf.demotionNotificationCh:
				// do nothing, re-enter loop with new follower state
			case <-rf.killCh:
				// do nothing, outer for loop will now exit
			}
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Choose an election timeout and wait for it to run to completion.
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

	// Your initialization code here (2A, 2B, 2C).
	// Mutex protected members
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nVotes = 0

	// Notification channels
	rf.validHeartbeatCh = make(chan bool)
	rf.validRequestVoteCh = make(chan bool)
	rf.demotionNotificationCh = make(chan bool)
	rf.majorityAchievedCh = make(chan bool)
	rf.killCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
