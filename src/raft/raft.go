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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

// Log Entry struct
type LogEntry struct {
	Command interface{}
	Term    int
}

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

	// paper defined
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int        // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int      // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// self defined
	lastTimeHeard   time.Time // the last time which the peer heard from the leader
	electionTimeout int       // the election time out
	isLeader        bool      // whether this server is leader
	applyCh         chan ApplyMsg
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
	isleader = rf.isLeader
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
	// Your code here (2C).
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
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "C%d asking for vote, T%d", args.CandidateId, args.Term)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {

		if args.Term > rf.currentTerm {
			// paper Rules for Servers: term T > currentTerm, convert to followers
			rf.isLeader = false
			rf.currentTerm = args.Term
			// term change, so the vote for candidate id need change
			rf.votedFor = -1
			Debug(dTerm, "S%d update term T%d", rf.me, rf.currentTerm)
		}

		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

			if args.LastLogTerm > rf.currentTerm || (args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= rf.lastApplied) {
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				// reset the election time out
				rf.lastTimeHeard = time.Now()
				Debug(dVote, "S%d Vote for S%d", rf.me, args.CandidateId)
			}

		}
	}

}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		Debug(dLog, "S%d received heartbeat from S%d", rf.me, args.LeaderId)
	}
	if len(args.Entries) != 0 {
		Debug(dLog, "Receive Append Entry E(%d,%d)", args.Term, args.LeaderCommit)
	}
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dTerm, "S%d receive outdate Append RPC form S%d", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// reset the election time out
	rf.lastTimeHeard = time.Now()

	if args.Term > rf.currentTerm {
		// paper Rules for Servers: term T > currentTerm, convert to followers
		rf.currentTerm = args.Term
		rf.isLeader = false
		// term change, so the vote for candidate id need change
		rf.votedFor = -1
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dLog, "Log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm")
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	conflictIndex := -1
	for i := 0; i < len(args.Entries); i++ {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex <= len(rf.log)-1 && rf.log[logIndex].Term != args.Entries[i].Term {
			conflictIndex = logIndex
			break
		}
	}

	if conflictIndex != -1 {
		rf.log = rf.log[:conflictIndex] // Truncate the log from conflictIndex
	}
	for i := len(rf.log) - args.PrevLogIndex - 1; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		// Send each newly committed entry on applyCh on each peer
		applyMsg := new(ApplyMsg)
		applyMsg.Command = rf.log[rf.lastApplied].Command
		applyMsg.CommandIndex = rf.lastApplied
		applyMsg.CommandValid = true
		rf.applyCh <- *applyMsg
	}
	Debug(dLog, "S%d replicate the entry from L%d", rf.me, args.LeaderId)
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
	rf.mu.Lock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	// not leader, return false
	if !rf.isLeader {
		rf.mu.Unlock()
		return index, term, false
	}
	Debug(dLeader, "L%d Start agreement", rf.me)
	// leader need to send AppendEntries RPC to other servers.
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{command, term})
	count := 1
	commited := false
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peerIndex int) {
				args := new(AppendEntriesArgs)
				reply := new(AppendEntriesReply)
				args.Term = term
				args.LeaderId = rf.me
				// if len(rf.log) != 2 {
				// 	args.PrevLogIndex = len(rf.log) - 1
				// 	args.PrevLogTerm = rf.log[len(rf.log)-1].Term
				// } else {
				// 	args.PrevLogIndex = 0
				// 	args.PrevLogTerm = 0
				// }
				args.PrevLogIndex = len(rf.log) - 2
				args.PrevLogTerm = rf.log[len(rf.log)-2].Term
				args.LeaderCommit = len(rf.log) - 1
				args.Entries = append(args.Entries, LogEntry{command, term})
				Debug(dInfo, "The Pass Entry num is %d", len(args.Entries))
				ok := rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
				for !ok {
					ok = rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
				}
				rf.mu.Lock()
				if reply.Success {
					count++
				}

				if count > len(rf.peers)/2 && !commited {
					commited = true
					rf.commitIndex++
					index = rf.commitIndex
					for rf.commitIndex > rf.lastApplied {
						rf.lastApplied++
						// Send each newly committed entry on applyCh on each peer
						applyMsg := new(ApplyMsg)
						applyMsg.Command = rf.log[rf.lastApplied].Command
						applyMsg.CommandIndex = rf.lastApplied
						applyMsg.CommandValid = true
						rf.applyCh <- *applyMsg
					}
					Debug(dLeader, "L%d Achieved Majority of servers commited the entry at %d", rf.me, rf.commitIndex)
				}
				rf.mu.Unlock()
			}(i)
		}
	}
	for !commited {
		time.Sleep(10 * time.Millisecond)
	}
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

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// fmt.Printf("rf.mu: %v\n", rf.mu)
		// if the time now is after the last heard time + election time out, then a leader election should be started.
		if time.Now().After(rf.lastTimeHeard.Add(time.Duration(rf.electionTimeout)*time.Millisecond)) && !rf.isLeader {
			Debug(dTimer, "S%d Start Leader Election", rf.me)
			// follower convert to candidate, current term increase
			rf.currentTerm++
			// now is candidate
			rf.isLeader = false
			// reset the candidate election time out
			rf.lastTimeHeard = time.Now()

			rf.electionTimeout = rand.Int()%750 + 750 // 750-1500ms for election time out

			// vote for self
			rf.votedFor = rf.me
			// copy of rf.currentTerm
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			count := 1
			// Debug(dInfo, "C%d Send %d Vote RPC", rf.me, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(peerIndex int) {
						Debug(dTimer, "C%d Send Request Vote for S%d", rf.me, peerIndex)
						args := new(RequestVoteArgs)
						args.Term = currentTerm
						args.CandidateId = rf.me
						if len(rf.log) != 1 {
							args.LastLogTerm = rf.log[len(rf.log)-1].Term
							args.LastLogIndex = len(rf.log) - 1
						} else {
							args.LastLogTerm = rf.currentTerm
							args.LastLogIndex = 0
						}

						reply := new(RequestVoteReply)
						// Debug(dTimer, "C%d Send Request Vote for S%d", rf.me, i)
						success := rf.sendRequestVote(peerIndex, args, reply)
						if success {

							rf.mu.Lock()
							defer rf.mu.Unlock()
							if reply.Term > rf.currentTerm {
								// paper Rules for Servers: term T > currentTerm, convert to followers
								Debug(dLeader, "Leader L%d convert to Follower", rf.me)
								rf.isLeader = false
							}
							if reply.VoteGranted {
								count++
								if count > len(rf.peers)/2 {
									Debug(dLeader, "S%d Achieved Majority for T%d (%d), converting to Leader", rf.me, rf.currentTerm, count)
									rf.isLeader = true
								}
							}
						} else {
							Debug(dTimer, "S%d Can't Send Vote Request to S%d", rf.me, peerIndex)
						}
					}(i)
				}
			}
			// Debug(dLeader, "S%d receive %d Vote", rf.me, count)
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 150
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// leader periodically sends heartbeats
func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader {
			// reset the candidate election time out
			// rf.lastTimeHeard = time.Now()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(peerIndex int) {
						args := new(AppendEntriesArgs)
						reply := new(AppendEntriesReply)
						args.Term = currentTerm
						args.LeaderId = rf.me
						if len(rf.log) != 1 {
							args.PrevLogIndex = len(rf.log) - 1
							args.PrevLogTerm = rf.log[len(rf.log)-1].Term
						} else {
							args.PrevLogIndex = 0
							args.PrevLogTerm = rf.currentTerm
						}
						rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
						rf.mu.Lock()

						if reply.Term > currentTerm {
							// paper Rules for Servers: term T > currentTerm, convert to followers
							rf.isLeader = false
						}
						rf.mu.Unlock()
					}(i)
				}
			}
		} else {
			rf.mu.Unlock()
		}

		// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
		time.Sleep(time.Duration(125) * time.Millisecond)
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
	rf.applyCh = applyCh // 2B
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.isLeader = false
	rf.lastTimeHeard = time.Now()
	rf.electionTimeout = rand.Int()%750 + 750 // 1000-1500ms for election time out
	rf.log = append(rf.log, LogEntry{nil, 0}) // the log need start from index 1
	Debug(dInfo, "S%d initial election time out is %dms", rf.me, rf.electionTimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartbeat
	go rf.heartbeat()
	return rf
}
