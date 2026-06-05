package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int
	votedFor    int
	log         []logEntry
	// voteLogIdx  int

	// Volatile state
	raftState   RaftState
	commitIndex int
	lastApplied int
	lastBeats   time.Time
	applyCh     chan raftapi.ApplyMsg

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int
}

type logEntry struct {
	Command any
	Term    int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.raftState == Leader
	return term, isleader
}

func beatsTimeout() time.Duration {
	return time.Duration(800+rand.Int63()%500) * time.Millisecond
}

// func voteTimeout() time.Duration {
// 	return time.Duration(300+rand.Int63()%400) * time.Millisecond
// }

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
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	bufferBytes := buffer.Bytes()
	rf.persister.Save(bufferBytes, nil)
	log.Printf("No.%d persist term:%d voteFor:%d, raftState:%d", rf.me, rf.currentTerm, rf.votedFor, rf.raftState)
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	// var voteLogIdx int
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil {
		log.Printf("No.%d readPersist error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		log.Printf("No.%d readPersist success, term: %d, voteFor: %d, log: %v, state: %d", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.raftState)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	CandidateId  int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Index   int
	CmtIdx  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d term: %d receive requestVote from %v, lock", rf.me, rf.currentTerm, args)
	reply.Term = rf.currentTerm
	changeFlag := false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.raftState = Follower
		rf.votedFor = -1
		changeFlag = true
	}
	maxLogIdx := len(rf.log) - 1
	lastLogTerm := rf.log[maxLogIdx].Term
	voteOnce := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if voteOnce &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= maxLogIdx)) {
		rf.votedFor = args.CandidateId
		rf.lastBeats = time.Now()
		reply.VoteGranted = true
		changeFlag = true
		log.Printf("%d, maxlogIdx: %d curterm:%d grant vote to %d in term %d", rf.me, maxLogIdx, rf.currentTerm, args.CandidateId, args.Term)
	}
	if changeFlag {
		go rf.persist()
	}
	log.Printf("%d state %d requestVote unlock, voteFor %d, granted %v, reply %v", rf.me, rf.raftState, rf.votedFor, reply.VoteGranted, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d, state: %d receive appendEntries from %d, lock", rf.me, rf.raftState, args.LeaderId)
	changeFlag := false
	reply.Term = rf.currentTerm
	reply.CmtIdx = rf.commitIndex
	lastIdx := len(rf.log) - 1
	lastEntry := rf.log[lastIdx]

	if args.Term < rf.currentTerm {
		// || lastEntry.Term > args.PrevLogTerm ||(lastEntry.Term == args.PrevLogTerm && lastIdx > args.PrevLogIndex) {
		reply.Success = false
		log.Printf("No.%d term:%d lastIdx:%d lastEntry.Term:%d PreLogIdx:%d PrevLogTerm:%d reject appendEntry from %d", rf.me, rf.currentTerm, lastIdx, lastEntry.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		changeFlag = true
	}

	if lastIdx > args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
		lastIdx = len(rf.log) - 1
		lastEntry = rf.log[lastIdx]
		changeFlag = true
		log.Printf("No.%d delete log entry from index %d", rf.me, args.PrevLogIndex+1)
	}
	if lastIdx == args.PrevLogIndex {
		if lastEntry.Term == args.PrevLogTerm {
			reply.Success = true
			if args.Entries != nil {
				rf.log = append(rf.log, args.Entries...)
				changeFlag = true
				log.Printf("No.%d append log %v at index %d", rf.me, args.Entries, len(rf.log)-1)
				lastIdx = len(rf.log) - 1
				lastEntry = rf.log[lastIdx]
			} else {
				log.Printf("No.%d receive heartbeats", rf.me)
			}
			if args.LeaderCommit > rf.commitIndex {
				commitLen := min(args.LeaderCommit, len(rf.log)-1)
				commitIdx := rf.commitIndex
				for commitIdx < commitLen {
					commitIdx++
					applyMsg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[commitIdx].Command,
						CommandIndex: commitIdx,
					}
					rf.applyCh <- applyMsg
					log.Printf("No.%d send applycCh %v", rf.me, applyMsg)
				}
				rf.commitIndex = commitLen
				rf.lastApplied = rf.commitIndex
				reply.CmtIdx = rf.commitIndex
				changeFlag = true
				log.Printf("No.%d receive appendEntries from %d, update commitIndex to %d", rf.me, args.LeaderId, rf.commitIndex)
			}
		} else if lastEntry.Term != args.PrevLogTerm {
			rf.log = rf.log[:lastIdx]
			changeFlag = true
			reply.Success = false
			log.Printf("No.%d detele log entry at index %d", rf.me, lastIdx)
		}
	} else {
		reply.Success = false
	}

	if rf.raftState != Follower || rf.votedFor != args.LeaderId {
		changeFlag = true
		rf.raftState = Follower
		rf.votedFor = args.LeaderId
	}
	rf.lastBeats = time.Now()
	lastIdx = len(rf.log) - 1
	lastEntry = rf.log[lastIdx]
	if changeFlag {
		go rf.persist()
	}
	log.Printf("%d state:%d lastIdx:%d entry:%v receive append from %d, unlock reply:%v", rf.me, rf.raftState, lastIdx, lastEntry, args.LeaderId, reply)
}

func (rf *Raft) sendAppendEntries() bool {
	for true {
		rf.mu.Lock()
		if rf.raftState != Leader {
			rf.mu.Unlock()
			return false
		}
		var entries []logEntry
		if len(rf.log)-1 > rf.commitIndex {
			entries = rf.log[rf.commitIndex+1:]
		}
		log.Printf("%d append lock, not committed entries: %v", rf.me, entries)
		replyCh := make(chan *AppendEntriesReply, 1)
		lastCommand := rf.log[len(rf.log)-1].Command
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					rf.mu.Lock()
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
						Entries:      rf.log[rf.nextIndex[server]:],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntry(server, args, reply)
					if ok {
						reply.Index = server
						replyCh <- reply
					} else {
						replyCh <- nil
					}
				}(i)
			}
		}
		commitCount := rf.appendEntriesHandle(replyCh)
		if commitCount < 0 {
			return false
		} else if len(entries) != 0 && commitCount > len(rf.peers)/2 {
			commitIdx := rf.commitIndex
			go func() {
				for i := range entries {
					commitIdx++
					applyMsg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      entries[i].Command,
						CommandIndex: commitIdx,
					}
					rf.applyCh <- applyMsg
					log.Printf("No.%d send applycCh %v", rf.me, applyMsg)
				}
			}()
			rf.commitIndex += len(entries)
			rf.lastApplied = rf.commitIndex
			log.Printf("No.%d receive %d commit logs: %d, command %v", rf.me, commitCount, rf.commitIndex, lastCommand)
		}
		log.Printf("%d sendAppendEntries() unlock", rf.me)
		time.Sleep(150 * time.Millisecond)
	}
	return true
}

func (rf *Raft) appendEntriesHandle(replyChan chan *AppendEntriesReply) int {
	commitCount := 1
	now := time.Now()
	for range len(rf.peers) - 1 {
		var reply *AppendEntriesReply
		// reply := <-replyChan
		// if reply == nil {
		// 	log.Printf("No.%d append timeout continue", rf.me)
		// 	continue
		// }
		select {
		case reply = <-replyChan:
			if reply == nil || time.Since(now) > 1000*time.Millisecond {
				log.Printf("No.%d append timeout continue+", rf.me)
				continue
			}
		case <-time.After(100 * time.Millisecond):
			log.Printf("No.%d append timeout continue-", rf.me)
			continue
		}

		rf.mu.Lock()
		server := reply.Index
		log.Printf("%d term:%d, server:%d reply:%v", rf.me, rf.currentTerm, server, reply)
		if reply.Term > rf.currentTerm || rf.raftState != Leader {
			rf.currentTerm = reply.Term
			rf.raftState = Follower
			rf.votedFor = -1
			go rf.persist()
			rf.lastBeats = time.Now()
			rf.mu.Unlock()
			return -1
		}
		if reply.Success {
			rf.matchIndex[server] = len(rf.log) - 1
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			commitCount++
		} else {
			log.Printf("No.%d reduce nextIdx %d to match No.%d", rf.me, rf.nextIndex[server], reply.Index)
			rf.nextIndex[server] = min(max(1, reply.CmtIdx+1), len(rf.log))
		}
		rf.mu.Unlock()
	}
	return commitCount
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("%d send appendRPC %v to %d", rf.me, args, server)
	// okChan := make(chan bool, 1)
	now := time.Now()
	// localReply := &AppendEntriesReply{}

	// go func() {
	// 	okChan <- rf.peers[server].Call("Raft.AppendEntries", args, localReply)
	// 	log.Printf("No.%d send append entry reply %v", rf.me, localReply)
	// }()

	// select {
	// case ok := <-okChan:
	// 	*reply = *localReply
	// 	log.Printf("%d append call %d last %d", rf.me, server, time.Since(now).Milliseconds())
	// 	log.Printf("%d append entries result: %t, %v", rf.me, ok, reply)
	// 	return ok
	// // 50ms can not pass test because timeout
	// case <-time.After(100 * time.Millisecond):
	// 	log.Printf("%d append call %d timeout (last %d ms)", rf.me, server, time.Since(now).Milliseconds())
	// 	return false
	// }
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Printf("%d append entries server %d result: %t, %v last %d", rf.me, server, ok, reply, time.Since(now).Milliseconds())
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
	// okChan := make(chan bool, 1)
	// localReply := &RequestVoteReply{}
	// go func() {
	// 	okChan <- rf.peers[server].Call("Raft.RequestVote", args, localReply)
	// }()
	// select {
	// case ok := <-okChan:
	// 	if ok {
	// 		*reply = *localReply
	// 		log.Printf("No.%d sendRequestVote %v", rf.me, reply)
	// 	}
	// 	return ok
	// case <-time.After(100 * time.Millisecond):
	// 	log.Printf("%d requestVote call %d timeout", rf.me, server)
	// 	return false
	// }
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftState == Leader {
		entry := logEntry{Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, entry)
		go rf.persist()
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
		log.Printf("No.%d receive command %v", rf.me, entry)
	}

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	// Your code here (3A)
	// Check if a leader election should be started.
	for true {
		rf.mu.Lock()
		lastHeartB := rf.lastBeats
		raftState := rf.raftState
		rf.mu.Unlock()
		log.Printf("No.%d ticker() lock , state: %v", rf.me, rf.raftState)

		if time.Since(lastHeartB) > beatsTimeout() && raftState != Leader {
			rf.mu.Lock()
			if raftState == Follower {
				rf.raftState = Candidate
			}
			rf.currentTerm++
			rf.votedFor = rf.me
			log.Printf("up to candidate %d term %d, time %d", rf.me, rf.currentTerm, time.Since(rf.lastBeats).Milliseconds())
			rf.lastBeats = time.Now()
			go rf.persist()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			// }
			// if time.Since(lastHeartB) > voteTimeout() {
			// send RequestVote RPCs to all other servers
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			replyChan := make(chan *RequestVoteReply, 1)
			for i := range rf.peers {
				if i != rf.me {
					go func(server int) {
						reply := &RequestVoteReply{}
						log.Printf("send requestVote from %d to %d, arg: %v", rf.me, i, args)
						now := time.Now()
						ok := rf.sendRequestVote(server, args, reply)
						if ok {
							replyChan <- reply
						} else {
							replyChan <- nil
						}
						log.Printf("No.%d fuck requestvote call %d last %d", rf.me, server, time.Since(now).Milliseconds())
						log.Printf("No.%d requestVote result: %t, %v", rf.me, ok, reply)
					}(i)
				}
			}
			rf.voteReplyHandle(replyChan)
			// }
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		log.Printf("%d ticker() time out", rf.me)
	}
}

func (rf *Raft) voteReplyHandle(replyChan chan *RequestVoteReply) {
	voteCount := 1
	for range len(rf.peers) - 1 {
		reply := <-replyChan
		if reply == nil {
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			log.Printf("No.%d 's vote is denied", rf.me)
			rf.currentTerm = reply.Term
			rf.raftState = Follower
			rf.votedFor = -1
			go rf.persist()
			rf.mu.Unlock()
			return
		} else if reply.VoteGranted {
			// count votes
			voteCount++
			log.Printf("%d get a vote %d", rf.me, voteCount)
		}
		if rf.raftState == Follower {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if voteCount > len(rf.peers)/2 {
			log.Printf("%d end vote", rf.me)
			rf.mu.Lock()
			rf.raftState = Leader
			go rf.persist()
			// initialize leader state
			if rf.nextIndex == nil {
				rf.nextIndex = make([]int, len(rf.peers))
			}
			if rf.matchIndex == nil {
				rf.matchIndex = make([]int, len(rf.peers))
			}
			log.Printf("%d commitIdx: %d", rf.me, rf.commitIndex)
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			log.Printf("No.%d send append entries", rf.me)
			rf.mu.Unlock()
			go rf.sendAppendEntries()
			break
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.raftState = Follower
	rf.log = []logEntry{{Command: nil, Term: 0}} // dummy log entry to make log index start at 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastBeats = time.Now()
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
