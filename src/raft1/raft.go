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

	// Volatile state
	raftState    RaftState
	commitIndex  int
	lastApplied  int
	lastBeats    time.Time
	applyCh      chan raftapi.ApplyMsg
	snapshot     []byte
	snapshotIdx  int
	snapshotTerm int

	// Volatile state on leader
	nextIndex     []int
	matchIndex    []int
	appendRunning bool
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
	return time.Duration(800+rand.Int63()%300) * time.Millisecond
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.snapshotIdx)
	encoder.Encode(rf.snapshotTerm)
	bufferBytes := buffer.Bytes()
	if len(rf.snapshot) > 0 {
		rf.persister.Save(bufferBytes, snapshot)
	} else {
		rf.persister.Save(bufferBytes, nil)
	}
	log.Printf("No.%d persist term:%d voteFor:%d, raftState:%d", rf.me, rf.currentTerm, rf.votedFor, rf.raftState)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	var snapshotIdx int
	var snapshotTerm int
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&snapshotIdx) != nil ||
		decoder.Decode(&snapshotTerm) != nil {
		log.Printf("No.%d readPersist error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.snapshotIdx = snapshotIdx
		rf.snapshotTerm = snapshotTerm
		rf.commitIndex = snapshotIdx
		rf.lastApplied = snapshotIdx
		log.Printf("No.%d readPersist success, term: %d", rf.me, rf.currentTerm)
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
	log.Printf("No.%d receive snapshot at index:%d", rf.me, index)
	rf.mu.Lock()
	diff := index - rf.snapshotIdx
	rf.snapshotTerm = rf.log[diff].Term
	rf.snapshot = snapshot
	rf.snapshotIdx = index
	rf.log = rf.log[diff:]
	rf.mu.Unlock()
	rf.persist(snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d receive installSnapshot from %d, lock", rf.me, args.LeaderId)
	changeFlag := false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("No.%d term:%d reject installSnapshot from %d", rf.me, rf.currentTerm, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		changeFlag = true
	}
	if rf.raftState != Follower || rf.votedFor != args.LeaderId {
		changeFlag = true
		rf.raftState = Follower
		rf.votedFor = args.LeaderId
	}
	rf.lastBeats = time.Now()

	if rf.snapshotIdx < args.LastIncludedIndex {
		rf.log = []logEntry{{Command: nil, Term: args.LastIncludedTerm}}
		changeFlag = true
		rf.snapshotIdx = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		applyMsg := raftapi.ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
		}
		rf.applyCh <- applyMsg
		log.Printf("No.%d send applyCh snapshot %v", rf.me, applyMsg)
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	if changeFlag {
		go rf.persist(args.Data)
	}
	log.Printf("%d state:%d receive installSnapshot from %d, unlock reply:%v", rf.me, rf.raftState, args.LeaderId, reply)
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
	maxLogIdx, lastLogTerm := rf.lastEntry()
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
		go rf.persist(rf.snapshot)
	}
	log.Printf("%d state %d requestVote unlock, voteFor %d, granted %v", rf.me, rf.raftState, rf.votedFor, reply.VoteGranted)
}

func (rf *Raft) lastEntry() (int, int) {
	term := rf.snapshotTerm
	lastIndex := 0
	length := len(rf.log)
	if length > 0 {
		lastIndex = length - 1
		term = rf.log[lastIndex].Term
	}
	return lastIndex + rf.snapshotIdx, term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d, state: %d receive appendEntries from %d, lock", rf.me, rf.raftState, args.LeaderId)
	changeFlag := false
	reply.Term = rf.currentTerm
	reply.CmtIdx = rf.commitIndex
	lastIdx, lastTerm := rf.lastEntry()
	log.Printf("No.%d lastIdx:%d lastTerm:%d", rf.me, lastIdx, lastTerm)

	if args.Term < rf.currentTerm {
		reply.Success = false
		log.Printf("No.%d term:%d lastIdx:%d lastEntry.Term:%d PreLogIdx:%d PrevLogTerm:%d reject appendEntry from %d", rf.me, rf.currentTerm, lastIdx, lastTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		changeFlag = true
	}

	if lastIdx > args.PrevLogIndex && args.PrevLogIndex >= rf.snapshotIdx {
		rf.log = rf.log[:args.PrevLogIndex+1-rf.snapshotIdx]
		lastIdx, lastTerm = rf.lastEntry()
		changeFlag = true
		log.Printf("No.%d delete log entry from index %d", rf.me, args.PrevLogIndex+1)
	}
	if lastIdx == args.PrevLogIndex {
		if lastTerm == args.PrevLogTerm {
			reply.Success = true
			if args.Entries != nil {
				rf.log = append(rf.log, args.Entries...)
				changeFlag = true
				log.Printf("No.%d append log %v at index %d", rf.me, args.Entries, len(rf.log)-1)
				lastIdx, lastTerm = rf.lastEntry()
			} else {
				log.Printf("No.%d receive heartbeats", rf.me)
			}
			if args.LeaderCommit > rf.commitIndex {
				commitLen := min(args.LeaderCommit, rf.snapshotIdx+len(rf.log))
				commitIdx := rf.commitIndex
				argsMsg := []raftapi.ApplyMsg{}
				for commitIdx < commitLen {
					commitIdx++
					applyMsg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[commitIdx-rf.snapshotIdx].Command,
						CommandIndex: commitIdx,
					}
					argsMsg = append(argsMsg, applyMsg)
					log.Printf("No.%d send applycCh %v, commitIdx: %d, commitLen: %d, snapshotIdx:%d", rf.me, applyMsg, commitIdx, commitLen, rf.snapshotIdx)
				}
				go rf.sendApplyMsg(argsMsg)
				rf.commitIndex = commitLen
				rf.lastApplied = rf.commitIndex
				reply.CmtIdx = rf.commitIndex
				log.Printf("No.%d receive appendEntries from %d, update commitIndex to %d", rf.me, args.LeaderId, rf.commitIndex)
			}
		} else if lastTerm != args.PrevLogTerm {
			rf.log = rf.log[:lastIdx-rf.snapshotIdx]
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
	lastIdx, lastTerm = rf.lastEntry()
	if changeFlag {
		go rf.persist(rf.snapshot)
	}
	log.Printf("%d state:%d lastIdx:%d lastTerm:%v receive append from %d, unlock reply:%v", rf.me, rf.raftState, lastIdx, lastTerm, args.LeaderId, reply)
}

func (rf *Raft) sendApplyMsg(args []raftapi.ApplyMsg) {
	for i := range args {
		rf.applyCh <- args[i]
	}
}
func (rf *Raft) sendAppendEntries() bool {
	for true {
		rf.mu.Lock()
		log.Printf("No.%d start send append entries, state:%d", rf.me, rf.raftState)
		if rf.raftState != Leader {
			rf.appendRunning = false
			rf.mu.Unlock()
			return false
		}
		rf.appendRunning = true
		var entries []logEntry
		// if rf.commitIndex+1 >= rf.snapshotIdx {
		entries = rf.log[rf.commitIndex+1-rf.snapshotIdx:]
		// }
		log.Printf("%d append lock, not committed entries: %v", rf.me, entries)
		replyCh := make(chan *AppendEntriesReply, 2)
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					rf.sendEntriesOrSnapshot(server, replyCh)
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
				}
				log.Printf("No.%d send applycCh %d", rf.me, commitIdx)
			}()
			rf.commitIndex += len(entries)
			rf.lastApplied = rf.commitIndex
			log.Printf("No.%d receive %d commit logs: %d", rf.me, commitCount, rf.commitIndex)
		}
		time.Sleep(150 * time.Millisecond)
	}
	return true
}

func (rf *Raft) sendEntriesOrSnapshot(server int, replyCh chan *AppendEntriesReply) {
	rf.mu.Lock()
	log.Printf("No.%d send %d append arg from %d, len %d", rf.me, server, rf.nextIndex[server], len(rf.log))
	preLogTerm := rf.snapshotTerm
	entries := []logEntry{}
	if rf.nextIndex[server] >= rf.snapshotIdx {
		entries = rf.log[rf.nextIndex[server]-rf.snapshotIdx:]
		if rf.nextIndex[server] > rf.snapshotIdx {
			preLogTerm = rf.log[rf.nextIndex[server]-1-rf.snapshotIdx].Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  preLogTerm,
			Entries:      entries,
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
	} else {
		rf.mu.Unlock()
		sendInstallOk := rf.sendInstallSnapshot(server)
		replyCh <- nil
		if !sendInstallOk {
			log.Printf("No.%d send installSnapshot to No.%d failed", rf.me, server)
			return
		}
	}
}

func (rf *Raft) appendEntriesHandle(replyChan chan *AppendEntriesReply) int {
	commitCount := 1
	now := time.Now()
	for range len(rf.peers) - 1 {
		var reply *AppendEntriesReply
		select {
		case reply = <-replyChan:
			if reply == nil || time.Since(now) > 1000*time.Millisecond {
				log.Printf("No.%d append timeout continue+", rf.me)
				continue
			}
		case <-time.After(400 * time.Millisecond):
			log.Printf("No.%d append timeout continue-", rf.me)
			continue
		}

		server := reply.Index
		log.Printf("%d term:%d, handle server:%d's append reply", rf.me, rf.currentTerm, server)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm || rf.raftState != Leader {
			rf.currentTerm = reply.Term
			rf.raftState = Follower
			rf.votedFor = -1
			go rf.persist(rf.snapshot)
			rf.lastBeats = time.Now()
			rf.appendRunning = false
			rf.mu.Unlock()
			return -1
		}
		if reply.Success {
			rf.matchIndex[server] = len(rf.log) - 1 + rf.snapshotIdx
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.mu.Unlock()
			commitCount++
		} else {
			if reply.CmtIdx >= rf.snapshotIdx {
				log.Printf("No.%d reduce nextIdx %d to match No.%d", rf.me, rf.nextIndex[server], reply.Index)
				rf.nextIndex[server] = min(max(1, reply.CmtIdx+1), len(rf.log)+rf.snapshotIdx)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				sendInstallOk := rf.sendInstallSnapshot(server)
				if !sendInstallOk {
					log.Printf("No.%d send installSnapshot to No.%d failed", rf.me, server)
					return -1
				}
			}
		}
	}
	return commitCount
}

func (rf *Raft) sendInstallSnapshot(server int) bool {
	rf.mu.Lock()
	log.Printf("No.%d send installSnapshot to No.%d, snapshotIdx: %d, nextIdx: %d", rf.me, server, rf.snapshotIdx, rf.nextIndex[server])
	installSnapshotArgs := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIdx,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()
	installSnapshotReply := &InstallSnapshotReply{}
	rf.peers[server].Call("Raft.InstallSnapshot", installSnapshotArgs, installSnapshotReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if installSnapshotReply.Term > rf.currentTerm {
		rf.currentTerm = installSnapshotReply.Term
		rf.raftState = Follower
		rf.votedFor = -1
		go rf.persist(rf.snapshot)
		rf.appendRunning = false
		rf.lastBeats = time.Now()
		return false
	}
	rf.matchIndex[server] = rf.snapshotIdx
	rf.nextIndex[server] = rf.snapshotIdx + 1
	return true
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("%d send to %d appendRPC %v", rf.me, server, args)
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
		go rf.persist(rf.snapshot)
		index = len(rf.log) - 1 + rf.snapshotIdx
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
		log.Printf("No.%d ticker() lock , state: %v", rf.me, rf.raftState)
		rf.mu.Unlock()

		if time.Since(lastHeartB) > beatsTimeout() && raftState != Leader {
			rf.mu.Lock()
			if raftState == Follower {
				rf.raftState = Candidate
			}
			rf.currentTerm++
			rf.votedFor = rf.me
			log.Printf("up to candidate %d term %d, time %d", rf.me, rf.currentTerm, time.Since(rf.lastBeats).Milliseconds())
			rf.lastBeats = time.Now()
			go rf.persist(rf.snapshot)
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			lastLogIndex, lastLogTerm := rf.lastEntry()
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
	now := time.Now()
	for range len(rf.peers) - 1 {
		var reply *RequestVoteReply
		select {
		case reply = <-replyChan:
			if reply == nil || time.Since(now) > 1000*time.Millisecond {
				log.Printf("No.%d request vote timeout continue+", rf.me)
				continue
			}
		case <-time.After(400 * time.Millisecond):
			log.Printf("No.%d request vote timeout continue-", rf.me)
			continue
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			log.Printf("No.%d 's vote is denied", rf.me)
			rf.currentTerm = reply.Term
			rf.raftState = Follower
			rf.votedFor = -1
			go rf.persist(rf.snapshot)
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
			go rf.persist(rf.snapshot)
			// initialize leader state
			if rf.nextIndex == nil {
				rf.nextIndex = make([]int, len(rf.peers))
			}
			if rf.matchIndex == nil {
				rf.matchIndex = make([]int, len(rf.peers))
			}
			log.Printf("%d commitIdx: %d snapIdx:%d", rf.me, rf.commitIndex, rf.snapshotIdx)
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log) + rf.snapshotIdx
				rf.matchIndex[i] = 0
			}
			log.Printf("No.%d become leader", rf.me)
			if !rf.appendRunning {
				go rf.sendAppendEntries()
			}
			rf.mu.Unlock()
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
	rf.snapshot = persister.ReadSnapshot()
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
