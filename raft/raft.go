// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap/log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// print debug information
const (
	debugElection      bool = false
	debugStateTransfer      = false
	debugHeartBeat          = false
	debugLogAppend          = false
	debugMessage            = false
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

type VoteResult uint8

const (
	VoteWon VoteResult = iota
	VoteLost
	VotePending
)

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.rand.Intn(n)
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// my raft data

	// randomizedElectionTimeout is a random number between
	// [electionTimeout, 2 * electionTimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	//step                      stepFunc
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	// if use memory storage, have a dummy entry at term 0, index 0
	raftLog := newLog(c.Storage)
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            nil,
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	progress := r.Prs[to]
	term, _ := r.RaftLog.Term(progress.Next - 1)
	ents, _ := r.RaftLog.EntriesAfter(progress.Next)
	// if there are no new entries, don't send append message
	if len(ents) == 0 {
		return false
	}
	m.LogTerm = term
	m.Index = progress.Next - 1
	m.Entries = ents
	m.Commit = r.RaftLog.committed

	r.send(m)
	return true
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
			if debugLogAppend {
				fmt.Printf("%x send append entry to %x at term %d\n", r.id, peer, r.Term)
			}
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	_, ok := r.Prs[to]
	if !ok {
		log.Panic("peer not in the cluster")
	}
	heartBeatMessage := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Entries: nil,
	}
	r.send(heartBeatMessage)
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
			if debugHeartBeat {
				fmt.Printf("%x send heartbeat to %x at term %d\n", r.id, peer, r.Term)
			}
		}
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	if err != nil {
		log.Panic(err.Error())
	}
	requestVoteMessage := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	}
	r.msgs = append(r.msgs, requestVoteMessage)
}

func (r *Raft) bcastRequestVote() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
			if debugElection {
				fmt.Printf("%x send requestVote to %x at term %d\n", r.id, peer, r.Term)
			}
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				Term:    0, // local message
			})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				Term:    0, // local message
			})
			if err != nil {
				return
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	//r.step = stepFollower
	r.Lead = lead
	r.State = StateFollower
	if debugStateTransfer {
		fmt.Printf("%x became follower in term %d\n", r.id, r.Term)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	//r.step = stepCandidate
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
	if debugStateTransfer {
		fmt.Printf("%x became candidate in term %d\n", r.id, r.Term)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	//r.step = stepLeader
	r.State = StateLeader
	r.Lead = r.id

	//only update Prs in becomeLeader
	//lastIndex := r.RaftLog.LastIndex()
	//for peer := range r.Prs {
	//	r.Prs[peer].Next = lastIndex + 1
	//	r.Prs[peer].Match = 0
	//}

	lastIndex := r.RaftLog.LastIndex()
	emptyEnt := &pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
		Data:  nil,
	}
	lastIndex = r.RaftLog.Append(emptyEnt)
	// update leader's progress
	r.Prs[r.id] = &Progress{
		Match: lastIndex,
		Next:  lastIndex + 1,
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
	if debugStateTransfer {
		fmt.Printf("%x became leader in term %d\n", r.id, r.Term)
	}

	// update commit index
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// handle common message in this function
	switch {
	case m.Term == 0:
		// if m.Term == 0, m is local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		if debugMessage {
			fmt.Printf("%x [term: %d] ignored a %s message with lower term from %x [term: %d]\n",
				r.id, r.Term, m.MsgType, m.From, m.Term)
		}
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgRequestVote {
			// Any of the three message types is OK
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From ||
			r.Vote == None && r.Lead == None
		if debugElection {
			fmt.Printf("canVote: %v, isUpToDate: %v\n", canVote, r.RaftLog.isUpToDate(m.Index, m.LogTerm))
		}
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				Term:    r.Term,
			})
			r.Vote = m.From
			r.electionElapsed = 0
		} else {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	default:
		// handle unique message in each state
		//fmt.Printf("%v %d enter step %v\n", r.State, r.id, r.step)
		var err error = nil
		switch r.State {
		case StateFollower:
			err = stepFollower(r, m)
		case StateCandidate:
			err = stepCandidate(r, m)
		case StateLeader:
			err = stepLeader(r, m)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *Raft, m pb.Message) error

func stepLeader(r *Raft, m pb.Message) error {
	//fmt.Printf("%d enter stepLeader\n", r.id)
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgBeat:
		//fmt.Printf("%d enter bcastheartbeat\n", r.id)
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			panic(fmt.Sprintf("%x stepped empty MsgProp", r.id))
		}
		r.RaftLog.Append(m.Entries...)
		r.bcastAppend()
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	//fmt.Printf("%d enter stepCandidate\n", r.id)
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, !m.Reject)
		if debugElection {
			fmt.Printf("%x has received %d %s votes and %d vote rejections\n", r.id, gr, m.MsgType, rj)
		}
		switch res {
		case VoteWon:
			r.becomeLeader()
			r.bcastAppend()
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	//fmt.Printf("%d enter stepFollower\n", r.id)
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// shouldn't modify committed entries
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			Reject:  false,
		})
		return
	}

	if lastNewIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   lastNewIndex,
			Reject:  false,
		})
	} else {
		//hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex := m.Index - 1
		hintTerm, _ := r.RaftLog.Term(hintIndex)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Index:   hintIndex,
			LogTerm: hintTerm,
			Reject:  true,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Entries: nil,
		Commit:  r.RaftLog.committed,
		Reject:  false,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.resetVotes()
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0
		if peer == r.id {
			r.Prs[peer].Match = lastIndex
		}
	}
	r.leadTransferee = None
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// reset votes to empty map
func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		if debugElection {
			fmt.Printf("%x ignoring MsgHup because already leader\n", r.id)
		}
		return
	}
	// TODO judge config message

	r.campaign()
}

// campaign transitions the raft instance to candidate state and broadcasts request vote messages
func (r *Raft) campaign() {
	r.becomeCandidate()
	if _, _, result := r.poll(r.id, true); result == VoteWon {
		r.becomeLeader()
	}
	// broadcast request vote
	for peer := range r.Prs {
		if peer != r.id {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				LogTerm: r.RaftLog.LastTerm(),
				Index:   r.RaftLog.LastIndex(),
			})
		}
	}
}

// poll record vote and calculate vote result
func (r *Raft) poll(id uint64, v bool) (granted int, rejected int, result VoteResult) {
	r.recordVote(id, v)

	return r.TallyVotes()
}

func (r *Raft) recordVote(id uint64, voteGranted bool) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = voteGranted
	}
}

func (r *Raft) TallyVotes() (granted int, rejected int, result VoteResult) {
	for id, _ := range r.Prs {
		v, voted := r.votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	q := len(r.Prs)/2 + 1
	if granted >= q {
		result = VoteWon
	} else if rejected >= q {
		result = VoteLost
	} else {
		result = VotePending
	}
	return granted, rejected, result
}
