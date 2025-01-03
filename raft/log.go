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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage is nil")
	}
	raftLog := &RaftLog{
		storage:         storage,
		pendingSnapshot: nil,
	}
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	raftLog.committed = hardState.Commit
	raftLog.applied = firstIndex - 1
	raftLog.stabled = lastIndex
	raftLog.entries = entries

	return raftLog
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, stabled=%d", l.committed, l.applied, l.stabled)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	entries := l.entries
	// remove dummy entry
	if len(entries) > 0 && entries[0].Term == 0 {
		entries = entries[1:]
	}
	return entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled < firstIndex {
			return l.entries
		}
		if l.stabled < l.LastIndex() {
			return l.entries[l.stabled-firstIndex+1:]
		}
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
// if the log is empty, return last index of the storage
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		i, _ := l.storage.LastIndex()
		return i
	}
	return l.entries[len(l.entries)-1].Index
}

// FirstIndex return the first index of the log entries
// if the log is empty, return first index of the storage
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex, lastIndex := l.FirstIndex(), l.LastIndex()
		if i >= firstIndex && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}
	// len(l.entries) == 0 or i < firstIndex or i > lastIndex
	// should query the storage
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}

// my function

// return the last index after append entries
func (l *RaftLog) Append(entries ...*pb.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastIndex()
	}
	ents := convertPointerToEntry(entries)
	l.truncateAndAppend(ents)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(entries []pb.Entry) {
	after := entries[0].Index
	switch {
	case after == l.stabled+uint64(len(l.entries))+1:
		l.entries = append(l.entries, entries...)
		if debugLogAppend {
			fmt.Printf("append %d entries at index %d\n", len(entries), after)
		}
	case after <= l.stabled:
		if debugLogAppend {
			fmt.Printf("replace the unstable entries from index %d\n", after)
		}
		l.stabled = after - 1
		l.entries = entries
	default:
		if debugLogAppend {
			fmt.Printf("truncate the unstable entries before index %d\n", after)
		}
		l.entries = append([]pb.Entry{}, l.slice(l.stabled+1, after)...)
		l.entries = append(l.entries, entries...)
	}
}

// return the entries from i to the end
func (l *RaftLog) EntriesAfter(i uint64) ([]*pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	res := convertEntryToPointer(l.slice(i, l.LastIndex()+1))
	return res, nil
}

func (l *RaftLog) slice(lo, hi uint64) []pb.Entry {
	l.mustCheckOutOfBounds(lo, hi)
	off := l.entries[0].Index
	return l.entries[lo-off : hi-off]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		log.Panic(fmt.Sprintf("invalid unstable.slice %d > %d", lo, hi))
	}
	upper := l.stabled + uint64(len(l.entries)) + 1
	if lo <= l.stabled || hi > upper {
		log.Panic(fmt.Sprintf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, l.stabled+1, upper))
	}
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	lastIndex := l.LastIndex()
	lastTerm := l.LastTerm()
	return term > lastTerm || (term == lastTerm && lasti >= lastIndex)
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panic(fmt.Sprintf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex()))
		}
		l.committed = tocommit
	}
}

func convertPointerToEntry(entries []*pb.Entry) []pb.Entry {
	ents := make([]pb.Entry, len(entries))
	for i, entry := range entries {
		ents[i] = *entry
	}
	return ents
}

func convertEntryToPointer(entries []pb.Entry) []*pb.Entry {
	ents := make([]*pb.Entry, len(entries))
	for i := 0; i < len(entries); i++ {
		ents[i] = &entries[i]
	}
	return ents
}

func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		panic(fmt.Sprintf("unexpected error when getting the last term (%v)", err))
	}
	return t
}

func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panic(fmt.Sprintf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed))
		default:
			offset := index + 1
			l.Append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) matchTerm(index, logTerm uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == logTerm
}

func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, entry := range ents {
		if !l.matchTerm(entry.Index, entry.Term) {
			if entry.Index <= l.LastIndex() {
				existingTerm, _ := l.Term(entry.Index)
				if debugLogAppend {
					fmt.Printf("found conflict at index %d [existing term: %d, conflicting term: %d]",
						entry.Index, existingTerm, entry.Term)
				}
			}
			return entry.Index
		}
	}
	return 0
}
