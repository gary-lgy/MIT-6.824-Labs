package raft

import (
	"fmt"
	"log"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Debugging
const Debug = false

func dprint(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func serverDPrint(id int, state raftServerState, source string, format string, args ...interface{}) {
	allArgs := append([]interface{}{id, state, source}, args...)
	dprint("%d | %-10s | %-13s | "+format, allArgs...)
}

func stringifyState(rf *Raft) string {
	if !Debug {
		return ""
	}

	logEntries := make([]LogEntry, 0, len(rf.log))
	for _, entry := range rf.log {
		// Store the command as a pointer but the term as a value
		// So that the output will be useful
		logEntries = append(logEntries, LogEntry{Term: entry.Term, Command: &entry.Command})
	}
	return fmt.Sprintf("%+v",
		struct {
			me               int
			currentTerm      int
			votedFor         int
			log              []LogEntry
			state            raftServerState
			commitIndex      int
			lastApplied      int
			leaderId         int
			nextIndex        []int
			matchIndex       []int
			numVotesGathered int
		}{
			me:               rf.me,
			currentTerm:      rf.currentTerm,
			votedFor:         rf.votedFor,
			log:              logEntries,
			state:            rf.state,
			commitIndex:      rf.commitIndex,
			lastApplied:      rf.lastApplied,
			leaderId:         rf.leaderId,
			nextIndex:        rf.nextIndex,
			matchIndex:       rf.matchIndex,
			numVotesGathered: rf.numVotesGathered,
		})
}

func stringifyCommand(command interface{}) string {
	if !Debug {
		return ""
	}
	stringifiedCommand := fmt.Sprintf("%+v", command)
	if len(stringifiedCommand) > 10 {
		stringifiedCommand = stringifiedCommand[:10] + "..."
	}
	return stringifiedCommand
}

func Assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}

// Takes in inclusive, 1-based search bounds.
// If a matching entry is found, return the 1-based index of the entry.
// Otherwise, return a negative number.
func binarySearchLogEntry(
	left, right int,
	log []*LogEntry,
	wantFirstEntry bool,
	matchFunc func(*LogEntry) bool,
) int {
	Assert(left >= 1)
	Assert(right <= len(log))
	Assert(left <= right)

	for left < right {
		mid := left + (right-left)/2
		if !wantFirstEntry {
			// If we're looking for the last entry, use a right-biased middle point to guarantee progress
			mid = left + (right-left+1)/2
		}
		if matchFunc(log[mid-1]) {
			if wantFirstEntry {
				right = mid
			} else {
				left = mid
			}
		} else {
			if wantFirstEntry {
				left = mid + 1
			} else {
				right = mid - 1
			}
		}
	}

	Assert(left == right)
	if matchFunc(log[left-1]) {
		return left
	} else {
		return -left
	}
}
