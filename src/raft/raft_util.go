package raft

import (
	"math/rand"
	"time"
)

func getRandomTimeout(lowerBound, upperBound time.Duration) time.Duration {
	timeRange := upperBound - lowerBound
	variance := rand.Int63n(timeRange.Milliseconds())
	return lowerBound + time.Duration(variance)*time.Millisecond
}

// Returns a strict majority
func majority(totalNum int) int {
	return (totalNum)/2 + 1
}

func isMajority(num int, totalNum int) bool {
	return num >= majority(totalNum)
}

// Determine if the first log is more up-to-date than the second.
func isMoreUpToDate(lastEntryTerm1, logLength1, lastEntryTerm2, logLength2 int) bool {
	if lastEntryTerm1 != lastEntryTerm2 {
		return lastEntryTerm1 > lastEntryTerm2
	} else {
		return logLength1 > logLength2
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
	assert(left >= 1)
	assert(right <= len(log))
	assert(left <= right)

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

	assert(left == right)
	if matchFunc(log[left-1]) {
		return left
	} else {
		return -left
	}
}
