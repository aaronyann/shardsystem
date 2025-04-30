package paxosrsm

import (
	"time"

	"umich.edu/eecs491/proj4/paxos"
)

// Request types
type GetSeqRequest struct {
	ResultCh chan int
}

type UpdateSeqRequest struct {
	Seq     int
	DoneCh  chan struct{}
}

type UpdateDoneRequest struct {
	Seq     int
	DoneCh  chan struct{}
}

type CheckAppliedRequest struct {
	Seq      int
	ResultCh chan bool
}

type MarkAppliedRequest struct {
	Seq     int
	DoneCh  chan struct{}
}

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	// Channels for state access
	getSeq       chan GetSeqRequest
	updateSeq    chan UpdateSeqRequest
	updateDone   chan UpdateDoneRequest
	checkApplied chan CheckAppliedRequest
	markApplied  chan MarkAppliedRequest
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl = PaxosRSMImpl{
		getSeq:       make(chan GetSeqRequest),
		updateSeq:    make(chan UpdateSeqRequest),
		updateDone:   make(chan UpdateDoneRequest),
		checkApplied: make(chan CheckAppliedRequest),
		markApplied:  make(chan MarkAppliedRequest),
	}
	
	// Start state manager goroutine
	go rsm.stateManager()
}

// Background goroutine to manage state
func (rsm *PaxosRSM) stateManager() {
	nextSeq := 0
	doneSeq := -1
	applied := make(map[int]bool)
	
	// Create a ticker that triggers every 10 seconds
	cleanupTicker := time.NewTicker(70 * time.Millisecond)
	defer cleanupTicker.Stop()  // Ensure the ticker is stopped when the function exits

	for {
		select {
		case req := <-rsm.impl.getSeq:
			req.ResultCh <- nextSeq
			close(req.ResultCh)
			
		case req := <-rsm.impl.updateSeq:
			if req.Seq > nextSeq {
				nextSeq = req.Seq
			}
			close(req.DoneCh)
			
		case req := <-rsm.impl.updateDone:
			if req.Seq > doneSeq {
				doneSeq = req.Seq
			}
			close(req.DoneCh)
			
		case req := <-rsm.impl.checkApplied:
			req.ResultCh <- applied[req.Seq]
			close(req.ResultCh)
			
		case req := <-rsm.impl.markApplied:
			applied[req.Seq] = true
			close(req.DoneCh)

		// This case will execute periodically every 100 ms
		case <-cleanupTicker.C:
			// Clean up applied map
			for k := range applied {
				if k < doneSeq {
					delete(applied, k)
				}
			}
		}
	}
}

// Helper functions for state access
func (rsm *PaxosRSM) GetNextSeq() int {
	resultCh := make(chan int, 1)
	rsm.impl.getSeq <- GetSeqRequest{ResultCh: resultCh}
	return <-resultCh
}

func (rsm *PaxosRSM) updateNextSeq(seq int) {
	doneCh := make(chan struct{}, 1)
	rsm.impl.updateSeq <- UpdateSeqRequest{Seq: seq, DoneCh: doneCh}
	<-doneCh
}

func (rsm *PaxosRSM) updateDone(seq int) {
	doneCh := make(chan struct{}, 1)
	rsm.impl.updateDone <- UpdateDoneRequest{Seq: seq, DoneCh: doneCh}
	<-doneCh
}

func (rsm *PaxosRSM) isApplied(seq int) bool {
	resultCh := make(chan bool, 1)
	rsm.impl.checkApplied <- CheckAppliedRequest{Seq: seq, ResultCh: resultCh}
	return <-resultCh
}

func (rsm *PaxosRSM) markApplied(seq int) {
	doneCh := make(chan struct{}, 1)
	rsm.impl.markApplied <- MarkAppliedRequest{Seq: seq, DoneCh: doneCh}
	<-doneCh
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	seq := rsm.GetNextSeq()
	
	for {
		// Check if this instance is already decided
		status, decidedVal := rsm.px.Status(seq)
		
		if status == paxos.Decided {
			// Apply the operation if not already applied
			if !rsm.isApplied(seq) {
				rsm.applyOp(decidedVal)
				rsm.markApplied(seq)
			}
			
			// Mark done
			rsm.updateDone(seq)
			rsm.px.Done(seq)
			
			// If it's our value, we're done
			if rsm.equals(decidedVal, v) {
				rsm.updateNextSeq(seq + 1)
				return
			}
			
			// Not our value, try the next sequence
			seq++
			if seq > rsm.GetNextSeq() {
				rsm.updateNextSeq(seq)
			}
			continue
		}
		
		// Try to propose our value
		rsm.px.Start(seq, v)
		
		// Wait for decision with exponential backoff
		timeout := false
		to := 10 * time.Millisecond
		start := time.Now()
		maxWait := 3 * time.Second
		
		for time.Since(start) < maxWait && !timeout {
			status, decidedVal := rsm.px.Status(seq)
			
			if status == paxos.Decided {
				// Apply the operation if not already applied
				if !rsm.isApplied(seq) {
					rsm.applyOp(decidedVal)
					rsm.markApplied(seq)
				}
				
				// Mark done
				rsm.updateDone(seq)
				rsm.px.Done(seq)
				
				// If it's our value, we're done
				if rsm.equals(decidedVal, v) {
					rsm.updateNextSeq(seq + 1)
					return
				}
				
				// Not our value, break out and try next sequence
				timeout = true
				break
			}
			
			time.Sleep(to)
			if to < 100*time.Millisecond {
				to *= 2
			}
		}
		
		// Move to next sequence
		seq++
		if seq > rsm.GetNextSeq() {
			rsm.updateNextSeq(seq)
		}
	}
}