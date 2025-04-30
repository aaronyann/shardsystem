package paxos

import (
	"math"
	"time"

	"umich.edu/eecs491/proj4/common"
)

// Structure for instance state
type PaxosInstanceState struct {
	highestPrepare int         // highest prepare number seen
	highestAccept  int         // highest accept number seen
	acceptVal      interface{} // value associated with highestAccept
	isDecided      bool        // whether consensus has been reached
	decidedValue   interface{} // final consensus value
}

// Reply structures for different operations
type StatusResponse struct {
	InstanceFate Fate
	InstanceVal  interface{}
}

type DecisionCheckResponse struct {
	HasDecided bool
	ConsensusValue interface{}
}

// Operation types for channel-based communication
type PrepareOperation struct {
	Arguments  *PrepareArgs
	Response   *PrepareReply
	Completed  chan bool
}

type AcceptOperation struct {
	Arguments  *AcceptArgs
	Response   *AcceptReply
	Completed  chan bool
}

type InformOperation struct {
	Arguments  *InformArgs
	Response   *InformReply
	Completed  chan bool
}

// additions to Paxos state.
type PaxosImpl struct {
	instanceMap   map[int]*PaxosInstanceState // map from seq num to instance state
	doneValues    []int             // highest instance each peer is done with
	localDoneMax  int               // highest instance this peer is done with
	maxSequence   int               // highest sequence number seen

	// Separate channels for different operation types
	prepareChannel chan PrepareOperation
	acceptChannel  chan AcceptOperation
	informChannel  chan InformOperation
	statusChannel  chan struct {
		seqNum int
		responseChannel chan StatusResponse
	}
	doneChannel chan struct {
		seqNum int
		confirmChannel chan bool
	}
	minChannel      chan chan int
	maxChannel      chan chan int
	decisionCheckChannel chan struct {
		seqNum int
		responseChannel chan DecisionCheckResponse
	}
	setDecisionChannel chan struct {
		seqNum int
		value interface{}
		confirmChannel chan bool
	}
}

// Initialize implementation components
func (px *Paxos) initImpl() {
	px.impl = PaxosImpl{
		instanceMap:   make(map[int]*PaxosInstanceState),
		doneValues:    make([]int, len(px.peers)),
		localDoneMax:  -1,
		maxSequence:   -1,

		// Initialize channels with buffering to prevent deadlocks
		prepareChannel: make(chan PrepareOperation, 100),
		acceptChannel:  make(chan AcceptOperation, 100),
		informChannel:  make(chan InformOperation, 100),
		statusChannel: make(chan struct {
			seqNum int
			responseChannel chan StatusResponse
		}, 100),
		doneChannel: make(chan struct {
			seqNum int
			confirmChannel chan bool
		}, 100),
		minChannel: make(chan chan int, 100),
		maxChannel: make(chan chan int, 100),
		decisionCheckChannel: make(chan struct {
			seqNum int
			responseChannel chan DecisionCheckResponse
		}, 100),
		setDecisionChannel: make(chan struct {
			seqNum int
			value interface{}
			confirmChannel chan bool
		}, 100),
	}

	// Initialize all doneValues to -1 (nothing done yet)
	for i := range px.impl.doneValues {
		px.impl.doneValues[i] = -1
	}

	// Start goroutine to process all channels
	go px.processEvents()
}

// Main event processing loop
func (px *Paxos) processEvents() {
	for !px.isdead() {
		select {
		case op := <-px.impl.prepareChannel:
			px.processPrepareOp(op)
		case op := <-px.impl.acceptChannel:
			px.processAcceptOp(op)
		case op := <-px.impl.informChannel:
			px.processInformOp(op)
		case req := <-px.impl.statusChannel:
			px.processStatusRequest(req.seqNum, req.responseChannel)
		case req := <-px.impl.doneChannel:
			px.processDoneRequest(req.seqNum, req.confirmChannel)
		case responseChannel := <-px.impl.minChannel:
			px.processMinRequest(responseChannel)
		case responseChannel := <-px.impl.maxChannel:
			px.processMaxRequest(responseChannel)
		case req := <-px.impl.decisionCheckChannel:
			px.processDecisionCheckRequest(req.seqNum, req.responseChannel)
		case req := <-px.impl.setDecisionChannel:
			px.processSetDecisionRequest(req.seqNum, req.value, req.confirmChannel)
		case <-px.term:
			return
		}
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// If this instance is less than Min(), ignore it
	if seq < px.Min() {
		return
	}

	// Update maxSequence
	if seq > px.impl.maxSequence {
		px.impl.maxSequence = seq
	}

	// Start a new consensus attempt for this instance
	go px.runConsensus(seq, v)
}

// Runs the consensus algorithm for a specific instance
func (px *Paxos) runConsensus(seq int, v interface{}) {
	if px.isdead() {
		return
	}

	// Generate a unique ballot number with the peer ID as part of it
	n := px.me + (seq * 1000)
	majority := len(px.peers)/2 + 1

	for !px.isdead() {

		// Increment ballot number to ensure uniqueness
		n = n + len(px.peers)

		// PREPARE PHASE
		prepareOK := 0
		highestAccept := -1
		var highestAcceptVal interface{}
		
		// Track acceptance counts for early majority detection
		acceptCountMap := make(map[int]int)
		var consensusReached bool = false

		// Send prepare to all peers
		for i := range px.peers {
			args := &PrepareArgs{
				Seq:    seq,
				N:      n,
				PeerID: px.me,
				Done:   px.impl.localDoneMax,
			}

			var reply PrepareReply

			if i == px.me {
				px.Prepare(args, &reply)
			} else if !px.isdead() {
				if !common.Call(px.peers[i], "Paxos.Prepare", args, &reply) {
					continue
				}
			} else {
				break
			}

			// Process reply
			if reply.Reply == OK {
				prepareOK++

				// Update our knowledge of other peer's done value
				if reply.Done > px.impl.doneValues[reply.PeerID] {
					px.impl.doneValues[reply.PeerID] = reply.Done
				}

				// Check if peer has accepted a value with higher ballot
				if reply.N_a > highestAccept {
					highestAccept = reply.N_a
					highestAcceptVal = reply.V_a
				}
				
				// Track accept number counts for early majority detection
				if reply.N_a > 0 {
					acceptCountMap[reply.N_a]++
					// Check if we have a majority for this accept number
					if acceptCountMap[reply.N_a] >= majority {
						consensusReached = true
						highestAcceptVal = reply.V_a
						break
					}
				}
			}

			// Check if instance became decided during prepare phase
			decisionCheckReply := make(chan DecisionCheckResponse)
			px.impl.decisionCheckChannel <- struct {
				seqNum int
				responseChannel chan DecisionCheckResponse
			}{seq, decisionCheckReply}

			checkResult := <-decisionCheckReply
			if checkResult.HasDecided {
				px.broadcastDecision(seq, checkResult.ConsensusValue)
				return
			}
		}
		
		// If we detected a majority for some N_a in prepare phase
		if consensusReached {
			// Mark instance as decided
			confirmCh := make(chan bool)
			px.impl.setDecisionChannel <- struct {
				seqNum int
				value interface{}
				confirmChannel chan bool
			}{seq, highestAcceptVal, confirmCh}
			<-confirmCh

			// Inform all peers
			px.broadcastDecision(seq, highestAcceptVal)
			return
		}

		// If didn't get majority of prepare OKs, retry
		if prepareOK < majority {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Choose value for accept phase - if any acceptors had values, use highest ballot's value
		proposeVal := v
		if highestAccept != -1 && highestAcceptVal != nil {
			proposeVal = highestAcceptVal
		}

		// ACCEPT PHASE
		acceptOK := 0

		// Send accept to all peers
		for i := range px.peers {
			args := &AcceptArgs{
				Seq:    seq,
				N:      n,
				Value:  proposeVal,
				PeerID: px.me,
				Done:   px.impl.localDoneMax,
			}

			var reply AcceptReply

			if i == px.me {
				px.Accept(args, &reply)
			} else if !px.isdead() {
				if !common.Call(px.peers[i], "Paxos.Accept", args, &reply) {
					continue
				}
			} else {
				break
			}

			// Process reply
			if reply.Reply == OK {
				acceptOK++

				// Update our knowledge of other peer's done value
				if reply.Done > px.impl.doneValues[reply.PeerID] {
					px.impl.doneValues[reply.PeerID] = reply.Done
				}
			}

			// Check if instance became decided during accept phase
			decisionCheckReply := make(chan DecisionCheckResponse)
			px.impl.decisionCheckChannel <- struct {
				seqNum int
				responseChannel chan DecisionCheckResponse
			}{seq, decisionCheckReply}

			checkResult := <-decisionCheckReply
			if checkResult.HasDecided {
				px.broadcastDecision(seq, checkResult.ConsensusValue)
				return
			}
		}

		// If got majority of accept OKs, mark as decided
		if acceptOK >= majority {
			// Mark instance as decided
			confirmCh := make(chan bool)
			px.impl.setDecisionChannel <- struct {
				seqNum int
				value interface{}
				confirmChannel chan bool
			}{seq, proposeVal, confirmCh}
			<-confirmCh

			// Inform all peers
			px.broadcastDecision(seq, proposeVal)
			return
		}

		// If didn't get majority, wait and retry
		time.Sleep(50 * time.Millisecond)
	}
}

// Helper function to broadcast decision to all peers
func (px *Paxos) broadcastDecision(seq int, value interface{}) {
	for i := range px.peers {
		if px.isdead() {
			return
		}

		args := &InformArgs{
			Seq:    seq,
			Value:  value,
			PeerID: px.me,
			Done:   px.impl.localDoneMax,
		}

		var reply InformReply

		if i == px.me {
			px.Inform(args, &reply)
		} else {
			common.Call(px.peers[i], "Paxos.Inform", args, &reply)
		}
	}
}

// the application on this machine is done with
// all instances <= seq.
func (px *Paxos) Done(seq int) {
	confirmCh := make(chan bool)
	px.impl.doneChannel <- struct {
		seqNum int
		confirmChannel chan bool
	}{seq, confirmCh}
	<-confirmCh
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	responseChannel := make(chan int)
	px.impl.maxChannel <- responseChannel
	return <-responseChannel
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i.
func (px *Paxos) Min() int {
	responseChannel := make(chan int)
	px.impl.minChannel <- responseChannel
	return <-responseChannel
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	responseChannel := make(chan StatusResponse)
	px.impl.statusChannel <- struct {
		seqNum int
		responseChannel chan StatusResponse
	}{seq, responseChannel}

	result := <-responseChannel
	return result.InstanceFate, result.InstanceVal
}

// Handler for the Done operation
func (px *Paxos) processDoneRequest(seq int, confirmCh chan bool) {
	if seq > px.impl.localDoneMax {
		px.impl.localDoneMax = seq
		px.impl.doneValues[px.me] = seq
	}
	confirmCh <- true
}

// Handler for the Max operation
func (px *Paxos) processMaxRequest(responseChannel chan int) {
	responseChannel <- px.impl.maxSequence
}

// Handler for the Min operation
func (px *Paxos) processMinRequest(responseChannel chan int) {
	min := math.MaxInt32
	for i := 0; i < len(px.impl.doneValues); i++ {
		if px.impl.doneValues[i] < min {
			min = px.impl.doneValues[i]
		}
	}

	// Clean up forgotten instances
	for seq := range px.impl.instanceMap {
		if seq <= min {
			delete(px.impl.instanceMap, seq)
		}
	}

	responseChannel <- min + 1
}

// Handler for the Status operation
func (px *Paxos) processStatusRequest(seq int, responseChannel chan StatusResponse) {
	// Check if this instance is forgotten
	min := math.MaxInt32
	for i := 0; i < len(px.impl.doneValues); i++ {
		if px.impl.doneValues[i] < min {
			min = px.impl.doneValues[i]
		}
	}

	if seq <= min {
		responseChannel <- StatusResponse{Forgotten, nil}
		return
	}

	// Check instance status
	instance, exists := px.impl.instanceMap[seq]
	if !exists {
		responseChannel <- StatusResponse{Pending, nil}
	} else if instance.isDecided {
		responseChannel <- StatusResponse{Decided, instance.decidedValue}
	} else {
		responseChannel <- StatusResponse{Pending, nil}
	}
}

// Handler for checking if an instance is decided
func (px *Paxos) processDecisionCheckRequest(seq int, responseChannel chan DecisionCheckResponse) {
	instance, exists := px.impl.instanceMap[seq]
	if exists && instance.isDecided {
		responseChannel <- DecisionCheckResponse{true, instance.decidedValue}
	} else {
		responseChannel <- DecisionCheckResponse{false, nil}
	}
}

// Handler for marking an instance as decided
func (px *Paxos) processSetDecisionRequest(seq int, value interface{}, confirmCh chan bool) {
	inst, exists := px.impl.instanceMap[seq]
	if !exists {
		inst = &PaxosInstanceState{
			highestPrepare: -1,
			highestAccept: -1,
		}
		px.impl.instanceMap[seq] = inst
	}

	inst.isDecided = true
	inst.decidedValue = value

	// Update maxSequence if necessary
	if seq > px.impl.maxSequence {
		px.impl.maxSequence = seq
	}

	confirmCh <- true
}

// Handler for Prepare RPC
func (px *Paxos) processPrepareOp(op PrepareOperation) {
	args := op.Arguments
	reply := op.Response

	// Update our knowledge of peer's done value
	if args.Done > px.impl.doneValues[args.PeerID] {
		px.impl.doneValues[args.PeerID] = args.Done
	}

	// Set basic reply fields
	reply.PeerID = px.me
	reply.Done = px.impl.localDoneMax

	// Check if instance is forgotten
	min := math.MaxInt32
	for i := 0; i < len(px.impl.doneValues); i++ {
		if px.impl.doneValues[i] < min {
			min = px.impl.doneValues[i]
		}
	}

	if args.Seq <= min {
		reply.Reply = Reject
		op.Completed <- true
		return
	}

	// Get or create instance
	inst, exists := px.impl.instanceMap[args.Seq]
	if !exists {
		inst = &PaxosInstanceState{
			highestPrepare: -1,
			highestAccept: -1,
		}
		px.impl.instanceMap[args.Seq] = inst
	}

	// Handle already decided instances
	if inst.isDecided {
		reply.Reply = OK
		reply.N_p = inst.highestPrepare
		reply.N_a = inst.highestAccept
		reply.V_a = inst.decidedValue // Return decided value
		op.Completed <- true
		return
	}

	// Normal Paxos prepare protocol
	if args.N > inst.highestPrepare {
		// Accept prepare
		inst.highestPrepare = args.N
		reply.Reply = OK
		reply.N_p = inst.highestPrepare
		reply.N_a = inst.highestAccept
		reply.V_a = inst.acceptVal
	} else {
		// Reject prepare
		reply.Reply = Reject
		reply.N_p = inst.highestPrepare
	}

	op.Completed <- true
}

// Handler for Accept RPC
func (px *Paxos) processAcceptOp(op AcceptOperation) {
	args := op.Arguments
	reply := op.Response

	// Update our knowledge of peer's done value
	if args.Done > px.impl.doneValues[args.PeerID] {
		px.impl.doneValues[args.PeerID] = args.Done
	}

	// Set basic reply fields
	reply.PeerID = px.me
	reply.Done = px.impl.localDoneMax

	// Check if instance is forgotten
	min := math.MaxInt32
	for i := 0; i < len(px.impl.doneValues); i++ {
		if px.impl.doneValues[i] < min {
			min = px.impl.doneValues[i]
		}
	}

	if args.Seq <= min {
		reply.Reply = Reject
		op.Completed <- true
		return
	}

	// Get or create instance
	inst, exists := px.impl.instanceMap[args.Seq]
	if !exists {
		inst = &PaxosInstanceState{
			highestPrepare: -1,
			highestAccept: -1,
		}
		px.impl.instanceMap[args.Seq] = inst
	}

	// Handle already decided instances
	if inst.isDecided {
		reply.Reply = OK
		reply.N_p = inst.highestPrepare
		reply.N_a = inst.highestAccept
		reply.V_a = inst.decidedValue // Return decided value
		op.Completed <- true
		return
	}

	// Normal Paxos accept protocol
	if args.N >= inst.highestPrepare {
		// Accept
		inst.highestPrepare = args.N
		inst.highestAccept = args.N
		inst.acceptVal = args.Value

		reply.Reply = OK
		reply.N_p = inst.highestPrepare
		reply.N_a = inst.highestAccept
		reply.V_a = inst.acceptVal
	} else {
		// Reject
		reply.Reply = Reject
		reply.N_p = inst.highestPrepare
	}

	op.Completed <- true
}

// Handler for Inform RPC
func (px *Paxos) processInformOp(op InformOperation) {
	args := op.Arguments
	reply := op.Response

	// Update our knowledge of peer's done value
	if args.Done > px.impl.doneValues[args.PeerID] {
		px.impl.doneValues[args.PeerID] = args.Done
	}

	// Set basic reply fields
	reply.Reply = OK
	reply.PeerID = px.me
	reply.Done = px.impl.localDoneMax

	// Check if instance is forgotten
	min := math.MaxInt32
	for i := 0; i < len(px.impl.doneValues); i++ {
		if px.impl.doneValues[i] < min {
			min = px.impl.doneValues[i]
		}
	}

	if args.Seq <= min {
		op.Completed <- true
		return
	}

	// Get or create instance
	inst, exists := px.impl.instanceMap[args.Seq]
	if !exists {
		inst = &PaxosInstanceState{
			highestPrepare: -1,
			highestAccept: -1,
		}
		px.impl.instanceMap[args.Seq] = inst
	}

	// Mark as decided
	inst.isDecided = true
	inst.decidedValue = args.Value

	// Update maxSequence if necessary
	if args.Seq > px.impl.maxSequence {
		px.impl.maxSequence = args.Seq
	}

	op.Completed <- true
}

// Prepare (paxos phase one)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if px.isdead() {
		return nil
	}

	completed := make(chan bool)
	op := PrepareOperation{
		Arguments: args,
		Response: reply,
		Completed: completed,
	}

	px.impl.prepareChannel <- op
	<-completed

	return nil
}

// Accept (paxos phase two)
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if px.isdead() {
		return nil
	}

	completed := make(chan bool)
	op := AcceptOperation{
		Arguments: args,
		Response: reply,
		Completed: completed,
	}

	px.impl.acceptChannel <- op
	<-completed

	return nil
}

// Inform (the Decided optimization)
func (px *Paxos) Inform(args *InformArgs, reply *InformReply) error {
	if px.isdead() {
		return nil
	}

	completed := make(chan bool)
	op := InformOperation{
		Arguments: args,
		Response: reply,
		Completed: completed,
	}

	px.impl.informChannel <- op
	<-completed

	return nil
}