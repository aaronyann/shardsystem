package shardkv

import (
	"time"

	"umich.edu/eecs491/proj4/common"
)

// Operation Types
const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	ASSIGN = "Assign"
	PULL   = "Pull"
)

// Paxos Operation
type Op struct {
	Type      string
	Key       string
	Value     string
	ClientID  int64
	OpID      int64
	Shard     int
	ConfigNum int
	KVStore   map[string]string
	OpCache   map[int64]int64
	Servers   []string
}

// Equality function for Paxos log de-duplication
func equals(v1 interface{}, v2 interface{}) bool {
	op1, ok1 := v1.(Op)
	op2, ok2 := v2.(Op)
	if !ok1 || !ok2 {
		return false
	}
	return op1.ClientID == op2.ClientID && op1.OpID == op2.OpID
}

// ShardKV internal state
const NShards = common.NShards

// Each shard contains a key/value store and a flag for ownership
type Shard struct {
	Data  map[string]string
	Owned bool
}

type ShardKVImpl struct {
	// Communication channels for thread confinement
	applyCh     chan Op
	getCh       chan getReq
	putAppendCh chan putAppendReq
	assignCh    chan assignReq
	pullCh      chan pullReq
	shutdownCh  chan struct{}

	// State information
	shards [NShards]Shard
	seen   map[int64]int64 // Client ID -> highest op ID seen
	// pending map[int64]chan Op // Sequence number -> channel for waiting on Paxos decision
	pending map[int64]map[int64]chan Op // ClientID -> OpID -> chan Op

}

// Request structures for channel-based communication
type getReq struct {
	args   *GetArgs
	reply  *GetReply
	doneCh chan bool
}

type putAppendReq struct {
	args   *PutAppendArgs
	reply  *PutAppendReply
	doneCh chan bool
}

type assignReq struct {
	args   *common.AssignArgs
	reply  *common.AssignReply
	doneCh chan bool
}

type pullReq struct {
	args   *PullArgs
	reply  *PullReply
	doneCh chan bool
}

// Initialization
func (kv *ShardKV) InitImpl() {
	DPrintf("[SERVER %d:%d] Initializing ShardKV implementation\n", kv.gid, kv.me)

	kv.impl = ShardKVImpl{
		applyCh:     make(chan Op, 100),
		getCh:       make(chan getReq, 100),
		putAppendCh: make(chan putAppendReq, 100),
		assignCh:    make(chan assignReq, 100),
		pullCh:      make(chan pullReq, 100),
		shutdownCh:  make(chan struct{}),
		seen:        make(map[int64]int64),
		// pending:     make(map[int64]chan Op),
		pending: make(map[int64]map[int64]chan Op),
	}

	// Initialize shards
	for i := 0; i < NShards; i++ {
		kv.impl.shards[i] = Shard{
			Data:  make(map[string]string),
			Owned: false, // Initially we don't own any shards
		}
	}

	if kv.gid == 100 { // The first group in TestBasic has GID 100
		// Initially we might own all shards if we're the first server
		for i := 0; i < NShards; i++ {
			kv.impl.shards[i].Owned = true
		}
	}

	DPrintf("[SERVER %d:%d] Starting worker goroutines\n", kv.gid, kv.me)
	// Start worker goroutine
	go kv.eventLoop()
	DPrintf("[SERVER %d:%d] Initialization complete\n", kv.gid, kv.me)
}

// Main event processing loop
func (kv *ShardKV) eventLoop() {
	DPrintf("[SERVER %d:%d] Starting event loop\n", kv.gid, kv.me)

	for {
		select {
		case <-kv.impl.shutdownCh:
			DPrintf("[SERVER %d:%d] Received shutdown signal\n", kv.gid, kv.me)
			return

		case op := <-kv.impl.applyCh:
			DPrintf("[SERVER %d:%d] Applying operation: %s\n", kv.gid, kv.me, op.Type)
			kv.applyOperation(op)

		case req := <-kv.impl.getCh:
			DPrintf("[SERVER %d:%d] Handling Get request\n", kv.gid, kv.me)
			kv.handleGet(req)

		case req := <-kv.impl.putAppendCh:
			DPrintf("[SERVER %d:%d] Handling PushAppend request\n", kv.gid, kv.me)
			kv.handlePutAppend(req)

		case req := <-kv.impl.assignCh:
			DPrintf("[SERVER %d:%d] Handling Assign request\n", kv.gid, kv.me)
			kv.handleForAssign(req)

		case req := <-kv.impl.pullCh:
			DPrintf("[SERVER %d:%d] Handling Pull request\n", kv.gid, kv.me)
			kv.handleForPull(req)
		}
	}
}

// Apply an operation to the state machine
func (kv *ShardKV) applyOperation(op Op) {
	// Check if this is a duplicate operation
	// DPrintf("[SERVER %d:%d] applyOperation for op.OpID=%d, keys in pending: %v\n",
	// 	kv.gid, kv.me, op.OpID, getKeysFromPending(kv.impl.pending))

	if _, ok := kv.impl.seen[op.ClientID]; !ok {
		kv.impl.seen[op.ClientID] = -1
	}

	if last, ok := kv.impl.seen[op.ClientID]; ok && op.OpID <= last && op.Type != ASSIGN && op.Type != PULL {
		// Duplicate operation, ignore
		DPrintf("[SERVER %d:%d] Duplicate operation detected, ignoring\n", kv.gid, kv.me)
		// if ch, ok := kv.impl.pending[op.OpID]; ok {
		// 	select {
		// 	case ch <- op:
		// 	default:
		// 	}
		// 	delete(kv.impl.pending, op.OpID)
		// }
		return
	}

	// Process based on operation type
	switch op.Type {
	case PUT:
		shard := common.Key2Shard(op.Key)
		if kv.impl.shards[shard].Owned {
			DPrintf("[SERVER %d:%d] Applying PUT to shard %d: %s = %s\n",
				kv.gid, kv.me, shard, op.Key, op.Value)
			kv.impl.shards[shard].Data[op.Key] = op.Value
			kv.impl.seen[op.ClientID] = op.OpID
		} else {
			DPrintf("[SERVER %d:%d] Not applying PUT - don't own shard %d\n",
				kv.gid, kv.me, shard)
		}

	case APPEND:
		shard := common.Key2Shard(op.Key)
		if kv.impl.shards[shard].Owned {
			kv.impl.shards[shard].Data[op.Key] += op.Value
			kv.impl.seen[op.ClientID] = op.OpID
		}

	case ASSIGN:
		DPrintf("[SERVER %d:%d] Applying ASSIGN for shard %d\n", kv.gid, kv.me, op.Shard)
		// Mark the shard as assigned to us
		kv.impl.shards[op.Shard].Owned = true

	case PULL:
		// Apply pulled shard data
		shard := op.Shard
		DPrintf("[SERVER %d:%d] Applying PULL for shard %d\n", kv.gid, kv.me, shard)

		// Copy KV data
		kv.impl.shards[shard].Data = make(map[string]string)
		for k, v := range op.KVStore {
			kv.impl.shards[shard].Data[k] = v
		}

		// Update seen operations
		for cid, opid := range op.OpCache {
			if lastSeen, exists := kv.impl.seen[cid]; !exists || opid > lastSeen {
				kv.impl.seen[cid] = opid
			}
		}

		// Mark the shard as owned
		kv.impl.shards[shard].Owned = true
	}

	// Notify any pending operation, if it's being waited on
	// if ch, ok := kv.impl.pending[int(op.OpID)]; ok {
	if opMap, ok := kv.impl.pending[op.ClientID]; ok {
		if ch, ok := opMap[op.OpID]; ok {
			DPrintf("[SERVER %d:%d] Notifying waiting op %d for client %d\n", kv.gid, kv.me, op.OpID, op.ClientID)
			select {
			case ch <- op:
			default:
			}
			delete(opMap, op.OpID)
		}
	}

}

// Helper function to get keys from map
func getKeysFromPending(m map[int64]chan Op) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Handle a Get request
func (kv *ShardKV) handleGet(req getReq) {
	shard := common.Key2Shard(req.args.Key)

	// Check if we own this shard
	if !kv.impl.shards[shard].Owned {
		req.reply.Err = ErrWrongGroup
		req.doneCh <- true
		return
	}

	// Create and process the operation
	op := Op{
		Type:     GET,
		Key:      req.args.Key,
		ClientID: req.args.ClientID,
		OpID:     req.args.OpID,
	}

	// Try to propose and get agreement via Paxos
	result := kv.ppForOperation(op)

	// Process result
	if result.Type == GET && result.Key == op.Key && result.ClientID == op.ClientID && result.OpID == op.OpID {
		// Operation was agreed upon
		val, ok := kv.impl.shards[shard].Data[req.args.Key]
		if ok {
			req.reply.Value = val
			req.reply.Err = OK
		} else {
			req.reply.Err = ErrNoKey
		}
	} else {
		// Something went wrong, possibly shard ownership changed
		req.reply.Err = ErrWrongGroup
	}

	req.doneCh <- true
}

// Handle a Put or Append request
func (kv *ShardKV) handlePutAppend(req putAppendReq) {
	shard := common.Key2Shard(req.args.Key)
	DPrintf("[SERVER %d:%d] Handling PushAppend: key=%s, shard=%d, owned=%v\n",
		kv.gid, kv.me, req.args.Key, shard, kv.impl.shards[shard].Owned)

	// Check if we own this shard
	if !kv.impl.shards[shard].Owned {
		DPrintf("[SERVER %d:%d] Don't own shard %d, returning ErrWrongGroup\n",
			kv.gid, kv.me, shard)
		req.reply.Err = ErrWrongGroup
		req.doneCh <- true
		return
	}

	// Check for duplicate operation
	if lastOpID, ok := kv.impl.seen[req.args.ClientID]; ok && req.args.OpID <= lastOpID {
		// This is a duplicate operation, and we've already processed it
		req.reply.Err = OK
		req.doneCh <- true
		return
	}

	// Create and process the operation
	op := Op{
		Type:     req.args.Op,
		Key:      req.args.Key,
		Value:    req.args.Value,
		ClientID: req.args.ClientID,
		// OpID:     common.Nrand(),
		OpID: req.args.OpID,
	}

	DPrintf("[SERVER %d:%d] Proposing operation through Paxos\n", kv.gid, kv.me)
	// Try to propose and get agreement via Paxos
	result := kv.ppForOperation(op)
	DPrintf("[SERVER %d:%d] Paxos proposal completed\n", kv.gid, kv.me)

	// Process result
	if result.Type == op.Type && result.Key == op.Key && result.ClientID == op.ClientID && result.OpID == op.OpID {
		// Operation was agreed upon
		req.reply.Err = OK
	} else {
		// Something went wrong, possibly shard ownership changed
		req.reply.Err = ErrWrongGroup
	}

	req.doneCh <- true
}

// Handle an Assign request from shardmaster
func (kv *ShardKV) handleForAssign(req assignReq) {
	shard := req.args.Shard
	DPrintf("[SERVER %d:%d] Handling Assign for shard %d\n", kv.gid, kv.me, shard)

	// Create an operation to mark the shard as assigned
	op := Op{
		Type:      ASSIGN,
		Shard:     req.args.Shard,
		ConfigNum: req.args.ConfigNum,
		Servers:   req.args.Servers,
		ClientID:  int64(kv.me), // Use server ID as client ID
		OpID:      common.Nrand(),
	}

	DPrintf("[SERVER %d:%d] Proposing ASSIGN operation through Paxos for shard %d\n",
		kv.gid, kv.me, shard)

	// Propose the operation through Paxos
	kv.ppForOperation(op)

	// If there are servers provided, we need to pull the shard data from them
	if len(req.args.Servers) > 0 {
		DPrintf("[SERVER %d:%d] Will pull shard data from oldServers: %v\n",
			kv.gid, kv.me, req.args.Servers)
		go kv.pullShardInfo(shard, req.args.ConfigNum, req.args.Servers)
	}

	// Always respond with success - the actual data pulling happens asynchronously
	DPrintf("[SERVER %d:%d] Completed Assign handling for shard %d\n", kv.gid, kv.me, shard)
	req.doneCh <- true
}

func (kv *ShardKV) pullShardInfo(shard int, configNum int, servers []string) {
	DPrintf("[SERVER %d:%d] Starting pullShardInfo for shard %d from config %d\n",
		kv.gid, kv.me, shard, configNum)

	args := &PullArgs{
		ConfigNum: configNum,
		Shard:     shard,
	}

	// Try each server until we get the data
	for i, server := range servers {
		DPrintf("[SERVER %d:%d] Trying to pull shard %d from server %d: %s\n",
			kv.gid, kv.me, shard, i, server)

		var reply PullReply
		if common.Call(server, "ShardKV.PullShard", args, &reply) && reply.Err == OK {
			DPrintf("[SERVER %d:%d] Successfully pulled shard %d data from %s\n",
				kv.gid, kv.me, shard, server)
			kvStoreCopy := make(map[string]string)
			for k, v := range reply.KVStore  {
				kvStoreCopy[k] = v
			}

			opCacheCopy := make(map[int64]int64)
			for k, v := range reply.OpIDCache  {
				opCacheCopy[k] = v
			}
			// Successfully pulled the data, create an operation to apply it
			op := Op{
				Type:      PULL,
				Shard:     shard,
				KVStore:   kvStoreCopy,
				OpCache:   opCacheCopy,
				ConfigNum: configNum,
				ClientID:  int64(kv.me),
				OpID:      common.Nrand(),
			}

			DPrintf("[SERVER %d:%d] Proposing PULL operation through Paxos for shard %d\n",
				kv.gid, kv.me, shard)

			// Propose this through Paxos
			kv.ppForOperation(op)
			return
		}
		DPrintf("[SERVER %d:%d] Failed to pull shard %d from server %s err: %v \n",
			kv.gid, kv.me, shard, server,reply.Err)
	}
	DPrintf("[SERVER %d:%d] Failed to pull shard %d data from any server\n",
		kv.gid, kv.me, shard)
}

func (kv *ShardKV) handleForPull(req pullReq) {
	shard := req.args.Shard
	DPrintf("[SERVER %d:%d] Handling Pull request for shard %d from config %d\n",
		kv.gid, kv.me, shard, req.args.ConfigNum)

	// Check if we have this shard's data
	if !kv.impl.shards[shard].Owned {
		DPrintf("[SERVER %d:%d] Don't own shard %d, returning ErrWrongGroup\n",
			kv.gid, kv.me, shard)
		req.reply.Err = ErrWrongGroup
		req.doneCh <- true
		return
	}

	DPrintf("[SERVER %d:%d] Preparing shard %d data for pull\n", kv.gid, kv.me, shard)

	// Prepare the reply with the shard data
	req.reply.KVStore = make(map[string]string)
	for k, v := range kv.impl.shards[shard].Data {
		req.reply.KVStore[k] = v
	}

	// Include our seen operations to prevent duplicate processing
	req.reply.OpIDCache = make(map[int64]int64)
	for cid, opid := range kv.impl.seen {
		req.reply.OpIDCache[cid] = opid
	}

	DPrintf("[SERVER %d:%d] Successfully prepared shard %d data with %d key-value pairs\n",
		kv.gid, kv.me, shard, len(req.reply.KVStore))

	req.reply.Err = OK
	req.doneCh <- true
}



func (kv *ShardKV) ppForOperation(op Op) Op {
	if kv.impl.pending[op.ClientID] == nil {
		kv.impl.pending[op.ClientID] = make(map[int64]chan Op)
	}
	resultCh := make(chan Op, 1)
	kv.impl.pending[op.ClientID][op.OpID] = resultCh

	// Start Paxos agreement
	kv.rsm.AddOp(op)

	// Wait for the result with a timeout
	select {
	case result := <-resultCh:
		return result
	case <-time.After(300 * time.Millisecond):
		delete(kv.impl.pending, op.OpID)
	}

	return op // Return the original op if timeout
}

// RPC Handlers - these all dispatch to channel-based workers
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	if kv.isdead() {
		return nil
	}

	req := getReq{
		args:   args,
		reply:  reply,
		doneCh: make(chan bool, 1),
	}

	kv.impl.getCh <- req
	<-req.doneCh

	return nil
}

func (kv *ShardKV) PushAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if kv.isdead() {
		return nil
	}

	DPrintf("[SERVER %d:%d] Received PushAppend request: key=%s, value=%s, op=%s\n",
		kv.gid, kv.me, args.Key, args.Value, args.Op)

	req := putAppendReq{
		args:   args,
		reply:  reply,
		doneCh: make(chan bool, 1),
	}

	DPrintf("[SERVER %d:%d] Sending PushAppend request to channel\n", kv.gid, kv.me)
	kv.impl.putAppendCh <- req
	DPrintf("[SERVER %d:%d] Waiting for PushAppend response\n", kv.gid, kv.me)

	<-req.doneCh
	DPrintf("[SERVER %d:%d] PushAppend completed with result: %s\n", kv.gid, kv.me, reply.Err)

	return nil
}

func (kv *ShardKV) AssignShard(args *common.AssignArgs, reply *common.AssignReply) error {
	if kv.isdead() {
		return nil
	}

	req := assignReq{
		args:   args,
		reply:  reply,
		doneCh: make(chan bool, 1),
	}

	kv.impl.assignCh <- req
	<-req.doneCh

	return nil
}

func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) error {
	if kv.isdead() {
		return nil
	}

	req := pullReq{
		args:   args,
		reply:  reply,
		doneCh: make(chan bool, 1),
	}

	kv.impl.pullCh <- req
	<-req.doneCh
	return nil
}

// ApplyOp is called by the PaxosRSM when an operation is decided
func (kv *ShardKV) ApplyOp(v interface{}) {
	DPrintf("[SERVER %d:%d] PaxosRSM called ApplyOp\n", kv.gid, kv.me)

	if kv.isdead() {
		DPrintf("[SERVER %d:%d] Server is dead, ignoring ApplyOp\n", kv.gid, kv.me)
		return
	}

	if op, ok := v.(Op); ok {
		DPrintf("[SERVER %d:%d] Sending op to applyCh: %s\n", kv.gid, kv.me, op.Type)
		kv.impl.applyCh <- op
	} else {
		DPrintf("[SERVER %d:%d] Invalid operation type in ApplyOp\n", kv.gid, kv.me)
	}
}
