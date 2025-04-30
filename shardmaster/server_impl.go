package shardmaster

import (
	"sort"
	"time"

	"umich.edu/eecs491/proj4/common"
)

// Define operation types as constants
const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//
type Op struct {
	Type      string       // "Join", "Leave", "Move", or "Query"
	GID       int64        // Group ID for Join/Leave/Move
	Servers   []string     // Servers for Join
	Shard     int          // Shard for Move
	Num       int          // Config number for Query
	ClientID  int64        // For duplicate detection
	OpID      int64        // For duplicate detection
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1, ok1 := v1.(Op)
	op2, ok2 := v2.(Op)

	if !ok1 || !ok2 {
		return false
	}

	// Two operations are equal if they have the same ClientID and OpID
	return op1.ClientID == op2.ClientID && op1.OpID == op2.OpID
}

// Request types for channel-based communication
type configOp struct {
	op   Op        // The operation to perform
	done chan bool // Signal when done
}

type queryOp struct {
	num    int         // Config number to query
	result chan Config // Channel for result
}

//
// additions to ShardMaster state
//
type ShardMasterImpl struct {
	configs     []Config              // All configurations
	lastApplied map[int64]int64       // Track last applied operation per client

	configCh    chan configOp         // Channel for config operations
	queryCh     chan queryOp          // Channel for query operations
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
	sm.impl = ShardMasterImpl{
		configs:     make([]Config, 1),
		lastApplied: make(map[int64]int64), //clientID -> opID?
		configCh:    make(chan configOp, 100),
		queryCh:     make(chan queryOp, 100),
	}

	// Create initial config (number 0)
	sm.impl.configs[0] = Config{
		Num:    0,
		Groups: make(map[int64][]string),
		Shards: [common.NShards]int64{}, // All shards assigned to GID 0 by default
	}

	// Start the configuration handler goroutine
	go sm.configHandler()
}

// configHandler processes all config operations
func (sm *ShardMaster) configHandler() {
	for {
		select {
		case op := <-sm.impl.configCh:
			// Check if this is a duplicate operation
			if lastOp, exists := sm.impl.lastApplied[op.op.ClientID]; exists && lastOp >= op.op.OpID {
				// Already processed this operation, just signal done
				op.done <- true
				continue
			}

			// Process the operation based on its type
			switch op.op.Type {
			case JOIN:
				sm.applyJoin(op.op.GID, op.op.Servers)
			case LEAVE:
				sm.applyLeave(op.op.GID)
			case MOVE:
				sm.applyMove(op.op.Shard, op.op.GID)
			}

			// Record that we've processed this operation
			sm.impl.lastApplied[op.op.ClientID] = op.op.OpID

			// Signal that we're done
			op.done <- true

		case q := <-sm.impl.queryCh:
			// Get the requested config
			config := sm.getConfig(q.num)

			// Send the result
			q.result <- config
		}
	}
}

// Helper function to get a configuration
func (sm *ShardMaster) getConfig(num int) Config {
	if num == -1 || num >= len(sm.impl.configs) {
		// Return the latest config
		return sm.impl.configs[len(sm.impl.configs)-1]
	}

	// Return the requested config
	return sm.impl.configs[num]
}

//
// RPC handlers for Join, Leave, Move, and Query RPCs
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	op := Op{
		Type:     JOIN,
		GID:      args.GID,
		Servers:  args.Servers,
		ClientID: common.Nrand(), // Generate a unique client ID
		OpID:     common.Nrand(), // Generate a unique operation ID
	}

	// Use RSM to reach consensus and wait for operation to complete
	sm.waitForConsensus(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := Op{
		Type:     LEAVE, // Fix typo: was "Join"
		GID:      args.GID,
		ClientID: common.Nrand(),
		OpID:     common.Nrand(),
	}

	// Use RSM to reach consensus and wait for operation to complete
	sm.waitForConsensus(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{
		Type:     MOVE,
		GID:      args.GID,
		Shard:    args.Shard,
		ClientID: common.Nrand(),
		OpID:     common.Nrand(),
	}

	// Use RSM to reach consensus and wait for operation to complete
	sm.waitForConsensus(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// For Query operations that request the latest config, make sure
	// all pending operations are applied first
	if args.Num == -1 {
		// Create a no-op to ensure all previous operations are processed
		op := Op{
			Type:     QUERY,
			ClientID: common.Nrand(),
			OpID:     common.Nrand(),
		}

		// Submit to Paxos to ensure all previous operations are processed
		sm.rsm.AddOp(op)
	}

	result := make(chan Config)
	sm.impl.queryCh <- queryOp{num: args.Num, result: result}
	config := <-result

	reply.Config = config

	return nil
}

// Helper method to wait for Paxos consensus
func (sm *ShardMaster) waitForConsensus(op Op) {
	// First, submit to Paxos to reach consensus
	sm.rsm.AddOp(op)

	// Check if we need to wait for the operation to be applied
	if lastOp, exists := sm.impl.lastApplied[op.ClientID]; !exists || lastOp < op.OpID {
		// Operation hasn't been applied yet, wait for it
		done := make(chan bool)
		sm.impl.configCh <- configOp{op: op, done: done}
		<-done
	}
}

//
// Execute operation encoded in decided value v and update local state
//
func (sm *ShardMaster) ApplyOp(v interface{}) {
	op, ok := v.(Op)
	if !ok {
		return
	}

	// Only process non-query operations in ApplyOp
	if op.Type == JOIN || op.Type == LEAVE || op.Type == MOVE {
		// Send to channel for processing to avoid race conditions
		done := make(chan bool)
		sm.impl.configCh <- configOp{op: op, done: done}
		<-done
	}
}

//
// Helper functions for applying operations
//
func (sm *ShardMaster) applyJoin(gid int64, servers []string) {
	// Get the latest configuration
	lastConfig := sm.impl.configs[len(sm.impl.configs)-1]

	// Check if the GID already exists
	if _, exists := lastConfig.Groups[gid]; exists {
		// GID already exists, nothing to do
		return
	}

	// Create a new configuration based on the latest one
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	// Copy existing groups
	for g, s := range lastConfig.Groups {
		newConfig.Groups[g] = append([]string{}, s...)
	}

	// Add the new group
	newConfig.Groups[gid] = append([]string{}, servers...)

	// Rebalance shards
	sm.rebalanceShards(&newConfig)

	// Notify groups about changed shards
	for shard := 0; shard < common.NShards; shard++ {
		oldGID := lastConfig.Shards[shard]
		newGID := newConfig.Shards[shard]

		if oldGID != newGID && newGID != 0 {
			// Shard has moved to a new group
			servers, ok := newConfig.Groups[newGID]
			if ok && oldGID != 0 {
				// Get servers from the old group
				oldServers, oldOk := lastConfig.Groups[oldGID]
				if oldOk {
					// Create AssignShard arguments
					args := common.AssignArgs{
						ConfigNum: newConfig.Num,
						Shard:     shard,
						Servers:   oldServers,
					}
					// Call the ShardKV AssignShard RPC
					go sm.assignShardToGroup(servers, args)
				}
			}
		}
	}

	// Add to configurations
	sm.impl.configs = append(sm.impl.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(gid int64) {
	// Get the latest configuration
	lastConfig := sm.impl.configs[len(sm.impl.configs)-1]

	// Check if the GID exists
	if _, exists := lastConfig.Groups[gid]; !exists {
		// GID doesn't exist, nothing to do
		return
	}

	// Create a new configuration based on the latest one
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	// Copy existing groups except the one being removed
	for g, servers := range lastConfig.Groups {
		if g != gid {
			newConfig.Groups[g] = append([]string{}, servers...)
		}
	}

	oldServers := lastConfig.Groups[gid]

	// Reassign shards from the leaving group
	for i := 0; i < common.NShards; i++ {
		if newConfig.Shards[i] == gid {
			newConfig.Shards[i] = 0 // Temporarily assign to invalid GID
		}
	}

	// Rebalance shards
	sm.rebalanceShards(&newConfig)

	// Notify groups about changed shards
	for shard := 0; shard < common.NShards; shard++ {
		oldGID := lastConfig.Shards[shard]
		newGID := newConfig.Shards[shard]


		if oldGID != newGID && newGID != 0 {
			// Shard has moved to a new group
			servers, ok := newConfig.Groups[newGID]
			if ok && oldGID == gid {
				// Create AssignShard arguments using the servers of the leaving group
				args := common.AssignArgs{
					ConfigNum: newConfig.Num,
					Shard:     shard,
					Servers:   oldServers,
				}

				// Call the ShardKV AssignShard RPC
				go sm.assignShardToGroup(servers, args)
			}
		}
	}


	// Add to configurations
	sm.impl.configs = append(sm.impl.configs, newConfig)
}

func (sm *ShardMaster) applyMove(shard int, gid int64) {
	// Get the latest configuration
	lastConfig := sm.impl.configs[len(sm.impl.configs)-1]

	// Create a new configuration based on the latest one
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	// Copy existing groups
	for g, servers := range lastConfig.Groups {
		newConfig.Groups[g] = append([]string{}, servers...)
	}

	oldGID := lastConfig.Shards[shard]

	// Move the specified shard to the specified GID
	// (Only if the GID exists, otherwise we could assign to an invalid group)
	if _, exists := newConfig.Groups[gid]; exists || gid == 0 {
		newConfig.Shards[shard] = gid
	}

	// Notify the new group about the moved shard
	if oldGID != gid && gid != 0 {
		// Get servers for the new group
		servers, ok := newConfig.Groups[gid]
		if ok && oldGID != 0 {
			// Get servers from the old group
			oldServers, oldOk := lastConfig.Groups[oldGID]
			if oldOk {
				// Create AssignShard arguments
				args := common.AssignArgs{
					ConfigNum: newConfig.Num,
					Shard:     shard,
					Servers:   oldServers,
				}

				// Call the ShardKV AssignShard RPC
				go sm.assignShardToGroup(servers, args)
			}
		}
	}
	// Add to configurations
	sm.impl.configs = append(sm.impl.configs, newConfig)
}

func (sm *ShardMaster) rebalanceShards(config *Config) {
	numGroups := len(config.Groups)
	if numGroups == 0 {
		// Assign all shards to GID 0 (invalid) if no groups exist
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// Create a list of valid GIDs
	var gids []int64
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	// Sort GIDs for deterministic assignment
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})

	// Track which shards are assigned to which groups
	gidToShards := make(map[int64][]int)
	for i, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], i)
	}

	// Calculate ideal number of shards per group
	shardsPerGroup := common.NShards / numGroups
	extraShards := common.NShards % numGroups

	// Calculate target number of shards for each group
	targetShards := make(map[int64]int)
	for i, gid := range gids {
		targetShards[gid] = shardsPerGroup
		if i < extraShards {
			targetShards[gid]++
		}
	}

	// First, handle unassigned shards or shards assigned to invalid GIDs
	var unassignedShards []int
	for i, gid := range config.Shards {
		_, validGroup := config.Groups[gid]
		if gid == 0 || !validGroup {
			unassignedShards = append(unassignedShards, i)
			config.Shards[i] = 0 // Mark as unassigned
		}
	}

	// Identify overloaded and underloaded groups
	var overloaded, underloaded []int64
	for _, gid := range gids {
		currentCount := len(gidToShards[gid])
		target := targetShards[gid]

		if currentCount > target {
			// Group has too many shards
			diff := currentCount - target
			for i := 0; i < diff; i++ {
				overloaded = append(overloaded, gid)
			}
		} else if currentCount < target {
			// Group needs more shards
			diff := target - currentCount
			for i := 0; i < diff; i++ {
				underloaded = append(underloaded, gid)
			}
		}
	}

	// Assign unassigned shards to underloaded groups
	for len(unassignedShards) > 0 && len(underloaded) > 0 {
		shardIndex := unassignedShards[0]
		gid := underloaded[0]

		config.Shards[shardIndex] = gid
		gidToShards[gid] = append(gidToShards[gid], shardIndex)

		unassignedShards = unassignedShards[1:]
		underloaded = underloaded[1:]
	}

	// Move shards from overloaded to underloaded groups
	for len(overloaded) > 0 && len(underloaded) > 0 {
		fromGid := overloaded[0]
		toGid := underloaded[0]

		// Find a shard to move
		if len(gidToShards[fromGid]) > 0 {
			shardToMove := gidToShards[fromGid][0]
			gidToShards[fromGid] = gidToShards[fromGid][1:]

			config.Shards[shardToMove] = toGid
			gidToShards[toGid] = append(gidToShards[toGid], shardToMove)
		}

		overloaded = overloaded[1:]
		underloaded = underloaded[1:]
	}

	// If there are still unassigned shards, distribute them among the groups
	if len(unassignedShards) > 0 {
		for i, shardIndex := range unassignedShards {
			gid := gids[i%len(gids)]
			config.Shards[shardIndex] = gid
		}
	}
}

// TODO: 以上都是跑过测验确认没问题的，以下部分会和shardkv的测验相关，
// 跑shardkv的测验的话，需要您回来确认以下function的应用和逻辑

// Call AssignShard on a group of servers
func (sm *ShardMaster) assignShardToGroup(servers []string, args common.AssignArgs) {
	for _, server := range servers {
		var reply common.AssignReply
		ok := common.Call(server, "ShardKV.AssignShard", &args, &reply)
		if ok {
			return // Successfully assigned
		}

		// Try next server if this one failed
		time.Sleep(100 * time.Millisecond)
	}
}