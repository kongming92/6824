package shardmaster

// import "fmt"
import "time"
import "math"
import "sort"

func (sm *ShardMaster) StartAndWait(op Op) interface{} {
  seq := sm.px.Max() + 1
  sm.px.Start(seq, op)
  return sm.WaitAndReturn(seq, op)
}

// Waits for seq to be the next command to be executed
// Then wait for that command to be decided in the Paxos log
func (sm *ShardMaster) WaitAndReturn(seq int, op Op) interface{} {
  // xid := op.Xid
  to := 10 * time.Millisecond

  newSeq := seq

  for !sm.dead {
    decided, value := sm.px.Status(sm.lastExecuted + 1)
    if decided {
      thisOp := value.(Op)
      result := sm.DoCmd(&thisOp)

      sm.lastExecuted += 1

      if sm.lastExecuted == newSeq {
        if thisOp.Xid == op.Xid {
          sm.px.Done(sm.lastExecuted)
          return result

        } else {
          newSeq = sm.px.Max() + 1
          sm.px.Start(newSeq, op)
        }
      }

    } else {
      if to > time.Second {
        nop := Op{"nop", nrand(), ""}
        sm.px.Start(sm.lastExecuted + 1, nop)
        to = 10 * time.Millisecond
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }

  return ""
}

// The DoCmd, DoGet, DoPut functions are NOT threadsafe!

func (sm *ShardMaster) DoCmd(op *Op) interface{} {
  if op.OpType == "Join" {
    return sm.DoJoin(op.Args.(JoinArgs))
  } else if op.OpType == "Leave" {
    return sm.DoLeave(op.Args.(LeaveArgs))
  } else if op.OpType == "Move" {
    return sm.DoMove(op.Args.(MoveArgs))
  } else if op.OpType == "Query" {
    return sm.DoQuery(op.Args.(QueryArgs))
  }
  return ""
}

func (sm *ShardMaster) DoJoin(args JoinArgs) JoinReply {
  // fmt.Println("JOIN FOR", args.GID)
  config := sm.GetNextCopy()
  config.Groups[args.GID] = args.Servers

  // Figure out the distribution of how many shards per group
  divideBy := 0
  nGroups := len(config.Groups)
  nShardsRemaining := NShards
  shardDist := make([]int, nGroups)
  for i := 0; i < nGroups; i++ {
    divideBy = nGroups - i
    shardDist[i] = int(math.Ceil(float64(nShardsRemaining) / float64(divideBy)))
    nShardsRemaining = NShards - shardDist[i]
  }

  // Sort the gids so that we can be deterministic
  // about how many shards to to each group
  gids := make(int64arr, 0, nGroups)
  for gid := range(config.Groups) {
    gids = append(gids, gid)
  }
  sort.Sort(gids)

  // Now map each gid to the corresponding number
  // of shards assigned
  gidsToShards := make(map[int64]int)
  for i, gid := range(gids) {
    gidsToShards[gid] = shardDist[i]
  }

  currentGidsToShards := make(map[int64]int)
  for i, gid := range(config.Shards) {
    if currentGidsToShards[gid] < gidsToShards[gid] {
      config.Shards[i] = gid
      _, ok := currentGidsToShards[gid]
      if ok {
        currentGidsToShards[gid] += 1
      } else {
        currentGidsToShards[gid] = 1
      }
    } else {
      config.Shards[i] = args.GID
    }
  }

  sm.configs = append(sm.configs, config)
  return JoinReply{}
}

func (sm *ShardMaster) DoLeave(args LeaveArgs) LeaveReply {
  // fmt.Println("LEAVE FOR", args.GID)

  config := sm.GetNextCopy()
  delete(config.Groups, args.GID)

  // Sort the gids so that we can be deterministic
  // about how many shards to to each group
  nGroups := len(config.Groups)
  gids := make(int64arr, 0, nGroups)
  for gid := range(config.Groups) {
    gids = append(gids, gid)
  }
  sort.Sort(gids)
  // fmt.Println("GIDS", gids)
  // fmt.Println("SHARDS", config.Shards)

  removeCount := 0
  for _, gid := range(config.Shards) {
    if gid == args.GID {
      removeCount += 1
    }
  }

  for removeCount > 0 {
    counts := make(map[int64]int)
    for _, gid := range(config.Shards) {
      if gid != args.GID {
        _, ok := counts[gid]
        if ok {
          counts[gid] += 1
        } else {
          counts[gid] = 1
        }
      }
    }

    // fmt.Println("COUNTS", counts)

    min := len(config.Shards)
    minGid := int64(0)
    for _, gid := range(gids) {
      // fmt.Println("GID & COUNTS", gid, counts)
      if counts[gid] < min && gid != args.GID {
        // fmt.Println(counts[gid])
        min = counts[gid]
        minGid = gid
      }
    }

    for i, gid := range(config.Shards) {
      if gid == args.GID {
        config.Shards[i] = minGid
        removeCount -= 1
        break
      }
    }
  }

  sm.configs = append(sm.configs, config)
  return LeaveReply{}
}

func (sm *ShardMaster) DoMove(args MoveArgs) MoveReply {
  config := sm.GetNextCopy()
  config.Shards[args.Shard] = args.GID
  sm.configs = append(sm.configs, config)
  return MoveReply{}
}

func (sm *ShardMaster) DoQuery(args QueryArgs) QueryReply {
  reply := QueryReply{}
  if args.Num == -1 {
    reply.Config = sm.configs[len(sm.configs) - 1]
  } else {
    reply.Config = sm.configs[args.Num]
  }
  return reply
}

