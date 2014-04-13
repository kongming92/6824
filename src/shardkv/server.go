package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  // Your definitions here.
  OpType string
  Key string
  Value string
  PutHash bool
  ClientId int64
  Seq int
  ConfigEnd int
}

type ConfigState struct {
  Table map[string]string
  PutSeqs map[int64]int
  PutResponses map[int64]string
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos
  gid int64 // my replica group ID

  // Your definitions here.
  lastExecuted int
  table map[string]string
  putSeqs map[int64]int
  putResponses map[int64]string
  localConfig shardmaster.Config
  history map[int]ConfigState
}

// Entry point to put something into the Paxos log
func (kv *ShardKV) StartAndWait(op Op) (string, Err) {
  seq := kv.px.Max() + 1
  kv.px.Start(seq, op)
  return kv.WaitAndReturn(seq, &op)
}

// Waits for seq to be the next command to be executed
// Then wait for that command to be decided in the Paxos log
func (kv *ShardKV) WaitAndReturn(seq int, op *Op) (string, Err) {
  to := 10 * time.Millisecond
  newSeq := seq

  for !kv.dead {
    decided, value := kv.px.Status(kv.lastExecuted + 1)
    if decided {
      thisOp := value.(Op)
      result, err := kv.DoCmd(&thisOp)

      kv.lastExecuted += 1

      if kv.lastExecuted == newSeq {
        if thisOp.ClientId == op.ClientId && thisOp.Seq == op.Seq {
          kv.px.Done(kv.lastExecuted)
          return result, err

        } else {
          newSeq = kv.px.Max() + 1
          kv.px.Start(newSeq, *op)
        }
      }

    } else {
      if to > time.Second {
        nop := Op{"nop", "", "", false, 0, 0, 0}
        kv.px.Start(kv.lastExecuted + 1, nop)
        to = 10 * time.Millisecond
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }

  return "", OK
}

// The DoCmd, DoGet, DoPut functions are NOT threadsafe!
func (kv *ShardKV) DoCmd(op *Op) (string, Err) {
  if (op.OpType == "Get" || op.OpType == "Put") && kv.gid != kv.localConfig.Shards[key2shard(op.Key)] {
    return "", ErrWrongGroup
  }

  if op.OpType == "Get" {
    return kv.DoGet(op.Key)

  } else if op.OpType == "Put" {
    seq, ok := kv.putSeqs[op.ClientId]
    if ok && seq >= op.Seq {
      return kv.putResponses[op.ClientId], OK
    }

    response, err := kv.DoPut(op.Key, op.Value, op.PutHash)
    kv.putSeqs[op.ClientId] = op.Seq
    kv.putResponses[op.ClientId] = response
    return response, err

  } else if op.OpType == "Reconfig" {
    return kv.DoReconfig(op.ConfigEnd)
  }
  return "", OK
}

func (kv *ShardKV) DoGet(key string) (string, Err) {
  value, exists := kv.table[key]

  if exists {
    return value, OK
  }
  return "", ErrNoKey
}

func (kv *ShardKV) DoPut(key string, value string, putHash bool) (string, Err) {
  prev, exists := kv.table[key]
  if !exists {
    prev = ""
  }

  if putHash {
    kv.table[key] = strconv.Itoa(int(hash(prev + value)))
  } else {
    kv.table[key] = value
  }

  return prev, OK
}

func (kv *ShardKV) DoReconfig(end int) (string, Err) {

  if kv.localConfig.Num == end {
    return "", OK
  }

  kv.history[kv.localConfig.Num] = ConfigState{kv.table, kv.putSeqs, kv.putResponses}
  nextConfig := kv.sm.Query(end)
  needToGet := make([]int, 0, shardmaster.NShards)

  for i, gid := range kv.localConfig.Shards {
    // We are responsible for a shard next config
    if gid != kv.gid && nextConfig.Shards[i] == kv.gid {
      needToGet = append(needToGet, i)
    }
  }

  if len(needToGet) > 0 {
    // For each shard
    for _, shard := range needToGet {
      if kv.dead {
        break
      }

      servers := kv.localConfig.Groups[kv.localConfig.Shards[shard]]
      i := 0
      for i < len(servers) && !kv.dead {
        var reply FetchReply
        args := FetchArgs{kv.localConfig.Num}
        ok := call(servers[i], "ShardKV.Fetch", args, &reply)

        if ok && reply.OK {
          // Update the table
          newTable := reply.ConfigState.Table
          for k, v := range newTable {
            if key2shard(k) == shard {
              kv.table[k] = v
            }
          }

          // update client Put info
          for clientId, seq := range reply.ConfigState.PutSeqs {
            if kv.putSeqs[clientId] < seq {
              kv.putSeqs[clientId] = seq
              kv.putResponses[clientId] = reply.ConfigState.PutResponses[clientId]
            }
          }

          break
        }

        i = (i+1) % len(servers)
      }
    }
  }

  kv.localConfig = nextConfig
  return "", OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  result, err := kv.StartAndWait(Op{"Get", args.Key, "", false, args.ClientId, args.Seq, 0})
  reply.Err = err
  reply.Value = result
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  result, err := kv.StartAndWait(Op{"Put", args.Key, args.Value, args.DoHash, args.ClientId, args.Seq, 0})
  reply.Err = err
  reply.PreviousValue = result
  return nil
}

func (kv *ShardKV) Fetch(args *FetchArgs, reply *FetchReply) error {
  // kv.mu.Lock()
  // defer kv.mu.Unlock()

  state, ok := kv.history[args.ConfigNum]
  if ok {
    reply.ConfigState = state
    reply.OK = true
  } else {
    reply.OK = false
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  if kv.dead {
    return
  }

  lastNum := kv.sm.Query(-1).Num

  if kv.localConfig.Num < lastNum {
    kv.StartAndWait(Op{"Reconfig", "", "", false, 0, 0, kv.localConfig.Num + 1})
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetArgs{})
  gob.Register(PutArgs{})
  gob.Register(FetchArgs{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.lastExecuted = -1
  kv.table = make(map[string]string)
  kv.putSeqs = make(map[int64]int)
  kv.putResponses = make(map[int64]string)
  kv.localConfig = kv.sm.Query(-1)
  kv.history = make(map[int]ConfigState)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
