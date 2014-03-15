package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
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
  // Field names must start with capital letters,
  // otherwise RPC will break.
  OpType string
  Key string
  Value string
  PutHash bool
  Xid string
  ClientId int64
  Seq int
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  lastExecuted int
  table map[string]string
  putSeqs map[int64]int
  putResponses map[int64]string
}

// Waits for seq to be the next command to be executed
// Then wait for that command to be decided in the Paxos log
func (kv *KVPaxos) WaitAndReturn(seq int, op *Op) (string, Err) {
  xid := op.Xid
  to := 10 * time.Millisecond

  newSeq := seq

  for !kv.dead {
    decided, value := kv.px.Status(kv.lastExecuted + 1)
    if decided {
      thisOp := value.(Op)
      result, err := kv.DoCmd(&thisOp)

      kv.lastExecuted += 1

      if kv.lastExecuted == newSeq {
        if thisOp.Xid == xid {
          kv.px.Done(kv.lastExecuted)
          return result, err

        } else {
          newSeq = kv.px.Max() + 1
          kv.px.Start(newSeq, *op)
        }
      }

    } else {
      if to > time.Second {
        nop := Op{"nop", "", "", false, "", 0, 0}
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

func (kv *KVPaxos) DoCmd(op *Op) (string, Err) {
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
  }
  return "", OK
}

func (kv *KVPaxos) DoGet(key string) (string, Err) {
  value, exists := kv.table[key]

  if exists {
    return value, OK
  }
  return "", ErrNoKey
}

func (kv *KVPaxos) DoPut(key string, value string, putHash bool) (string, Err) {
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

func (kv *KVPaxos) StartAndWait(op Op) (string, Err) {
  seq := kv.px.Max() + 1
  kv.px.Start(seq, op)
  return kv.WaitAndReturn(seq, &op)
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  result, err := kv.StartAndWait(Op{"Get", args.Key, "", false, args.Xid, 0, 0})
  reply.Err = err
  reply.Value = result
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  result, err := kv.StartAndWait(Op{"Put", args.Key, args.Value, args.DoHash, args.Xid, args.ClientId, args.Seq})
  reply.Err = err
  reply.PreviousValue = result
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.lastExecuted = -1
  kv.table = make(map[string]string)
  kv.putSeqs = make(map[int64]int)
  kv.putResponses = make(map[int64]string)

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

