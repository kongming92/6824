package shardmaster

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
import "math/big"
import cryptrand "crypto/rand"
import "reflect"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  lastExecuted int
}

type Op struct {
  // Your data here.
  OpType string
  Xid int64
  Args interface{}
}

// Make int64 slices sortable:
// https://groups.google.com/forum/#!topic/golang-nuts/tyDC4S62nPo
type int64arr []int64
func (a int64arr) Len() int { return len(a) }
func (a int64arr) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := cryptrand.Int(cryptrand.Reader, max)
  x := bigx.Int64()
  return x
}

// Get a config, with the next config number, with all
// current things copied
// NOT THREADSAFE
func (sm *ShardMaster) GetNextCopy() Config {
  config := Config{}
  prevConfig := sm.configs[len(sm.configs) - 1]

  // Increment the config number
  config.Num = prevConfig.Num + 1

  // Copy Shards info
  for i, gid := range(prevConfig.Shards) {
    config.Shards[i] = gid
  }

  // Copy Groups info
  config.Groups = make(map[int64][]string)
  for k, v := range(prevConfig.Groups) {
    config.Groups[k] = v
  }

  return config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.StartAndWait(Op{"Join", nrand(), *args})
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.StartAndWait(Op{"Leave", nrand(), *args})
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.StartAndWait(Op{"Move", nrand(), *args})
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  res := sm.StartAndWait(Op{"Query", nrand(), *args})
  if reflect.TypeOf(res) == reflect.TypeOf(QueryReply{}) {
    resQuery := res.(QueryReply)
    reply.Config = resQuery.Config
  }
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(QueryArgs{})
  gob.Register(JoinArgs{})
  gob.Register(MoveArgs{})
  gob.Register(LeaveArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.lastExecuted = -1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
