package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  view viewservice.View
  table map[string]string
  mu sync.Mutex
  seen map[string]bool
  old map[string]string
}

func (pb *PBServer) doPut(args *PutArgs) string {
  prev, exists := pb.table[args.Key]
  if !exists {
    prev = ""
  }

  if args.DoHash {
    pb.table[args.Key] = strconv.Itoa(int(hash(prev + args.Value)))
  } else {
    pb.table[args.Key] = args.Value
  }

  return prev
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // If you're not the primary, don't serve anything
  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
    reply.PreviousValue = ""
    return nil
  }

  bkArgs := &BKPutArgs{args, pb.view.Viewnum}
  var bkReply PutReply
  success := false
  for pb.view.Backup != "" && !success {
    success = call(pb.view.Backup, "PBServer.BKPut", bkArgs, &bkReply)
    if !success {
      view, _ := pb.vs.Ping(pb.view.Viewnum)
      if view.Primary == pb.me {
        pb.view = view
      } else {
        reply.Err = ErrWrongServer
        reply.PreviousValue = ""
        return nil
      }
    }
  }

  if bkReply.Err != ErrWrongServer {
    var prev string
    if pb.seen[args.Xid] {
      prev = pb.old[args.Xid]
    } else {
      prev = pb.doPut(args)
      pb.seen[args.Xid] = true
      pb.old[args.Xid] = prev
    }
    reply.Err = OK
    reply.PreviousValue = prev
  } else {
    reply.Err = ErrWrongServer
    reply.PreviousValue = ""
  }

  return nil
}

func (pb *PBServer) BKGet(args *BKGetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // If you're not the backup, don't serve anything
  if pb.view.Backup != pb.me || pb.view.Viewnum != args.Viewnum{
    reply.Err = ErrWrongServer
    reply.Value = ""
    return nil
  }

  if pb.seen[args.Args.Xid] {
    reply.Value = pb.old[args.Args.Xid]
  } else {
    val := pb.table[args.Args.Key]
    reply.Value = val
    pb.seen[args.Args.Xid] = true
    pb.old[args.Args.Xid] = val
  }
  reply.Err = OK
  return nil
}

func (pb *PBServer) BKPut(args *BKPutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // If you're not the backup, don't serve anything
  if pb.view.Backup != pb.me || pb.view.Viewnum != args.Viewnum {
    reply.Err = ErrWrongServer
    reply.PreviousValue = ""
    return nil
  }

  if pb.seen[args.Args.Xid] {
    reply.PreviousValue = pb.old[args.Args.Xid]
  } else {
    prev := pb.doPut(args.Args)
    reply.PreviousValue = prev
    pb.seen[args.Args.Xid] = true
    pb.old[args.Args.Xid] = prev
  }
  reply.Err = OK
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // If you're not the primary, don't serve anything
  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
    reply.Value = ""
    return nil
  }

  bkArgs := &BKGetArgs{args, pb.view.Viewnum}
  var bkReply GetReply
  success := false
  for pb.view.Backup != "" && !success {
    success = call(pb.view.Backup, "PBServer.BKGet", bkArgs, &bkReply)
    if !success {
      view, _ := pb.vs.Ping(pb.view.Viewnum)
      if view.Primary == pb.me {
        pb.view = view
      } else {
        reply.Err = ErrWrongServer
        reply.Value = ""
        return nil
      }
    }
  }
  if bkReply.Err != ErrWrongServer {
    value, exists := pb.table[args.Key]

    if exists {
      reply.Err = OK
      reply.Value = value
    } else {
      reply.Err = ErrNoKey
      reply.Value = ""
    }
  } else {
    reply.Err = ErrWrongServer
    reply.Value = ""
  }

  return nil
}

func (pb *PBServer) SetEntire(args *SetEntireArgs, reply *SetEntireReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  pb.table = args.Table
  pb.seen = args.Seen
  pb.old = args.Old
  reply.OK = OK
  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  oldView := pb.view
  view, _ := pb.vs.Ping(pb.view.Viewnum)
  pb.view = view

  if pb.me == pb.view.Primary && pb.view.Backup != oldView.Backup && pb.view.Backup != ""{
    args := &SetEntireArgs{pb.table, pb.seen, pb.old}
    var reply SetEntireReply
    for {
      ok := call(pb.view.Backup, "PBServer.SetEntire", args, &reply)
      if ok && reply.OK == OK {
        break
      }
      time.Sleep(viewservice.PingInterval)
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.View{}
  pb.table = make(map[string]string)
  pb.seen = make(map[string]bool)
  pb.old = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
