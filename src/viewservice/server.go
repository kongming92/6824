
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  recent map[string]time.Time
  current View
  next View
  ackd bool

  // Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  vs.recent[args.Me] = time.Now()

  if args.Viewnum == 0 {
    if vs.current.Viewnum == 0 { // just started, accept first thing as primary
      vs.current = View{1, args.Me, ""}
      vs.next = View{1, args.Me, ""}
    } else if vs.current.Backup == "" && args.Me != vs.current.Primary {
      // no backup, new server connects
      vs.setNext(vs.current.Primary, args.Me)
    } else if args.Me == vs.current.Primary {
      // primary goes down, set primary to backup
      vs.setNext(vs.current.Backup, "")
    } else if args.Me == vs.current.Backup {
      // backup goes down, remove backup
      vs.setNext(vs.current.Primary, "")
    }
  } else if args.Me != vs.current.Primary && vs.current.Backup == "" {
    vs.setNext(vs.current.Primary, args.Me)
  }

  // check if the current ping is the primary
  // if primary is pinging with current viewnum, it is ack'd
  // this means we can change views if there's something to be changed
  if args.Me == vs.current.Primary && args.Viewnum == vs.current.Viewnum {
    vs.ackd = true
  }

  // if next is different (it is updated) then set it to be the current
  if vs.ackd && vs.current != vs.next {
    vs.current = vs.next
    vs.ackd = false
  }

  reply.View = vs.current

  return nil
}

func (vs *ViewServer) setNext(primary, backup string) {
  vs.next = View{vs.current.Viewnum + 1, primary, backup}
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.current
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  for server, t := range(vs.recent) {
    if (time.Since(t)) > (PingInterval * DeadPings) {
      if server == vs.current.Primary {
        vs.setNext(vs.current.Backup, "")
      } else if server == vs.current.Backup {
        vs.setNext(vs.current.Primary, "")
      }
    }
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me

  // Your vs.* initializations here.
  vs.recent = make(map[string]time.Time)
  vs.current = View{0, "", ""}
  vs.next = vs.current
  vs.ackd = false

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
