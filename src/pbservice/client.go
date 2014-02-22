package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
// import "time"
import "crypto/rand"
import "math/big"
import "strconv"

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  primary string
  seq int
  clientId int64
}

func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  ck.primary = ck.vs.Primary()
  ck.seq = 0
  ck.clientId = nrand()
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
  // Your code here.
  defer func() {
    ck.seq += 1
  }()

  for ck.primary == "" {
    ck.primary = ck.vs.Primary()
  }
  tries := 0
  xid := strconv.FormatInt(ck.clientId, 10) + strconv.Itoa(ck.seq)

  args := &GetArgs{key, xid}
  var reply GetReply

  for {
    ok := call(ck.primary, "PBServer.Get", args, &reply)
    if ok {
      if reply.Err == OK {
        return reply.Value
      } else if reply.Err == ErrNoKey {
        return ""
      } else if reply.Err == ErrWrongServer {
        ck.primary = ck.vs.Primary()
      }
    } else if tries == viewservice.DeadPings {
      ck.primary = ck.vs.Primary()
      tries = 0
    } else {
      tries += 1
    }
  }
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // Your code here.
  defer func() {
    ck.seq += 1
  }()

  for ck.primary == "" {
    ck.primary = ck.vs.Primary()
  }

  // fmt.Println("call to put")
  // fmt.Println(ck)
  tries := 0
  xid := strconv.FormatInt(ck.clientId, 10) + strconv.Itoa(ck.seq)

  args := &PutArgs{key, value, dohash, xid}
  var reply PutReply
  for {
    ok := call(ck.primary, "PBServer.Put", args, &reply)
    if ok {
      if reply.Err == OK {
        return reply.PreviousValue
      } else if reply.Err == ErrWrongServer {
        ck.primary = ck.vs.Primary()
      }
    } else if tries == viewservice.DeadPings {
      ck.primary = ck.vs.Primary()
      tries = 0
    } else {
      tries += 1
    }
  }
}

func (ck *Clerk) Put(key string, value string) {
  // fmt.Println(key + " " + value)
  ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
