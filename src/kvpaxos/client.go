package kvpaxos

import "net/rpc"
import "fmt"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"
import "strconv"
import "time"

const MAX_TRIES = 10

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  seq int
  clientId int64
  currentServer int
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.seq = 0
  ck.clientId = nrand()
  ck.currentServer = 0
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  defer func() {
    ck.seq += 1
  }()

  tries := 0
  xid := strconv.FormatInt(ck.clientId, 10) + strconv.Itoa(ck.seq)

  args := &GetArgs{key, xid}
  var reply GetReply

  for {
    ok := call(ck.servers[ck.currentServer], "KVPaxos.Get", args, &reply)
    if ok {
      if reply.Err == OK {
        return reply.Value
      } else if reply.Err == ErrNoKey {
        return ""
      }
    } else if tries == MAX_TRIES {
      ck.currentServer = mathrand.Intn(len(ck.servers))
      tries = 0
    } else {
      tries += 1
    }
    time.Sleep(200 * time.Millisecond)
  }
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  defer func() {
    ck.seq += 1
  }()

  tries := 0
  xid := strconv.FormatInt(ck.clientId, 10) + strconv.Itoa(ck.seq)

  args := &PutArgs{key, value, dohash, xid, ck.clientId, ck.seq}
  var reply PutReply
  for {
    ok := call(ck.servers[ck.currentServer], "KVPaxos.Put", args, &reply)
    if ok && reply.Err == OK {
      return reply.PreviousValue
    } else if tries == MAX_TRIES {
      ck.currentServer = mathrand.Intn(len(ck.servers))
      tries = 0
    } else {
      tries += 1
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
