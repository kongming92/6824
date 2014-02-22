package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  Xid string
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Xid string
}

type GetReply struct {
  Err Err
  Value string
}

type SetEntireArgs struct {
  Table map[string]string
  Seen map[string]bool
  Old map[string]string
}

type SetEntireReply struct {
  OK string
}

type BKGetArgs struct {
  Args *GetArgs
  Viewnum uint
}

type BKPutArgs struct {
  Args *PutArgs
  Viewnum uint
}

// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

