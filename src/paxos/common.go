package paxos

const(
  OK = "OK"
  Reject = "Reject"
)

type PaxosInstance struct {
  instanceNum int
  n_p int
  n_a int
  v_a interface{}
  decided bool
}

type PrepareArgs struct {
  InstanceNum int
  N int     // the sequence
}

type PrepareReply struct {
  InstanceNum int
  Status string
  N_p int
  N_a int
  V_a interface{}
}

type AcceptArgs struct {
  InstanceNum int
  N int
  V interface{}
}

type AcceptReply struct {
  InstanceNum int
  Status string
  N int
}

type DecidedArgs struct {
  InstanceNum int
  N int
  V interface{}
}

type DecidedReply struct {}

type DoneArgs struct {
  PeerNum int
  InstanceNum int
}

type DoneReply struct {}

func NewPaxosInstance(n int) PaxosInstance {
  return PaxosInstance{n, -1, -1, "", false}
}
