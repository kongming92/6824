package paxos

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.InstanceNum < px.MinLocked() {
    *reply = PrepareReply{args.InstanceNum, Reject, args.N, -1, nil}
    return nil
  }

  instance, ok := px.instances[args.InstanceNum]
  if !ok {
    instance = NewPaxosInstance(args.InstanceNum)
  }

  if args.N > instance.n_p {
    instance.n_p = args.N
    *reply = PrepareReply{instance.instanceNum, OK, args.N, instance.n_a, instance.v_a}
  } else {
    *reply = PrepareReply{instance.instanceNum, Reject, instance.n_p, -1, nil}
  }

  px.instances[args.InstanceNum] = instance
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.InstanceNum < px.MinLocked() {
    *reply = AcceptReply{args.InstanceNum, Reject, args.N}
    return nil
  }

  instance, ok := px.instances[args.InstanceNum]
  if !ok {
    instance = NewPaxosInstance(args.InstanceNum)
  }

  if px.me != args.Me {
    px.dones[args.Me] = args.Done
  }

  if args.N >= instance.n_p {
    instance.n_p = args.N
    instance.n_a = args.N
    instance.v_a = args.V
    *reply = AcceptReply{instance.instanceNum, OK, args.N}
  } else {
    *reply = AcceptReply{instance.instanceNum, Reject, instance.n_p}
  }

  px.instances[args.InstanceNum] = instance
  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.InstanceNum < px.MinLocked() {
    return nil
  }

  instance, ok := px.instances[args.InstanceNum]
  if !ok {
    instance = NewPaxosInstance(args.InstanceNum)
  }
  instance.decided = true
  instance.n_a = args.N
  instance.v_a = args.V
  px.instances[args.InstanceNum] = instance
  return nil
}

func (px *Paxos) SetDone(args *DoneArgs, reply *DoneReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.InstanceNum >= px.MinLocked() {
    px.dones[args.PeerNum] = args.InstanceNum
  }
  return nil
}
