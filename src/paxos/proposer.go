package paxos

func (px *Paxos) Propose(seq int, v interface{}) {
  decided := false
  largest := 0
  for !decided && px.dead == false {
    n := getRandom(largest, px.me)
    largest = n

    prepareOk, largestNPrepare, _, v_prime := px.ProposePrepare(seq, n)

    if largest < largestNPrepare {
      largest = largestNPrepare
    }

    if !prepareOk {
      continue
    }

    if v_prime == nil {
      v_prime = v
    }

    acceptOk, _, largestNAccept := px.ProposeAccept(seq, n, v_prime)

    if largest < largestNAccept {
      largest = largestNAccept
    }

    if !acceptOk {
      continue
    }

    px.ProposeDecided(seq, n, v_prime)
    decided = true
  }
}

func getRandom(largest int, me int) (int) {
  num := (largest / 1000) + 1
  return num * 1000 + me
}

func (px *Paxos) ProposePrepare(seq int, n int) (bool, int, int, interface{}) {

  responses := make([]bool, len(px.peers))
  highestN_p := -1
  highestN_a := -1
  var highestV interface{}

  for i, peer := range(px.peers) {

    if px.dead {
      break
    }
    // Send Prepare
    prepareArgs := &PrepareArgs{seq, n}
    success := false
    var reply PrepareReply

    if i == px.me { // local function call for proposer/acceptor on same server
      success = true
      px.Prepare(prepareArgs, &reply)

    } else {  // do RPC
      success = call(peer, "Paxos.Prepare", prepareArgs, &reply)
    }

    responses[i] = false

    if success {
      n_p := reply.N_p
      n_a := reply.N_a
      v_a := reply.V_a

      // Get the highest n_p
      if n_p > highestN_p {
        highestN_p = n_p
      }

      // Get the highest n_a and corresponding v_a
      if reply.Status == OK {
        responses[i] = true
        if n_a > highestN_a {
          highestN_a = n_a
          highestV = v_a
        }
      }
    }
  }

  numSuccess := 0
  for _, val := range responses {
    if val {
      numSuccess += 1
    }
  }

  return (numSuccess > len(px.peers) / 2), highestN_p, highestN_a, highestV
}

func (px *Paxos) ProposeAccept(seq int, n int, v_prime interface{}) (bool, int, int) {

  responses := make([]bool, len(px.peers))
  highestN_p := -1

  px.mu.Lock()
  done := px.dones[px.me]
  px.mu.Unlock()

  for i, peer := range px.peers {

    if px.dead {
      break
    }

    // Send Accept
    acceptArgs := &AcceptArgs{seq, n, v_prime, px.me, done}
    success := false
    var reply AcceptReply

    if i == px.me {
      success = true
      px.Accept(acceptArgs, &reply)
    } else {
      success = call(peer, "Paxos.Accept", acceptArgs, &reply)
    }

    responses[i] = false
    if success {
      if reply.N > highestN_p {
        highestN_p = reply.N
      }
      if reply.Status == OK {
        responses[i] = true
      }
    }
  }

  numSuccess := 0
  for _, val := range responses {
    if val {
      numSuccess += 1
    }
  }

  return (numSuccess > len(px.peers) / 2), n, highestN_p
}

func (px *Paxos) ProposeDecided(seq int, n int, v interface{}) {
  for i, peer := range px.peers {

    if px.dead {
      break
    }

    decidedArgs := &DecidedArgs{seq, n, v}
    var reply DecidedReply

    if i == px.me {
      px.Decided(decidedArgs, &reply)
    } else {
      call(peer, "Paxos.Decided", decidedArgs, &reply)
    }
  }
}
