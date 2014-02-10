package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) assignTask(worker string, num int, numOther int, jobType JobType) {
  go func() {
    args := &DoJobArgs{mr.file, jobType, num, numOther}
    var reply DoJobReply
    success := call(worker, "Worker.DoJob", args, &reply)

    // success is true if the worker responded
    if success {
      // the worker might have errored so we have to redo.
      // But if it responded it is still up and can take more requests
      if reply.OK {
        mr.jobDoneChannel <- jobType  // success!
      } else {
        mr.redoJobChannel <- JobInfo{jobType, num, numOther}
      }
      mr.registerChannel <- worker
    } else {
      // if success is false, then assume the worker is down
      mr.redoJobChannel <- JobInfo{jobType, num, numOther}
    }
  }()
}

func (mr *MapReduce) RunMaster() *list.List {

  mapNumber := 0
  reduceNumber := 0

  for {
    select {
      case workerName := <- mr.registerChannel:

        // Each job (map or reduce) either sends something
        // down mr.jobDoneChannel or mr.redoJobChannel
        // Redos are triggered only by things sent down mr.redoJobChannel
        // Thus by the time mr.mapDone == mr.nMap, exactly mr.nMap map jobs have
        // successfully completed

        // Likewise, if mr.mapDone < mr.nMap, at least one job either has
        // never been assigned, or has failed

        if mr.mapDone < mr.nMap {
          select {
            case redoJob := <- mr.redoJobChannel:
              mr.assignTask(workerName, redoJob.Number, redoJob.NumOther, redoJob.Type)

            default:
              if mapNumber < mr.nMap {
                mr.assignTask(workerName, mapNumber, mr.nReduce, Map)
                mapNumber++
              }
          }
        } else if mr.reduceDone < mr.nReduce {
          select {
            case redoJob := <- mr.redoJobChannel:
              mr.assignTask(workerName, redoJob.Number, redoJob.NumOther, redoJob.Type)

            default:
              if reduceNumber < mr.nReduce {
                mr.assignTask(workerName, reduceNumber, mr.nMap, Reduce)
                reduceNumber++
              }
          }
        } else {
          break
        }

      case jobType := <- mr.jobDoneChannel:
        if jobType == "Map" {
          mr.mapDone++
        } else {
          mr.reduceDone++
        }

      default:
        break
    }

    if mr.mapDone == mr.nMap && mr.reduceDone == mr.nReduce {
      // We are done
      break
    }
  }

  return mr.KillWorkers()
}
