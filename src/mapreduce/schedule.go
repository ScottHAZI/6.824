package mapreduce

import (
	"fmt"
	"sync"
)

// Task holds task number and file name
type Task struct {
	Number int
	File   string
}

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	/*
		// method 1
		nleft := ntasks
		ncomplete := 0
		taskChan := make(chan Task, ntasks)
		var file string
		for i := 0; i < ntasks; i++ {
			if phase == mapPhase {
				file = mapFiles[i]
			} else {
				file = ""
			}
			taskChan <- Task{i, file}
		}

		var mux sync.Mutex
		for ncomplete != ntasks {
			mux.Lock()
			if nleft == 0 {
				// goroutine似乎要通过一些显式的方式切换，当nleft为0且没有time.Sleep的时候就一直会在主routinue里运行，不会切换，陷入死循环
				time.Sleep(time.Second)
				mux.Unlock()
				continue
			}
			mux.Unlock()

			task := <-taskChan
			//fmt.Println("wait idle worker")
			workerAddr := <-registerChan
			mux.Lock()
			nleft--
			//fmt.Println("nleft", nleft)
			mux.Unlock()
			//fmt.Println("create goroutine, task", task.Number)
			go func(task Task, workerAddr string) {
				//fmt.Println("in goroutine")
				taskArgs := new(DoTaskArgs)
				taskArgs.JobName = jobName
				taskArgs.File = task.File
				taskArgs.Phase = phase
				taskArgs.TaskNumber = task.Number
				taskArgs.NumOtherPhase = n_other

				//mux.Lock()
				//fmt.Println("run task ", task.Number, "nleft ", nleft)
				//mux.Unlock()
				done := call(workerAddr, "Worker.DoTask", taskArgs, nil)
				if done {
					//fmt.Printf("complete task %d\n", task.Number)
					mux.Lock()
					ncomplete++
					mux.Unlock()
				} else {
					//fmt.Printf("fail to complete task %d\n", task.Number)
					taskChan <- task
					mux.Lock()
					nleft++
					mux.Unlock()
				}
				// registerChan is not buffered channel and thus will stuck here at the end if we directly call registerChan <- workerAddr
				go func() { registerChan <- workerAddr }()
			}(task, workerAddr)
		}
	*/

	// method 2
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNum int) {
			taskArgs := new(DoTaskArgs)
			taskArgs.JobName = jobName
			taskArgs.File = mapFiles[taskNum] // reducePhase时，DoTaskArgs里的File会被忽略掉
			taskArgs.Phase = phase
			taskArgs.TaskNumber = taskNum
			taskArgs.NumOtherPhase = n_other

			for {
				workerAddr := <-registerChan

				ok := call(workerAddr, "Worker.DoTask", taskArgs, nil)
				// registerChan is not a buffered channel thus we need to call it in another goroutine otherwise it will stuck
				go func() { registerChan <- workerAddr }()
				if ok {
					break
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
