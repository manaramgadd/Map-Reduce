package mapreduce

import (
	"fmt"
	"sync"
)

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for j := 0; j < ntasks; j++ {
		//fmt.Println(j)
		go func() {
			defer wg.Done()
			if phase == mapPhase {
				//fmt.Println("in map phase")
				mapTask := RunTaskArgs{mr.jobName, mr.files[j], mapPhase, j, numOtherPhase}
				//fmt.Println("khalst map phase")
				srv := <-mr.registerChannel

				//fmt.Println("ana khalst el awel srv call")
				state := call(srv, "Worker.RunTask", &mapTask, nil)
				//fmt.Println("ana khalst el awel state call", state)
				//go func() { mr.registerChannel <- srv }()
				for state == false {
					//fmt.Println("in for loop state: ", state)

					srv = <-mr.registerChannel

					state = call(srv, "Worker.RunTask", &mapTask, nil)

					//go func() { mr.registerChannel <- srv }()
				}

				//fmt.Println("out")
				go func() { mr.registerChannel <- srv }()

			} else {
				fmt.Println("in reduce phase")
				reduceTask := RunTaskArgs{mr.jobName, "", reducePhase, j, numOtherPhase}

				srv := <-mr.registerChannel

				state := call(srv, "Worker.RunTask", &reduceTask, nil)
				////go func() { mr.registerChannel <- srv }()
				for state == false {

					srv = <-mr.registerChannel

					state = call(srv, "Worker.RunTask", &reduceTask, nil)

					//go func() { mr.registerChannel <- srv }()

				}

				//fmt.Println("OUTTTT")
				go func() { mr.registerChannel <- srv }()
			}

		}()

	}

	wg.Wait()
	//fmt.Println("BARA")

	debug("Schedule: %v phase done\n", phase)
}
