package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	mapStatus            map[string]int
	reduceStatus         map[int]int
	NReduce              int
	mu                   sync.RWMutex
	IntermediateFileList map[int][]string
	fileListLock         sync.RWMutex
}

const (
	NOT_STARTED = iota
	MAP_IN_PROGRESS
	MAP_DONE
	REDUCE_DONE
	REDUCE_IN_PROGRESS
)

var tasksFinished = false

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) IntermediateFiles(args *IntermediateTaskArg, reply *IntermediateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapStatus[args.FileName] = MAP_DONE
	for _, file := range args.IntermediateFiles {
		intermediateFile := strings.Split(file, "-")
		fileIndex, _ := strconv.Atoi(intermediateFile[len(intermediateFile)-1])
		c.IntermediateFileList[fileIndex] = append(c.IntermediateFileList[fileIndex], file)
	}

	reply.Ok = true
	return nil
}

func (c *Coordinator) ReduceTask(args *ReduceTaskArg, reply *ReduceTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.ReduceIndex] = REDUCE_DONE
	reply.Ok = true
	return nil
}

func (c *Coordinator) AllMapTaskCompleted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, status := range c.mapStatus {
		if status != MAP_DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllReduceTaskCompleted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, status := range c.reduceStatus {
		if status != REDUCE_DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) RestartTask(tasktype string, identifier string) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			fmt.Print("Restarting task ", tasktype, " ", identifier, "\n")
			if tasktype == "map" {
				c.mapStatus[identifier] = NOT_STARTED
			} else if tasktype == "reduce" {
				reduceIndex, _ := strconv.Atoi(identifier)
				c.reduceStatus[reduceIndex] = NOT_STARTED
			}
			c.mu.Unlock()
			return
		default:
			c.mu.RLock()
			// fmt.Print("Checking task ", tasktype, " ", identifier, "\n")
			if tasktype == "map" {
				if c.mapStatus[identifier] == MAP_DONE {
					c.mu.RUnlock()
					return
				}
				c.mu.RUnlock()
			} else {
				reduceIndex, _ := strconv.Atoi(identifier)
				c.mu.RLock()
				if c.reduceStatus[reduceIndex] == REDUCE_DONE {
					c.mu.RUnlock()
					return
				}
				c.mu.RUnlock()
			}
		}
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if !c.AllMapTaskCompleted() {
		reply.Msg = "map"
	} else if !c.AllReduceTaskCompleted() {
		reply.Msg = "reduce"
	} else {
		reply.Msg = "end"
		tasksFinished = true
		return nil
	}
	if reply.Msg == "map" {
		c.mu.Lock()
		defer c.mu.Unlock()
		var filename string
		for file, status := range c.mapStatus {
			if status == NOT_STARTED {
				filename = file
				break
			}
		}

		// all map tasks are under progress but not completed.
		if filename == "" {
			reply.Msg = "wait"
			return nil
		}

		c.mapStatus[filename] = MAP_IN_PROGRESS
		reply.MapFileName = filename
		reply.NReduce = c.NReduce
		go c.RestartTask("map", filename)
	}

	if reply.Msg == "reduce" {
		reduceIndex := -1
		for index, status := range c.reduceStatus {
			if status == NOT_STARTED {
				reduceIndex = index
				break
			}
		}

		if reduceIndex == -1 {
			reply.Msg = "wait"
			return nil
		}

		c.reduceStatus[reduceIndex] = REDUCE_IN_PROGRESS
		reply.IntermediateFiles = c.IntermediateFileList[reduceIndex]
		go c.RestartTask("reduce", strconv.Itoa(reduceIndex))
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return tasksFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapStatus: make(map[string]int), reduceStatus: make(map[int]int), mu: sync.RWMutex{}, IntermediateFileList: make(map[int][]string), fileListLock: sync.RWMutex{}}
	c.NReduce = nReduce
	for _, file := range files {
		c.mapStatus[file] = NOT_STARTED
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = NOT_STARTED
	}

	c.server()
	return &c
}
