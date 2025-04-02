package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// import "fmt"

// import "context"

type Coordinator struct {
	// Your definitions here.
	nReduce         int
	M               int   //num of task
	maptaskstate    []int // 0:未分配; 1:分配maptask ; 2:maptask done;
	reducetaskstate []int // 0:未分配; 1:分配task ; 2:task done;
	worker          []int //
	mapdone         int
	reducedone      int
	tasks           []Task
	mu              sync.Mutex
	maptasktimer    []time.Time
	reducetasktimer []time.Time
}

type Task struct {
	Filename string
	Start    int
	Size     int
}

var done = make(chan struct{})
var once sync.Once

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapdone < c.M {
		tid := -1
		for i := 0; i < c.M; i++ {
			if c.maptaskstate[i] == 0 {
				c.maptaskstate[i] = 1
				tid = i
				break
			}
		}
		if tid == -1 {
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 1
		reply.TaskId = tid
		reply.Task = c.tasks[tid]
		reply.NReduce = c.nReduce

		c.maptasktimer[tid] = time.Now()
		// ctx ,cancel := context.WithTimeout(context.Background(),10*time.Second)
		// defer cancel()

		// c.pollWithContext(ctx,1,tid)
		return nil
	}
	if c.reducedone < c.nReduce {
		tid := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reducetaskstate[i] == 0 {
				c.reducetaskstate[i] = 1
				tid = i
				break
			}
		}
		if tid == -1 {
			reply.TaskType = 0
			return nil
		}
		reply.TaskType = 2
		reply.TaskId = tid
		reply.NReduce = c.nReduce
		reply.Worker = c.worker

		c.reducetasktimer[tid] = time.Now()
		return nil
	}

	return errors.New("task done")
}

func (c *Coordinator) poll(timeout time.Duration) {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			if c.mapdone < c.M {
				for i, s := range c.maptaskstate {
					if s == 1 && time.Since(c.maptasktimer[i]) > timeout {
						c.mu.Lock()

						c.maptaskstate[i] = 0

						c.mu.Unlock()
					}
				}
			} else if c.reducedone < c.nReduce {
				for i, s := range c.reducetaskstate {
					if s == 1 && time.Since(c.reducetasktimer[i]) > timeout {
						c.mu.Lock()

						c.reducetaskstate[i] = 0

						c.mu.Unlock()
					}
				}
			}
		}
	}
}

func (c *Coordinator) TaskReport(args *TaskReportArgs, reply *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == 0 {
		if args.TaskId < 0 || args.TaskId >= c.M {
			return errors.New("error Task report")
		}
		c.maptaskstate[args.TaskId] = 2
		c.worker[args.TaskId] = args.Wid
		c.mapdone++
	} else if args.TaskType == 1 {
		if args.TaskId < 0 || args.TaskId >= c.nReduce {
			return errors.New("error Task report")
		}
		c.reducetaskstate[args.TaskId] = 2
		c.reducedone++

	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.
	ret = c.nReduce == c.reducedone

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	initfilesystem()
	// tasknum := splitfile(files)
	c.tasks = splitTask(files)

	// for i,t := range c.tasks {
	// 	fmt.Println(i," "+t.Filename+" ",t.Start," ",t.Size)
	// }
	c.nReduce = nReduce
	c.M = len(c.tasks)
	c.mapdone = 0
	c.reducedone = 0
	c.maptaskstate = make([]int, c.M)
	c.worker = make([]int, c.M)
	c.maptasktimer = make([]time.Time, c.M)
	c.reducetasktimer = make([]time.Time, c.nReduce)
	c.reducetaskstate = make([]int, c.nReduce)

	go c.poll(10 * time.Second)

	c.server()
	return &c
}

// func (c *Coordinator) GetTaskData(args *TaskDataArgs, reply *TaskDataReply) error{
// 	filename := taskpath+"task"+strconv.Itoa(args.TaskId)+".txt";
// 	buf := make([]byte, 1024*64)
// 	file,err := os.Open(filename)
// 	if err != nil {
// 		return errors.New("file open failed")
// 	}
// 	defer file.Close()

// 	reader := bufio.NewReader(file)
// 	n,err := reader.Read(buf)
// 	if err != nil{
// 		return errors.New("file read failed")
// 	}
// 	reply.Data = buf
// 	reply.Size = n
// 	return nil
// }
