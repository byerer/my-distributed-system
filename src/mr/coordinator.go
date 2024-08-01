package mr

import (
	"context"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP = iota
	REDUCE
)

type Task struct {
	ToMap    *MapTask
	ToReduce *ReduceTask
	BeginAt  time.Time
}

type MapTask struct {
	Filename string
	MapID    int
}

type ReduceTask struct {
	ReduceID int
}

type List struct {
	WaitingList []*Task
	DoingList   []*Task
	mu          sync.Mutex // 添加锁
}

type Coordinator struct {
	// Your definitions here.
	MapList    List
	ReduceList List
	nReduce    int
	MapCnt     int

	Cancel context.CancelFunc
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	c.MapList.mu.Lock()
	defer c.MapList.mu.Unlock()
	c.ReduceList.mu.Lock()
	defer c.ReduceList.mu.Unlock()

	if len(c.MapList.WaitingList) != 0 {
		resp.Kind = MAP
		resp.Filename = c.MapList.WaitingList[0].ToMap.Filename
		resp.NReduce = c.nReduce
		resp.MapID = c.MapList.WaitingList[0].ToMap.MapID
		task := c.MapList.WaitingList[0]
		task.BeginAt = time.Now()
		c.MapList.DoingList = append(c.MapList.DoingList, task)
		c.MapList.WaitingList = c.MapList.WaitingList[1:]
	} else if len(c.MapList.DoingList) == 0 && len(c.ReduceList.WaitingList) != 0 {
		resp.Kind = REDUCE
		resp.ReduceID = c.ReduceList.WaitingList[0].ToReduce.ReduceID
		resp.MapCnt = c.MapCnt
		c.ReduceList.DoingList = append(c.ReduceList.DoingList, c.ReduceList.WaitingList[0])
		c.ReduceList.DoingList[0].BeginAt = time.Now()
		c.ReduceList.WaitingList = c.ReduceList.WaitingList[1:]
	} else if len(c.ReduceList.WaitingList) == 0 && len(c.MapList.DoingList) == 0 && len(c.ReduceList.DoingList) == 0 && len(c.MapList.WaitingList) == 0 {
		resp.AllDone = true
	} else {
		resp.Flag = true
	}
	return nil
}

func (c *Coordinator) FinishTask(req *FinishTaskRequest, resp *FinishTaskResponse) error {
	c.MapList.mu.Lock()
	defer c.MapList.mu.Unlock()
	c.ReduceList.mu.Lock()
	defer c.ReduceList.mu.Unlock()

	switch req.Kind {
	case MAP:
		for i, task := range c.MapList.DoingList {
			if task.ToMap.MapID == req.MapID {
				c.MapList.DoingList = append(c.MapList.DoingList[:i], c.MapList.DoingList[i+1:]...)
				break
			}
		}
	case REDUCE:
		for i, task := range c.ReduceList.DoingList {
			if task.ToReduce.ReduceID == req.ReduceID {
				c.ReduceList.DoingList = append(c.ReduceList.DoingList[:i], c.ReduceList.DoingList[i+1:]...)
			}
		}
	}
	return nil
}

func (c *Coordinator) UpdateTime(req *UpdateTimeReq, resp *UpdateTimeResp) error {
	c.MapList.mu.Lock()
	defer c.MapList.mu.Unlock()
	c.ReduceList.mu.Lock()
	defer c.ReduceList.mu.Unlock()

	switch req.Kind {
	case MAP:
		for _, task := range c.MapList.DoingList {
			if task.ToMap.MapID == req.MapID {
				task.BeginAt = time.Now()
				break
			}
		}
	case REDUCE:
		for _, task := range c.ReduceList.DoingList {
			if task.ToReduce.ReduceID == req.ReduceID {
				task.BeginAt = time.Now()
				break
			}
		}
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

func (c *Coordinator) Check(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // 监听取消信号
			return // 退出函数
		case <-time.After(5 * time.Second): // 使用time.After替代time.Sleep
			c.MapList.mu.Lock()
			for i := len(c.MapList.DoingList) - 1; i >= 0; i-- {
				task := c.MapList.DoingList[i]
				if time.Now().Sub(task.BeginAt) > 10*time.Second {
					c.MapList.WaitingList = append(c.MapList.WaitingList, task)
					c.MapList.DoingList = append(c.MapList.DoingList[:i], c.MapList.DoingList[i+1:]...)
				}
			}
			c.MapList.mu.Unlock()

			c.ReduceList.mu.Lock()
			for i := len(c.ReduceList.DoingList) - 1; i >= 0; i-- {
				task := c.ReduceList.DoingList[i]
				if time.Now().Sub(task.BeginAt) > 10*time.Second {
					c.ReduceList.WaitingList = append(c.ReduceList.WaitingList, task)
					c.ReduceList.DoingList = append(c.ReduceList.DoingList[:i], c.ReduceList.DoingList[i+1:]...)
				}
			}
			c.ReduceList.mu.Unlock()
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.MapList.mu.Lock()
	defer c.MapList.mu.Unlock()
	c.ReduceList.mu.Lock()
	defer c.ReduceList.mu.Unlock()

	if len(c.MapList.WaitingList) != 0 || len(c.MapList.DoingList) != 0 {
		return ret
	}
	if len(c.ReduceList.WaitingList) != 0 || len(c.ReduceList.DoingList) != 0 {
		return ret
	}
	ret = true
	c.Cancel()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		MapCnt:  len(files),
	}
	// init
	for i, filename := range files {
		mapTask := &MapTask{
			Filename: filename,
			MapID:    i,
		}
		c.MapList.WaitingList = append(c.MapList.WaitingList, &Task{
			ToMap: mapTask,
		})
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := &ReduceTask{
			ReduceID: i,
		}
		c.ReduceList.WaitingList = append(c.ReduceList.WaitingList, &Task{
			ToReduce: reduceTask,
		})
	}
	c.server()
	var ctx context.Context
	ctx, c.Cancel = context.WithCancel(context.Background())
	go c.Check(ctx)

	return &c
}
