package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    chan *TaskMap
	reduceTasks chan *TaskReduce

	// 记录 task 的分配情况，key 是 taskId，value 是 task
	// 如果 task 已经完成，则需要从 taskRecord 中移除
	taskRecord map[int64]Tasker

	completedTasks sync.Map // 已经完成的 task
	timeoutTasks   sync.Map // 超时 task
	nReduce        int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// gob: type not registered for interface
	// 如果使用 gob 序列化接口类型，需要提前注册该接口的所有子类，否则会报上面的错误
	gob.Register(&TaskMap{})
	gob.Register(&TaskReduce{})

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// 两个任务队列全为空，则整个系统结束
	return len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
}

// Apply 用于 worker 向 master 通过 rpc 申请一个 task
func (c *Coordinator) Apply(args *ApplyArgs, reply *ApplyReply) error {
	var t Tasker
	//log.Printf("mapTasks num: %v, reduceTasks num: %v \n", len(c.mapTasks), len(c.reduceTasks))
	// map 阶段还未完成，只分配 map task
	if len(c.mapTasks) > 0 {
		t = <-c.mapTasks
		//log.Printf("%+v \n", t)
		reply.Task = t
		//log.Printf("%+v \n", reply.Task)
		reply.ReduceN = c.nReduce
	} else { // map 阶段完成后，再开始分配 reduce task
		t = <-c.reduceTasks
		reply.Task = t
		reply.ReduceN = c.nReduce
	}

	// TODO 暂时忽略 worker 失效的实现
	//go func() {
	//	// 如果 10 秒后 worker 还没有完成，则将该 task 重新添加到队列
	//	time.AfterFunc(time.Second*10, func() {
	//		if _, ok := c.completedTasks.Load(t.Id()); ok {
	//			switch t.(type) {
	//			case *TaskMap:
	//				c.mapTasks <- t.(*TaskMap)
	//			case *TaskReduce:
	//				c.reduceTasks <- t.(*TaskReduce)
	//			}
	//			// 同时将该 task 添加到超时列表中，便于后续忽视执行该 task 的机器所生成的结果文件
	//			c.timeoutTasks.Store(args.TaskId, t)
	//		}
	//	})
	//}()

	return nil
}

// Report 用于 worker 完成 task 后，向 master 进行报告
func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.completedTasks.Store(args.TaskId, nil)
	switch args.Task.(type) {
	case *TaskMap: // 如果是 map 任务，需要根据结果生成新的 reduce 任务
		log.Println("a map task report")
		t := args.Task.(*TaskMap)
		rt := NewTaskReduce(t.Result)
		c.reduceTasks <- rt
		log.Printf(
			"push a reduce task to reduce chan, cur len: %v \n",
			len(c.reduceTasks),
		)
	case *TaskReduce:

	}

	// TODO 判断该 task 是否超时
	return nil
}

//func (c *Coordinator) CheckTimeoutTask() {
//	for {
//		for _, task := range c.taskRecord {
//			useTime := time.Now().Sub(task.StartTime())
//			// 10 秒后还没有完成
//			if useTime > time.Second*10 {
//
//			}
//		}
//	}
//}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     int64(nReduce),
		mapTasks:    make(chan *TaskMap, len(files)),
		reduceTasks: make(chan *TaskReduce, nReduce),
		taskRecord:  make(map[int64]Tasker),
	}
	log.Println(files)

	// Your code here.

	// 系统启动时，先生成 map 任务
	for _, file := range files {
		t := NewTaskMap("../main/" + file)
		//log.Printf("%+v \n", t)
		c.mapTasks <- t
		c.taskRecord[t.Id_] = t
	}

	c.server()
	return &c
}
