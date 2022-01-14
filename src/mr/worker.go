package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	gob.Register(&TaskMap{})
	gob.Register(&TaskReduce{})

	sockname := coordinatorSock()
	dial, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		panic(err)
	}

	// 通过 rpc 不断获取 task
	for {
		var (
			args  = &ApplyArgs{}
			reply = &ApplyReply{}
		)

		// 调用 rpc （通过同步的方式）
		if err := dial.Call("Coordinator.Apply", args, reply); err != nil {
			log.Println("rpc call [Coordinator.Apply] error: ", err)
			return err
		}
		//log.Printf("%+v \n", reply.Task)
		//log.Println("reduce num: ", reply.ReduceN)

		task := reply.Task
		//log.Printf("%+v \n", task.(*TaskMap))
		switch task.(type) {
		case *TaskMap:
			log.Println("start map task...")
			tt := task.(*TaskMap)
			//log.Printf("%+v \n", tt)
			if err := tt.Do(mapf); err != nil {
				return err
			}
			// TODO 循环的意义
			for i := 0; int64(i) < reply.ReduceN; i++ {
				// 将结果写入到磁盘，先创建一个临时文件，在写入完成后，再修改文件名
				f, err := ioutil.TempFile("./", fmt.Sprintf("map-temp-%v", tt.Id_))
				if err != nil {
					return fmt.Errorf("create map result temp file error: %v", err)
				}

				_, err = f.Write(tt.ResultJson())
				if err != nil {
					return fmt.Errorf("write map result to temp file error: %v", err)
				}

				curDir, _ := os.Getwd()
				// 执行完成后，重命名临时文件（根据文件名来判断 worker 是否异常退出）
				newFileName := fmt.Sprintf("map-output-%v", tt.Id_)
				if err := os.Rename(f.Name(), newFileName); err != nil {
					return fmt.Errorf("rename temp file error: %v", err)
				}

				rand.Seed(time.Now().Unix())
				var (
					args = &ReportArgs{
						Task:            tt,
						WorkerId:        rand.Int63(),
						StorageLocation: curDir + newFileName,
					}

					reply = &ReportReply{}
				)

				// 报告给 master
				if err := dial.Call("Coordinator.Report", args, reply); err != nil {
					return err
				}
			}
		case *TaskReduce:
			log.Println("start work reduce...")
			tt := task.(*TaskReduce)
			if err := tt.Do(reducef); err != nil {
				return err
			}

			name := fmt.Sprintf("mr-output-%v", tt.Id_)
			f, err := os.Create(name)
			if err != nil {
				return err
			}

			for _, s := range tt.Result {
				f.Write([]byte(s))
			}

			f.Close()
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
