package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

type TaskType int64 // 16868

const (
	_ TaskType = iota
	TypeMap
	TypeReduce
)

var id int64 // 全局 id，用于分配给 task

type MapFunc func(filename string, content string) (res []KeyValue)
type ReduceFunc func(key string, values []string) (res string)

type Tasker interface {
	Id() int64
	ResultJson() []byte // 该结果用于写入到文件
	StartTime() time.Time
	//Type() TaskType
	//Do() error
	//Done() chan struct{}
}

var _ Tasker = &TaskMap{}
var _ Tasker = &TaskReduce{}

// --------------------- TaskMap ---------------------

type TaskMap struct {
	Id_             int64
	File            string
	Result          []KeyValue // map 处理结果
	ResultJson_     []byte     // 该结果用于写入文件
	StorageLocation string
	StartTime_      time.Time
	Done            bool
	Timeout         bool
}

func NewTaskMap(file string) *TaskMap {
	t := &TaskMap{
		Id_:        atomic.AddInt64(&id, 1),
		File:       file,
		StartTime_: time.Now(),
	}
	return t
}

func (t *TaskMap) Id() int64 {
	return t.Id_
}

//func (t *TaskMap) Type() TaskType {
//	return t.typ
//}

func (t *TaskMap) ResultJson() []byte {
	return t.ResultJson_
}

func (t *TaskMap) StartTime() time.Time {
	return t.StartTime_
}

//func (t *TaskMap) Done() chan struct{} {
//	return t.done
//}

func (t *TaskMap) Do(fn MapFunc) error {
	f, err := os.Open(t.File)
	if err != nil {
		return err
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	r := fn(t.File, string(content))
	t.Result = append(t.Result, r...)
	f.Close()

	j, err := json.Marshal(t.Result)
	if err != nil {
		return err
	}
	t.ResultJson_ = j
	//t.done <- struct{}{}
	return nil
}

// --------------------- TaskReduce ---------------------

type TaskReduce struct {
	Id_         int64
	MapResult   []KeyValue // map 处理结果
	ResultJson_ []byte
	Result      []string
	StartTime_  time.Time
	Done        bool
	Timeout     bool
}

func NewTaskReduce(mapResult []KeyValue) *TaskReduce {
	t := &TaskReduce{
		Id_:        atomic.AddInt64(&id, 1),
		MapResult:  mapResult,
		StartTime_: time.Now(),
	}
	return t
}

func (t *TaskReduce) Id() int64 {
	return t.Id_
}

//func (t *TaskReduce) Type() TaskType {
//	return t.typ
//}

//func (t *TaskReduce) Done() chan struct{} {
//	return t.done
//}

func (t *TaskReduce) ResultJson() []byte {
	return t.ResultJson_
}

func (t *TaskReduce) StartTime() time.Time {
	return t.StartTime_
}

func (t *TaskReduce) Do(fn ReduceFunc) error {
	intermediate := t.MapResult
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	var result []string
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) &&
			intermediate[i].Key == intermediate[j].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := fn(intermediate[i].Key, values)
		result = append(result, fmt.Sprintf("%v %v\n", intermediate[i].Key, output))

		i = j
	}

	j, err := json.Marshal(result)
	if err != nil {
		return err
	}
	t.ResultJson_ = j
	//t.done <- struct{}{}
	return nil
}
