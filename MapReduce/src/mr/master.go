package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

func logg(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

type TaskKey struct { // 任务的唯一标识
	Task_type   string // {"map", "reduce" , "no_task", "exit"}
	Task_number int    // 各个任务根据 map 和 reduce 自己找到对应的文件, 输出文件名也是
}

type TaskInfo struct { // 任务的标识和运行信息
	Key        TaskKey
	State      string // {"idle","in-progress","completed"}
	Start_time int64
}

type Master struct {
	// Your definitions here
	Task_infos     []TaskInfo
	Mu             sync.RWMutex
	Map_is_done    bool
	Reduce_is_done bool
	Input_files    []string
	NMap           int
	NReduce        int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	/*
		worker 向 master 请求 task
		在 master 所有任务已完成时，通知 worker 退出。
	*/
	idle_task_key, has_task := m.SeletOneIdleTask()
	if has_task { // 成功派发任务
		// 准备输入文件名
		input_files := []string{}
		if idle_task_key.Task_type == "map" {
			input_files = append([]string{}, m.Input_files[idle_task_key.Task_number])
		} else {
			for i := 0; i < m.NMap; i++ {
				j := idle_task_key.Task_number
				filename := fmt.Sprintf("mr-%v-%v", i, j)
				input_files = append(input_files, filename)
			}
		}
		reply.NReduce = m.NReduce
		reply.Input_files = input_files
		reply.Key = idle_task_key
		m.SetTaskState(idle_task_key, "in-progress")
	} else {
		key := TaskKey{}
		if !m.Reduce_is_done {
			key.Task_type = "no_task"
		} else {
			key.Task_type = "exit"
		}
		reply.Key = key
	}
	return nil
}
func (m *Master) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	/*
		worker 通知 master 任务完成
		（worker 必须保证此时文件也已经准备好了）
	*/
	m.SetTaskState(args.Key, "completed")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) AddTask(key TaskKey) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	logg("添加任务: %v %v", key.Task_type, key.Task_number)
	task_info := TaskInfo{}
	task_info.State = "idle"
	task_info.Key = key
	task_info.Start_time = 0
	m.Task_infos = append(m.Task_infos, task_info)
}
func (m *Master) SetTaskState(key TaskKey, new_state string) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	logg("设置任务状态: %v %v %v", key.Task_type, key.Task_number, new_state)
	for i := 0; i < len(m.Task_infos); i++ {
		if m.Task_infos[i].Key == key {
			m.Task_infos[i].State = new_state
			if new_state == "in-progress" {
				m.Task_infos[i].Start_time = time.Now().Unix()
			}
			break
		}
	}
}
func (m *Master) SeletOneIdleTask() (TaskKey, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	for i := 0; i < len(m.Task_infos); i++ {
		if m.Task_infos[i].State == "idle" {
			key := m.Task_infos[i].Key
			logg("选择一个空闲任务: %v %v", key.Task_type, key.Task_number)
			return key, true
		}
	}
	return TaskKey{}, false
}
func (m *Master) StartMap(files []string, nReduce int) {
	/*
	 把 map 任务添加进任务列表
	*/
	m.NMap = len(files)
	m.NReduce = nReduce
	for i := 0; i < len(files); i++ {
		task_key := TaskKey{
			Task_type:   "map",
			Task_number: i,
		}
		m.AddTask(task_key)
	}

}

func (m *Master) StartReduce() {
	/*
	 把 map reduce 任务，添加进任务列表 （要保证 mr-X-Y 文件都存在，X 对应 map个数,Y 对应 nReduce）
	*/
	for i := 0; i < m.NReduce; i++ {
		task_key := TaskKey{
			Task_type:   "reduce",
			Task_number: i,
		}
		m.AddTask(task_key)
	}
}

func (m *Master) Done() bool {
	/*
			一秒被调用一次
			1. 检查 map 是否完成，完成则开始 reduce
			2. 检查 reduce 是否完成，完成则返回 true
			3. 检查是否有任务超时, 有超时则重新分配任务
			检查是否有任务超时
		//
	*/
	now := time.Now().Unix()
	is_all_done := true // 当前阶段 {"map","reduce"} 的任务是否全部执行完成
	logg("开始检查所有任务完成情况......")
	for i := 0; i < len(m.Task_infos); i++ {
		// 更新完成信息
		switch m.Task_infos[i].State {
		case "idle":
			is_all_done = false
			logg("发现 idle %v %v", m.Task_infos[i].Key.Task_type, m.Task_infos[i].Key.Task_number)
		case "in-progress":
			is_all_done = false
			logg("发现 in-progress %v %v", m.Task_infos[i].Key.Task_type, m.Task_infos[i].Key.Task_number)
			// 检查是否超时, 超时则重置为 idle
			if now-m.Task_infos[i].Start_time > 10 {
				key := m.Task_infos[i].Key
				m.SetTaskState(key, "idle")
				logg("任务超时: %v %v %vs", key.Task_type, key.Task_number, now-m.Task_infos[i].Start_time)
			}
		case "completed":
			logg("发现 completed %v %v", m.Task_infos[i].Key.Task_type, m.Task_infos[i].Key.Task_number)
		default:
			logg("发现 default %v %v %v", m.Task_infos[i].State, m.Task_infos[i].Key.Task_type, m.Task_infos[i].Key.Task_number)
		}

	}
	if len(m.Task_infos) < m.NMap {
		is_all_done = false
	}
	if m.Map_is_done && len(m.Task_infos) < m.NMap+m.NReduce {
		is_all_done = false
	}
	logg("is_all_done:%v", is_all_done)
	if m.Map_is_done {
		// 是否 map reduce 都执行完成, 如果完成，检查输出文件 mr-out-Y
		if is_all_done {
			for j := 0; j < m.NReduce; j++ {
				filename := fmt.Sprintf("mr-out-%v", j)
				_, err := os.Stat(filename)
				if os.IsNotExist(err) {
					logg("发现文件%v不存在，重新分配任务", filename)
					key := TaskKey{
						Task_type:   "reduce",
						Task_number: j,
					}
					m.SetTaskState(key, "idle")
					is_all_done = false
				}
			}
		}
		m.Reduce_is_done = is_all_done
	} else {
		// 是否 map 执行完成, 如果执行完成，则检查文件是否准备好 mr-X-Y
		if is_all_done {
			for i := 0; i < m.NMap; i++ {
				for j := 0; j < m.NReduce; j++ {
					filename := fmt.Sprintf("mr-%v-%v", i, j)
					_, err := os.Stat(filename)
					if os.IsNotExist(err) {
						logg("发现文件%v不存在，重新分配任务", filename)
						key := TaskKey{
							Task_type:   "map",
							Task_number: i,
						}
						m.SetTaskState(key, "idle")
						is_all_done = false
					}
				}
			}
		}
		m.Map_is_done = is_all_done
		if is_all_done {
			m.StartReduce() // map 完成 ，执行 reduce
		}
	}
	return m.Reduce_is_done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.Input_files = files
	m.StartMap(files, nReduce) // 把 map 任务添加进来
	m.server()
	return &m
}
