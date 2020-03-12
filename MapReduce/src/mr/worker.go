package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	StartLoop(mapf, reducef)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func StartLoop(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	/*
		loop
	*/
	for {
		reply := CallTaskRequest()
		switch reply.Key.Task_type {
		case "map":
			doMap(mapf, reply.Key, reply.Input_files, reply.NReduce)
		case "reduce":
			doReduce(reducef, reply.Key, reply.Input_files)
		case "no_task":
			logg("worker pid %v, 没有任务", os.Getpid())
			time.Sleep(1)
		case "exit":
			logg("任务已完成，worker 关闭.")
			os.Exit(0)
		}
		time.Sleep(1)
	}
}
func doMap(mapf func(string, string) []KeyValue, task_key TaskKey, input_files []string, nReduce int) {
	/*
		1. 读取输入文件
		2. 生成中间 kv
		3. 排序 kv, 输出到文件（用临时文件）mr-X-Y
	*/
	logg("启动一个 map worker , pid:%v", os.Getpid())
	intermediate := []KeyValue{}
	for _, filename := range input_files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	logg("map %v, 得到了 %v 个中间结果", task_key.Task_number, len(intermediate))
	// 划分到 nReduce 个slice里
	hashed_intermediate := make([][]KeyValue, nReduce)
	for i := 0; i < len(intermediate); i++ {
		j := ihash(intermediate[i].Key) % nReduce
		hashed_intermediate[j] = append(hashed_intermediate[j], intermediate[i])
	}
	// 保存到文件
	for i := 0; i < len(hashed_intermediate); i++ {
		filename := fmt.Sprintf("mr-%d-%d", task_key.Task_number, i)
		f, err := ioutil.TempFile("", filename+"*")
		if err != nil {
			logg("map 创建文件%v失败！"+err.Error(), filename)
			os.Exit(0)
		}
		enc := json.NewEncoder(f)
		for j := 0; j < len(hashed_intermediate[i]); j++ {
			err := enc.Encode(&hashed_intermediate[i][j])
			if err != nil {
				logg("json错误:" + err.Error())
			}
		}
		err = os.Rename(f.Name(), filename)
		if err != nil {
			logg("Rename错误" + err.Error())
		}
	}
	//a, err := ioutil.TempFile("", "")
	//tmp_files := []

	CallTaskComplete(task_key)
}
func doReduce(reducef func(string, []string) string, task_key TaskKey, input_files []string) {
	/*
		1. 读取输入文件(有多个 json 格式的文件)
		2. 输出处理结果 mr-out-Y
	*/
	logg("启动一个 reduce worker , pid:%v", os.Getpid())
	kva := []KeyValue{}
	for _, filename := range input_files {
		file, err := os.Open(filename)
		if err != nil {
			logg("reduce 打不开文件" + err.Error())
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
	logg("reduce %v 读取了%v 个kv, 第一个 %v %v", task_key.Task_number, len(kva), kva[0].Key, kva[0].Value)
	// 创建文件 mr-out-Y
	filename := fmt.Sprintf("mr-out-%d", task_key.Task_number)
	f, err := ioutil.TempFile("", filename+"*")
	if err != nil {
		logg("reduce 创建文件%v失败！"+err.Error(), filename)
		os.Exit(0)
	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		k := kva[i].Key
		values := []string{} //处理一个 key
		j := i
		for ; j < len(kva) && kva[i].Key == kva[j].Key; j++ {
			values = append(values, kva[j].Value)
		}
		i = j
		output := reducef(k, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", k, output)
	}
	// 文件改名
	os.Rename(f.Name(), filename)
	CallTaskComplete(task_key)
}
func CallTaskRequest() TaskRequestReply {
	/*
		向 master 请求一个任务
	*/
	args := TaskRequestArgs{}
	reply := TaskRequestReply{}
	call("Master.TaskRequest", &args, &reply)
	return reply
}

func CallTaskComplete(key TaskKey) {
	args := TaskCompleteArgs{
		Key: key,
	}
	reply := TaskCompleteReply{}
	call("Master.TaskComplete", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
