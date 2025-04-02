package mr

import "fmt"
// import "log"
import "sort"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "strconv"
import "bufio"
import "strings"
// import "6.5840/mr"
// import "io/ioutil"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerStruct struct {
	mapf func(string ,string)[]KeyValue
	reducef func(string, []string) string
	// listener net.Listener
	// Socketname string
	wid int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// // 开启worker server 监听
// // rpc注册 worker 服务
// func (w *WorkerStruct) server(){
// 	rpc.Register(w)
// 	rpc.HandleHTTP()
// 	Socketname := w.Socketname
// 	os.Remove(Socketname)
// 	l,e := net.Listen("unix",Socketname)
// 	w.Listener = l
// 	if e != nil{
// 		log.Fatal("listen error",e)
// 	}
// 	go http.Serve(w.Listener,nil)
// }

// the RPC argument and reply types are defined in rpc.go.
func (w *WorkerStruct) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 停止worker
// func (w *WorkerStruct) stop(){
// 	if w.Listener != nil {
// 		w.Listener.Close()
// 		os.Remove(w.Socketname)
// 	}
// 	// clearDirectory(w.Mypath)
// }

// func (w *WorkerStruct)setWorkerSock() {
// 	s := "/var/tmp/"
// 	s += strconv.Itoa(os.Getpid())
// 	w.Socketname = s
// }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := WorkerStruct{}
	w.mapf = mapf
	w.reducef = reducef
	w.wid = os.Getpid()
	// w.setWorkerSock()

	
	for w.requestTask() {
		
	}
	// time.Sleep(3 * time.Second)
	// close(done)
	// go w.server()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// w.stop()
}


func (w *WorkerStruct) requestTask()bool{
	args := TaskRequestArgs{}

	// args.Socketname = w.Socketname
	args.Wid = w.wid

	reply := TaskRequestReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		// fmt.Println("request for task",reply.TaskType,reply.TaskId)
		if reply.TaskType==0{
			// fmt.Println("wait for task",reply.TaskType,reply.TaskId)
			time.Sleep(3 * time.Second)
		}else if reply.TaskType==1{
			ok_done := w.ProcessMapTask(reply.TaskId,reply.NReduce,reply.Task)
			if ok_done {
				TaskReport(0,reply.TaskId,w.wid)
			}else {

			}
			// time.Sleep(0.1 * time.Second)
		}else {
			ok_done := w.ProcessReduceTask(reply.TaskId,reply.Worker)
			// for i,wid := range reply.Worker {
			// 	fmt.Println("(i,wid):",i,wid)
			// }
			if ok_done {
				TaskReport(1,reply.TaskId,w.wid)
			}else {

			}
		}
		return true
	} else {
		// fmt.Println("no task")
		return false
	}
}

func (w *WorkerStruct) ProcessMapTask(taskId int, r int,task Task) bool{
	intermediate := []KeyValue{}

	filename := task.Filename;
	// content := readfile(filename)
	content := readTask(task)

	kva := w.mapf(filename,content)
	intermediate = append(intermediate,kva...)
	// sort.Sort(ByKey(intermediate))

	ofiles := make([]*os.File,r)

	for i := 0; i < r; i++ {
        fname := mapPath+"map-out-"+strconv.Itoa(w.wid)+"-"+strconv.Itoa(taskId)+"-"+strconv.Itoa(i)+".txt"
		// fname := mapPath+"map-out-"+strconv.Itoa(w.wid)+"-"+strconv.Itoa(i)+".txt"
        file, err := os.Create(fname)
        if err != nil {
            // 如果创建失败，关闭已经创建的所有文件
            for j := 0; j < i; j++ {
                ofiles[j].Close()
            }
			fmt.Printf("creat map outputfile fail")
            return false
        }
        ofiles[i] = file
    }

	for i:=0;i<len(intermediate);i++{
		index := ihash(intermediate[i].Key) % r
		fmt.Fprintf(ofiles[index], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	}

	for i:=0;i<r;i++{
		ofiles[i].Close()
	}

	return true
}



func (w *WorkerStruct) ProcessReduceTask(taskId int,worker []int) bool{
	kva := []KeyValue{}
	for i,wid := range worker {
		filename := mapPath+"map-out-"+strconv.Itoa(wid)+"-"+strconv.Itoa(i)+"-"+strconv.Itoa(taskId)+".txt"
		file,err := os.Open(filename)
		if err!=nil {
			fmt.Println("open file fail",filename)
			return false
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, " ")
			if len(parts)!=2 {
				continue
			}
			kva = append(kva,KeyValue{Key:parts[0],Value:parts[1]})
		}
	}
	// fmt.Println("len of kva",len(kva))

	sort.Sort(ByKey(kva))

	// oname := reducePath+"mr-out-"+strconv.Itoa(taskId)
	oname := "./mr-out-"+strconv.Itoa(taskId)
	// fmt.Println("oouputfile ",oname)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Println("file open fail",err)
		return false
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()



	return true
}

func TaskReport(taskType int, taskId int,wid int){
	args := TaskReportArgs{}
	args.TaskType = taskType
	args.TaskId = taskId
	args.Wid = wid
	reply := TaskReportReply{}

	call("Coordinator.TaskReport", &args, &reply)
}


// func GetTaskData(TaskId int,filepath string)bool{
// 	args := TaskDataArgs{}
// 	args.TaskId = TaskId
// 	reply := TaskDataReply{}
// 	ok := call("Coordinator.GetTaskData",&args, &reply)
// 	if ok {
// 		fmt.Printf("get data success\n")
// 		// err := createDirectory(filepath)
// 		// 	if err != nil{
// 		// 		return false
// 		// 	}
// 		// if writefile(taskPath+"task-"+strconv.Itoa(TaskId)+".txt", reply.Data){
// 		// 	fmt.Printf("save data success\n")
// 		// }else {
// 		// 	fmt.Printf("save data fail\n")
// 		// 	return false
// 		// }
// 	} else {
// 		fmt.Printf("get data fail!\n")
// 		return false
// 	}
// 	return true
// } 



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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
