package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
OuterLoop:
	for {
		time.Sleep(250 * time.Millisecond)
		response, _ := GetMapTask()
		switch response.Msg {
		case "map":
			RunMapTask(response.MapFileName, mapf, response.NReduce)
		case "reduce":
			RunReduceTask(response.IntermediateFiles, reducef)
		case "wait":
			time.Sleep(1 * time.Second)
		default:
			break OuterLoop
		}
	}
}

func RunReduceTask(intermediateFiles []string, reducef func(string, []string) string) {
	intermediateFile := strings.Split(intermediateFiles[0], "-")
	fileIndex, _ := strconv.Atoi(intermediateFile[len(intermediateFile)-1])
	oname := fmt.Sprintf("mr-out-%v", fileIndex)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		kva := []KeyValue{}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

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
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}
	}

	respondWithReduceFile(fileIndex)
}

func RunMapTask(fileName string, mapf func(string, string) []KeyValue, NReduce int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	intermediate := mapf(fileName, string(content))

	sort.Sort(ByKey(intermediate))
	intermediateFiles := []string{}
	openFilePointers := map[int]*os.File{}
	for i := 0; i < NReduce; i++ {
		oname := fmt.Sprintf("mr-%s-%d", fileName, i)
		openFilePointers[i], _ = os.Create(oname)
		intermediateFiles = append(intermediateFiles, oname)
		defer openFilePointers[i].Close()
	}

	for _, kv := range intermediate {
		key := ihash(kv.Key) % NReduce
		enc := json.NewEncoder(openFilePointers[key])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write %v", kv)
		}
	}

	respondWithIntermediateFiles(intermediateFiles, fileName)
}

func respondWithIntermediateFiles(intermediate []string, filename string) {
	args := IntermediateTaskArg{}
	args.FileName = filename
	args.IntermediateFiles = intermediate
	reply := IntermediateTaskReply{}

	ok := call("Coordinator.IntermediateFiles", &args, &reply)
	if !ok {
		os.Exit(1)
		log.Fatalf("failed to respond with reduce file")
	}
}

func respondWithReduceFile(reduceIndex int) {
	args := ReduceTaskArg{}
	args.ReduceIndex = reduceIndex
	reply := ReduceTaskResponse{}

	ok := call("Coordinator.ReduceTask", &args, &reply)
	if !ok {
		os.Exit(1)
		log.Fatalf("failed to respond with reduce file")
	}
}

func GetMapTask() (GetTaskReply, error) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply, nil
	}
	return GetTaskReply{}, fmt.Errorf("call failed")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
