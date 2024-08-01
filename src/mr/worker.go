package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		time.Sleep(2 * time.Second)
		GetTaskReq := GetTaskRequest{}
		GetTaskResp := GetTaskResponse{}
		ok := call("Coordinator.GetTask", &GetTaskReq, &GetTaskResp)
		if !ok || GetTaskResp.AllDone {
			break
		} else if GetTaskResp.Flag {
			continue
		}
		updateTimeReq := &UpdateTimeReq{
			Kind:     GetTaskResp.Kind,
			MapID:    GetTaskResp.MapID,
			ReduceID: GetTaskResp.ReduceID,
		}
		go GapCall("Coordinator.UpdateTime", updateTimeReq, &UpdateTimeResp{})
		switch GetTaskResp.Kind {
		case MAP:
			MapID := GetTaskResp.MapID
			file, err := os.Open(GetTaskResp.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", GetTaskResp.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", GetTaskResp.Filename)
			}
			file.Close()
			intermediate := []KeyValue{}
			kva := mapf(GetTaskResp.Filename, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			tempFiles := []*os.File{}
			for i := 0; i < GetTaskResp.NReduce; i++ {
				filename := fmt.Sprintf("tmp-mr-%v-%v", MapID, i)
				tempFile, err := os.CreateTemp("./", filename)
				if err != nil {
					log.Fatalf("cannot open %v: %v", filename, err)
				}
				tempFiles = append(tempFiles, tempFile)
			}
			for _, kv := range intermediate {
				encoder := json.NewEncoder(tempFiles[ihash(kv.Key)%GetTaskResp.NReduce])
				if err := encoder.Encode(&kv); err != nil {
					log.Fatalf("cannot encode %v: %v", kv, err)
				}
			}
			for i, tempFile := range tempFiles {
				if err := tempFile.Close(); err != nil {
					log.Fatalf("failed to close file: %v", err)
				}
				if err = os.Rename(tempFile.Name(), fmt.Sprintf("mr-%v-%v", MapID, i)); err != nil {
					log.Fatalf("cannot rename %v: %v", tempFile.Name(), err)
				}
			}
			finishTaskReq := FinishTaskRequest{
				Kind:  MAP,
				MapID: GetTaskResp.MapID,
			}
			call("Coordinator.FinishTask", &finishTaskReq, &FinishTaskResponse{})
		case REDUCE:
			intermediate := []KeyValue{}
			for i := 0; i < GetTaskResp.MapCnt; i++ {
				filename := fmt.Sprintf("mr-%v-%v", i, GetTaskResp.ReduceID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", GetTaskResp.Filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%v", GetTaskResp.ReduceID)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			finishTaskReq := &FinishTaskRequest{
				Kind:     REDUCE,
				ReduceID: GetTaskResp.ReduceID,
			}
			call("Coordinator.FinishTask", finishTaskReq, &FinishTaskResponse{})
		}
	}

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

func GapCall(rpcname string, args interface{}, reply interface{}) {
	for {
		time.Sleep(5 * time.Second)
		ok := call(rpcname, args, reply)
		if ok {
			break
		}
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
