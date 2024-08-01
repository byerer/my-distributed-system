package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	KV     map[string]string
	PutMap sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.KV[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.KV[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.KV[args.Key]
	if ok {
		kv.KV[args.Key] = value + args.Value
	} else {
		kv.KV[args.Key] = args.Value
	}
	reply.Value = value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.KV = make(map[string]string)
	// You may need initialization code here.

	return kv
}
