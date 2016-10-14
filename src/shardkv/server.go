package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"log"
	"time"
	"fmt"
	"bytes"
	"shardmaster"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action string
	Args interface{}
}

type OpReply struct {
	Action string
	Args interface{}
	Reply interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// as shard func is fixed, store k-v data per shard
	kvs [shardmaster.NShards]map[string]string
	// client requests, client id mapping to its max sequence
	cRequests map[int64]int64
	// client notify channel
	cNotifyChan map[int]chan OpReply
	mck       *shardmaster.Clerk
	config   shardmaster.Config

	// used as client information when sending install shards to other groups
	cid,seq int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Action:Get,Args:*args}

	reply.WrongLeader = true
	reply.Err = ""

	index,_,isLeader := kv.rf.Start(op)
	if isLeader {
		var channel chan OpReply
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan OpReply, 1)
		}
		channel = kv.cNotifyChan[index]
		kv.mu.Unlock()

		select{
		case rep := <- channel:
			if recArgs,ok := rep.Args.(GetArgs); ok{
				if recArgs.Cid == args.Cid && recArgs.Seq == args.Seq{
					*reply = rep.Reply.(GetReply)
					reply.WrongLeader = false
				}else{
					reply.Err = Error
				}
			}
		case <-time.After(200*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d:%d Get %v with reply %v",kv.gid,kv.me, *args, *reply))
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Action:args.Op,Args:*args}

	reply.WrongLeader = true
	reply.Err = ""

	index,_,isLeader := kv.rf.Start(op)
	if isLeader {
		var channel chan OpReply
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan OpReply, 1)
		}
		channel = kv.cNotifyChan[index]
		kv.mu.Unlock()

		select{
		case rep := <- channel:
			if recArgs,ok := rep.Args.(PutAppendArgs); ok{
				if recArgs.Cid == args.Cid && recArgs.Seq == args.Seq{
					reply.WrongLeader = false
					reply.Err = rep.Reply.(PutAppendReply).Err
				}else{
					reply.Err = Error
				}
			}
		case <-time.After(200*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d:%d PutAppend %v with reply %v",kv.gid,kv.me,*args, *reply))
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	op := Op{Action:Transfer,Args:*args}

	reply.WrongLeader = true
	reply.Err = ""

	index,_,isLeader := kv.rf.Start(op)
	if isLeader {
		var channel chan OpReply
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan OpReply, 1)
		}
		channel = kv.cNotifyChan[index]
		kv.mu.Unlock()

		select{
		case rep := <- channel:
			if recArgs,ok := rep.Args.(TransferShardArgs); ok{
				if recArgs.Cid == args.Cid && recArgs.Seq == args.Seq{
					*reply = rep.Reply.(TransferShardReply)
				}else{
					reply.Err = Error
				}
			}
		case <-time.After(200*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d:%d TransferShard %v with reply %v",kv.gid,kv.me,*args, *reply))
}

func (kv *ShardKV) ReConfig(args *ReConfigArgs, reply *ReConfigReply) {
	op := Op{Action:ReConfig,Args:*args}

	reply.WrongLeader = true
	reply.Err = ""

	index,_,isLeader := kv.rf.Start(op)
	if isLeader {
		var channel chan OpReply
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan OpReply, 1)
		}
		channel = kv.cNotifyChan[index]
		kv.mu.Unlock()

		select{
		case rep := <- channel:
			if recArgs,ok := rep.Args.(ReConfigArgs); ok{
				if recArgs.NewConfig.Num == args.NewConfig.Num{
					reply.Err = rep.Reply.(ReConfigReply).Err
					reply.WrongLeader = false
				}else{
					reply.Err = Error
				}
			}
		case <-time.After(200*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d:%d ReConfig %v with reply %v",kv.gid,kv.me, *args, *reply))
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	//gob.Register(OpReply{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(TransferShardArgs{})
	gob.Register(TransferShardReply{})
	gob.Register(ReConfigArgs{})
	gob.Register(ReConfigReply{})
	gob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	for i := range kv.kvs {
		kv.kvs[i] = make(map[string]string)
	}
	kv.cNotifyChan = make(map[int]chan OpReply)
	kv.cRequests = make(map[int64]int64)
	kv.mck = shardmaster.MakeClerk(masters)
	kv.cid = nrand()
	kv.seq = 0

	go kv.configDaemon()
	go kv.workerDaemon()

	return kv
}

func (kv *ShardKV) configDaemon(){
	for {
		if _, isLeader := kv.rf.GetState(); isLeader{
			cf := kv.mck.Query(-1)
			for i:=kv.config.Num+1;i<=cf.Num;i++{
				newConfig := kv.mck.Query(i)

				// get diff and prepare reconfig
				args,ok := kv.prepareReConfig(newConfig)
				if ok {
					if !kv.StartReConfig(args){
						break
					}
				}else{
					break
				}
			}
		}

		time.Sleep(100*time.Millisecond)
	}
}

func (kv *ShardKV) prepareReConfig(newConfig shardmaster.Config) (ReConfigArgs,bool){
	retOk := true
	reConfigArgs := ReConfigArgs{NewConfig:newConfig}
	for i :=0;i<shardmaster.NShards;i++{
		reConfigArgs.Kvs[i] = make(map[string]string)
	}
	reConfigArgs.ClientRequests = make(map[int64]int64)

	wanted := map[int][]int{}
	kv.mu.Lock()
	// new shards from other groups in newConfig
	for shard,gid := range newConfig.Shards{
		lastGid := kv.config.Shards[shard]
		if lastGid != 0 && lastGid != kv.gid && gid == kv.gid{
			_,ok := wanted[lastGid]
			if !ok {
				wanted[lastGid] = []int{shard}
			} else{
				wanted[lastGid] = append(wanted[lastGid],shard)
			}
		}
	}
	kv.mu.Unlock()

	// ask for data
	var mutex sync.Mutex
	var waitGroup sync.WaitGroup

	for gid,shards := range wanted {
		waitGroup.Add(1)

		go func(gid int, shards []int){
			defer waitGroup.Done()

			var args TransferShardArgs
			//kv.mu.Lock()
			//args.Cid = kv.cid
			//kv.seq++
			//args.Seq = kv.seq
			//kv.mu.Unlock()
			args.Gid = gid
			args.Shards = shards
			args.ConfigNum = newConfig.Num

			servers := kv.config.Groups[gid]
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])

				var reply TransferShardReply

				ok := srv.Call("ShardKV.TransferShard2", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					mutex.Lock()
					for shard,kvs := range reply.Transferred{
						for k,v := range kvs{
							reConfigArgs.Kvs[shard][k] = v
						}
					}
					for c,s := range reply.ClientRequests{
						_,ok := reConfigArgs.ClientRequests[c]
						if !ok{
							reConfigArgs.ClientRequests[c] = s
						} else{
							if reConfigArgs.ClientRequests[c] < s{
								reConfigArgs.ClientRequests[c] = s
							}
						}
					}
					mutex.Unlock()

					return
				}
			}

			mutex.Lock()
			retOk = false
			mutex.Unlock()
		}(gid,shards)
	}

	waitGroup.Wait()

	return reConfigArgs,retOk
}

func (kv *ShardKV) TransferShard2(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.ConfigNum{
		reply.Err = NotReady
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK

	reply.ClientRequests = make(map[int64]int64)
	reply.Transferred = [shardmaster.NShards]map[string]string{}
	for i := range reply.Transferred {
		reply.Transferred[i] = make(map[string]string)
	}

	for _,shard := range args.Shards{
		for k,v := range kv.kvs[shard]{
			reply.Transferred[shard][k] = v
		}
	}

	for c,s := range kv.cRequests{
		reply.ClientRequests[c] = s
	}
}

func (kv *ShardKV) StartReConfig(args ReConfigArgs) bool{
	args.Cid = kv.cid
	kv.mu.Lock()
	kv.seq++
	args.Seq = kv.seq
	kv.mu.Unlock()

	var reply ReConfigReply
	kv.ReConfig(&args, &reply)

	return reply.WrongLeader == false && reply.Err == OK
}

func (kv *ShardKV) workerDaemon(){
	for {
		select {
		case rep := <-kv.applyCh:
			if rep.UseSnapshot {
				var lastIndex int
				var lastTerm int

				r := bytes.NewBuffer(rep.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()

				d.Decode(&lastIndex)
				d.Decode(&lastTerm)
				kv.kvs = [shardmaster.NShards]map[string]string{}
				for i := range kv.kvs {
					kv.kvs[i] = make(map[string]string)
				}
				kv.cRequests = make(map[int64]int64)
				d.Decode(&kv.kvs)
				d.Decode(&kv.cRequests)
				d.Decode(&kv.config)

				kv.mu.Unlock()
			}else {
				msg, ok:= rep.Command.(Op)
				if ok{
					kv.doCommand(&msg, rep.Index)
					kv.snapshot(rep.Index)
				}
			}
		}
	}
}

func (kv *ShardKV) doCommand(msg *Op, index int){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opReply := &OpReply{Action:msg.Action,Args:msg.Args}

	if msg.Action == Get {
		kv.doGet(msg,opReply)
	}else if msg.Action == Put {
		kv.doPut(msg,opReply)
	}else if msg.Action == Append {
		kv.doAppend(msg,opReply)
	}else if msg.Action == ReConfig {
		kv.doReConfig(msg,opReply)
	}else if msg.Action == Transfer {
		kv.doTransfer(msg,opReply)
	}else{}

	// send msg to wake up client wait
	channel, ok := kv.cNotifyChan[index]
	if !ok {
		channel = make(chan OpReply, 1)
		kv.cNotifyChan[index] = channel
	}else{
		channel <- *opReply
	}
}

func (kv *ShardKV) doGet(msg *Op, reply *OpReply){
	response := GetReply{WrongLeader:true,Err:Error}

	if args,ok := msg.Args.(GetArgs);ok{
		shard := key2shard(args.Key)
		if kv.validShard(shard){
			if kv.validRequests(args.Cid,args.Seq){
				kv.cRequests[args.Cid] = args.Seq
			}

			response.WrongLeader = false
			v,b := kv.kvs[shard][args.Key]
			if b {
				response.Err = OK
				response.Value = v
			}else{
				response.Err = ErrNoKey
			}
		}else{
			response.Err = ErrWrongGroup
		}
	}

	reply.Reply = response
}

func (kv *ShardKV) doPut(msg *Op, reply *OpReply){
	response := PutAppendReply{WrongLeader:true,Err:Error}

	if args,ok := msg.Args.(PutAppendArgs);ok{
		shard := key2shard(args.Key)
		if kv.validShard(shard){
			if kv.validRequests(args.Cid,args.Seq){
				response.WrongLeader = false
				response.Err = OK
				kv.kvs[shard][args.Key] = args.Value

				kv.cRequests[args.Cid] = args.Seq
			}
		}else{
			response.Err = ErrWrongGroup
		}
	}

	reply.Reply = response
}

func (kv *ShardKV) doAppend(msg *Op, reply *OpReply){
	response := PutAppendReply{WrongLeader:true,Err:Error}

	if args,ok := msg.Args.(PutAppendArgs);ok{
		shard := key2shard(args.Key)
		if kv.validShard(shard){
			if kv.validRequests(args.Cid,args.Seq){
				response.WrongLeader = false
				response.Err = OK
				kv.kvs[shard][args.Key] += args.Value

				kv.cRequests[args.Cid] = args.Seq
			}
		}else{
			response.Err = ErrWrongGroup
		}
	}

	reply.Reply = response
}

func (kv *ShardKV) doReConfig(msg *Op, reply *OpReply){
	response := ReConfigReply{WrongLeader:true,Err:Error}

	if args,ok := msg.Args.(ReConfigArgs);ok{
		//if args.NewConfig.Num > kv.config.Num && kv.validRequests(args.Cid,args.Seq){
		if args.NewConfig.Num > kv.config.Num{
			response.WrongLeader = false
			response.Err = OK

			for shard,data := range args.Kvs{
				for k,v := range data{
					kv.kvs[shard][k] = v
				}
			}

			for c,s := range args.ClientRequests{
				if seq,ok := kv.cRequests[c];ok{
					if s > seq{
						kv.cRequests[c] = s
					}
				}else{
					kv.cRequests[c] = s
				}
			}

			kv.cRequests[args.Cid] = args.Seq

			kv.config = args.NewConfig
		}
	}

	reply.Reply = response
}

func (kv *ShardKV) doTransfer(msg *Op, reply *OpReply){
	response := TransferShardReply{WrongLeader:true,Err:Error}

	if args,ok := msg.Args.(TransferShardArgs);ok{
		if args.Gid != kv.gid{
			response.Err = ErrWrongGroup
		} else if kv.config.Num < args.ConfigNum{
			response.Err = NotReady
		} else{
			if kv.validRequests(args.Cid,args.Seq){
				response.WrongLeader = false
				response.Err = OK

				response.ClientRequests = make(map[int64]int64)
				response.Transferred = [shardmaster.NShards]map[string]string{}
				for i := range response.Transferred {
					response.Transferred[i] = make(map[string]string)
				}

				for _,shard := range args.Shards{
					for k,v := range kv.kvs[shard]{
						response.Transferred[shard][k] = v
					}
				}

				for c,s := range kv.cRequests{
					response.ClientRequests[c] = s
				}

				kv.cRequests[args.Cid] = args.Seq
			}
		}
	}

	reply.Reply = response
}

func (kv *ShardKV) snapshot(index int){
	kv.mu.Lock()
	// check if snapshot
	if kv.maxraftstate > 0 && kv.rf.GetRaftLogSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.kvs)
		e.Encode(kv.cRequests)
		e.Encode(kv.config)
		data := w.Bytes()
		go kv.rf.DoSnapshot(data, index)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) validShard(shard int) bool{
	if kv.config.Shards[shard] == kv.gid{
		return true
	}else{
		return false
	}
}

func (kv *ShardKV) validRequests(cid,seq int64) bool{
	return seq > kv.cRequests[cid]
}