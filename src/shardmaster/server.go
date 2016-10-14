package shardmaster

import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"fmt"
	"time"
	"log"
	"runtime/debug"
	"os"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	oldRequests map[int64]int64 // preserve client request
	res map[int]chan OpReply // per client request channel

	configs []Config // indexed by config num
}


type Op struct {
	Cid int64
	Seq int64
	Action string
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int
}

type OpReply struct {
	Cid int64
	Seq int64
	Conf Config
	WrongLeader bool
	Err         Err
}

func (sm *ShardMaster) lastConf() *Config{
	return &sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) newConfig() *Config{
	lastConfig := sm.lastConf()

	var config Config

	config.Num = len(sm.configs)
	config.Shards = [NShards]int{}
	config.Groups = make(map[int][]string)

	for k,v := range lastConfig.Groups{
		config.Groups[k] = v
	}

	for i := range config.Shards{
		config.Shards[i] = lastConfig.Shards[i]
	}

	return &config
}

func (sm *ShardMaster) logToRaft(arg *Op, reply *OpReply){
	op := Op{Action:arg.Action,
		Cid:arg.Cid,
		Seq:arg.Seq,
		Servers:arg.Servers,
		GIDs:arg.GIDs,
		Shard:arg.Shard,
		GID:arg.GID,
		Num:arg.Num}

	reply.WrongLeader = true
	reply.Err = ""
	reply.Seq = op.Seq
	reply.Cid = op.Cid

	index,_,isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		_,ok := sm.res[index]
		if !ok{
			sm.res[index] = make(chan OpReply, 1)
		}
		sm.mu.Unlock()

		select{
		case rep := <- sm.res[index]:
			if rep.Cid == op.Cid && rep.Seq == op.Seq{
				reply.WrongLeader = false
				reply.Conf = rep.Conf
			}else{
				reply.Err = Error
			}
		case <-time.After(time.Duration(100)*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	sm.mu.Lock()
	delete(sm.res, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Action:"Join",Cid:args.Cid,Seq:args.Seq,Servers:args.Servers}

	var opReply OpReply
	sm.logToRaft(&op, &opReply)

	reply.WrongLeader = opReply.WrongLeader
	reply.Err = ""

	DPrintf(fmt.Sprintf("Server %d Join %v with reply %v",sm.me, *args, *reply))
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Action:"Leave",Cid:args.Cid,Seq:args.Seq,GIDs:args.GIDs}

	var opReply OpReply
	sm.logToRaft(&op, &opReply)

	reply.WrongLeader = opReply.WrongLeader
	reply.Err = ""

	DPrintf(fmt.Sprintf("Server %d Leave %v with reply %v",sm.me, *args, *reply))
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Action:"Move",Cid:args.Cid,Seq:args.Seq,Shard:args.Shard,GID:args.GID}

	var opReply OpReply
	sm.logToRaft(&op, &opReply)

	reply.WrongLeader = opReply.WrongLeader
	reply.Err = ""

	DPrintf(fmt.Sprintf("Server %d Move %v with reply %v",sm.me, *args, *reply))
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Action:"Query",Cid:args.Cid,Seq:args.Seq,Num:args.Num}

	var opReply OpReply
	sm.logToRaft(&op, &opReply)

	reply.WrongLeader = opReply.WrongLeader
	reply.Err = ""
	reply.Config = opReply.Conf

	DPrintf(fmt.Sprintf("Server %d Query %v with reply %v",sm.me, *args, *reply))
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0
	sm.configs[0].Shards = [NShards]int{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.res = make(map[int]chan OpReply)
	sm.oldRequests = make(map[int64]int64)
	go func(){
		for {
			select {
			case rep := <-sm.applyCh:
				msg, ok:= rep.Command.(Op)
				if ok{
					sm.execute(&msg, rep.Index)
				}
			}
		}
	}()

	return sm
}

func (sm *ShardMaster) execute(msg *Op, index int){
	sm.mu.Lock()

	opReply := &OpReply{Cid:msg.Cid,Seq:msg.Seq,WrongLeader:true}

	// execute command
	if msg.Seq > sm.oldRequests[msg.Cid]{
		switch msg.Action{
		case "Join":
			opReply.WrongLeader = false
			sm.doJoin(msg)
		case "Leave":
			opReply.WrongLeader = false
			sm.doLeave(msg)
		case "Move":
			opReply.WrongLeader = false
			sm.doMove(msg)
		case "Query":
			opReply.WrongLeader = false
			sm.doQuery(msg, opReply)
		default:
			break;
		}

		sm.oldRequests[msg.Cid] = msg.Seq
	}else{
		if msg.Action == "Query"{
			if msg.Num == -1 || msg.Num >= len(sm.configs){
				opReply.Conf = sm.configs[len(sm.configs)-1]
			}else{
				opReply.Conf = sm.configs[msg.Num]
			}
		}
	}

	// send msg to wake up client wait
	channel, ok := sm.res[index]
	if !ok {
		channel = make(chan OpReply, 1)
		sm.res[index] = channel
	}else{
		channel <- *opReply
	}

	sm.mu.Unlock()
}

func (sm *ShardMaster) shardsCount(config *Config) map[int][]int{
	shardsCount := map[int][]int{}
	for k := range config.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range config.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}

func (sm *ShardMaster) gidOfMostShards(shardsCount map[int][]int) int{
	max := -1
	gid := 0

	for g, shards := range shardsCount{
		if max < len(shards){
			max = len(shards)
			gid = g
		}
	}

	return gid
}

func (sm *ShardMaster) gidOfLeastShards(shardsCount map[int][]int) int{
	min := -1
	gid := 0

	for g, shards := range shardsCount{
		if min == -1 || min > len(shards){
			min = len(shards)
			gid = g
		}
	}

	return gid
}

func (sm *ShardMaster) rebalance(config *Config, action string, gid int){
	shardsCount := sm.shardsCount(config)
	switch action {
	case "join":
		mean := NShards / len(config.Groups)
		for i:=0;i<mean;i++{
			maxGid := sm.gidOfMostShards(shardsCount)
			if len(shardsCount[maxGid]) == 0 {
				log.Println("ReBalanceShards: max gid does not have shards")
				debug.PrintStack()
				os.Exit(-1)
			}
			config.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case "leave":
		delShards := shardsCount[gid]
		delete(shardsCount, gid)
		for _,shard := range delShards{
			minGid := sm.gidOfLeastShards(shardsCount)
			config.Shards[shard] = minGid
			shardsCount[minGid] = append(shardsCount[minGid],shard)
		}
	}
}

func (sm *ShardMaster) doJoin(msg *Op){
	config := sm.newConfig()

	for gid,servers := range msg.Servers{
		if _,exist := config.Groups[gid];!exist{
			config.Groups[gid] = servers
			sm.rebalance(config, "join", gid)
		}
	}

	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) doLeave(msg *Op){
	config := sm.newConfig()

	for _,gid:= range msg.GIDs {
		if _,exist := config.Groups[gid];exist{
			delete(config.Groups, gid)
			sm.rebalance(config, "leave", gid)
		}
	}

	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) doMove(msg *Op){
	config := sm.newConfig()

	config.Shards[msg.Shard] = msg.GID

	sm.configs = append(sm.configs, *config)
}

func (sm *ShardMaster) doQuery(msg *Op, reply *OpReply){
	if msg.Num == -1 || msg.Num >= len(sm.configs){
		reply.Conf = sm.configs[len(sm.configs)-1]
	}else{
		reply.Conf = sm.configs[msg.Num]
	}
}