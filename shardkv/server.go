package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"umich.edu/eecs491/proj4/paxos"
	"umich.edu/eecs491/proj4/paxosrsm"
)

const Debug = 0

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	l          net.Listener
	me         int
	term       chan interface{}
	killFunc   func()
	unreliable int32 // for testing
	rsm        *paxosrsm.PaxosRSM

	gid int64 // my replica group ID

	impl ShardKVImpl
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.killFunc()

	//kv.l.Close()
	//kv.rsm.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	select {
	case <-kv.term:
		return true
	default:
		return false
	}
}

func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// me is the index of this server in servers[].
func StartServer(gid int64, servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.term = make(chan interface{})
	kv.killFunc = sync.OnceFunc(func() {
		close(kv.term)
		if kv.l != nil {
			kv.l.Close()
		}
		kv.rsm.Kill()
	})
	kv.gid = gid
	kv.InitImpl()

	rpcs := rpc.NewServer()
	err := rpcs.Register(kv)
	if err != nil {
		log.Fatalf("RPC registration error: %v", err)
	}
	px := paxos.Make(servers, me, rpcs)
	kv.rsm = paxosrsm.MakeRSM(me, px, kv.ApplyOp, equals)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	//log.Printf("Listening on %s", servers[me])
	kv.l = l

	go func() {
		for !kv.isdead() {
			conn, err := kv.l.Accept()
			if err == nil && !kv.isdead() {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !kv.isdead() {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
