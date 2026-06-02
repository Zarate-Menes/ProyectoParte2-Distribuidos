package mutex

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/hub"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

func newLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

type mutexDisp struct{ eng *Engine }

func (d *mutexDisp) Dispatch(msg dto.Message) {
	switch msg.Type {
	case dto.TypeLockRequest:
		d.eng.HandleLockRequest(msg)
	case dto.TypeLockGrant:
		d.eng.HandleLockGrant(msg)
	case dto.TypeLockDeny:
		d.eng.HandleLockDeny(msg)
	case dto.TypeLockRelease:
		d.eng.HandleLockRelease(msg)
	}
}

func buildEngine(t *testing.T, self node.Node, all []node.Node, masterID int) *Engine {
	t.Helper()
	log := newLogger(t)
	state := nodestate.New(self, all, masterID)
	pool := sender.NewPool(log)
	t.Cleanup(pool.CloseAll)
	return New(self, state, pool, log)
}

func wireEngine(t *testing.T, e *Engine, self node.Node) node.Node {
	t.Helper()
	store := msgstore.New(100)
	log := newLogger(t)
	h := hub.New(self.Name, log, store)
	h.SetDispatcher(&mutexDisp{eng: e})
	go h.Run()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go h.Serve(conn)
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	return node.Node{ID: self.ID, Name: self.Name, Host: "127.0.0.1", Port: port}
}

func TestAcquireLocal_ImmediateGrant(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	e := buildEngine(t, self, []node.Node{self}, 1) // self is master
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release, err := e.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if release == nil {
		t.Fatal("expected non-nil release func")
	}
	e.mu.Lock()
	held := e.holder != ""
	e.mu.Unlock()
	if !held {
		t.Fatal("holder should be set after acquire")
	}
	release()
	e.mu.Lock()
	free := e.holder == ""
	e.mu.Unlock()
	if !free {
		t.Fatal("holder should be cleared after release")
	}
}

func TestAcquireLocal_SecondAcquireQueues(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	e := buildEngine(t, self, []node.Node{self}, 1)
	ctx := context.Background()

	release1, err := e.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan struct{})
	go func() {
		r, err := e.Acquire(ctx)
		if err != nil {
			return
		}
		close(acquired)
		r()
	}()

	// release first lock — second should unblock
	time.Sleep(50 * time.Millisecond)
	release1()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second acquire did not unblock after release")
	}
}

func TestAcquireLocal_ThreeSequentialLocks(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	e := buildEngine(t, self, []node.Node{self}, 1)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		r, err := e.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		r()
	}
}

func TestHandleLockRequest_GrantsWhenFree(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	requester := node.Node{ID: 2, Name: "req", Host: "127.0.0.1", Port: 19980}
	e := buildEngine(t, self, []node.Node{self, requester}, 1)

	p := dto.LockPayload{RequestID: "req-1", Resource: "engineer_assignment"}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeLockRequest, FromNode: "req", Content: string(data)}
	e.HandleLockRequest(msg)

	e.mu.Lock()
	held := e.holder == "req-1"
	e.mu.Unlock()
	if !held {
		t.Fatal("holder should be set to req-1")
	}
}

func TestHandleLockRequest_QueuesWhenHeld(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	requester := node.Node{ID: 2, Name: "req", Host: "127.0.0.1", Port: 19981}
	e := buildEngine(t, self, []node.Node{self, requester}, 1)

	// manually hold the lock
	e.mu.Lock()
	e.holder = "existing-holder"
	e.mu.Unlock()

	p := dto.LockPayload{RequestID: "req-2", Resource: "engineer_assignment"}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeLockRequest, FromNode: "req", Content: string(data)}
	e.HandleLockRequest(msg)

	e.mu.Lock()
	queued := len(e.queue)
	e.mu.Unlock()
	if queued != 1 {
		t.Fatalf("want 1 queued, got %d", queued)
	}
}

func TestHandleLockRelease_GrantsToNextInQueue(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	// next requester has no reachable port — just test internal state
	e := buildEngine(t, self, []node.Node{self}, 1)
	e.mu.Lock()
	e.holder = "holder-1"
	e.queue = []pendingReq{{requestID: "holder-2", fromNode: self.Name}}
	e.grants["holder-2"] = make(chan bool, 1)
	e.mu.Unlock()

	e.releaseLocal("holder-1")

	e.mu.Lock()
	newHolder := e.holder
	e.mu.Unlock()
	if newHolder != "holder-2" {
		t.Fatalf("want holder-2, got %q", newHolder)
	}
}

func TestAcquireRemote_NoMaster_ReturnsError(t *testing.T) {
	self := node.Node{ID: 2, Name: "peer"}
	// masterID=99 not in all list
	log := newLogger(t)
	state := nodestate.New(self, []node.Node{self}, 99)
	pool := sender.NewPool(log)
	t.Cleanup(pool.CloseAll)
	e := New(self, state, pool, log)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := e.acquireRemote(ctx)
	if err == nil {
		t.Fatal("expected error when no master available")
	}
}

func TestAcquireRemote_Timeout(t *testing.T) {
	oldTimeout := lockTimeout
	lockTimeout = 100 * time.Millisecond
	t.Cleanup(func() { lockTimeout = oldTimeout })

	master := node.Node{ID: 1, Name: "master", Host: "127.0.0.1", Port: 19982} // not listening
	self := node.Node{ID: 2, Name: "peer"}
	e := buildEngine(t, self, []node.Node{master, self}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := e.acquireRemote(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
