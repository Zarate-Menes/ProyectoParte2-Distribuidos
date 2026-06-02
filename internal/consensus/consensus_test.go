package consensus

import (
	"context"
	"encoding/json"
	"net"
	"sync"
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

type minDisp struct {
	eng *Engine
}

func (d *minDisp) Dispatch(msg dto.Message) {
	switch msg.Type {
	case dto.TypePropose:
		d.eng.HandlePropose(msg)
	case dto.TypeVoteYes, dto.TypeVoteNo:
		d.eng.HandleVote(msg)
	case dto.TypeCommit:
		d.eng.HandleCommit(msg)
	}
}

type rig struct {
	n         node.Node
	state     *nodestate.State
	pool      *sender.Pool
	engine    *Engine
	mu        sync.Mutex
	committed []string
}

func buildRig(t *testing.T, self node.Node, all []node.Node, masterID int) *rig {
	t.Helper()
	log := newLogger(t)
	state := nodestate.New(self, all, masterID)
	pool := sender.NewPool(log)
	t.Cleanup(pool.CloseAll)

	r := &rig{n: self, state: state, pool: pool}
	handler := func(op, data string) error {
		r.mu.Lock()
		r.committed = append(r.committed, op)
		r.mu.Unlock()
		return nil
	}
	r.engine = New(self, state, pool, log, handler)
	return r
}

func wireRig(t *testing.T, r *rig) node.Node {
	t.Helper()
	store := msgstore.New(100)
	log := newLogger(t)
	h := hub.New(r.n.Name, log, store)
	disp := &minDisp{eng: r.engine}
	h.SetDispatcher(disp)
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
	return node.Node{ID: r.n.ID, Name: r.n.Name, Host: "127.0.0.1", Port: port}
}

func waitCommit(t *testing.T, r *rig, op string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, c := range r.committed {
			if c == op {
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for commit of op=%q", op)
}

func TestPropose_SingleNode_CommitsImmediately(t *testing.T) {
	self := node.Node{ID: 1, Name: "solo", Host: "127.0.0.1", Port: 5001}
	r := buildRig(t, self, []node.Node{self}, 1)
	// no peers — single node, self vote is enough
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.engine.Propose(ctx, "INSERT_TICKET", "{}"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.committed) == 0 || r.committed[0] != "INSERT_TICKET" {
		t.Fatalf("expected INSERT_TICKET committed, got %v", r.committed)
	}
}

func TestPropose_TwoNodes_QuorumReached(t *testing.T) {
	n1 := node.Node{ID: 1, Name: "node1"}
	n2 := node.Node{ID: 2, Name: "node2"}
	all := []node.Node{n1, n2}

	r1 := buildRig(t, n1, all, 1)
	r2 := buildRig(t, n2, all, 1)

	// wire real TCP listeners
	live1 := wireRig(t, r1)
	live2 := wireRig(t, r2)

	// update state with real ports
	r1.state = nodestate.New(live1, []node.Node{live1, live2}, 1)
	r2.state = nodestate.New(live2, []node.Node{live1, live2}, 1)
	r1.engine.state = r1.state
	r2.engine.state = r2.state

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r1.engine.Propose(ctx, "INSERT_INGENIERO", `{"id":1}`); err != nil {
		t.Fatalf("propose: %v", err)
	}
	waitCommit(t, r1, "INSERT_INGENIERO", 3*time.Second)
}

func TestHandleVote_UnknownRoundIgnored(t *testing.T) {
	self := node.Node{ID: 1, Name: "n"}
	r := buildRig(t, self, []node.Node{self}, 1)
	msg := dto.Message{
		Type:    dto.TypeVoteYes,
		Content: `{"round_id":"nonexistent"}`,
	}
	// should not panic or deadlock
	done := make(chan struct{})
	go func() { r.engine.HandleVote(msg); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HandleVote blocked on unknown round")
	}
}

func TestHandlePropose_UnknownProposerIgnored(t *testing.T) {
	self := node.Node{ID: 1, Name: "n"}
	r := buildRig(t, self, []node.Node{self}, 1)
	p := dto.ProposePayload{RoundID: "r1", Operation: "OP", Data: "{}"}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypePropose, FromNode: "unknown", Content: string(data)}
	done := make(chan struct{})
	go func() { r.engine.HandlePropose(msg); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HandlePropose blocked on unknown proposer")
	}
}

func TestHandleCommit_CallsHandler(t *testing.T) {
	self := node.Node{ID: 1, Name: "n"}
	r := buildRig(t, self, []node.Node{self}, 1)
	p := dto.CommitPayload{RoundID: "r1", Operation: "CLOSE_TICKET", Data: `{"id_ticket":5}`}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeCommit, Content: string(data)}
	r.engine.HandleCommit(msg)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.committed) == 0 || r.committed[0] != "CLOSE_TICKET" {
		t.Fatalf("handler not called, committed=%v", r.committed)
	}
}

func TestPropose_ContextCancelled_ReturnsError(t *testing.T) {
	self := node.Node{ID: 1, Name: "n"}
	peer := node.Node{ID: 2, Name: "peer", Host: "127.0.0.1", Port: 19998} // not listening
	r := buildRig(t, self, []node.Node{self, peer}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err := r.engine.Propose(ctx, "OP", "{}")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}
