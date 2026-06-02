package heartbeat

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/election"
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

type hbDisp struct{ mon *Monitor }

func (d *hbDisp) Dispatch(msg dto.Message) {
	switch msg.Type {
	case dto.TypePing:
		d.mon.HandlePing(msg)
	case dto.TypePong:
		d.mon.HandlePong(msg)
	}
}

func buildMonitor(t *testing.T, self node.Node, all []node.Node, masterID int) (*Monitor, *nodestate.State) {
	t.Helper()
	log := newLogger(t)
	state := nodestate.New(self, all, masterID)
	pool := sender.NewPool(log)
	t.Cleanup(pool.CloseAll)
	elec := election.New(self, state, pool, log)
	mon := New(self, state, pool, elec, log)
	return mon, state
}

func wireMonitor(t *testing.T, mon *Monitor, self node.Node) node.Node {
	t.Helper()
	store := msgstore.New(100)
	log := newLogger(t)
	h := hub.New(self.Name, log, store)
	h.SetDispatcher(&hbDisp{mon: mon})
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

func TestHandlePong_SignalsPongChannel(t *testing.T) {
	self := node.Node{ID: 1, Name: "n1"}
	peer := node.Node{ID: 2, Name: "n2"}
	mon, _ := buildMonitor(t, self, []node.Node{self, peer}, 1)

	ch := make(chan struct{}, 1)
	mon.mu.Lock()
	mon.pongChs[peer.ID] = ch
	mon.mu.Unlock()

	msg := dto.Message{Type: dto.TypePong, FromNode: peer.Name}
	mon.HandlePong(msg)

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("pong channel not signaled")
	}
}

func TestHandlePong_UnknownNode_NoOp(t *testing.T) {
	self := node.Node{ID: 1, Name: "n1"}
	mon, _ := buildMonitor(t, self, []node.Node{self}, 1)
	msg := dto.Message{Type: dto.TypePong, FromNode: "ghost"}
	done := make(chan struct{})
	go func() { mon.HandlePong(msg); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HandlePong blocked on unknown node")
	}
}

func TestDeclareDead_MarksDead(t *testing.T) {
	self := node.Node{ID: 1, Name: "n1"}
	peer := node.Node{ID: 2, Name: "n2"}
	mon, state := buildMonitor(t, self, []node.Node{self, peer}, 1)
	mon.declareDead(peer.ID)
	if state.IsAlive(peer.ID) {
		t.Fatal("node should be dead")
	}
}

func TestDeclareDead_AlreadyDead_NoDoubleAction(t *testing.T) {
	self := node.Node{ID: 1, Name: "n1"}
	peer := node.Node{ID: 2, Name: "n2"}
	mon, state := buildMonitor(t, self, []node.Node{self, peer}, 1)
	state.MarkDead(peer.ID) // already dead

	called := 0
	mon.OnNodeDead = func(id int) { called++ }
	mon.declareDead(peer.ID) // should be a no-op
	if called != 0 {
		t.Fatal("OnNodeDead should not be called for already-dead node")
	}
}

func TestDeclareDead_NonMasterPeerAndSelfIsMaster_CallsOnNodeDead(t *testing.T) {
	self := node.Node{ID: 1, Name: "master"}
	peer := node.Node{ID: 2, Name: "peer"}
	mon, _ := buildMonitor(t, self, []node.Node{self, peer}, self.ID)

	called := make(chan int, 1)
	mon.OnNodeDead = func(id int) { called <- id }

	mon.declareDead(peer.ID)
	select {
	case id := <-called:
		if id != peer.ID {
			t.Fatalf("want peer.ID=%d, got %d", peer.ID, id)
		}
	case <-time.After(time.Second):
		t.Fatal("OnNodeDead not called")
	}
}

func TestPingOne_MarksPeerAliveOnPong(t *testing.T) {
	oldPongWait := pongWait
	pongWait = 500 * time.Millisecond
	t.Cleanup(func() { pongWait = oldPongWait })

	n1 := node.Node{ID: 1, Name: "n1"}
	n2 := node.Node{ID: 2, Name: "n2"}

	mon1, state1 := buildMonitor(t, n1, []node.Node{n1, n2}, 1)
	mon2, _ := buildMonitor(t, n2, []node.Node{n1, n2}, 1)

	live1 := wireMonitor(t, mon1, n1)
	live2 := wireMonitor(t, mon2, n2)

	// update pools with real ports
	all := []node.Node{live1, live2}
	mon1.state = nodestate.New(live1, all, 1)
	mon2.state = nodestate.New(live2, all, 1)
	state1 = mon1.state

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mon1.pingOne(ctx, live2)
	if !state1.IsAlive(live2.ID) {
		t.Fatal("peer should be marked alive after pong")
	}
	mon1.mu.Lock()
	missed := mon1.missed[live2.ID]
	mon1.mu.Unlock()
	if missed != 0 {
		t.Fatalf("missed should be 0, got %d", missed)
	}
}

func TestPingOne_MarksMissedOnNoPong(t *testing.T) {
	oldPongWait := pongWait
	pongWait = 50 * time.Millisecond
	t.Cleanup(func() { pongWait = oldPongWait })

	self := node.Node{ID: 1, Name: "n1"}
	dead := node.Node{ID: 2, Name: "n2", Host: "127.0.0.1", Port: 19970} // no listener
	mon, _ := buildMonitor(t, self, []node.Node{self, dead}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mon.pingOne(ctx, dead)

	mon.mu.Lock()
	missed := mon.missed[dead.ID]
	mon.mu.Unlock()
	if missed != 1 {
		t.Fatalf("want missed=1, got %d", missed)
	}
}
