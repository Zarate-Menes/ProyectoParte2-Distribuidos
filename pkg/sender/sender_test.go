package sender

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/pkg/dto"
	"node_messager/pkg/hub"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
)

func newLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

// startHub creates a real TCP listener backed by a hub. Returns the node and a
// store that accumulates every message received by the hub.
func startHub(t *testing.T, id int, name string) (node.Node, *msgstore.Store) {
	t.Helper()
	store := msgstore.New(100)
	log := newLogger(t)
	h := hub.New(name, log, store)
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
	n := node.Node{ID: id, Name: name, Host: "127.0.0.1", Port: port}
	return n, store
}

func newTestPool(t *testing.T) *Pool {
	t.Helper()
	p := NewPool(newLogger(t))
	t.Cleanup(p.CloseAll)
	return p
}

func waitForStore(t *testing.T, store *msgstore.Store, want int, timeout time.Duration) []msgstore.Entry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, _ := store.Latest(100)
		if len(entries) >= want {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, _ := store.Latest(100)
	t.Fatalf("timeout: want %d messages in store, got %d", want, len(entries))
	return nil
}

func TestPool_Send_DeliversSingleMessage(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender", Host: "127.0.0.1", Port: 0}
	to, store := startHub(t, 1, "target")
	pool := newTestPool(t)

	if err := pool.Send(from, to, dto.TypePing, ""); err != nil {
		t.Fatal(err)
	}

	entries := waitForStore(t, store, 1, 2*time.Second)
	if entries[0].Msg.Type != dto.TypePing {
		t.Fatalf("want TypePing, got %q", entries[0].Msg.Type)
	}
}

func TestPool_SendJSON_MarshalsThenSends(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender"}
	to, store := startHub(t, 1, "target")
	pool := newTestPool(t)

	payload := dto.ProposePayload{RoundID: "round-xyz", Operation: "INSERT_TICKET", Data: "{}"}
	if err := pool.SendJSON(from, to, dto.TypePropose, payload); err != nil {
		t.Fatal(err)
	}

	entries := waitForStore(t, store, 1, 2*time.Second)
	if entries[0].Msg.Type != dto.TypePropose {
		t.Fatalf("want TypePropose, got %q", entries[0].Msg.Type)
	}
	var p dto.ProposePayload
	if err := json.Unmarshal([]byte(entries[0].Msg.Content), &p); err != nil {
		t.Fatalf("unmarshal content: %v", err)
	}
	if p.RoundID != "round-xyz" {
		t.Fatalf("want round-xyz, got %q", p.RoundID)
	}
}

func TestPool_BroadcastJSON_SendsToAllTargets(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender"}
	nodeA, storeA := startHub(t, 1, "nodeA")
	nodeB, storeB := startHub(t, 2, "nodeB")
	pool := newTestPool(t)

	errs := pool.BroadcastJSON(from, []node.Node{nodeA, nodeB}, dto.TypePing, struct{}{})
	if len(errs) > 0 {
		t.Fatalf("broadcast errors: %v", errs)
	}

	waitForStore(t, storeA, 1, 2*time.Second)
	waitForStore(t, storeB, 1, 2*time.Second)
}

func TestPool_Broadcast_ErrorForUnreachableNode(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender"}
	unreachable := node.Node{ID: 1, Name: "dead", Host: "127.0.0.1", Port: 19999}
	pool := newTestPool(t)

	errs := pool.Broadcast(from, []node.Node{unreachable}, dto.TypePing, "")
	if len(errs) == 0 {
		t.Fatal("expected error for unreachable node")
	}
}

func TestPool_Send_ReusesConnection(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender"}
	to, _ := startHub(t, 1, "target")
	pool := newTestPool(t)

	_ = pool.Send(from, to, dto.TypePing, "")
	_ = pool.Send(from, to, dto.TypePing, "")

	pool.mu.Lock()
	n := len(pool.conns)
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("want 1 connection in pool, got %d", n)
	}
}

func TestPool_CloseAll_MarksConnectionsClosed(t *testing.T) {
	from := node.Node{ID: 99, Name: "sender"}
	to, _ := startHub(t, 1, "target")
	pool := newTestPool(t)

	_ = pool.Send(from, to, dto.TypePing, "")
	pool.mu.Lock()
	client := pool.conns[to.ID]
	pool.mu.Unlock()

	pool.CloseAll()

	// give readLoop time to detect close
	time.Sleep(50 * time.Millisecond)
	if !client.IsClosed() {
		t.Fatal("connection should be closed after CloseAll")
	}
}
