package cli

import (
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/pkg/dto"
	"node_messager/pkg/hub"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
)

func testLog(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	return log.Sugar()
}

// startTestHub starts a hub on a random port backed by an in-memory store.
func startTestHub(t *testing.T, id int, name string) (*msgstore.Store, node.Node) {
	t.Helper()
	store := msgstore.New(100)
	log, _ := zap.NewDevelopment()
	h := hub.New(name, log.Sugar(), store)
	go h.Run()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

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
	return store, node.Node{ID: id, Name: name, Host: "127.0.0.1", Port: port}
}

// waitEntries polls store until at least want entries exist or timeout fires.
func waitEntries(t *testing.T, s *msgstore.Store, want int, timeout time.Duration) []msgstore.Entry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, _ := s.Latest(want + 10)
		if len(entries) >= want {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout: want %d entries in store", want)
	return nil
}

func TestSendMsg_SavesSentEntryInSenderStore(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	receiverStore, receiverNode := startTestHub(t, 2, "nodeB")
	senderNode := node.Node{ID: 1, Name: "nodeA"}
	senderStore := msgstore.New(100)
	stores := map[int]*msgstore.Store{
		senderNode.ID:   senderStore,
		receiverNode.ID: receiverStore,
	}

	if err := sendMsg(pool, senderNode, receiverNode, "hello nodeB", stores, testLog(t)); err != nil {
		t.Fatalf("sendMsg: %v", err)
	}

	entries := waitEntries(t, senderStore, 1, 2*time.Second)
	if entries[0].Type != msgstore.Sent {
		t.Errorf("type: want %q got %q", msgstore.Sent, entries[0].Type)
	}
	if entries[0].Msg.Content != "hello nodeB" {
		t.Errorf("content: want %q got %q", "hello nodeB", entries[0].Msg.Content)
	}
	if entries[0].Msg.FromNode != senderNode.Name {
		t.Errorf("from_node: want %q got %q", senderNode.Name, entries[0].Msg.FromNode)
	}
	if entries[0].Msg.ToNode != receiverNode.Name {
		t.Errorf("to_node: want %q got %q", receiverNode.Name, entries[0].Msg.ToNode)
	}
}

func TestSendMsg_SavesReceivedEntryInReceiverStore(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	receiverStore, receiverNode := startTestHub(t, 2, "nodeB")
	senderNode := node.Node{ID: 1, Name: "nodeA"}
	stores := map[int]*msgstore.Store{
		senderNode.ID:   msgstore.New(100),
		receiverNode.ID: receiverStore,
	}

	if err := sendMsg(pool, senderNode, receiverNode, "msg from A to B", stores, testLog(t)); err != nil {
		t.Fatalf("sendMsg: %v", err)
	}

	entries := waitEntries(t, receiverStore, 1, 2*time.Second)
	if entries[0].Type != msgstore.Received {
		t.Errorf("type: want %q got %q", msgstore.Received, entries[0].Type)
	}
	if entries[0].Msg.Content != "msg from A to B" {
		t.Errorf("content: want %q got %q", "msg from A to B", entries[0].Msg.Content)
	}
	if entries[0].Msg.FromNode != senderNode.Name {
		t.Errorf("from_node: want %q got %q", senderNode.Name, entries[0].Msg.FromNode)
	}
}

func TestSendMsg_SenderStoreHasOnlyOneSentEntry(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	receiverStore, receiverNode := startTestHub(t, 2, "nodeB")
	senderNode := node.Node{ID: 1, Name: "nodeA"}
	senderStore := msgstore.New(100)
	stores := map[int]*msgstore.Store{
		senderNode.ID:   senderStore,
		receiverNode.ID: receiverStore,
	}

	if err := sendMsg(pool, senderNode, receiverNode, "once", stores, testLog(t)); err != nil {
		t.Fatalf("sendMsg: %v", err)
	}

	// wait for receiver to process so we know the full picture
	waitEntries(t, receiverStore, 1, 2*time.Second)

	entries, _ := senderStore.Latest(10)
	if len(entries) != 1 {
		t.Errorf("sender store: want 1 entry, got %d", len(entries))
	}
}

func TestBroadcast_SavesSentEntryPerTargetInSenderStore(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	store1, node1 := startTestHub(t, 2, "nodeB")
	store2, node2 := startTestHub(t, 3, "nodeC")

	senderNode := node.Node{ID: 1, Name: "nodeA"}
	senderStore := msgstore.New(100)
	stores := map[int]*msgstore.Store{
		senderNode.ID: senderStore,
		node1.ID:      store1,
		node2.ID:      store2,
	}

	targets := []node.Node{node1, node2}
	if errs := broadcast(pool, senderNode, targets, "broadcast!", stores, testLog(t)); len(errs) > 0 {
		t.Fatalf("broadcast errors: %v", errs)
	}

	entries := waitEntries(t, senderStore, 2, 2*time.Second)
	for i, e := range entries {
		if e.Type != msgstore.Sent {
			t.Errorf("entry[%d] type: want %q got %q", i, msgstore.Sent, e.Type)
		}
		if e.Msg.Content != "broadcast!" {
			t.Errorf("entry[%d] content: want %q got %q", i, "broadcast!", e.Msg.Content)
		}
		if e.Msg.FromNode != senderNode.Name {
			t.Errorf("entry[%d] from_node: want %q got %q", i, senderNode.Name, e.Msg.FromNode)
		}
	}
}

func TestBroadcast_SavesReceivedEntryInEachReceiverStore(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	store1, node1 := startTestHub(t, 2, "nodeB")
	store2, node2 := startTestHub(t, 3, "nodeC")

	senderNode := node.Node{ID: 1, Name: "nodeA"}
	stores := map[int]*msgstore.Store{
		senderNode.ID: msgstore.New(100),
		node1.ID:      store1,
		node2.ID:      store2,
	}

	targets := []node.Node{node1, node2}
	if errs := broadcast(pool, senderNode, targets, "hello all", stores, testLog(t)); len(errs) > 0 {
		t.Fatalf("broadcast errors: %v", errs)
	}

	for _, tc := range []struct {
		name  string
		store *msgstore.Store
	}{
		{"nodeB", store1},
		{"nodeC", store2},
	} {
		entries := waitEntries(t, tc.store, 1, 2*time.Second)
		if entries[0].Type != msgstore.Received {
			t.Errorf("[%s] type: want %q got %q", tc.name, msgstore.Received, entries[0].Type)
		}
		if entries[0].Msg.Content != "hello all" {
			t.Errorf("[%s] content: want %q got %q", tc.name, "hello all", entries[0].Msg.Content)
		}
	}
}

func TestBroadcast_SenderStoreHasNoReceivedEntries(t *testing.T) {
	pool := newConnPool(testLog(t))
	defer pool.closeAll()

	store1, node1 := startTestHub(t, 2, "nodeB")
	store2, node2 := startTestHub(t, 3, "nodeC")

	senderNode := node.Node{ID: 1, Name: "nodeA"}
	senderStore := msgstore.New(100)
	stores := map[int]*msgstore.Store{
		senderNode.ID: senderStore,
		node1.ID:      store1,
		node2.ID:      store2,
	}

	targets := []node.Node{node1, node2}
	if errs := broadcast(pool, senderNode, targets, "no echo", stores, testLog(t)); len(errs) > 0 {
		t.Fatalf("broadcast errors: %v", errs)
	}

	// wait for receivers to confirm delivery, then verify sender has no Received entries
	waitEntries(t, store1, 1, 2*time.Second)
	waitEntries(t, store2, 1, 2*time.Second)

	entries, _ := senderStore.Latest(20)
	for _, e := range entries {
		if e.Type == msgstore.Received {
			t.Errorf("sender store must not have Received entries, found one: %+v", e)
		}
	}
}

// ── formatEntries tests (mirrors tui.formatEntries behaviour) ─────────────────

func saveMsg(t *testing.T, store *msgstore.Store, id, typ, from, to, content string, et msgstore.EntryType) {
	t.Helper()
	m := dto.Message{ID: id, Type: typ, FromNode: from, ToNode: to, Content: content}
	if err := store.Save(m, et); err != nil {
		t.Fatalf("save %s: %v", id, err)
	}
}

func TestFormatEntries_EmptyStore_ReturnsNoMessagesText(t *testing.T) {
	store := msgstore.New(100)
	entries, _ := store.Latest(50)
	result := formatEntries("nodeA", entries)
	if !strings.Contains(result, "No messages for nodeA yet.") {
		t.Errorf("want no-messages text, got: %q", result)
	}
}

func TestFormatEntries_ReceivedAppears(t *testing.T) {
	store := msgstore.New(100)
	saveMsg(t, store, "bc-1", "broadcast", "nodeA", "", "hello all", msgstore.Received)
	entries, _ := store.Latest(50)
	result := formatEntries("nodeA", entries)
	if !strings.Contains(result, "hello all") {
		t.Errorf("received message missing from output: %q", result)
	}
	if !strings.Contains(result, string(msgstore.Received)) {
		t.Errorf("entry type %q not in output: %q", msgstore.Received, result)
	}
}

func TestFormatEntries_SentAppears(t *testing.T) {
	store := msgstore.New(100)
	saveMsg(t, store, "dm-1", "direct", "nodeA", "nodeB", "private", msgstore.Sent)
	entries, _ := store.Latest(50)
	result := formatEntries("nodeA", entries)
	if !strings.Contains(result, "private") {
		t.Errorf("sent message missing from output: %q", result)
	}
}

func TestFormatEntries_ShowsBothSentAndReceived(t *testing.T) {
	store := msgstore.New(100)
	saveMsg(t, store, "s-1", "direct", "hostNode", "nodeB", "sent msg", msgstore.Sent)
	saveMsg(t, store, "r-1", "broadcast", "nodeB", "", "received msg", msgstore.Received)
	entries, _ := store.Latest(50)
	result := formatEntries("hostNode", entries)
	if !strings.Contains(result, "sent msg") {
		t.Errorf("sent message missing from output: %q", result)
	}
	if !strings.Contains(result, "received msg") {
		t.Errorf("received message missing from output: %q", result)
	}
}

func TestFormatEntries_HostMode_ShowsOnlyHostNodeMessages(t *testing.T) {
	hostStore := msgstore.New(100)
	remoteStore := msgstore.New(100)

	saveMsg(t, hostStore, "h-1", "direct", "host", "nodeB", "host sent", msgstore.Sent)
	saveMsg(t, remoteStore, "r-1", "broadcast", "nodeB", "", "remote only", msgstore.Received)

	hostEntries, _ := hostStore.Latest(50)
	result := formatEntries("host", hostEntries)

	if !strings.Contains(result, "host sent") {
		t.Errorf("host message missing: %q", result)
	}
	if strings.Contains(result, "remote only") {
		t.Errorf("remote message must not appear in host output: %q", result)
	}
}
