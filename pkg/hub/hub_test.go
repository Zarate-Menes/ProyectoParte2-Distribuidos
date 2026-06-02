package hub

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/pkg/dto"
	"node_messager/pkg/msgstore"
)

func newTestHub(t *testing.T) (*Hub, *msgstore.Store, string, string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "hub-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()

	store, err := msgstore.NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	log, _ := zap.NewDevelopment()
	h := New("test", log.Sugar(), store)
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

	return h, store, path, ln.Addr().String()
}

func connectTCP(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func sendJSON(t *testing.T, conn net.Conn, m dto.Message) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "%s\n", data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func waitForFileEntries(t *testing.T, path string, want int, timeout time.Duration) []msgstore.Entry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries := readEntries(t, path)
		if len(entries) >= want {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d entries in file", want)
	return nil
}

func readEntries(t *testing.T, path string) []msgstore.Entry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close() //nolint:errcheck

	var entries []msgstore.Entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e msgstore.Entry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		entries = append(entries, e)
	}
	return entries
}

func TestHub_BroadcastMessageSavedToFile(t *testing.T) {
	_, _, path, addr := newTestHub(t)

	conn := connectTCP(t, addr)
	defer conn.Close() //nolint:errcheck

	m := dto.Message{
		ID:       "bc-1",
		Type:     "broadcast",
		FromNode: "nodeA",
		ToNode:   "",
		Content:  "hello all",
	}
	sendJSON(t, conn, m)

	entries := waitForFileEntries(t, path, 1, 2*time.Second)
	if entries[0].Msg.ID != m.ID {
		t.Errorf("want id %q got %q", m.ID, entries[0].Msg.ID)
	}
	if entries[0].Type != msgstore.Received {
		t.Errorf("want type %q got %q", msgstore.Received, entries[0].Type)
	}
}

func TestHub_DirectMessageSavedToFile(t *testing.T) {
	_, _, path, addr := newTestHub(t)

	conn := connectTCP(t, addr)
	defer conn.Close() //nolint:errcheck

	m := dto.Message{
		ID:       "dm-1",
		Type:     "direct",
		FromNode: "nodeA",
		ToNode:   "nodeB",
		Content:  "private message",
	}
	sendJSON(t, conn, m)

	entries := waitForFileEntries(t, path, 1, 2*time.Second)
	if entries[0].Msg.ID != m.ID {
		t.Errorf("want id %q got %q", m.ID, entries[0].Msg.ID)
	}
	if entries[0].Msg.ToNode != m.ToNode {
		t.Errorf("want to_node %q got %q", m.ToNode, entries[0].Msg.ToNode)
	}
}

func recvJSON(t *testing.T, conn net.Conn, timeout time.Duration) dto.Message {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatalf("no message received: %v", scanner.Err())
	}
	var m dto.Message
	if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func TestHub_NoFanOut(t *testing.T) {
	_, _, _, addr := newTestHub(t)

	sender := connectTCP(t, addr)
	defer sender.Close() //nolint:errcheck
	receiver := connectTCP(t, addr)
	defer receiver.Close() //nolint:errcheck

	time.Sleep(20 * time.Millisecond)

	m := dto.Message{
		ID:       "no-fanout-1",
		Type:     "direct",
		FromNode: "nodeA",
		ToNode:   "nodeB",
		Content:  "hello",
	}
	sendJSON(t, sender, m)

	_ = receiver.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	scanner := bufio.NewScanner(receiver)
	if scanner.Scan() {
		t.Fatalf("hub should NOT fan-out messages to other clients, but got: %s", scanner.Text())
	}
}

func TestHub_ConnectionStableUnderLoad(t *testing.T) {
	_, _, path, addr := newTestHub(t)

	conn := connectTCP(t, addr)
	defer conn.Close() //nolint:errcheck

	time.Sleep(20 * time.Millisecond)

	const total = 500
	for i := 0; i < total; i++ {
		m := dto.Message{
			ID:       fmt.Sprintf("load-%d", i),
			Type:     "PING",
			FromNode: "nodeA",
			Content:  "",
		}
		sendJSON(t, conn, m)
	}

	entries := waitForFileEntries(t, path, total, 5*time.Second)
	if len(entries) < total {
		t.Errorf("expected %d stored messages, got %d — connection was dropped", total, len(entries))
	}
}

func TestHub_MultipleMixedMessagesSavedToFile(t *testing.T) {
	_, _, path, addr := newTestHub(t)

	conn := connectTCP(t, addr)
	defer conn.Close() //nolint:errcheck

	messages := []dto.Message{
		{ID: "1", Type: "broadcast", FromNode: "nodeA", Content: "hi all"},
		{ID: "2", Type: "direct", FromNode: "nodeA", ToNode: "nodeB", Content: "hey B"},
		{ID: "3", Type: "broadcast", FromNode: "nodeB", Content: "yo"},
	}
	for _, m := range messages {
		sendJSON(t, conn, m)
	}

	entries := waitForFileEntries(t, path, len(messages), 2*time.Second)
	ids := make(map[string]bool)
	for _, e := range entries {
		ids[e.Msg.ID] = true
	}
	for _, m := range messages {
		if !ids[m.ID] {
			t.Errorf("message id %q not found in file", m.ID)
		}
	}
}
