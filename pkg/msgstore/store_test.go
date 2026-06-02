package msgstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"node_messager/pkg/dto"
)

func msg(id, t, from, to, content string) dto.Message {
	return dto.Message{ID: id, Type: t, FromNode: from, ToNode: to, Content: content}
}

func TestSave_WritesReceivedEntryToFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "store-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	m := msg("1", "broadcast", "nodeA", "", "hello everyone")
	if err := store.Save(m, Received); err != nil {
		t.Fatal(err)
	}

	entries := readFileEntries(t, path)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].Msg.ID != m.ID {
		t.Errorf("msg id: want %q got %q", m.ID, entries[0].Msg.ID)
	}
	if entries[0].Type != Received {
		t.Errorf("entry type: want %q got %q", Received, entries[0].Type)
	}
}

func TestSave_WritesSentEntryToFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "store-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	m := msg("2", "direct", "nodeA", "nodeB", "private msg")
	if err := store.Save(m, Sent); err != nil {
		t.Fatal(err)
	}

	entries := readFileEntries(t, path)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].Msg.ID != m.ID {
		t.Errorf("msg id: want %q got %q", m.ID, entries[0].Msg.ID)
	}
	if entries[0].Type != Sent {
		t.Errorf("entry type: want %q got %q", Sent, entries[0].Type)
	}
}

func TestSave_MultipleMessagesAllWrittenToFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "store-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	messages := []dto.Message{
		msg("10", "broadcast", "nodeA", "", "msg one"),
		msg("11", "direct", "nodeB", "nodeC", "msg two"),
		msg("12", "broadcast", "nodeA", "", "msg three"),
	}
	for _, m := range messages {
		if err := store.Save(m, Received); err != nil {
			t.Fatalf("save %s: %v", m.ID, err)
		}
	}

	entries := readFileEntries(t, path)
	if len(entries) != len(messages) {
		t.Fatalf("want %d entries, got %d", len(messages), len(entries))
	}
	for i, e := range entries {
		if e.Msg.ID != messages[i].ID {
			t.Errorf("entry[%d] id: want %q got %q", i, messages[i].ID, e.Msg.ID)
		}
	}
}

func TestLatest_AfterSave_ReturnsAllEntries(t *testing.T) {
	store := New(100)

	msgs := []dto.Message{
		msg("1", "broadcast", "nodeA", "", "hi all"),
		msg("2", "direct", "nodeA", "nodeB", "hey B"),
	}
	for _, m := range msgs {
		if err := store.Save(m, Received); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := store.Latest(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(msgs) {
		t.Fatalf("want %d entries, got %d", len(msgs), len(entries))
	}
	for i, e := range entries {
		if e.Msg.ID != msgs[i].ID {
			t.Errorf("entry[%d] id: want %q got %q", i, msgs[i].ID, e.Msg.ID)
		}
	}
}

func TestLatest_LimitsToN(t *testing.T) {
	store := New(100)

	for i := range 5 {
		m := msg(fmt.Sprintf("%d", i), "broadcast", "nodeA", "", "content")
		store.Save(m, Received) //nolint:errcheck
	}

	entries, err := store.Latest(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	if entries[0].Msg.ID != "2" {
		t.Errorf("oldest of last 3: want id %q got %q", "2", entries[0].Msg.ID)
	}
	if entries[2].Msg.ID != "4" {
		t.Errorf("newest: want id %q got %q", "4", entries[2].Msg.ID)
	}
}

func TestLatest_SaveToFile_ThenLatest_ReturnsEntries(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "store-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	m := msg("99", "broadcast", "nodeA", "", "persist and read back")
	if err := store.Save(m, Received); err != nil {
		t.Fatal(err)
	}

	entries, err := store.Latest(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].Msg.ID != m.ID {
		t.Errorf("want id %q got %q", m.ID, entries[0].Msg.ID)
	}
	if entries[0].Msg.Content != m.Content {
		t.Errorf("want content %q got %q", m.Content, entries[0].Msg.Content)
	}
}

func TestNewWithFile_LoadsExistingEntries_AfterRestart(t *testing.T) {
	path := fmt.Sprintf("%s/node.jsonl", t.TempDir())

	// first "run": save two messages
	store1, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}
	store1.Save(msg("1", "broadcast", "nodeA", "", "first run msg"), Received)  //nolint:errcheck
	store1.Save(msg("2", "direct", "nodeA", "nodeB", "another msg"), Sent)      //nolint:errcheck

	// second "run": new store on same file (simulates restart)
	store2, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := store2.Latest(50)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("after restart want 2 entries, got %d", len(entries))
	}
	if entries[0].Msg.ID != "1" {
		t.Errorf("entry[0] id: want %q got %q", "1", entries[0].Msg.ID)
	}
	if entries[1].Msg.ID != "2" {
		t.Errorf("entry[1] id: want %q got %q", "2", entries[1].Msg.ID)
	}
}

func TestNewWithFile_LoadsExistingEntries_ThenContinuesSaving(t *testing.T) {
	path := fmt.Sprintf("%s/node.jsonl", t.TempDir())

	store1, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}
	store1.Save(msg("old-1", "broadcast", "nodeA", "", "old msg"), Received) //nolint:errcheck

	store2, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}
	store2.Save(msg("new-1", "direct", "nodeA", "nodeB", "new msg"), Sent) //nolint:errcheck

	entries, err := store2.Latest(50)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries (old+new), got %d", len(entries))
	}

	fileEntries := readFileEntries(t, path)
	if len(fileEntries) != 2 {
		t.Fatalf("want 2 lines in file, got %d", len(fileEntries))
	}
}

func TestNewWithFile_RespectsMax_WhenLoadingFromFile(t *testing.T) {
	path := fmt.Sprintf("%s/node.jsonl", t.TempDir())

	// write 5 entries directly to file
	store1, _ := NewWithFile(100, path)
	for i := range 5 {
		store1.Save(msg(fmt.Sprintf("%d", i), "broadcast", "nodeA", "", "x"), Received) //nolint:errcheck
	}

	// reload with max=3
	store2, err := NewWithFile(3, path)
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := store2.Latest(50)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries (max), got %d", len(entries))
	}
	if entries[0].Msg.ID != "2" {
		t.Errorf("oldest kept: want id %q got %q", "2", entries[0].Msg.ID)
	}
}

func TestNewWithFile_CreatesFile_WhenNotExists(t *testing.T) {
	path := fmt.Sprintf("%s/new-node.jsonl", t.TempDir())

	// file must not exist before
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file to not exist before test")
	}

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatalf("NewWithFile: %v", err)
	}
	_ = store

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("file was not created by NewWithFile")
	}
}

func TestNewWithFile_NoMessages_LatestReturnsEmpty(t *testing.T) {
	path := fmt.Sprintf("%s/empty-node.jsonl", t.TempDir())

	store, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	entries, err := store.Latest(50)
	if err != nil {
		t.Fatalf("Latest on empty store: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("want 0 entries, got %d", len(entries))
	}
}

func TestNewWithFile_NoMessages_FileExistsAndIsEmpty(t *testing.T) {
	path := fmt.Sprintf("%s/virgin-node.jsonl", t.TempDir())

	_, err := NewWithFile(100, path)
	if err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("want empty file (0 bytes), got %d bytes", info.Size())
	}
}

func readFileEntries(t *testing.T, path string) []Entry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck

	var entries []Entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("unmarshal line: %v", err)
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}
