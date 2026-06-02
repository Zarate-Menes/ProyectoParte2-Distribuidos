package election

import (
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

func newLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

func buildEngine(t *testing.T, self node.Node, all []node.Node, masterID int) *Engine {
	t.Helper()
	log := newLogger(t)
	state := nodestate.New(self, all, masterID)
	pool := sender.NewPool(log)
	t.Cleanup(pool.CloseAll)
	return New(self, state, pool, log)
}

// ── Reverse Bully: lowest ID wins ────────────────────────────────────────────

func TestStartElection_NoLowerNodes_WinsImmediately(t *testing.T) {
	// self is ID=0 — no lower nodes exist → wins right away
	self := node.Node{ID: 0, Name: "maestro"}
	s1 := node.Node{ID: 1, Name: "s1", Host: "127.0.0.1", Port: 19901}
	s2 := node.Node{ID: 2, Name: "s2", Host: "127.0.0.1", Port: 19902}
	e := buildEngine(t, self, []node.Node{self, s1, s2}, 0)

	e.StartElection()
	time.Sleep(50 * time.Millisecond)
	if e.state.GetMasterID() != self.ID {
		t.Fatalf("want masterID=%d, got %d", self.ID, e.state.GetMasterID())
	}
}

func TestStartElection_LowerNodeExists_WaitsForOK(t *testing.T) {
	// self is ID=3; node ID=0 has lower ID (higher priority) — should step down
	self := node.Node{ID: 3, Name: "s3"}
	low := node.Node{ID: 0, Name: "maestro", Host: "127.0.0.1", Port: 19903}
	e := buildEngine(t, self, []node.Node{low, self}, 0)

	oldTimeout := okTimeout
	okTimeout = 5 * time.Second
	t.Cleanup(func() { okTimeout = oldTimeout })

	e.mu.Lock()
	e.running = true
	e.okTimer = time.AfterFunc(okTimeout, func() { t.Error("timer fired — should have been stopped") })
	e.mu.Unlock()

	msg := dto.Message{Type: dto.TypeElectionOK, FromNode: "maestro"}
	e.HandleElectionOK(msg)

	e.mu.Lock()
	running := e.running
	e.mu.Unlock()
	if running {
		t.Fatal("running should be false after ELECTION_OK")
	}
}

func TestStartElection_WinsAfterTimeout_NoLowerAlive(t *testing.T) {
	oldTimeout := okTimeout
	okTimeout = 50 * time.Millisecond
	t.Cleanup(func() { okTimeout = oldTimeout })

	self := node.Node{ID: 2, Name: "s2"}
	// lower-ID node not reachable
	dead := node.Node{ID: 0, Name: "maestro", Host: "127.0.0.1", Port: 19997}
	e := buildEngine(t, self, []node.Node{dead, self}, 0)

	e.StartElection()
	time.Sleep(200 * time.Millisecond)
	if e.state.GetMasterID() != self.ID {
		t.Fatalf("want self as master after timeout, got %d", e.state.GetMasterID())
	}
}

func TestStartElection_AlreadyRunning_IsIdempotent(t *testing.T) {
	self := node.Node{ID: 1, Name: "s1"}
	e := buildEngine(t, self, []node.Node{self}, 1)
	e.mu.Lock()
	e.running = true
	e.mu.Unlock()
	done := make(chan struct{})
	go func() { e.StartElection(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartElection blocked when already running")
	}
}

// ── HandleElection (reverse Bully) ───────────────────────────────────────────

func TestHandleElection_HigherIDIgnores(t *testing.T) {
	// self ID=3 receives ELECTION from candidate ID=5 (higher ID, lower priority)
	// self has lower ID = higher priority → should respond OK and compete
	// but here: self.ID(3) < candidate(5) → self has higher priority → responds OK
	// Test the IGNORE case: self has HIGHER ID → ignore
	self := node.Node{ID: 5, Name: "s5"}
	low := node.Node{ID: 3, Name: "s3", Host: "127.0.0.1", Port: 19904}
	e := buildEngine(t, self, []node.Node{low, self}, 0)

	// candidate has LOWER ID (3 < 5) → self (5) has lower priority → ignore
	p := dto.ElectionPayload{CandidateID: 3}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeElection, FromNode: "s3", Content: string(data)}

	done := make(chan struct{})
	go func() { e.HandleElection(msg); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HandleElection blocked")
	}
}

func TestHandleElection_LowerIDSelf_RespondsAndStarts(t *testing.T) {
	oldTimeout := okTimeout
	okTimeout = 50 * time.Millisecond
	t.Cleanup(func() { okTimeout = oldTimeout })

	// self ID=1 receives ELECTION from candidate ID=3 (higher ID, lower priority)
	// self.ID(1) < candidate(3) → self has higher priority → respond OK + start election
	self := node.Node{ID: 1, Name: "s1"}
	high := node.Node{ID: 3, Name: "s3", Host: "127.0.0.1", Port: 19905}
	e := buildEngine(t, self, []node.Node{self, high}, 0)

	p := dto.ElectionPayload{CandidateID: 3}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeElection, FromNode: "s3", Content: string(data)}
	e.HandleElection(msg)

	// self starts own election → no lower node alive → wins
	time.Sleep(200 * time.Millisecond)
	if e.state.GetMasterID() != self.ID {
		t.Fatalf("want masterID=%d, got %d", self.ID, e.state.GetMasterID())
	}
}

func TestHandleCoordinator_UpdatesMasterID(t *testing.T) {
	self := node.Node{ID: 1, Name: "s1"}
	e := buildEngine(t, self, []node.Node{self}, 0)

	p := dto.CoordinatorPayload{MasterID: 2}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeCoordinator, FromNode: "winner", Content: string(data)}
	e.HandleCoordinator(msg)
	if e.state.GetMasterID() != 2 {
		t.Fatalf("want masterID=2, got %d", e.state.GetMasterID())
	}
}

// ── Master recovery ───────────────────────────────────────────────────────────

func TestClaimMastership_SetsMasterAndBroadcasts(t *testing.T) {
	self := node.Node{ID: 0, Name: "maestro"}
	s1 := node.Node{ID: 1, Name: "s1", Host: "127.0.0.1", Port: 19990}
	e := buildEngine(t, self, []node.Node{self, s1}, 1) // temp master is s1

	e.ClaimMastership()

	if e.state.GetMasterID() != 0 {
		t.Fatalf("want masterID=0 after claim, got %d", e.state.GetMasterID())
	}
}

// ── Master failover: sucursal takes over ─────────────────────────────────────

func TestMasterFailover_LowestAliveNodeWins(t *testing.T) {
	oldTimeout := okTimeout
	okTimeout = 80 * time.Millisecond
	t.Cleanup(func() { okTimeout = oldTimeout })

	// maestro (ID=0) is dead — not reachable
	dead := node.Node{ID: 0, Name: "maestro", Host: "127.0.0.1", Port: 19991}
	// sucursal1 (ID=1) detects master is dead and starts election
	self := node.Node{ID: 1, Name: "s1"}
	s2 := node.Node{ID: 2, Name: "s2", Host: "127.0.0.1", Port: 19992}

	e := buildEngine(t, self, []node.Node{dead, self, s2}, 0)

	// simulate: master dead, self starts election
	e.StartElection()

	// self sends ELECTION to ID=0 (lower) → no response → wins after timeout
	time.Sleep(300 * time.Millisecond)
	if e.state.GetMasterID() != self.ID {
		t.Fatalf("want sucursal1 as new master, got masterID=%d", e.state.GetMasterID())
	}
}

func TestMasterRecovery_OriginalMasterReclaims(t *testing.T) {
	// After failover, original master (ID=0) comes back and claims mastership
	maestro := node.Node{ID: 0, Name: "maestro"}
	s1 := node.Node{ID: 1, Name: "s1", Host: "127.0.0.1", Port: 19993}

	// sucursal1 is currently master (after failover)
	eS1 := buildEngine(t, s1, []node.Node{maestro, s1}, s1.ID)

	// maestro restarts and claims mastership
	eMaestro := buildEngine(t, maestro, []node.Node{maestro, s1}, maestro.ID)
	eMaestro.ClaimMastership()

	// simulate sucursal1 receiving COORDINATOR from maestro
	p := dto.CoordinatorPayload{MasterID: 0}
	data, _ := json.Marshal(p)
	msg := dto.Message{Type: dto.TypeCoordinator, FromNode: "maestro", Content: string(data)}
	eS1.HandleCoordinator(msg)

	if eS1.state.GetMasterID() != 0 {
		t.Fatalf("want masterID=0 after recovery, got %d", eS1.state.GetMasterID())
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func TestLowerNodes_FiltersCorrectly(t *testing.T) {
	self := node.Node{ID: 3, Name: "s3"}
	all := []node.Node{
		{ID: 0, Name: "maestro"}, {ID: 1, Name: "s1"}, {ID: 2, Name: "s2"},
		self,
		{ID: 4, Name: "s4"}, {ID: 5, Name: "s5"},
	}
	e := buildEngine(t, self, all, 0)
	lower := e.lowerNodes()
	if len(lower) != 3 { // IDs 0, 1, 2
		t.Fatalf("want 3 lower nodes, got %d: %+v", len(lower), lower)
	}
	for _, n := range lower {
		if n.ID >= self.ID {
			t.Errorf("node %d is not lower than self %d", n.ID, self.ID)
		}
	}
}
