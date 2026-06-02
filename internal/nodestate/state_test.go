package nodestate

import (
	"sync"
	"testing"

	"node_messager/pkg/node"
)

func makeNodes() (node.Node, node.Node, node.Node) {
	a := node.Node{ID: 1, Name: "nodeA", Host: "127.0.0.1", Port: 5001}
	b := node.Node{ID: 2, Name: "nodeB", Host: "127.0.0.1", Port: 5002}
	c := node.Node{ID: 3, Name: "nodeC", Host: "127.0.0.1", Port: 5003}
	return a, b, c
}

func TestNew_AllNodesMarkedAlive(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	if s.AliveCount() != 3 {
		t.Fatalf("want 3 alive, got %d", s.AliveCount())
	}
	for _, id := range []int{1, 2, 3} {
		if !s.IsAlive(id) {
			t.Errorf("node %d should be alive", id)
		}
	}
}

func TestSelf_ReturnsSelf(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	if s.Self().ID != a.ID {
		t.Fatalf("want self.ID=%d, got %d", a.ID, s.Self().ID)
	}
}

func TestAll_ReturnsCopy(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	all := s.All()
	if len(all) != 3 {
		t.Fatalf("want 3, got %d", len(all))
	}
	// mutate returned slice — internal state must not change
	all[0].ID = 999
	if s.All()[0].ID == 999 {
		t.Fatal("All() returned internal slice reference instead of copy")
	}
}

func TestPeers_ExcludesSelf(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	peers := s.Peers()
	if len(peers) != 2 {
		t.Fatalf("want 2 peers, got %d", len(peers))
	}
	for _, p := range peers {
		if p.ID == a.ID {
			t.Error("self should not be in peers")
		}
	}
}

func TestAlivePeers_ExcludesDeadAndSelf(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	s.MarkDead(b.ID)
	alive := s.AlivePeers()
	if len(alive) != 1 || alive[0].ID != c.ID {
		t.Fatalf("want only nodeC alive, got %+v", alive)
	}
}

func TestAliveCount_DecrementsOnMarkDead(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	s.MarkDead(b.ID)
	if s.AliveCount() != 2 {
		t.Fatalf("want 2, got %d", s.AliveCount())
	}
}

func TestMarkAlive_ResurrectsNode(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	s.MarkDead(b.ID)
	s.MarkAlive(b.ID)
	if !s.IsAlive(b.ID) {
		t.Fatal("node should be alive after MarkAlive")
	}
	if s.AliveCount() != 3 {
		t.Fatalf("want 3, got %d", s.AliveCount())
	}
}

func TestGetSetMasterID(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	if s.GetMasterID() != 1 {
		t.Fatalf("want 1, got %d", s.GetMasterID())
	}
	s.SetMasterID(3)
	if s.GetMasterID() != 3 {
		t.Fatalf("want 3, got %d", s.GetMasterID())
	}
}

func TestIsMaster_TrueWhenSelfIsMaster(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, a.ID)
	if !s.IsMaster() {
		t.Fatal("self should be master")
	}
}

func TestIsMaster_FalseWhenOtherIsMaster(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, b.ID)
	if s.IsMaster() {
		t.Fatal("self should not be master")
	}
}

func TestGetMasterNode_ReturnsCorrectNode(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, c.ID)
	m := s.GetMasterNode()
	if m == nil || m.ID != c.ID {
		t.Fatalf("want masterID=%d, got %v", c.ID, m)
	}
}

func TestGetMasterNode_ReturnsNilWhenUnknown(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 99)
	if s.GetMasterNode() != nil {
		t.Fatal("want nil for unknown master ID")
	}
}

func TestNodeByName_Found(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	n := s.NodeByName("nodeB")
	if n == nil || n.ID != b.ID {
		t.Fatalf("want nodeB, got %v", n)
	}
}

func TestNodeByName_NotFound(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	if s.NodeByName("ghost") != nil {
		t.Fatal("want nil for unknown name")
	}
}

func TestNodeByID_Found(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	n := s.NodeByID(b.ID)
	if n == nil || n.Name != "nodeB" {
		t.Fatalf("want nodeB, got %v", n)
	}
}

func TestConcurrentMarkAliveMarkDead_NoRace(t *testing.T) {
	a, b, c := makeNodes()
	s := New(a, []node.Node{a, b, c}, 1)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.MarkDead(b.ID) }()
		go func() { defer wg.Done(); s.MarkAlive(b.ID) }()
	}
	wg.Wait()
}
