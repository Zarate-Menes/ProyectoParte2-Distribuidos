package service

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/db"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/hub"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

type queryDisp struct{ svc *TicketService }

func (d *queryDisp) Dispatch(msg dto.Message) {
	switch msg.Type {
	case dto.TypeQuery:
		d.svc.HandleQuery(msg)
	case dto.TypeQueryResponse:
		d.svc.HandleQueryResponse(msg)
	}
}

type testNode struct {
	n     node.Node
	state *nodestate.State
	pool  *sender.Pool
	db    *db.DB
	svc   *TicketService
}

func setupNode(t *testing.T, id int, name string, all []node.Node) testNode {
	t.Helper()
	log, _ := zap.NewDevelopment()
	sugar := log.Sugar()

	self := node.Node{ID: id, Name: name}
	state := nodestate.New(self, all, 1)
	pool := sender.NewPool(sugar)
	t.Cleanup(pool.CloseAll)

	database, err := db.OpenAt(t.TempDir() + "/" + name + ".db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	svc := New(self, state, database, pool, nil, nil, sugar)

	store := msgstore.New(100)
	h := hub.New(name, sugar, store)
	disp := &queryDisp{svc: svc}
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
	live := node.Node{ID: id, Name: name, Host: "127.0.0.1", Port: port}
	return testNode{n: live, state: state, pool: pool, db: database, svc: svc}
}

func TestListAll_ReturnsDataFromRemoteNodes(t *testing.T) {
	n1 := node.Node{ID: 1, Name: "suc1", Host: "127.0.0.1", Port: 0}
	n2 := node.Node{ID: 2, Name: "suc2", Host: "127.0.0.1", Port: 0}
	allPlaceholder := []node.Node{n1, n2}

	tn1 := setupNode(t, 1, "suc1", allPlaceholder)
	tn2 := setupNode(t, 2, "suc2", allPlaceholder)

	all := []node.Node{tn1.n, tn2.n}
	tn1.state = nodestate.New(tn1.n, all, 1)
	tn2.state = nodestate.New(tn2.n, all, 1)
	tn1.svc.state = tn1.state
	tn2.svc.state = tn2.state

	if err := tn1.db.InsertIngeniero(10001, "Alice", 1); err != nil {
		t.Fatal(err)
	}
	if err := tn2.db.InsertIngeniero(20001, "Bob", 2); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := tn1.svc.ListAll(ctx, "INGENIEROS")
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}

	names := make(map[string]bool)
	for _, r := range rows {
		if eng, ok := r.(dto.IngenieroRow); ok {
			names[eng.Nombre] = true
		}
	}
	if !names["Alice"] {
		t.Error("missing local engineer Alice")
	}
	if !names["Bob"] {
		t.Error("missing remote engineer Bob — cross-node query failed")
	}
}

func TestListAll_StableUnderLoad(t *testing.T) {
	n1 := node.Node{ID: 1, Name: "suc1", Host: "127.0.0.1", Port: 0}
	n2 := node.Node{ID: 2, Name: "suc2", Host: "127.0.0.1", Port: 0}
	allPlaceholder := []node.Node{n1, n2}

	tn1 := setupNode(t, 1, "suc1", allPlaceholder)
	tn2 := setupNode(t, 2, "suc2", allPlaceholder)

	all := []node.Node{tn1.n, tn2.n}
	tn1.state = nodestate.New(tn1.n, all, 1)
	tn2.state = nodestate.New(tn2.n, all, 1)
	tn1.svc.state = tn1.state
	tn2.svc.state = tn2.state

	if err := tn2.db.InsertUsuario(20001, "Carlos", 2); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		rows, err := tn1.svc.ListAll(ctx, "USUARIOS")
		if err != nil {
			t.Fatalf("ListAll iteration %d: %v", i, err)
		}
		found := false
		for _, r := range rows {
			if u, ok := r.(dto.UsuarioRow); ok && u.Nombre == "Carlos" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("iteration %d: remote user Carlos not found — query stability issue", i)
		}
	}
}
