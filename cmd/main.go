package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/adapters/cli"
	"node_messager/internal/config"
	"node_messager/internal/consensus"
	"node_messager/internal/db"
	"node_messager/internal/dispatcher"
	"node_messager/internal/election"
	"node_messager/internal/heartbeat"
	"node_messager/internal/mutex"
	"node_messager/internal/nodestate"
	"node_messager/internal/service"
	"node_messager/pkg/dto"
	logger "node_messager/pkg/logger"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
	tcpserver "node_messager/pkg/tcp_server"
)

// overridden at build time: go build -ldflags "-X main.debug=false" ./cmd
var debug = "true"

func main() {
	startupLog := logger.NewLogger(true, true)

	cfg, err := config.LoadConfig("nodes.json")
	if err != nil {
		startupLog.Fatalf("load config: %v", err)
	}

	for _, dir := range []string{"logs", "messages", "data", "tickets"} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			startupLog.Fatalf("create %s dir: %v", dir, err)
		}
	}

	debugMode := debug == "true"

	// In VM mode only the host node runs locally; in dev mode all sucursales run.
	serveNodes := cfg.Nodes
	if cfg.HostNode != nil {
		serveNodes = []node.Node{*cfg.HostNode}
	}

	stores := make(map[int]*msgstore.Store, len(cfg.Nodes)+1)
	for _, n := range cfg.Nodes {
		isLocal := cfg.HostNode == nil || n.ID == cfg.HostNode.ID
		if isLocal {
			store, err := msgstore.NewWithFile(50, fmt.Sprintf("messages/%s.jsonl", n.Name))
			if err != nil {
				startupLog.Fatalf("[%s] open message file: %v", n.Name, err)
			}
			stores[n.ID] = store
		} else {
			stores[n.ID] = msgstore.New(50)
		}
	}
	// guarantee host node always has a store (guards against host.id not matching any node)
	if cfg.HostNode != nil {
		if _, ok := stores[cfg.HostNode.ID]; !ok {
			store, err := msgstore.NewWithFile(50, fmt.Sprintf("messages/%s.jsonl", cfg.HostNode.Name))
			if err != nil {
				startupLog.Fatalf("[%s] open message file: %v", cfg.HostNode.Name, err)
			}
			stores[cfg.HostNode.ID] = store
			startupLog.Warnf("host node id=%d not found in nodes list — created store anyway", cfg.HostNode.ID)
		}
	}

	nodeLogs := make(map[int]*zap.SugaredLogger, len(serveNodes))
	nodeServices := make(map[int]*service.TicketService, len(serveNodes))
	var nodeDbs []*db.DB
	var readyChs []<-chan struct{}

	for _, n := range serveNodes {
		n := n

		f, err := os.OpenFile(fmt.Sprintf("logs/%s.log", n.Name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			startupLog.Fatalf("[%s] open log file: %v", n.Name, err)
		}

		nodeLog := logger.NewLoggerToWriter(f, debugMode)
		nodeLogs[n.ID] = nodeLog
		nodeCtx := logger.SetContextLogger(context.Background(), nodeLog)

		nodeDB, err := db.Open(n.Name)
		if err != nil {
			startupLog.Fatalf("[%s] open db: %v", n.Name, err)
		}
		nodeDbs = append(nodeDbs, nodeDB)
		if v, err := nodeDB.Version(); err == nil {
			startupLog.Infof("[%s] db schema version: %d", n.Name, v)
		}
		if seedData, err := db.LoadSeedData("seed.json"); err == nil {
			maxCounter, err := nodeDB.Seed(n.ID, seedData)
			if err != nil {
				startupLog.Fatalf("[%s] seed db: %v", n.Name, err)
			}
			if maxCounter > 0 {
				service.SetMinIDCounter(maxCounter)
			}
		}

		ns := nodestate.New(n, cfg.Nodes, cfg.MasterID)
		pool := sender.NewPool(nodeLog)
		commitHandler := buildCommitHandler(n, nodeDB, nodeLog)

		consEngine := consensus.New(n, ns, pool, nodeLog, commitHandler)
		mtxEngine := mutex.New(n, ns, pool, nodeLog)
		elecEngine := election.New(n, ns, pool, nodeLog)
		hbMonitor := heartbeat.New(n, ns, pool, elecEngine, nodeLog)
		svc := service.New(n, ns, nodeDB, pool, consEngine, mtxEngine, nodeLog)

		hbMonitor.OnNodeDead = func(deadID int) {
			svc.RedistributeTickets(nodeCtx, deadID)
		}

		nodeServices[n.ID] = svc

		disp := dispatcher.New(nodeCtx, ns, consEngine, mtxEngine, elecEngine, hbMonitor, svc, nodeLog)

		// initial master sucursal announces itself on startup
		if n.ID == cfg.MasterID {
			go func(e *election.Engine) {
				time.Sleep(2 * time.Second)
				e.ClaimMastership()
			}(elecEngine)
		}

		srv := tcpserver.New(n, stores[n.ID])
		srv.SetDispatcher(disp)
		readyChs = append(readyChs, srv.Ready)
		go func() {
			if err := srv.Start(nodeCtx); err != nil {
				nodeLog.Errorf("[%s] server error: %s", n.Name, err)
			}
		}()

		go hbMonitor.Run(nodeCtx)
	}
	for _, ch := range readyChs {
		<-ch
	}

	// CLI uses the host sucursal in VM mode, or first sucursal in dev mode.
	var cliSvc *service.TicketService
	var cliHostNode *node.Node

	if cfg.HostNode != nil {
		cliSvc = nodeServices[cfg.HostNode.ID]
		cliHostNode = cfg.HostNode
	} else if len(cfg.Nodes) > 0 {
		cliSvc = nodeServices[cfg.Nodes[0].ID]
	}

	defer func() {
		for _, d := range nodeDbs {
			_ = d.Close()
		}
	}()

	if err := cli.Run(cfg.Nodes, stores, cliHostNode, cliSvc, startupLog, nodeLogs); err != nil {
		startupLog.Fatalf("cli error: %v", err)
	}
}

func buildCommitHandler(self node.Node, nodeDB *db.DB, log *zap.SugaredLogger) func(operation, data string) error {
	return func(operation, data string) error {
		switch operation {
		case "INSERT_USUARIO":
			var r dto.UsuarioRow
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return err
			}
			if r.SucursalID != self.ID {
				return nil
			}
			return nodeDB.InsertUsuario(r.ID, r.Nombre, r.SucursalID)

		case "INSERT_INGENIERO":
			var r dto.IngenieroRow
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return err
			}
			if r.SucursalID != self.ID {
				return nil
			}
			return nodeDB.InsertIngeniero(r.ID, r.Nombre, r.SucursalID)

		case "INSERT_DISPOSITIVO":
			var r dto.DispositivoRow
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return err
			}
			if r.SucursalID != self.ID {
				return nil
			}
			return nodeDB.InsertDispositivo(r.ID, r.Nombre, r.Tipo, r.SucursalID, r.IngenieroID)

		case "INSERT_TICKET", "REASSIGN_TICKET":
			var r dto.TicketRow
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return err
			}
			if r.IDSucursal != self.ID {
				return nil
			}
			t := db.Ticket{
				ID: int64(r.ID), IDUsuario: r.IDUsuario, IDIngeniero: r.IDIngeniero,
				IDSucursal: r.IDSucursal, IDDispositivo: r.IDDispositivo,
				Estado: r.Estado, Folio: r.Folio, CreatedAt: r.CreatedAt,
			}
			return nodeDB.InsertTicketFull(t)

		case "UPDATE_TICKET_FOLIO":
			var p struct {
				IDTicket int    `json:"id_ticket"`
				Folio    string `json:"folio"`
			}
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return err
			}
			return nodeDB.UpdateTicketFolio(int64(p.IDTicket), p.Folio)

		case "CLOSE_TICKET":
			var p struct {
				IDTicket    int64 `json:"id_ticket"`
				IDIngeniero int   `json:"id_ingeniero"`
			}
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return err
			}
			if err := nodeDB.CloseTicket(p.IDTicket); err != nil {
				return err
			}
			return nodeDB.SetIngenieroDisponible(p.IDIngeniero, 1)

		case "UPDATE_INGENIERO_DISPONIBLE":
			var r dto.IngenieroRow
			if err := json.Unmarshal([]byte(data), &r); err != nil {
				return err
			}
			return nodeDB.SetIngenieroDisponible(r.ID, r.Disponible)

		default:
			log.Warnf("[commit] unknown operation: %s", operation)
			return nil
		}
	}
}
