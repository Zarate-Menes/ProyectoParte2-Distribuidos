package heartbeat

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/election"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

var (
	// pingInterval es cada cuanto mandamos PING a los demas nodos
	// si lo ponemos muy seguido saturamos la red, si lo ponemos muy tarde
	// tardamos mucho en detectar que un nodo cayo
	pingInterval = 5 * time.Second
	// pongWait es cuanto tiempo esperamos la respuesta PONG antes de contar un fallo
	pongWait = 2 * time.Second
)

// maxMissed es cuantos PINGs sin respuesta necesitamos para declarar un nodo muerto
// con 3 fallos y 2s de espera, un nodo tarda ~15s en ser declarado muerto
const maxMissed = 3

// Monitor se encarga de revisar constantemente que los demas nodos esten vivos
// manda PING cada pingInterval y espera PONG — si no llega, cuenta el fallo
type Monitor struct {
	self     node.Node
	state    *nodestate.State
	pool     *sender.Pool
	election *election.Engine
	log      *zap.SugaredLogger

	mu  sync.Mutex
	// missed lleva el conteo de PINGs sin respuesta por cada nodo
	missed  map[int]int
	// pongChs son los canales donde esperamos el PONG de cada nodo
	pongChs map[int]chan struct{}
	// inFlight evita lanzar un segundo ping a un peer que aun no ha respondido
	inFlight map[int]bool

	// OnNodeDead se llama cuando un nodo es declarado muerto
	// se usa para redistribuir los tickets del nodo caido
	OnNodeDead func(deadNodeID int)
}

// New crea un nuevo Monitor de heartbeat
func New(self node.Node, state *nodestate.State, pool *sender.Pool, elec *election.Engine, log *zap.SugaredLogger) *Monitor {
	return &Monitor{
		self:     self,
		state:    state,
		pool:     pool,
		election: elec,
		log:      log,
		missed:   make(map[int]int),
		pongChs:  make(map[int]chan struct{}),
		inFlight: make(map[int]bool),
	}
}

// Run es la goroutine principal del heartbeat — corre mientras el nodo este vivo
// manda PING a todos los peers cada pingInterval
func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pingAll(ctx)
		}
	}
}

// pingAll manda PING a todos los peers en goroutines separadas
// para no bloquear si un nodo tarda en responder
func (m *Monitor) pingAll(ctx context.Context) {
	for _, peer := range m.state.Peers() {
		go m.pingOne(ctx, peer)
	}
}

// pingOne manda un PING a un nodo y espera su PONG
// si no llega en pongWait, cuenta un fallo — al llegar a maxMissed declara el nodo muerto
func (m *Monitor) pingOne(ctx context.Context, peer node.Node) {
	m.mu.Lock()
	if m.inFlight[peer.ID] {
		m.mu.Unlock()
		return
	}
	m.inFlight[peer.ID] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.inFlight, peer.ID)
		m.mu.Unlock()
	}()

	ch := make(chan struct{}, 1)
	m.mu.Lock()
	m.pongChs[peer.ID] = ch
	m.mu.Unlock()

	_ = m.pool.Send(m.self, peer, dto.TypePing, "")

	select {
	case <-ch:
		// recibimos PONG — nodo vivo, reseteamos el contador de fallos
		wasAlive := m.state.IsAlive(peer.ID)
		m.mu.Lock()
		m.missed[peer.ID] = 0
		m.mu.Unlock()
		m.state.MarkAlive(peer.ID)
		if !wasAlive {
			m.log.Infof("[heartbeat] node %d (%s) is back alive", peer.ID, peer.Name)
		}
	case <-time.After(pongWait):
		// no llego PONG a tiempo — contamos el fallo
		m.mu.Lock()
		m.missed[peer.ID]++
		missed := m.missed[peer.ID]
		m.mu.Unlock()
		if missed >= maxMissed {
			m.declareDead(peer.ID)
		}
	case <-ctx.Done():
	}

	m.mu.Lock()
	delete(m.pongChs, peer.ID)
	m.mu.Unlock()
}

// declareDead marca un nodo como muerto y decide que hacer segun el rol del nodo actual
// si el maestro murio — iniciamos eleccion
// si somos maestro — redistribuimos los tickets del nodo caido
// si somos nodo normal — avisamos al maestro para que el redistribuya
func (m *Monitor) declareDead(id int) {
	if !m.state.IsAlive(id) {
		return // ya estaba declarado muerto, evitamos duplicar acciones
	}
	m.state.MarkDead(id)
	n := m.state.NodeByID(id)
	name := "unknown"
	if n != nil {
		name = n.Name
	}
	m.log.Warnf("[heartbeat] node %d (%s) declared dead (missed %d pings)", id, name, maxMissed)

	masterID := m.state.GetMasterID()
	if id == masterID {
		// el maestro cayo — necesitamos elegir uno nuevo
		m.log.Warnf("[heartbeat] master is dead — starting election")
		go m.election.StartElection()
	} else if m.state.IsMaster() {
		// somos el maestro — redistribuimos los tickets del nodo caido
		if m.OnNodeDead != nil {
			go m.OnNodeDead(id)
		}
	} else {
		// somos un nodo normal — le avisamos al maestro para que redistribuya
		peer := m.state.GetMasterNode()
		if peer != nil {
			p := dto.NodeDeadPayload{DeadNodeID: id}
			data, _ := json.Marshal(p)
			_ = m.pool.Send(m.self, *peer, dto.TypeNodeDead, string(data))
		}
	}
}

// HandlePing responde con PONG cuando recibimos un PING de otro nodo
func (m *Monitor) HandlePing(msg dto.Message) {
	peer := m.state.NodeByName(msg.FromNode)
	if peer == nil {
		return
	}
	_ = m.pool.Send(m.self, *peer, dto.TypePong, "")
}

// HandlePong recibe el PONG y notifica al pingOne que esta esperando
func (m *Monitor) HandlePong(msg dto.Message) {
	peer := m.state.NodeByName(msg.FromNode)
	if peer == nil {
		return
	}
	m.mu.Lock()
	ch, ok := m.pongChs[peer.ID]
	m.mu.Unlock()
	if ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
