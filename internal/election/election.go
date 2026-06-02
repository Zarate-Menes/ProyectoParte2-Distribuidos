package election

import (
	"encoding/json"
	"sync"
	"time"

	"go.uber.org/zap"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

// okTimeout es el tiempo que esperamos a que un nodo con menor ID responda
// si nadie responde en ese tiempo, asumimos que somos el nodo con menor ID activo
// y nos declaramos maestro
var okTimeout = 3 * time.Second

// Engine es la estructura que maneja el algoritmo de eleccion de lider (Bully inverso)
// el nodo con menor ID tiene mayor prioridad y gana la eleccion
type Engine struct {
	self  node.Node
	state *nodestate.State
	pool  *sender.Pool
	log   *zap.SugaredLogger
	mu    sync.Mutex
	// running nos evita iniciar multiples elecciones al mismo tiempo en el mismo nodo
	running bool
	// okTimer es el temporizador que se activa cuando mandamos ELECTION a otros nodos
	// si nadie responde antes de que expire, nos declaramos maestro
	okTimer *time.Timer
}

// New crea un nuevo Engine de eleccion
func New(self node.Node, state *nodestate.State, pool *sender.Pool, log *zap.SugaredLogger) *Engine {
	return &Engine{self: self, state: state, pool: pool, log: log}
}

// StartElection inicia el algoritmo Bully inverso
// mandamos ELECTION a todos los nodos con ID menor (mayor prioridad)
// si ninguno responde en okTimeout, nos declaramos maestro
func (e *Engine) StartElection() {
	e.mu.Lock()
	// si ya hay una eleccion corriendo en este nodo, no iniciamos otra
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	e.log.Infof("[election] starting, self_id=%d", e.self.ID)

	lowerNodes := e.lowerNodes()
	if len(lowerNodes) == 0 {
		// no hay nodos con menor ID activos — somos el maestro
		e.declareVictory()
		return
	}

	// mandamos ELECTION a todos los nodos con menor ID
	p := dto.ElectionPayload{CandidateID: e.self.ID}
	for _, n := range lowerNodes {
		_ = e.pool.SendJSON(e.self, n, dto.TypeElection, p)
	}

	// iniciamos el temporizador — si nadie responde, ganamos
	e.mu.Lock()
	e.okTimer = time.AfterFunc(okTimeout, func() {
		e.mu.Lock()
		if !e.running {
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		e.declareVictory()
	})
	e.mu.Unlock()
}

// ClaimMastership lo usa el nodo maestro inicial al arrancar para anunciarse
// sin necesidad de correr una eleccion completa
func (e *Engine) ClaimMastership() {
	e.state.SetMasterID(e.self.ID)
	e.log.Infof("[election] claiming mastership, id=%d", e.self.ID)
	p := dto.CoordinatorPayload{MasterID: e.self.ID}
	e.pool.BroadcastJSON(e.self, e.state.Peers(), dto.TypeCoordinator, p)
}

// declareVictory se llama cuando ganamos la eleccion
// actualizamos el estado local y avisamos a todos los demas nodos que somos el nuevo maestro
func (e *Engine) declareVictory() {
	e.mu.Lock()
	e.running = false
	if e.okTimer != nil {
		e.okTimer.Stop()
	}
	e.mu.Unlock()

	e.state.SetMasterID(e.self.ID)
	e.log.Infof("[election] won — new master id=%d", e.self.ID)

	// mandamos COORDINATOR a todos para que sepan quien es el nuevo maestro
	p := dto.CoordinatorPayload{MasterID: e.self.ID}
	e.pool.BroadcastJSON(e.self, e.state.Peers(), dto.TypeCoordinator, p)
}

// HandleElection se llama cuando recibimos un mensaje ELECTION de otro nodo
// si el nodo que mando ELECTION tiene mayor ID que nosotros, respondemos OK
// y comenzamos nuestra propia eleccion porque tenemos mayor prioridad
func (e *Engine) HandleElection(msg dto.Message) {
	var p dto.ElectionPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}

	// si el candidato tiene menor o igual ID que nosotros, lo ignoramos
	// porque nosotros tenemos menor o igual prioridad
	if e.self.ID >= p.CandidateID {
		return
	}

	// el candidato tiene mayor ID — nosotros tenemos mayor prioridad
	// respondemos OK para que el candidato sepa que no va a ganar
	sender := e.state.NodeByName(msg.FromNode)
	if sender != nil {
		reply := dto.ElectionPayload{CandidateID: e.self.ID}
		_ = e.pool.SendJSON(e.self, *sender, dto.TypeElectionOK, reply)
	}

	// iniciamos nuestra propia eleccion
	go e.StartElection()
}

// HandleElectionOK se llama cuando un nodo con menor ID (mayor prioridad) nos responde
// esto significa que ese nodo va a competir por ser maestro, asi que nos retiramos
func (e *Engine) HandleElectionOK(msg dto.Message) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.running = false
	// cancelamos el temporizador porque ya no vamos a ganar
	if e.okTimer != nil {
		e.okTimer.Stop()
		e.okTimer = nil
	}
	e.log.Infof("[election] received OK from %s (lower ID, higher priority) — stepping down", msg.FromNode)
}

// HandleCoordinator se llama cuando un nodo anuncia que es el nuevo maestro
// actualizamos nuestro estado local para saber a quien mandar los LOCK_REQUEST
func (e *Engine) HandleCoordinator(msg dto.Message) {
	var p dto.CoordinatorPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	oldMaster := e.state.GetMasterID()
	e.state.SetMasterID(p.MasterID)
	if oldMaster != p.MasterID {
		e.log.Infof("[election] master changed: old=%d new=%d (announced by %s)", oldMaster, p.MasterID, msg.FromNode)
	} else {
		e.log.Infof("[election] master confirmed id=%d (announced by %s)", p.MasterID, msg.FromNode)
	}
}

// lowerNodes regresa los nodos vivos con ID menor al nuestro
// en el algoritmo Bully inverso, menor ID = mayor prioridad
func (e *Engine) lowerNodes() []node.Node {
	var out []node.Node
	for _, n := range e.state.AlivePeers() {
		if n.ID < e.self.ID {
			out = append(out, n)
		}
	}
	return out
}
