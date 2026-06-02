package mutex

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"
)

// lockTimeout es cuanto esperamos a que el maestro nos otorgue el lock
// si el maestro tarda mas de esto, asumimos que hubo un problema y reintentamos
var lockTimeout = 5 * time.Second

const (
	acquireMaxRetries = 3
	acquireRetryDelay = 1 * time.Second
)

// pendingReq representa una solicitud de lock en espera en la cola del maestro
type pendingReq struct {
	requestID string
	fromNode  string
}

// Engine maneja la exclusion mutua distribuida usando el nodo maestro como arbitro
// solo un nodo puede tener el lock a la vez — evita doble asignacion de ingenieros
type Engine struct {
	self  node.Node
	state *nodestate.State
	pool  *sender.Pool
	log   *zap.SugaredLogger
	mu    sync.Mutex
	// holder es el requestID del nodo que tiene el lock actualmente, "" significa libre
	holder string
	// queue es la fila de solicitudes esperando a que se libere el lock (orden FIFO)
	queue []pendingReq
	// grants son los canales donde esperamos que el maestro nos otorgue el lock
	grants map[string]chan bool
}

// New crea un nuevo Engine de exclusion mutua
func New(self node.Node, state *nodestate.State, pool *sender.Pool, log *zap.SugaredLogger) *Engine {
	return &Engine{
		self:   self,
		state:  state,
		pool:   pool,
		log:    log,
		grants: make(map[string]chan bool),
	}
}

// Acquire bloquea hasta obtener el lock distribuido
// regresa una funcion release que debe llamarse al terminar la operacion critica
// reintenta hasta acquireMaxRetries veces si falla
func (e *Engine) Acquire(ctx context.Context) (func(), error) {
	var (
		release func()
		lastErr error
	)
	for attempt := 1; attempt <= acquireMaxRetries; attempt++ {
		release, lastErr = e.acquire(ctx)
		if lastErr == nil {
			return release, nil
		}
		e.log.Warnf("[mutex] Acquire attempt=%d/%d failed: %v",
			attempt, acquireMaxRetries, lastErr)
		if attempt < acquireMaxRetries {
			select {
			case <-time.After(acquireRetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	e.log.Errorf("[mutex] Acquire failed after %d attempts: %v",
		acquireMaxRetries, lastErr)
	return nil, lastErr
}

// acquire decide si adquirir el lock localmente (si somos maestro) o remotamente
func (e *Engine) acquire(ctx context.Context) (func(), error) {
	if e.state.IsMaster() {
		return e.acquireLocal()
	}
	return e.acquireRemote(ctx)
}

// acquireLocal adquiere el lock en memoria cuando este nodo es el maestro
// si el lock esta libre lo toma inmediatamente, si no, se encola y espera
func (e *Engine) acquireLocal() (func(), error) {
	reqID := uuid.New().String()
	e.mu.Lock()
	if e.holder == "" {
		// lock libre — lo tomamos de inmediato
		e.holder = reqID
		e.mu.Unlock()
		return func() { e.releaseLocal(reqID) }, nil
	}
	// lock ocupado — nos encolamos y esperamos a que nos lo otorguen
	e.queue = append(e.queue, pendingReq{requestID: reqID, fromNode: e.self.Name})
	e.mu.Unlock()

	ch := make(chan bool, 1)
	e.mu.Lock()
	e.grants[reqID] = ch
	e.mu.Unlock()

	select {
	case <-ch:
		return func() { e.releaseLocal(reqID) }, nil
	case <-time.After(lockTimeout):
		return nil, fmt.Errorf("mutex: local acquire timeout")
	}
}

// releaseLocal libera el lock y otorga el siguiente en la cola (FIFO)
// si el siguiente es un nodo remoto, le manda LOCK_GRANT por TCP
func (e *Engine) releaseLocal(reqID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.holder != reqID {
		return
	}
	e.holder = ""
	if len(e.queue) > 0 {
		next := e.queue[0]
		e.queue = e.queue[1:]
		e.holder = next.requestID
		if ch, ok := e.grants[next.requestID]; ok {
			// el siguiente en la cola es local — notificamos via canal
			ch <- true
			delete(e.grants, next.requestID)
		} else {
			// el siguiente en la cola es un nodo remoto — mandamos LOCK_GRANT por TCP
			peer := e.state.NodeByName(next.fromNode)
			if peer != nil {
				p := dto.LockPayload{RequestID: next.requestID, Resource: "engineer_assignment"}
				_ = e.pool.SendJSON(e.self, *peer, dto.TypeLockGrant, p)
			}
		}
	}
}

// acquireRemote solicita el lock al maestro via TCP y espera la respuesta
// si el maestro responde LOCK_GRANT, retornamos con el lock adquirido
// si el maestro responde LOCK_DENY, quedamos encolados en el maestro y esperamos
func (e *Engine) acquireRemote(ctx context.Context) (func(), error) {
	master := e.state.GetMasterNode()
	if master == nil {
		return nil, fmt.Errorf("mutex: no master node found")
	}

	reqID := uuid.New().String()
	ch := make(chan bool, 1)

	e.mu.Lock()
	e.grants[reqID] = ch
	e.mu.Unlock()

	p := dto.LockPayload{RequestID: reqID, Resource: "engineer_assignment"}
	if err := e.pool.SendJSON(e.self, *master, dto.TypeLockRequest, p); err != nil {
		e.mu.Lock()
		delete(e.grants, reqID)
		e.mu.Unlock()
		return nil, fmt.Errorf("mutex: send lock request: %w", err)
	}

	select {
	case granted := <-ch:
		if !granted {
			return nil, fmt.Errorf("mutex: lock denied")
		}
		// tenemos el lock — la funcion release manda LOCK_RELEASE al maestro actual
		release := func() {
			currentMaster := e.state.GetMasterNode()
			if currentMaster == nil {
				return
			}
			p := dto.LockPayload{RequestID: reqID, Resource: "engineer_assignment"}
			_ = e.pool.SendJSON(e.self, *currentMaster, dto.TypeLockRelease, p)
		}
		return release, nil
	case <-time.After(lockTimeout):
		e.mu.Lock()
		delete(e.grants, reqID)
		e.mu.Unlock()
		return nil, fmt.Errorf("mutex: timeout waiting for lock grant")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HandleLockRequest se llama en el maestro cuando un nodo pide el lock
// si el lock esta libre lo otorga, si no, encola al nodo y le responde LOCK_DENY
func (e *Engine) HandleLockRequest(msg dto.Message) {
	var p dto.LockPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		e.log.Warnf("[mutex] bad lock request: %v", err)
		return
	}

	e.mu.Lock()
	if e.holder == "" {
		// lock libre — se lo otorgamos
		e.holder = p.RequestID
		e.mu.Unlock()
		peer := e.state.NodeByName(msg.FromNode)
		if peer != nil {
			_ = e.pool.SendJSON(e.self, *peer, dto.TypeLockGrant, p)
		}
	} else {
		// lock ocupado — encolamos al nodo y le avisamos que espere
		e.queue = append(e.queue, pendingReq{requestID: p.RequestID, fromNode: msg.FromNode})
		e.mu.Unlock()
		peer := e.state.NodeByName(msg.FromNode)
		if peer != nil {
			_ = e.pool.SendJSON(e.self, *peer, dto.TypeLockDeny, p)
		}
	}
}

// HandleLockGrant se llama cuando el maestro nos otorga el lock
// notificamos al acquireRemote que esta esperando en su canal
func (e *Engine) HandleLockGrant(msg dto.Message) {
	var p dto.LockPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	e.mu.Lock()
	ch, ok := e.grants[p.RequestID]
	e.mu.Unlock()
	if ok {
		ch <- true
	}
}

// HandleLockDeny se llama cuando el maestro nos dice que el lock esta ocupado
// no hacemos nada — ya quedamos encolados en el maestro y esperamos el LOCK_GRANT
func (e *Engine) HandleLockDeny(msg dto.Message) {
	// el maestro nos encolo — el LOCK_GRANT llegara cuando se libere el lock
}

// HandleLockRelease se llama en el maestro cuando un nodo libera el lock
// llama releaseLocal para otorgarlo al siguiente en la cola
func (e *Engine) HandleLockRelease(msg dto.Message) {
	var p dto.LockPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	e.releaseLocal(p.RequestID)
}
