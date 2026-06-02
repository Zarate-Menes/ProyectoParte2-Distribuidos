package nodestate

import (
	"sync"

	"node_messager/pkg/node"
)

// State guarda el estado en memoria del nodo actual en el cluster
// todos los engines (consensus, mutex, election, heartbeat) lo consultan
// para saber quienes estan vivos y quien es el maestro
type State struct {
	mu       sync.RWMutex
	self     node.Node
	all      []node.Node
	masterID int
	// alive lleva un mapa de id -> bool para saber que nodos estan activos
	alive map[int]bool
}

// New crea un nuevo State — al inicio todos los nodos se consideran vivos
func New(self node.Node, all []node.Node, masterID int) *State {
	alive := make(map[int]bool, len(all))
	for _, n := range all {
		alive[n.ID] = true
	}
	return &State{self: self, all: all, masterID: masterID, alive: alive}
}

// Self regresa el nodo actual
func (s *State) Self() node.Node { return s.self }

// All regresa todos los nodos del cluster incluyendo el nodo actual
func (s *State) All() []node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]node.Node, len(s.all))
	copy(out, s.all)
	return out
}

// Peers regresa todos los nodos del cluster excepto el nodo actual
func (s *State) Peers() []node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []node.Node
	for _, n := range s.all {
		if n.ID != s.self.ID {
			out = append(out, n)
		}
	}
	return out
}

// AlivePeers regresa solo los peers que el heartbeat ha marcado como vivos
// se usa para calcular el quorum en consensus y para saber a quien mandar mensajes
func (s *State) AlivePeers() []node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []node.Node
	for _, n := range s.all {
		if n.ID != s.self.ID && s.alive[n.ID] {
			out = append(out, n)
		}
	}
	return out
}

// AliveCount regresa cuantos nodos estan vivos incluyendo el nodo actual
func (s *State) AliveCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, alive := range s.alive {
		if alive {
			n++
		}
	}
	return n
}

// MarkAlive marca un nodo como vivo — lo llama heartbeat al recibir PONG
func (s *State) MarkAlive(id int) {
	s.mu.Lock()
	s.alive[id] = true
	s.mu.Unlock()
}

// MarkDead marca un nodo como muerto — lo llama heartbeat cuando supera maxMissed
func (s *State) MarkDead(id int) {
	s.mu.Lock()
	s.alive[id] = false
	s.mu.Unlock()
}

// IsAlive regresa si un nodo esta marcado como vivo
func (s *State) IsAlive(id int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.alive[id]
}

// GetMasterID regresa el ID del nodo maestro actual
func (s *State) GetMasterID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masterID
}

// SetMasterID actualiza el maestro — lo llama election cuando hay un nuevo coordinador
func (s *State) SetMasterID(id int) {
	s.mu.Lock()
	s.masterID = id
	s.mu.Unlock()
}

// IsMaster regresa si el nodo actual es el maestro
func (s *State) IsMaster() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.self.ID == s.masterID
}

// GetMasterNode regresa el nodo maestro completo para poder mandarle mensajes TCP
func (s *State) GetMasterNode() *node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.all {
		if n.ID == s.masterID {
			cp := n
			return &cp
		}
	}
	return nil
}

// NodeByName busca un nodo por nombre — se usa para responder mensajes TCP
// donde solo conocemos el nombre del nodo que mando el mensaje
func (s *State) NodeByName(name string) *node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.all {
		if n.Name == name {
			cp := n
			return &cp
		}
	}
	return nil
}

// NodeByID busca un nodo por ID — se usa para rutear mensajes al nodo propietario de un dato
func (s *State) NodeByID(id int) *node.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.all {
		if n.ID == id {
			cp := n
			return &cp
		}
	}
	return nil
}
