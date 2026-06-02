package sender

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/tcpclient"
)

// Pool maneja las conexiones TCP salientes a otros nodos
// reutiliza conexiones existentes para evitar el overhead de reconectarse en cada mensaje
type Pool struct {
	mu    sync.Mutex
	// conns es el mapa de id de nodo a su conexion TCP activa
	conns map[int]*tcpclient.Client
	log   *zap.SugaredLogger
}

// NewPool crea un nuevo Pool de conexiones
func NewPool(log *zap.SugaredLogger) *Pool {
	return &Pool{conns: make(map[int]*tcpclient.Client), log: log}
}

// CloseAll cierra todas las conexiones activas — se usa al apagar el nodo
func (p *Pool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, c := range p.conns {
		_ = c.Close()
		delete(p.conns, id)
	}
}

// get regresa la conexion existente al nodo o crea una nueva si no existe o esta cerrada
func (p *Pool) get(n node.Node) (*tcpclient.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[n.ID]; ok && !c.IsClosed() {
		return c, nil
	}
	// no hay conexion activa — creamos una nueva
	c, err := tcpclient.Connect(n.Host, n.Port)
	if err != nil {
		return nil, err
	}
	p.conns[n.ID] = c
	return c, nil
}

// Send manda un mensaje con contenido de texto al nodo destino
// si el envio falla por conexion muerta, la elimina del pool para que el proximo intento reconecte
func (p *Pool) Send(from node.Node, to node.Node, msgType, content string) error {
	c, err := p.get(to)
	if err != nil {
		return fmt.Errorf("connect %s: %w", to.Name, err)
	}
	m := dto.Message{
		ID:       uuid.New().String(),
		Type:     msgType,
		FromNode: from.Name,
		ToNode:   to.Name,
		Content:  content,
		SendAt:   time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := c.Send(data); err != nil {
		// conexion muerta — la removemos del pool para reconectar en el proximo intento
		p.mu.Lock()
		delete(p.conns, to.ID)
		p.mu.Unlock()
		return fmt.Errorf("send to %s: %w", to.Name, err)
	}
	return nil
}

// SendJSON serializa el payload a JSON y lo manda como contenido del mensaje
func (p *Pool) SendJSON(from node.Node, to node.Node, msgType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.Send(from, to, msgType, string(data))
}

// Broadcast manda el mismo mensaje a todos los nodos en targets
// regresa un mapa de errores por nodo — si el mapa esta vacio, todo salio bien
func (p *Pool) Broadcast(from node.Node, targets []node.Node, msgType, content string) map[int]error {
	errs := make(map[int]error)
	for _, t := range targets {
		if err := p.Send(from, t, msgType, content); err != nil {
			p.log.Warnf("[sender] broadcast to %s failed: %v", t.Name, err)
			errs[t.ID] = err
		}
	}
	return errs
}

// BroadcastJSON serializa el payload a JSON y lo manda a todos los nodos en targets
func (p *Pool) BroadcastJSON(from node.Node, targets []node.Node, msgType string, payload any) map[int]error {
	data, err := json.Marshal(payload)
	if err != nil {
		return map[int]error{-1: err}
	}
	return p.Broadcast(from, targets, msgType, string(data))
}
