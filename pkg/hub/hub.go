package hub

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"node_messager/pkg/dto"
	"node_messager/pkg/msgstore"
)

// Client representa una conexion TCP activa al hub
type Client struct {
	hub  *Hub
	conn net.Conn
	// send es el canal por donde el hub manda mensajes a este cliente
	send chan []byte
}

// Dispatcher es la interfaz que el hub usa para mandar mensajes al engine correcto
// el NodeDispatcher implementa esta interfaz
type Dispatcher interface {
	Dispatch(msg dto.Message)
}

// Hub maneja todas las conexiones TCP entrantes al nodo
// recibe mensajes, los guarda en el store y los pasa al dispatcher
type Hub struct {
	name      string
	clients   map[*Client]bool
	broadcast chan []byte
	register  chan *Client
	// unregister se usa para limpiar la conexion cuando un cliente se desconecta
	unregister chan *Client
	done       chan struct{}
	log        *zap.SugaredLogger
	store      *msgstore.Store
	dispatcher Dispatcher
}

// SetDispatcher asigna el dispatcher al hub — se llama una vez al inicio antes de Run
func (h *Hub) SetDispatcher(d Dispatcher) { h.dispatcher = d }

// New crea un nuevo Hub para el nodo con el nombre dado
func New(name string, log *zap.SugaredLogger, store *msgstore.Store) *Hub {
	return &Hub{
		name:       name,
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		done:       make(chan struct{}),
		log:        log,
		store:      store,
	}
}

// Stop signals the hub goroutine to exit
func (h *Hub) Stop() { close(h.done) }

// Run es el loop principal del hub — procesa conexiones, desconexiones y mensajes
// debe correr en su propia goroutine
func (h *Hub) Run() {
	for {
		select {
		case <-h.done:
			return
		case c := <-h.register:
			h.clients[c] = true
			h.log.Debugf("[%s] client connected, total=%d", h.name, len(h.clients))

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
				h.log.Debugf("[%s] client disconnected, total=%d", h.name, len(h.clients))
			}

		case data := <-h.broadcast:
			var msg dto.Message
			if err := json.Unmarshal(data, &msg); err != nil {
				h.log.Warnf("[%s] invalid message payload: %v", h.name, err)
				continue
			}
			// guardamos el mensaje recibido en el store para el historial
			if h.store != nil {
				if err := h.store.Save(msg, msgstore.Received); err != nil {
					h.log.Warnf("[%s] store save: %v", h.name, err)
				}
			}
			h.log.Infof("[%s] recv  type=%s from=%s to=%s id=%s — %q",
				h.name, msg.Type, msg.FromNode, msg.ToNode, msg.ID, msg.Content)

			// si el mensaje llega al nodo incorrecto, es señal de que las IPs en nodes.json estan mal
			if msg.ToNode != "" && msg.ToNode != h.name {
				h.log.Warnf("[%s] ROUTING ERROR: mensaje to=%s llego a este nodo — verifica IPs en nodes.json",
					h.name, msg.ToNode)
			}

			// mandamos el mensaje al dispatcher para que lo procese segun su tipo
			if h.dispatcher != nil {
				go h.dispatcher.Dispatch(msg)
			}
		}
	}
}

// Serve registra una conexion TCP nueva al hub y arranca sus goroutines de lectura y escritura
func (h *Hub) Serve(conn net.Conn) {
	c := &Client{hub: h, conn: conn, send: make(chan []byte, 256)}
	h.register <- c
	go c.writePump()
	go c.readPump()
}

// readPump lee lineas del TCP y las manda al canal broadcast del hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		if err := c.conn.Close(); err != nil {
			c.hub.log.Debugf("[%s] close error: %v", c.hub.name, err)
		}
	}()
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		buf := make([]byte, len(line))
		copy(buf, line)
		c.hub.broadcast <- buf
	}
}

// writePump escribe mensajes del canal send a la conexion TCP
func (c *Client) writePump() {
	defer func() {
		if err := c.conn.Close(); err != nil {
			c.hub.log.Debugf("[%s] close error: %v", c.hub.name, err)
		}
	}()
	for data := range c.send {
		if _, err := fmt.Fprintf(c.conn, "%s\n", data); err != nil {
			c.hub.log.Debugf("[%s] write error, closing connection: %v", c.hub.name, err)
			break
		}
		var msg dto.Message
		if err := json.Unmarshal(data, &msg); err == nil {
			c.hub.log.Debugf("[%s] ack   id=%s at=%s",
				c.hub.name, msg.ID, time.Now().UTC().Format(time.RFC3339))
		}
	}
}
