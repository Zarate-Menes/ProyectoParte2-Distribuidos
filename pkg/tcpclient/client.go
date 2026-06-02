package tcpclient

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
)

type Client struct {
	mu   sync.Mutex
	conn net.Conn
	Recv chan []byte
	done chan struct{}
}

func Connect(host string, port int) (*Client, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", addr, err)
	}
	c := &Client{
		conn: conn,
		Recv: make(chan []byte, 256),
		done: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func (c *Client) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Client) Send(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := fmt.Fprintf(c.conn, "%s\n", msg)
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) readLoop() {
	defer close(c.Recv)
	defer close(c.done)
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		// descartamos mensajes de fan-out — el sender pool nunca lee Recv
		// si no descartamos, Recv se llena, readLoop se bloquea y causa TCP backpressure
	}
}
