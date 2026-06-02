package tcpserver

import (
	"context"
	"fmt"
	"net"

	"node_messager/pkg/hub"
	"node_messager/pkg/logger"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
)

type tcpServer struct {
	node       node.Node
	store      *msgstore.Store
	dispatcher hub.Dispatcher
	Ready      chan struct{}
}

func New(n node.Node, store *msgstore.Store) *tcpServer {
	return &tcpServer{node: n, store: store, Ready: make(chan struct{})}
}

func (s *tcpServer) SetDispatcher(d hub.Dispatcher) { s.dispatcher = d }

func (s *tcpServer) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.node.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		close(s.Ready)
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	close(s.Ready)
	return s.serve(ctx, ln)
}

// serve runs the accept loop on an already-bound listener.
// Exported for testing: pass a pre-bound net.Listener on ":0".
func (s *tcpServer) serve(ctx context.Context, ln net.Listener) error {
	l := logger.GetContextLogger(ctx)
	h := hub.New(s.node.Name, l, s.store)
	if s.dispatcher != nil {
		h.SetDispatcher(s.dispatcher)
	}
	go h.Run()
	l.Infof("[%s] listening on %s", s.node.Name, ln.Addr())

	go func() {
		<-ctx.Done()
		h.Stop()
		if err := ln.Close(); err != nil {
			l.Debugf("[%s] listener close error: %v", s.node.Name, err)
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				l.Errorf("[%s] accept error: %v", s.node.Name, err)
				continue
			}
		}
		go h.Serve(conn)
	}
}
