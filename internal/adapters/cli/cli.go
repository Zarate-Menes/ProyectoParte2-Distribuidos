package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"node_messager/internal/service"
	"node_messager/pkg/dto"
	"node_messager/pkg/msgstore"
	"node_messager/pkg/node"
	"node_messager/pkg/tcpclient"
)

const logDir = "logs"
const logLines = 50

// ── connection pool ───────────────────────────────────────────────────────────

type connPool struct {
	mu    sync.Mutex
	conns map[int]*tcpclient.Client
	log   *zap.SugaredLogger
}

func newConnPool(log *zap.SugaredLogger) *connPool {
	return &connPool{conns: make(map[int]*tcpclient.Client), log: log}
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, c := range p.conns {
		if err := c.Close(); err != nil {
			p.log.Debugf("[pool] close error node_id=%d: %v", id, err)
		}
		delete(p.conns, id)
	}
}

func (p *connPool) get(n node.Node) (*tcpclient.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[n.ID]; ok && !c.IsClosed() {
		return c, nil
	}
	c, err := tcpclient.Connect(n.Host, n.Port)
	if err != nil {
		return nil, err
	}
	p.conns[n.ID] = c
	return c, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func separator() {
	fmt.Println()
}

// prompt reads a line with full line-editing support (backspace, arrows, etc.).
// Returns "" on EOF or Ctrl+C/Ctrl+D.
func prompt(rl *readline.Instance, label string) string {
	rl.SetPrompt(label)
	line, err := rl.Readline()
	if err != nil { // io.EOF or readline.ErrInterrupt
		return ""
	}
	return strings.TrimSpace(line)
}

func pickNode(rl *readline.Instance, nodes []node.Node, label string) (node.Node, bool) {
	fmt.Println(label)
	for i, n := range nodes {
		fmt.Printf("  %d) %-8s  %s:%d\n", i+1, n.Name, n.Host, n.Port)
	}
	fmt.Println()
	for {
		raw := prompt(rl, "choice> ")
		if raw == "" {
			return node.Node{}, false
		}
		for i, n := range nodes {
			if raw == fmt.Sprintf("%d", i+1) || strings.EqualFold(raw, n.Name) {
				return n, true
			}
		}
		fmt.Println("invalid — enter number or node name")
	}
}

func sendMsg(pool *connPool, from, to node.Node, content string, stores map[int]*msgstore.Store, log *zap.SugaredLogger) error {
	c, err := pool.get(to)
	if err != nil {
		return err
	}
	m := dto.Message{
		ID:       uuid.New().String(),
		Type:     dto.TypeMsg,
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
		return err
	}
	if s, ok := stores[from.ID]; ok {
		_ = s.Save(m, msgstore.Sent)
	}
	log.Infof("[%s] sent  type=%s to=%s id=%s — %q", from.Name, m.Type, to.Name, m.ID, content)
	return nil
}

func broadcast(pool *connPool, from node.Node, nodes []node.Node, content string, stores map[int]*msgstore.Store, log *zap.SugaredLogger) []string {
	id := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	var errs []string
	for _, n := range nodes {
		c, err := pool.get(n)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", n.Name, err))
			continue
		}
		m := dto.Message{
			ID:       id,
			Type:     dto.TypeBroadcast,
			FromNode: from.Name,
			ToNode:   n.Name,
			Content:  content,
			SendAt:   now,
		}
		data, _ := json.Marshal(m)
		if err := c.Send(data); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", n.Name, err))
			continue
		}
		if s, ok := stores[from.ID]; ok {
			_ = s.Save(m, msgstore.Sent)
		}
		log.Infof("[%s] sent  type=%s to=%s id=%s — %q", from.Name, m.Type, n.Name, id, content)
	}
	return errs
}

func tailLogFile(nodeName string, n int) {
	path := fmt.Sprintf("%s/%s.log", logDir, nodeName)
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("cannot open log file %s: %v\n", path, err)
		return
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if len(lines) == 0 {
		fmt.Printf("no logs for %s yet\n", nodeName)
		return
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	for _, l := range lines {
		fmt.Println(l)
	}
}

func formatEntries(nodeName string, entries []msgstore.Entry) string {
	if len(entries) == 0 {
		return fmt.Sprintf("No messages for %s yet.", nodeName)
	}
	var sb strings.Builder
	for _, e := range entries {
		fmt.Fprintf(&sb, "%s  %-10s  %-10s  from=%-8s  to=%-8s  %q\n",
			e.At.Format(time.RFC3339),
			e.Type,
			e.Msg.Type,
			e.Msg.FromNode,
			e.Msg.ToNode,
			e.Msg.Content,
		)
	}
	return strings.TrimRight(sb.String(), "\n")
}

func printEntries(nodeName string, entries []msgstore.Entry) {
	fmt.Println(formatEntries(nodeName, entries))
}

func nodeLog(nodeLogs map[int]*zap.SugaredLogger, nodeID int, fallback *zap.SugaredLogger) *zap.SugaredLogger {
	if l, ok := nodeLogs[nodeID]; ok {
		return l
	}
	return fallback
}

// ── main loop ─────────────────────────────────────────────────────────────────

func Run(nodes []node.Node, stores map[int]*msgstore.Store, hostNode *node.Node, svc *service.TicketService, log *zap.SugaredLogger, nodeLogs map[int]*zap.SugaredLogger) error {
	pool := newConnPool(log)
	defer pool.closeAll()

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return fmt.Errorf("readline init: %w", err)
	}
	defer rl.Close()

	ctx := context.Background()

	for {
		separator()
		fmt.Println("  node messager — distributed ticket system")
		fmt.Println()
		fmt.Println("  ── messaging ──")
		fmt.Println("  1) send message")
		fmt.Println("  2) broadcast")
		fmt.Println("  3) messages per node")
		fmt.Println("  4) logs per node")
		fmt.Println("  5) list nodes")
		fmt.Println()
		fmt.Println("  ── ticket system ──")
		fmt.Println("  7) raise ticket")
		fmt.Println("  8) close ticket")
		fmt.Println("  9) list tickets")
		fmt.Println(" 10) add user")
		fmt.Println(" 11) add engineer")
		fmt.Println(" 12) add device")
		fmt.Println(" 13) list all users")
		fmt.Println(" 14) list all engineers")
		fmt.Println(" 15) list all devices")
		fmt.Println()
		fmt.Println("  6) quit")
		separator()

		choice := prompt(rl, "> ")
		if choice == "" {
			// EOF or Ctrl+D — treat as quit
			return nil
		}

		switch choice {

		case "1", "send":
			separator()
			fmt.Println("  send message")
			separator()
			var from node.Node
			var ok bool
			if hostNode != nil {
				from = *hostNode
				fmt.Printf("  from: %s\n\n", from.Name)
			} else {
				if from, ok = pickNode(rl, nodes, "  from node:"); !ok {
					continue
				}
			}
			targets := make([]node.Node, 0, len(nodes)-1)
			for _, n := range nodes {
				if n.ID != from.ID {
					targets = append(targets, n)
				}
			}
			to, ok := pickNode(rl, targets, "  to node:")
			if !ok {
				continue
			}
			fmt.Printf("\n  %s → %s\n", from.Name, to.Name)
			content := prompt(rl, "  message: ")
			if content == "" {
				fmt.Println("\n  error: message cannot be empty")
				continue
			}
			separator()
			if err := sendMsg(pool, from, to, content, stores, nodeLog(nodeLogs, from.ID, log)); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Println("  ✓ sent")
			}

		case "2", "broadcast":
			separator()
			fmt.Println("  broadcast")
			separator()
			var from node.Node
			var ok bool
			if hostNode != nil {
				from = *hostNode
				fmt.Printf("  from: %s → all nodes\n\n", from.Name)
			} else {
				if from, ok = pickNode(rl, nodes, "  from node:"); !ok {
					continue
				}
				fmt.Printf("\n  %s → all nodes\n", from.Name)
			}
			content := prompt(rl, "  message: ")
			if content == "" {
				fmt.Println("\n  error: message cannot be empty")
				continue
			}
			separator()
			targets := make([]node.Node, 0, len(nodes)-1)
			for _, n := range nodes {
				if n.ID != from.ID {
					targets = append(targets, n)
				}
			}
			if errs := broadcast(pool, from, targets, content, stores, nodeLog(nodeLogs, from.ID, log)); len(errs) > 0 {
				for _, e := range errs {
					fmt.Printf("  error: %s\n", e)
				}
			} else {
				fmt.Println("  ✓ broadcast sent")
			}

		case "3", "messages":
			separator()
			fmt.Println("  messages per node")
			separator()
			if hostNode != nil {
				fmt.Printf("  messages — %s\n\n", hostNode.Name)
				entries, _ := stores[hostNode.ID].Latest(50)
				printEntries(hostNode.Name, entries)
			} else {
				n, ok := pickNode(rl, nodes, "  select node:")
				if !ok {
					continue
				}
				separator()
				fmt.Printf("  messages — %s\n\n", n.Name)
				entries, _ := stores[n.ID].Latest(50)
				printEntries(n.Name, entries)
			}

		case "4", "logs":
			separator()
			fmt.Println("  logs per node")
			separator()
			if hostNode != nil {
				fmt.Printf("  logs — %s\n\n", hostNode.Name)
				tailLogFile(hostNode.Name, logLines)
			} else {
				n, ok := pickNode(rl, nodes, "  select node:")
				if !ok {
					continue
				}
				separator()
				fmt.Printf("  logs — %s\n\n", n.Name)
				tailLogFile(n.Name, logLines)
			}

		case "5", "list":
			separator()
			fmt.Println("  nodes")
			separator()
			for _, n := range nodes {
				fmt.Printf("  %-8s  %s:%d\n", n.Name, n.Host, n.Port)
			}

		case "7", "ticket", "raise":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  raise ticket")
			separator()
			idUsuarioStr := prompt(rl, "  usuario ID: ")
			idUsuario, err := strconv.Atoi(idUsuarioStr)
			if err != nil {
				fmt.Printf("  invalid usuario ID: %v\n", err)
				continue
			}
			idDispositivoStr := prompt(rl, "  dispositivo ID: ")
			idDispositivo, err := strconv.Atoi(idDispositivoStr)
			if err != nil {
				fmt.Printf("  invalid dispositivo ID: %v\n", err)
				continue
			}
			separator()
			fmt.Println("  raising ticket (acquiring lock + consensus)...")
			if err := svc.RaiseTicket(ctx, idUsuario, idDispositivo); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Println("  ✓ ticket raised")
			}

		case "8", "close":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  close ticket")
			separator()
			rows, err := svc.ListAll(ctx, "TICKETS")
			if err != nil {
				fmt.Printf("  error listing tickets: %v\n", err)
				continue
			}
			var open []dto.TicketRow
			for _, r := range rows {
				if t, ok := r.(dto.TicketRow); ok && t.Estado == "ABIERTO" {
					open = append(open, t)
				}
			}
			if len(open) == 0 {
				fmt.Println("  no open tickets")
				continue
			}
			fmt.Printf("  %-4s  %-14s  %-10s  %-10s  %s\n", "#", "TICKET ID", "INGENIERO", "DISPOSITIVO", "FOLIO")
			for i, t := range open {
				fmt.Printf("  %-4d  %-14d  %-10d  %-10d  %s\n", i+1, t.ID, t.IDIngeniero, t.IDDispositivo, t.Folio)
			}
			separator()
			raw := prompt(rl, "  selecciona # ticket: ")
			idx, err := strconv.Atoi(raw)
			if err != nil || idx < 1 || idx > len(open) {
				fmt.Println("  selección inválida")
				continue
			}
			selected := open[idx-1]
			separator()
			if err := svc.CloseTicket(ctx, int64(selected.ID), selected.IDIngeniero); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Printf("  ✓ ticket %d cerrado\n", selected.ID)
			}

		case "9", "tickets":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  all tickets")
			separator()
			rows, err := svc.ListAll(ctx, "TICKETS")
			if err != nil {
				fmt.Printf("  error: %v\n", err)
				continue
			}
			if len(rows) == 0 {
				fmt.Println("  no tickets yet")
				continue
			}
			fmt.Printf("  %-10s  %-10s  %-10s  %-10s  %-10s  %-8s  %s\n",
				"ID", "USUARIO", "INGENIERO", "SUCURSAL", "DISPOSITIVO", "ESTADO", "FOLIO")
			for _, r := range rows {
				if t, ok := r.(dto.TicketRow); ok {
					fmt.Printf("  %-10d  %-10d  %-10d  %-10d  %-10d  %-8s  %s\n",
						t.ID, t.IDUsuario, t.IDIngeniero, t.IDSucursal, t.IDDispositivo, t.Estado, t.Folio)
				}
			}

		case "10", "adduser":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  add user")
			separator()
			nombre := prompt(rl, "  nombre: ")
			if nombre == "" {
				fmt.Println("  error: nombre required")
				continue
			}
			if err := svc.AddUsuario(ctx, nombre); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Println("  ✓ usuario added")
			}

		case "11", "addengineer":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  add engineer")
			separator()
			nombre := prompt(rl, "  nombre: ")
			if nombre == "" {
				fmt.Println("  error: nombre required")
				continue
			}
			if err := svc.AddIngeniero(ctx, nombre); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Println("  ✓ ingeniero added")
			}

		case "12", "adddevice":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  add device (master distributes equitably)")
			separator()
			nombre := prompt(rl, "  nombre: ")
			if nombre == "" {
				fmt.Println("  error: nombre required")
				continue
			}
			tipo := prompt(rl, "  tipo: ")
			if tipo == "" {
				fmt.Println("  error: tipo required")
				continue
			}
			if err := svc.AddDevice(ctx, nombre, tipo); err != nil {
				fmt.Printf("  error: %v\n", err)
			} else {
				fmt.Println("  ✓ device queued for distribution")
			}

		case "13", "users":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  all users (distributed)")
			separator()
			rows, err := svc.ListAll(ctx, "USUARIOS")
			if err != nil {
				fmt.Printf("  error: %v\n", err)
				continue
			}
			if len(rows) == 0 {
				fmt.Println("  no users yet")
				continue
			}
			fmt.Printf("  %-10s  %-20s  %s\n", "ID", "NOMBRE", "SUCURSAL")
			for _, r := range rows {
				if u, ok := r.(dto.UsuarioRow); ok {
					fmt.Printf("  %-10d  %-20s  %d\n", u.ID, u.Nombre, u.SucursalID)
				}
			}

		case "14", "engineers":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  all engineers (distributed)")
			separator()
			rows, err := svc.ListAll(ctx, "INGENIEROS")
			if err != nil {
				fmt.Printf("  error: %v\n", err)
				continue
			}
			if len(rows) == 0 {
				fmt.Println("  no engineers yet")
				continue
			}
			fmt.Printf("  %-10s  %-20s  %-10s  %s\n", "ID", "NOMBRE", "SUCURSAL", "DISPONIBLE")
			for _, r := range rows {
				if e, ok := r.(dto.IngenieroRow); ok {
					disp := "SI"
					if e.Disponible == 0 {
						disp = "NO"
					}
					fmt.Printf("  %-10d  %-20s  %-10d  %s\n", e.ID, e.Nombre, e.SucursalID, disp)
				}
			}

		case "15", "devices":
			if svc == nil {
				fmt.Println("  ticket service not available")
				continue
			}
			separator()
			fmt.Println("  all devices (distributed)")
			separator()
			rows, err := svc.ListAll(ctx, "DISPOSITIVOS")
			if err != nil {
				fmt.Printf("  error: %v\n", err)
				continue
			}
			if len(rows) == 0 {
				fmt.Println("  no devices yet")
				continue
			}
			fmt.Printf("  %-10s  %-20s  %-15s  %-10s  %s\n", "ID", "NOMBRE", "TIPO", "SUCURSAL", "INGENIERO")
			for _, r := range rows {
				if d, ok := r.(dto.DispositivoRow); ok {
					fmt.Printf("  %-10d  %-20s  %-15s  %-10d  %d\n", d.ID, d.Nombre, d.Tipo, d.SucursalID, d.IngenieroID)
				}
			}

		case "6", "q", "quit", "exit":
			return nil

		default:
			fmt.Println("  unknown command")
		}
	}
}

