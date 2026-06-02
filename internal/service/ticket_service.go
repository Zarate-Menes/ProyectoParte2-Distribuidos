package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"node_messager/internal/consensus"
	"node_messager/internal/db"
	"node_messager/internal/mutex"
	"node_messager/internal/nodestate"
	"node_messager/pkg/dto"
	"node_messager/pkg/node"
	"node_messager/pkg/sender"

	"go.uber.org/zap"
)

const queryTimeout = 3 * time.Second

// idCounter es una secuencia para generar un ID
// para un usuario el ID se genera con  NodeID * 10_000_000_000 + counter, esto para generar id unicos en cada nodo
// esto nos ganrantiza que no exista dos usuarios en dos nodos con el mismo ID
// NOTA: esto esta ganrantizo solo para un pequeño numero de IDs, esta solucion
// No escala correctamente para sistemas grandes
var idCounter = time.Now().UnixMilli()%1_000_000_000 + int64(rand.Intn(1000))

// newID regresa el ID para que no haga colicion en los demas nodos
func newID(nodeID int) int {
	n := atomic.AddInt64(&idCounter, 1)
	return nodeID*10_000_000_000 + int(n)
}

// SetMinIDCounter asegura que el contador arranque despues del ultimo ID del seed
// evita que newID genere IDs que ya existen en la base de datos
func SetMinIDCounter(min int64) {
	for {
		cur := atomic.LoadInt64(&idCounter)
		if cur >= min {
			return
		}
		if atomic.CompareAndSwapInt64(&idCounter, cur, min) {
			return
		}
	}
}

type TicketService struct {
	self      node.Node
	state     *nodestate.State
	db        *db.DB
	pool      *sender.Pool
	consensus *consensus.Engine
	mutex     *mutex.Engine
	log       *zap.SugaredLogger

	// queryMu nos ayuda a hacer un lock en memoria
	queryMu   sync.Mutex
	queryWait map[string]*queryCollector
}

// queryCollector agrupa las respuestas de una consulta distribuida
// expected indica cuantas respuestas esperamos (una por peer vivo)
// cuando results llega a expected, cerramos done para desbloquear ListAll
type queryCollector struct {
	table    string
	expected int
	results  []dto.QueryResponsePayload
	done     chan struct{}
}

// New crea un nuevo TicketService con todos sus engines
func New(
	self node.Node,
	state *nodestate.State,
	database *db.DB,
	pool *sender.Pool,
	cons *consensus.Engine,
	mtx *mutex.Engine,
	log *zap.SugaredLogger,
) *TicketService {
	return &TicketService{
		self:      self,
		state:     state,
		db:        database,
		pool:      pool,
		consensus: cons,
		mutex:     mtx,
		log:       log,
		queryWait: make(map[string]*queryCollector),
	}
}

// AddUsuario añade un usuario a la base de datos del nodo actual
func (s *TicketService) AddUsuario(ctx context.Context, nombre string) error {
	id := newID(s.self.ID)
	row := dto.UsuarioRow{ID: id, Nombre: nombre, SucursalID: s.self.ID}
	data, _ := json.Marshal(row)
	return s.consensus.Propose(ctx, "INSERT_USUARIO", string(data))
}

// AddIngeniero añade un ingeniero a la base de datos del nodo actual
// ademas de ponerlo en estado "disponible"
func (s *TicketService) AddIngeniero(ctx context.Context, nombre string) error {
	id := newID(s.self.ID)
	row := dto.IngenieroRow{ID: id, Nombre: nombre, SucursalID: s.self.ID, Disponible: 1}
	data, _ := json.Marshal(row)
	return s.consensus.Propose(ctx, "INSERT_INGENIERO", string(data))
}

// AddDevice manda el dispositivo al maestro para que lo distribuya equitativamente
// si somos el maestro, lo distribuimos directamente
func (s *TicketService) AddDevice(ctx context.Context, nombre, tipo string) error {
	if s.state.IsMaster() {
		return s.distributeDevice(ctx, nombre, tipo)
	}
	// send to master to handle distribution
	p := dto.AddDevicePayload{Nombre: nombre, Tipo: tipo}
	master := s.state.GetMasterNode()
	if master == nil {
		return fmt.Errorf("no master node available")
	}
	return s.pool.SendJSON(s.self, *master, dto.TypeAddDevice, p)
}

// distributeDevice asigna el dispositivo al ingeniero con menos dispositivos asignados
// esto solo lo ejecuta el maestro para garantizar distribucion equitativa
func (s *TicketService) distributeDevice(ctx context.Context, nombre, tipo string) error {
	// query all engineers across all nodes
	engRows, err := s.ListAll(ctx, "INGENIEROS")
	if err != nil {
		return fmt.Errorf("query engineers: %w", err)
	}
	if len(engRows) == 0 {
		return fmt.Errorf("no engineers available to assign device")
	}

	// count devices per engineer across all nodes
	devRows, err := s.ListAll(ctx, "DISPOSITIVOS")
	if err != nil {
		return fmt.Errorf("query devices: %w", err)
	}
	counts := make(map[int]int)
	for _, r := range devRows {
		if dev, ok := r.(dto.DispositivoRow); ok && dev.IngenieroID != 0 {
			counts[dev.IngenieroID]++
		}
	}

	// pick engineer with fewest devices
	var chosen *dto.IngenieroRow
	minCount := -1
	for _, r := range engRows {
		if eng, ok := r.(dto.IngenieroRow); ok {
			c := counts[eng.ID]
			if minCount < 0 || c < minCount {
				minCount = c
				cp := eng
				chosen = &cp
			}
		}
	}
	if chosen == nil {
		return fmt.Errorf("no engineer selected")
	}

	// device lives on the same sucursal as the engineer
	id := newID(chosen.SucursalID)
	row := dto.DispositivoRow{
		ID:          id,
		Nombre:      nombre,
		Tipo:        tipo,
		SucursalID:  chosen.SucursalID,
		IngenieroID: chosen.ID,
	}
	data, _ := json.Marshal(row)
	s.log.Infof("[service] assigning device %q to engineer %d (sucursal %d, current_devices=%d)",
		nombre, chosen.ID, chosen.SucursalID, minCount)
	return s.consensus.Propose(ctx, "INSERT_DISPOSITIVO", string(data))
}

// ── Ticket operations ─────────────────────────────────────────────────────────

const (
	raiseMaxRetries = 3
	raiseRetryDelay = 2 * time.Second
)

// RaiseTicket crea un nuevo ticket con exclusion mutua y consenso
// reintenta hasta raiseMaxRetries veces si falla para tolerar fallos transitorios
func (s *TicketService) RaiseTicket(ctx context.Context, idUsuario, idDispositivo int) error {
	var lastErr error
	for attempt := 1; attempt <= raiseMaxRetries; attempt++ {
		lastErr = s.raiseTicket(ctx, idUsuario, idDispositivo)
		if lastErr == nil {
			return nil
		}
		s.log.Warnf("[service] RaiseTicket attempt=%d/%d failed: %v",
			attempt, raiseMaxRetries, lastErr)
		if attempt < raiseMaxRetries {
			select {
			case <-time.After(raiseRetryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	s.log.Errorf("[service] RaiseTicket failed after %d attempts: %v",
		raiseMaxRetries, lastErr)
	return lastErr
}

// raiseTicket es la logica interna de crear un ticket:
// 1. adquiere el lock de exclusion mutua para evitar doble asignacion
// 2. busca un ingeniero disponible en todos los nodos
// 3. crea el ticket via consenso
// 4. genera el archivo de folio
// 5. marca al ingeniero como no disponible via consenso
func (s *TicketService) raiseTicket(ctx context.Context, idUsuario, idDispositivo int) error {
	release, err := s.mutex.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer release()

	// find available engineer across all nodes
	allRows, err := s.ListAll(ctx, "INGENIEROS")
	if err != nil {
		return fmt.Errorf("list engineers: %w", err)
	}

	var chosen *dto.IngenieroRow
	for _, r := range allRows {
		if ing, ok := r.(dto.IngenieroRow); ok && ing.Disponible == 1 {
			cp := ing
			chosen = &cp
			break
		}
	}
	if chosen == nil {
		return fmt.Errorf("no available engineer")
	}

	// pre-assign ticket ID so folio can be generated
	ticketID := newID(s.self.ID)
	ticket := dto.TicketRow{
		ID:            ticketID,
		IDUsuario:     idUsuario,
		IDIngeniero:   chosen.ID,
		IDSucursal:    s.self.ID,
		IDDispositivo: idDispositivo,
		Estado:        "ABIERTO",
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	data, _ := json.Marshal(ticket)
	if err := s.consensus.Propose(ctx, "INSERT_TICKET", string(data)); err != nil {
		return fmt.Errorf("consensus insert ticket: %w", err)
	}

	folio := fmt.Sprintf("%d-%d-%d-%d", idUsuario, chosen.ID, s.self.ID, ticketID)

	// persist folio in DB via consensus
	type folioUpdate struct {
		IDTicket int    `json:"id_ticket"`
		Folio    string `json:"folio"`
	}
	fu := folioUpdate{IDTicket: ticketID, Folio: folio}
	fuData, _ := json.Marshal(fu)
	if err := s.consensus.Propose(ctx, "UPDATE_TICKET_FOLIO", string(fuData)); err != nil {
		s.log.Warnf("[service] update ticket folio: %v", err)
	}

	// mark engineer unavailable
	engUpdate := dto.IngenieroRow{ID: chosen.ID, Disponible: 0}
	upData, _ := json.Marshal(engUpdate)
	if err := s.consensus.Propose(ctx, "UPDATE_INGENIERO_DISPONIBLE", string(upData)); err != nil {
		s.log.Warnf("[service] failed to mark engineer unavailable: %v", err)
	}

	s.log.Infof("[service] ticket raised: usuario=%d ingeniero=%d sucursal=%d folio=%s",
		idUsuario, chosen.ID, s.self.ID, folio)
	return nil
}

// CloseTicket cierra un ticket y libera al ingeniero asignado via consenso
// el ingeniero queda disponible para recibir nuevos tickets
func (s *TicketService) CloseTicket(ctx context.Context, idTicket int64, idIngeniero int) error {
	type closeData struct {
		IDTicket    int64 `json:"id_ticket"`
		IDIngeniero int   `json:"id_ingeniero"`
	}
	p := closeData{IDTicket: idTicket, IDIngeniero: idIngeniero}
	data, _ := json.Marshal(p)
	return s.consensus.Propose(ctx, "CLOSE_TICKET", string(data))
}

// RedistributeTickets reasigna los tickets abiertos de un nodo caido a ingenieros disponibles
// solo lo ejecuta el maestro cuando detecta que un nodo murio
func (s *TicketService) RedistributeTickets(ctx context.Context, deadNodeID int) {
	allTickets, err := s.ListAll(ctx, "TICKETS")
	if err != nil {
		s.log.Errorf("[service] redistribute: query tickets: %v", err)
		return
	}
	var tickets []dto.TicketRow
	for _, r := range allTickets {
		if t, ok := r.(dto.TicketRow); ok && t.Estado == "ABIERTO" && t.IDSucursal == deadNodeID {
			tickets = append(tickets, t)
		}
	}
	if len(tickets) == 0 {
		return
	}

	allRows, err := s.ListAll(ctx, "INGENIEROS")
	if err != nil {
		s.log.Errorf("[service] redistribute: list engineers: %v", err)
		return
	}

	var available []dto.IngenieroRow
	for _, r := range allRows {
		if ing, ok := r.(dto.IngenieroRow); ok && ing.Disponible == 1 && ing.SucursalID != deadNodeID {
			available = append(available, ing)
		}
	}

	for i, t := range tickets {
		if len(available) == 0 {
			s.log.Warnf("[service] redistribute: no more engineers available, %d tickets unassigned", len(tickets)-i)
			break
		}
		eng := available[0]
		available = available[1:]

		row := dto.TicketRow{
			ID:            t.ID,
			IDUsuario:     t.IDUsuario,
			IDIngeniero:   eng.ID,
			IDSucursal:    eng.SucursalID,
			IDDispositivo: t.IDDispositivo,
			Estado:        "ABIERTO",
			CreatedAt:     t.CreatedAt,
		}
		data, _ := json.Marshal(row)
		if err := s.consensus.Propose(ctx, "REASSIGN_TICKET", string(data)); err != nil {
			s.log.Errorf("[service] redistribute ticket %d: %v", t.ID, err)
		}
	}
}

// ── Cross-node queries ────────────────────────────────────────────────────────

// ListAll manda QUERY a todos los peers y combina sus respuestas con los datos locales
// es la forma de obtener todos los datos de una tabla distribuida entre los nodos
func (s *TicketService) ListAll(ctx context.Context, table string) ([]any, error) {
	queryID := fmt.Sprintf("%s-%d-%d", table, s.self.ID, time.Now().UnixNano())
	peers := s.state.AlivePeers()

	col := &queryCollector{
		table:    table,
		expected: len(peers),
		done:     make(chan struct{}),
	}
	s.queryMu.Lock()
	s.queryWait[queryID] = col
	s.queryMu.Unlock()

	defer func() {
		s.queryMu.Lock()
		delete(s.queryWait, queryID)
		s.queryMu.Unlock()
	}()

	p := dto.QueryPayload{
		Table:         table,
		RequesterID:   s.self.ID,
		RequesterHost: s.self.Host,
		RequesterPort: s.self.Port,
	}
	// embed query ID in the table field so responders can route back
	p.Table = queryID + "|" + table
	if broadcastErrs := s.pool.BroadcastJSON(s.self, peers, dto.TypeQuery, p); len(broadcastErrs) > 0 {
		// ajustamos expected por los nodos que no recibieron el query
		// si no lo hacemos, esperamos hasta timeout por respuestas que nunca llegan
		s.queryMu.Lock()
		col.expected -= len(broadcastErrs)
		if len(col.results) >= col.expected {
			select {
			case <-col.done:
			default:
				close(col.done)
			}
		}
		s.queryMu.Unlock()
		for id, err := range broadcastErrs {
			s.log.Warnf("[service] query broadcast to node %d failed: %v", id, err)
		}
	}

	// collect local rows
	local, err := s.localRows(table)
	if err != nil {
		return nil, err
	}

	// wait for remote responses
	if len(peers) > 0 {
		select {
		case <-col.done:
			s.log.Infof("[service] query %s: recibidas %d/%d respuestas", table, len(col.results), col.expected)
		case <-time.After(queryTimeout):
			s.log.Warnf("[service] query %s: timeout — solo %d/%d respuestas recibidas, verifica conectividad entre nodos",
				table, len(col.results), col.expected)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// merge
	result := append([]any{}, local...)
	s.queryMu.Lock()
	for _, resp := range col.results {
		rows, err := unmarshalRows(table, resp.Rows)
		if err == nil {
			result = append(result, rows...)
		}
	}
	s.queryMu.Unlock()
	return result, nil
}

// HandleQuery responde a un QUERY de otro nodo con las filas locales de esa tabla
func (s *TicketService) HandleQuery(msg dto.Message) {
	var p dto.QueryPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	// parse queryID|table
	queryID, table := splitQueryID(p.Table)

	rows, err := s.localRows(table)
	if err != nil {
		s.log.Warnf("[service] handle query %s: %v", table, err)
		return
	}

	rowData, _ := json.Marshal(rows)
	resp := dto.QueryResponsePayload{
		Table:  queryID + "|" + table,
		Rows:   rowData,
		NodeID: s.self.ID,
	}

	requester := s.state.NodeByID(p.RequesterID)
	if requester == nil {
		s.log.Warnf("[service] handle query: requester node %d not found", p.RequesterID)
		return
	}
	if err := s.pool.SendJSON(s.self, *requester, dto.TypeQueryResponse, resp); err != nil {
		s.log.Warnf("[service] handle query: send response to node %d: %v", p.RequesterID, err)
	}
}

// HandleQueryResponse recibe la respuesta de un peer y la agrega al queryCollector
// cuando llegaron todas las respuestas esperadas, cierra el canal done para desbloquear ListAll
func (s *TicketService) HandleQueryResponse(msg dto.Message) {
	var p dto.QueryResponsePayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	queryID, _ := splitQueryID(p.Table)

	s.queryMu.Lock()
	col, ok := s.queryWait[queryID]
	if !ok {
		s.log.Warnf("[service] query_response: queryID %q no encontrado — respuesta tarde o routing incorrecto (from=%s)",
			queryID, msg.FromNode)
	} else {
		col.results = append(col.results, p)
		if len(col.results) >= col.expected {
			select {
			case <-col.done:
			default:
				close(col.done)
			}
		}
	}
	s.queryMu.Unlock()
}

// HandleAddDevice se llama en el maestro cuando un nodo no-maestro quiere agregar un dispositivo
// el maestro es quien decide a que ingeniero asignarlo para distribucion equitativa
func (s *TicketService) HandleAddDevice(ctx context.Context, msg dto.Message) {
	if !s.state.IsMaster() {
		return
	}
	var p dto.AddDevicePayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		return
	}
	if err := s.distributeDevice(ctx, p.Nombre, p.Tipo); err != nil {
		s.log.Errorf("[service] distribute device: %v", err)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// localRows consulta la base de datos local y regresa las filas de la tabla pedida
func (s *TicketService) localRows(table string) ([]any, error) {
	switch table {
	case "INGENIEROS":
		rows, err := s.db.GetIngenieros()
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rows {
			out = append(out, dto.IngenieroRow{ID: r.ID, Nombre: r.Nombre, SucursalID: r.SucursalID, Disponible: r.Disponible})
		}
		return out, nil
	case "USUARIOS":
		rows, err := s.db.GetUsuarios()
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rows {
			out = append(out, dto.UsuarioRow{ID: r.ID, Nombre: r.Nombre, SucursalID: r.SucursalID})
		}
		return out, nil
	case "DISPOSITIVOS":
		rows, err := s.db.GetDispositivos()
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rows {
			out = append(out, dto.DispositivoRow{ID: r.ID, Nombre: r.Nombre, Tipo: r.Tipo, SucursalID: r.SucursalID, IngenieroID: r.IngenieroID})
		}
		return out, nil
	case "TICKETS":
		rows, err := s.db.GetTickets()
		if err != nil {
			return nil, err
		}
		var out []any
		for _, t := range rows {
			out = append(out, dto.TicketRow{
				ID: int(t.ID), IDUsuario: t.IDUsuario, IDIngeniero: t.IDIngeniero,
				IDSucursal: t.IDSucursal, IDDispositivo: t.IDDispositivo,
				Estado: t.Estado, Folio: t.Folio, CreatedAt: t.CreatedAt, ClosedAt: t.ClosedAt,
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unknown table: %s", table)
}

// unmarshalRows convierte el JSON de una respuesta QUERY_RESPONSE al tipo correcto segun la tabla
func unmarshalRows(table string, raw json.RawMessage) ([]any, error) {
	switch table {
	case "INGENIEROS":
		var rows []dto.IngenieroRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		out := make([]any, len(rows))
		for i, r := range rows {
			out[i] = r
		}
		return out, nil
	case "USUARIOS":
		var rows []dto.UsuarioRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		out := make([]any, len(rows))
		for i, r := range rows {
			out[i] = r
		}
		return out, nil
	case "DISPOSITIVOS":
		var rows []dto.DispositivoRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		out := make([]any, len(rows))
		for i, r := range rows {
			out[i] = r // IngenieroID included via JSON tag
		}
		return out, nil
	case "TICKETS":
		var rows []dto.TicketRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
		out := make([]any, len(rows))
		for i, r := range rows {
			out[i] = r
		}
		return out, nil
	}
	return nil, fmt.Errorf("unknown table: %s", table)
}

// splitQueryID separa el queryID de la tabla en el formato "queryID|tabla"
// usamos este formato para que el peer sepa a cual ListAll responder
func splitQueryID(raw string) (queryID, table string) {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '|' {
			return raw[:i], raw[i+1:]
		}
	}
	return "", raw
}
