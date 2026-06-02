package consensus

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

const voteTimeout = 3 * time.Second

const (
	proposeMaxRetries = 3
	proposeRetryDelay = 1 * time.Second
)

type round struct {
	roundID   string
	operation string
	data      string
	needed    int
	yes       int
	no        int
	done      chan result
}

type result struct {
	committed bool
	err       error
}

// CommitHandler is called on every node when a COMMIT is received.
type CommitHandler func(operation, data string) error

type Engine struct {
	self    node.Node
	state   *nodestate.State
	pool    *sender.Pool
	log     *zap.SugaredLogger
	mu      sync.Mutex
	rounds  map[string]*round
	handler CommitHandler
}

func New(self node.Node, state *nodestate.State, pool *sender.Pool, log *zap.SugaredLogger, handler CommitHandler) *Engine {
	return &Engine{
		self:    self,
		state:   state,
		pool:    pool,
		log:     log,
		rounds:  make(map[string]*round),
		handler: handler,
	}
}

// Propose broadcasts a PROPOSE to all alive peers, waits for quorum, then broadcasts COMMIT.
// Retries up to proposeMaxRetries times on transient failure.
func (e *Engine) Propose(ctx context.Context, operation, data string) error {
	var lastErr error
	for attempt := 1; attempt <= proposeMaxRetries; attempt++ {
		lastErr = e.propose(ctx, operation, data)
		if lastErr == nil {
			return nil
		}
		e.log.Warnf("[consensus] Propose op=%s attempt=%d/%d failed: %v",
			operation, attempt, proposeMaxRetries, lastErr)
		if attempt < proposeMaxRetries {
			select {
			case <-time.After(proposeRetryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	e.log.Errorf("[consensus] Propose op=%s failed after %d attempts: %v",
		operation, proposeMaxRetries, lastErr)
	return lastErr
}

func (e *Engine) propose(ctx context.Context, operation, data string) error {
	peers := e.state.AlivePeers()
	total := len(e.state.All())
	needed := total/2 + 1
	if needed < 1 {
		needed = 1
	}

	roundID := uuid.New().String()
	r := &round{
		roundID:   roundID,
		operation: operation,
		data:      data,
		needed:    needed,
		done:      make(chan result, 1),
	}

	e.mu.Lock()
	e.rounds[roundID] = r
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.rounds, roundID)
		e.mu.Unlock()
	}()

	payload := dto.ProposePayload{RoundID: roundID, Operation: operation, Data: data}
	e.pool.BroadcastJSON(e.self, peers, dto.TypePropose, payload)

	// self votes yes immediately
	e.mu.Lock()
	r.yes++
	committed := r.yes >= r.needed
	e.mu.Unlock()

	if committed {
		return e.doCommit(roundID, operation, data, peers)
	}

	select {
	case res := <-r.done:
		if !res.committed {
			return fmt.Errorf("consensus: no quorum (yes=%d needed=%d)", r.yes, r.needed)
		}
		return e.doCommit(roundID, operation, data, peers)
	case <-time.After(voteTimeout):
		return fmt.Errorf("consensus: timeout waiting for votes")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) doCommit(roundID, operation, data string, peers []node.Node) error {
	payload := dto.CommitPayload{RoundID: roundID, Operation: operation, Data: data}
	e.pool.BroadcastJSON(e.self, peers, dto.TypeCommit, payload)
	// apply locally
	if e.handler != nil {
		return e.handler(operation, data)
	}
	return nil
}

// HandleVote processes incoming VOTE_YES / VOTE_NO from peers.
func (e *Engine) HandleVote(msg dto.Message) {
	var p dto.VotePayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		e.log.Warnf("[consensus] bad vote payload: %v", err)
		return
	}

	e.mu.Lock()
	r, ok := e.rounds[p.RoundID]
	if !ok {
		e.mu.Unlock()
		return
	}
	if msg.Type == dto.TypeVoteYes {
		r.yes++
	} else {
		r.no++
	}
	if r.yes >= r.needed {
		select {
		case r.done <- result{committed: true}:
		default:
		}
	}
	e.mu.Unlock()
}

// HandlePropose is called when this node receives a PROPOSE. Always votes yes.
func (e *Engine) HandlePropose(msg dto.Message) {
	var p dto.ProposePayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		e.log.Warnf("[consensus] bad propose payload: %v", err)
		return
	}

	// look up proposer node to reply to
	proposerNode := e.state.NodeByName(msg.FromNode)
	if proposerNode == nil {
		e.log.Warnf("[consensus] unknown proposer: %s", msg.FromNode)
		return
	}

	vote := dto.VotePayload{RoundID: p.RoundID}
	if err := e.pool.SendJSON(e.self, *proposerNode, dto.TypeVoteYes, vote); err != nil {
		e.log.Warnf("[consensus] send vote to %s: %v", proposerNode.Name, err)
	}
}

// HandleCommit is called when this node receives a COMMIT. Applies the operation locally.
func (e *Engine) HandleCommit(msg dto.Message) {
	var p dto.CommitPayload
	if err := json.Unmarshal([]byte(msg.Content), &p); err != nil {
		e.log.Warnf("[consensus] bad commit payload: %v", err)
		return
	}
	if e.handler != nil {
		if err := e.handler(p.Operation, p.Data); err != nil {
			e.log.Errorf("[consensus] commit handler error op=%s: %v", p.Operation, err)
		}
	}
}
