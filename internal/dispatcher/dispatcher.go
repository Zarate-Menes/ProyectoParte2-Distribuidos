package dispatcher

import (
	"context"
	"encoding/json"

	"node_messager/internal/consensus"
	"node_messager/internal/election"
	"node_messager/internal/heartbeat"
	"node_messager/internal/mutex"
	"node_messager/internal/nodestate"
	"node_messager/internal/service"
	"node_messager/pkg/dto"

	"go.uber.org/zap"
)

// NodeDispatcher es una estructura que nos ayuda a controlar que accions se lleva a cabo
// y por quien, por lo que recibe un mensaje(accion) y dependiendo de que mensaje(accion)
// recibe , este llama un handler correcto.
type NodeDispatcher struct {
	// nos ayuda a conocer el estado del nodo
	state *nodestate.State
	// garantiza que una escritura sea aceptada por la mayoria de nodos antes de ejecutarse
	consensus *consensus.Engine
	// exclusion mutua distribuida entre nodos — evita que dos sucursales
	// asignen el mismo ingeniero a dos tickets al mismo tiempo
	mutex *mutex.Engine
	// nos ayuda a elegir un nuevo nodo maestro cuando el actual falla
	election *election.Engine
	// nos ayuda a constantemente saber el estado de salud de los nodos
	// en particular si el nodo maestro esta arriba
	heartbeat *heartbeat.Monitor
	// servicio de tickets, nos ayuda a crear, actualizar o buscar tickets
	svc *service.TicketService
	log *zap.SugaredLogger
	ctx context.Context
}

// New crea un nuevo NodeDispatcher con todos los engines necesarios para manejar los mensajes
func New(
	ctx context.Context,
	state *nodestate.State,
	cons *consensus.Engine,
	mtx *mutex.Engine,
	elec *election.Engine,
	hb *heartbeat.Monitor,
	svc *service.TicketService,
	log *zap.SugaredLogger,
) *NodeDispatcher {
	return &NodeDispatcher{
		state:     state,
		consensus: cons,
		mutex:     mtx,
		election:  elec,
		heartbeat: hb,
		svc:       svc,
		log:       log,
		ctx:       ctx,
	}
}

// Dispatch recibe cada mensaje TCP que llega al nodo y lo envia al handler correcto
// segun el tipo de mensaje — es el unico punto de entrada para todos los mensajes distribuidos
func (d *NodeDispatcher) Dispatch(msg dto.Message) {
	switch msg.Type {
	case dto.TypePing:
		d.heartbeat.HandlePing(msg)
	case dto.TypePong:
		d.heartbeat.HandlePong(msg)

	case dto.TypePropose:
		d.consensus.HandlePropose(msg)
	case dto.TypeVoteYes, dto.TypeVoteNo:
		d.consensus.HandleVote(msg)
	case dto.TypeCommit:
		d.consensus.HandleCommit(msg)

	case dto.TypeLockRequest:
		d.mutex.HandleLockRequest(msg)
	case dto.TypeLockGrant:
		d.mutex.HandleLockGrant(msg)
	case dto.TypeLockDeny:
		d.mutex.HandleLockDeny(msg)
	case dto.TypeLockRelease:
		d.mutex.HandleLockRelease(msg)

	case dto.TypeElection:
		d.election.HandleElection(msg)
	case dto.TypeElectionOK:
		d.election.HandleElectionOK(msg)
	case dto.TypeCoordinator:
		d.election.HandleCoordinator(msg)

	case dto.TypeQuery:
		d.svc.HandleQuery(msg)
	case dto.TypeQueryResponse:
		d.svc.HandleQueryResponse(msg)

	case dto.TypeAddDevice:
		go d.svc.HandleAddDevice(d.ctx, msg)

	case dto.TypeNodeDead:
		if d.state.IsMaster() {
			var p dto.NodeDeadPayload
			if err := json.Unmarshal([]byte(msg.Content), &p); err == nil {
				go d.svc.RedistributeTickets(d.ctx, p.DeadNodeID)
			}
		}

	default:
		// si no es ningun tipo de mensaje de arriba, ignoramos este mensaje
	}
}
