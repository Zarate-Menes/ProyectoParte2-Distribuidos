package dto

import "encoding/json"

type ProposePayload struct {
	RoundID   string `json:"round_id"`
	Operation string `json:"operation"`
	Data      string `json:"data"`
}

type VotePayload struct {
	RoundID string `json:"round_id"`
}

type CommitPayload struct {
	RoundID   string `json:"round_id"`
	Operation string `json:"operation"`
	Data      string `json:"data"`
}

type QueryPayload struct {
	Table        string `json:"table"`
	RequesterID  int    `json:"requester_id"`
	RequesterHost string `json:"requester_host"`
	RequesterPort int    `json:"requester_port"`
}

type QueryResponsePayload struct {
	Table    string          `json:"table"`
	Rows     json.RawMessage `json:"rows"`
	NodeID   int             `json:"node_id"`
}

type LockPayload struct {
	RequestID string `json:"request_id"`
	Resource  string `json:"resource"`
}

type ElectionPayload struct {
	CandidateID int `json:"candidate_id"`
}

type CoordinatorPayload struct {
	MasterID int `json:"master_id"`
}

type NodeDeadPayload struct {
	DeadNodeID int `json:"dead_node_id"`
}

type RedistributePayload struct {
	FromNodeID int           `json:"from_node_id"`
	Tickets    []TicketRow   `json:"tickets"`
}

type TicketRow struct {
	ID            int    `json:"id"`
	IDUsuario     int    `json:"id_usuario"`
	IDIngeniero   int    `json:"id_ingeniero"`
	IDSucursal    int    `json:"id_sucursal"`
	IDDispositivo int    `json:"id_dispositivo"`
	Estado        string `json:"estado"`
	Folio         string `json:"folio"`
	CreatedAt     string `json:"created_at"`
	ClosedAt      string `json:"closed_at"`
}

type IngenieroRow struct {
	ID         int    `json:"id"`
	Nombre     string `json:"nombre"`
	SucursalID int    `json:"sucursal_id"`
	Disponible int    `json:"disponible"`
}

type UsuarioRow struct {
	ID         int    `json:"id"`
	Nombre     string `json:"nombre"`
	SucursalID int    `json:"sucursal_id"`
}

type DispositivoRow struct {
	ID          int    `json:"id"`
	Nombre      string `json:"nombre"`
	Tipo        string `json:"tipo"`
	SucursalID  int    `json:"sucursal_id"`
	IngenieroID int    `json:"ingeniero_id"`
}

type AddDevicePayload struct {
	Nombre string `json:"nombre"`
	Tipo   string `json:"tipo"`
}

type DistributePayload struct {
	Device DispositivoRow `json:"device"`
}
