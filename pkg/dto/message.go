package dto

const (
	TypeMsg       = "MSG"
	TypeBroadcast = "BROADCAST"

	TypePing = "PING"
	TypePong = "PONG"

	TypePropose   = "PROPOSE"
	TypeVoteYes   = "VOTE_YES"
	TypeVoteNo    = "VOTE_NO"
	TypeCommit    = "COMMIT"
	TypeCommitAck = "COMMIT_ACK"

	TypeLockRequest = "LOCK_REQUEST"
	TypeLockGrant   = "LOCK_GRANT"
	TypeLockRelease = "LOCK_RELEASE"
	TypeLockDeny    = "LOCK_DENY"

	TypeElection    = "ELECTION"
	TypeElectionOK  = "ELECTION_OK"
	TypeCoordinator = "COORDINATOR"

	TypeQuery         = "QUERY"
	TypeQueryResponse = "QUERY_RESPONSE"

	TypeAssignTicket = "ASSIGN_TICKET"
	TypeCloseTicket  = "CLOSE_TICKET"

	TypeAddDevice  = "ADD_DEVICE"
	TypeDistribute = "DISTRIBUTE"

	TypeNodeDead     = "NODE_DEAD"
	TypeRedistribute = "REDISTRIBUTE"
)

type Message struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	Content  string `json:"content"`
	SendAt   string `json:"send_at"`
}
