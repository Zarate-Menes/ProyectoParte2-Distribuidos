package node

// Node representa un nodo del cluster con su informacion de red
// se usa para saber a donde mandar los mensajes TCP
type Node struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}
