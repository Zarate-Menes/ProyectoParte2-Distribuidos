package config

import (
	"fmt"

	kjson "github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"node_messager/pkg/node"
)

type Config struct {
	Nodes    []node.Node // todas las sucursales
	HostNode *node.Node  // esto es el nodo actual, para evitar llamarse a si mismo
	MasterID int         // esto es el ID del nodo maestro
}

// LoadConfig carga el archivo nodes.json para cargar la informacion de todos los nodos
func LoadConfig(path string) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), kjson.Parser()); err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := k.Unmarshal("nodes", &cfg.Nodes); err != nil {
		return Config{}, err
	}
	//  revisamos si en el archivo exist master_id
	// si no existe ponemos el primer nodo como el nodo maestro
	// en caso de no tener explicitamente un master_id
	cfg.MasterID = k.Int("master_id")
	if cfg.MasterID == 0 && len(cfg.Nodes) > 0 {
		cfg.MasterID = cfg.Nodes[0].ID
	}

	// buscamos cual es el host_id del nodo host, si no existe lanzamos un error
	if k.Exists("host_id") {
		hostID := k.Int("host_id")
		n := nodeByID(cfg.Nodes, hostID)
		if n == nil {
			return Config{}, fmt.Errorf("host_id %d not found in nodes list", hostID)
		}
		cfg.HostNode = n
	}
	return cfg, nil
}

// busca el nodo con el id dado, regresa null si no encontro nada
func nodeByID(nodes []node.Node, id int) *node.Node {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}
