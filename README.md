# node_messager — Sistema Distribuido de Tickets de Soporte

Sistema TCP distribuido de 4 sucursales con consenso, exclusión mutua y elección de líder.

## Requisitos

- Go 1.21+
- No requiere CGO (SQLite puro en Go)

## nodes.json

**Modo local (pruebas — todos los nodos en 1 proceso):**
```json
{
  "master_id": 1,
  "nodes": [
    { "id": 1, "name": "sucursal1", "host": "localhost", "port": 5001 },
    { "id": 2, "name": "sucursal2", "host": "localhost", "port": 5002 },
    { "id": 3, "name": "sucursal3", "host": "localhost", "port": 5003 },
    { "id": 4, "name": "sucursal4", "host": "localhost", "port": 5004 }
  ]
}
```

**Modo VM (1 proceso por máquina) — solo cambia `host_id` en cada VM:**
```json
{
  "master_id": 1,
  "host_id": 2,
  "nodes": [
    { "id": 1, "name": "sucursal1", "host": "192.168.100.102", "port": 5001 },
    { "id": 2, "name": "sucursal2", "host": "192.168.100.103", "port": 5001 },
    { "id": 3, "name": "sucursal3", "host": "192.168.100.104", "port": 5001 },
    { "id": 4, "name": "sucursal4", "host": "192.168.100.105", "port": 5001 }
  ]
}
```

| Campo | Descripción |
|-------|------------|
| `master_id` | ID de la sucursal que arranca como maestro |
| `host_id` | ID de la sucursal que corre en esta máquina (omitir en modo local) |
| `nodes` | Lista de todas las sucursales con sus IPs y puertos |

## Ejecutar

```bash
# modo local
cp nodes-ejemplo.json nodes.json && go run ./cmd

# modo VM (dentro de cada VM, con nodes.json ya configurado)
go run ./cmd
```

## Setup en VMs

```bash
# dentro de cada VM — activa NTP y crea nodes.json
bash setup.sh <host_id>

# ejemplo en VM2:
bash setup.sh 2
```

## Archivos en runtime

```
data/<sucursal>.db        SQLite por nodo (gitignored)
logs/<sucursal>.log       Logs TCP (gitignored)
messages/<sucursal>.jsonl Historial mensajes (gitignored)
```

## Tests

```bash
go test ./...
```
