set -euo pipefail

echo "=== Activando NTP ==="
timedatectl set-ntp true
timedatectl status | grep "NTP service" || true
echo "  ✓ NTP activo"

echo ""
echo "=== Creando directorios ==="
mkdir -p data logs messages tickets
echo "  ✓ directorios listos"

echo "Iniciar nodo: go run ./cmd"