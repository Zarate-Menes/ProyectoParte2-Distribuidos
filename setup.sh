
set -euo pipefail




# ── NTP ───────────────────────────────────────────────────────────────────────

echo "=== Activando NTP ==="
sudo timedatectl set-ntp true
timedatectl status | grep "NTP service" || true
echo "  ✓ NTP activo"

# ── directorios ───────────────────────────────────────────────────────────────

echo ""
echo "=== Creando directorios ==="
mkdir -p data logs messages tickets
echo "  ✓ directorios listos"


# ── listo ─────────────────────────────────────────────────────────────────────

echo ""
echo "=== VM${HOST_ID} lista ==="
echo "Iniciar nodo: go run ./cmd"
