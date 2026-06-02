package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	d := &DB{sql: sqlDB}
	if err := d.migrate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// ── Migrations ────────────────────────────────────────────────────────────────

func TestMigrate_SetsLatestSchemaVersion(t *testing.T) {
	d := openTestDB(t)
	v, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}
	want := migrations[len(migrations)-1].version
	if v != want {
		t.Fatalf("want version %d, got %d", want, v)
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	d := openTestDB(t)
	before, _ := d.Version()
	if err := d.migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	after, _ := d.Version()
	if after != before {
		t.Fatalf("version changed after second migrate: %d → %d", before, after)
	}
}

func TestMigrate_CreatesAllTables(t *testing.T) {
	d := openTestDB(t)
	tables := []string{"INGENIEROS", "USUARIOS", "DISPOSITIVOS", "TICKETS"}
	for _, tbl := range tables {
		row := d.sql.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl)
		var name string
		if err := row.Scan(&name); err != nil {
			t.Errorf("table %s not found: %v", tbl, err)
		}
	}
}

// ── INGENIEROS ────────────────────────────────────────────────────────────────

func TestInsertIngeniero_GetIngenieros_RoundTrip(t *testing.T) {
	d := openTestDB(t)
	if err := d.InsertIngeniero(1, "Ana", 10); err != nil {
		t.Fatal(err)
	}
	rows, err := d.GetIngenieros()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
	r := rows[0]
	if r.ID != 1 || r.Nombre != "Ana" || r.SucursalID != 10 || r.Disponible != 1 {
		t.Fatalf("unexpected row: %+v", r)
	}
}

func TestInsertIngeniero_IgnoreDuplicate(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertIngeniero(1, "Ana", 10)
	_ = d.InsertIngeniero(1, "Ana-dup", 99) // same ID → ignored
	rows, _ := d.GetIngenieros()
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

func TestGetIngenieroDisponible_ReturnsAvailable(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertIngeniero(5, "Luis", 1)
	ing, err := d.GetIngenieroDisponible()
	if err != nil {
		t.Fatal(err)
	}
	if ing == nil || ing.ID != 5 {
		t.Fatalf("expected ingeniero 5, got %v", ing)
	}
}

func TestGetIngenieroDisponible_ReturnsNilWhenNone(t *testing.T) {
	d := openTestDB(t)
	ing, err := d.GetIngenieroDisponible()
	if err != nil {
		t.Fatal(err)
	}
	if ing != nil {
		t.Fatalf("expected nil, got %+v", ing)
	}
}

func TestSetIngenieroDisponible_TogglesFlag(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertIngeniero(3, "Carlos", 1)
	_ = d.SetIngenieroDisponible(3, 0)
	ing, _ := d.GetIngenieroDisponible()
	if ing != nil {
		t.Fatal("expected no available engineer")
	}
	_ = d.SetIngenieroDisponible(3, 1)
	ing, _ = d.GetIngenieroDisponible()
	if ing == nil {
		t.Fatal("expected available engineer after re-enable")
	}
}

func TestCountIngenieros(t *testing.T) {
	d := openTestDB(t)
	n, _ := d.CountIngenieros()
	if n != 0 {
		t.Fatalf("empty DB: want 0, got %d", n)
	}
	_ = d.InsertIngeniero(1, "A", 1)
	_ = d.InsertIngeniero(2, "B", 1)
	n, _ = d.CountIngenieros()
	if n != 2 {
		t.Fatalf("want 2, got %d", n)
	}
}

// ── USUARIOS ──────────────────────────────────────────────────────────────────

func TestInsertUsuario_GetUsuarios_RoundTrip(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertUsuario(10, "Beatriz", 2)
	_ = d.InsertUsuario(11, "Diego", 2)
	rows, err := d.GetUsuarios()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	ids := map[int]bool{rows[0].ID: true, rows[1].ID: true}
	if !ids[10] || !ids[11] {
		t.Fatalf("unexpected IDs: %+v", rows)
	}
}

// ── DISPOSITIVOS ──────────────────────────────────────────────────────────────

func TestInsertDispositivo_GetDispositivos_RoundTrip(t *testing.T) {
	d := openTestDB(t)
	if err := d.InsertDispositivo(7, "Laptop-001", "Laptop", 3, 42); err != nil {
		t.Fatal(err)
	}
	rows, err := d.GetDispositivos()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
	r := rows[0]
	if r.ID != 7 || r.Nombre != "Laptop-001" || r.Tipo != "Laptop" || r.SucursalID != 3 || r.IngenieroID != 42 {
		t.Fatalf("unexpected: %+v", r)
	}
}

func TestCountDispositivos(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertDispositivo(1, "A", "T", 1, 10)
	_ = d.InsertDispositivo(2, "B", "T", 1, 10)
	_ = d.InsertDispositivo(3, "C", "T", 1, 20)
	n, _ := d.CountDispositivos()
	if n != 3 {
		t.Fatalf("want 3, got %d", n)
	}
}

func TestCountDispositivosByIngeniero(t *testing.T) {
	d := openTestDB(t)
	_ = d.InsertDispositivo(1, "A", "T", 1, 10)
	_ = d.InsertDispositivo(2, "B", "T", 1, 10)
	_ = d.InsertDispositivo(3, "C", "T", 1, 20)
	n, _ := d.CountDispositivosByIngeniero(10)
	if n != 2 {
		t.Fatalf("want 2 for engineer 10, got %d", n)
	}
	n, _ = d.CountDispositivosByIngeniero(20)
	if n != 1 {
		t.Fatalf("want 1 for engineer 20, got %d", n)
	}
}

// ── TICKETS ───────────────────────────────────────────────────────────────────

func TestInsertTicket_GetTickets_RoundTrip(t *testing.T) {
	d := openTestDB(t)
	id, err := d.InsertTicket(1, 2, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ticket ID")
	}
	rows, err := d.GetTickets()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 ticket, got %d", len(rows))
	}
	tk := rows[0]
	if tk.Estado != "ABIERTO" || tk.Folio != "" || tk.ClosedAt != "" {
		t.Fatalf("unexpected ticket state: %+v", tk)
	}
}

func TestInsertTicketFull_IgnoresDuplicate(t *testing.T) {
	d := openTestDB(t)
	tk := Ticket{ID: 99, IDUsuario: 1, IDIngeniero: 2, IDSucursal: 3, IDDispositivo: 4, Estado: "ABIERTO", CreatedAt: "2026-01-01T00:00:00Z"}
	_ = d.InsertTicketFull(tk)
	_ = d.InsertTicketFull(tk) // duplicate → ignored
	rows, _ := d.GetTickets()
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
}

func TestUpdateTicketFolio(t *testing.T) {
	d := openTestDB(t)
	id, _ := d.InsertTicket(1, 2, 3, 4)
	if err := d.UpdateTicketFolio(id, "1-2-3-4"); err != nil {
		t.Fatal(err)
	}
	rows, _ := d.GetTickets()
	if rows[0].Folio != "1-2-3-4" {
		t.Fatalf("folio not updated: %q", rows[0].Folio)
	}
}

func TestCloseTicket(t *testing.T) {
	d := openTestDB(t)
	id, _ := d.InsertTicket(1, 2, 3, 4)
	if err := d.CloseTicket(id); err != nil {
		t.Fatal(err)
	}
	rows, _ := d.GetTickets()
	if rows[0].Estado != "CERRADO" {
		t.Fatalf("want CERRADO, got %q", rows[0].Estado)
	}
	if rows[0].ClosedAt == "" {
		t.Fatal("closed_at not set")
	}
}

func TestGetOpenTicketsBySucursal_FiltersCorrectly(t *testing.T) {
	d := openTestDB(t)
	// insert ticket for sucursal=1 (open) and sucursal=2 (open)
	tk1 := Ticket{ID: 1, IDUsuario: 1, IDIngeniero: 1, IDSucursal: 1, IDDispositivo: 1, Estado: "ABIERTO", CreatedAt: "2026-01-01T00:00:00Z"}
	tk2 := Ticket{ID: 2, IDUsuario: 2, IDIngeniero: 2, IDSucursal: 2, IDDispositivo: 2, Estado: "ABIERTO", CreatedAt: "2026-01-01T00:00:00Z"}
	_ = d.InsertTicketFull(tk1)
	_ = d.InsertTicketFull(tk2)
	_ = d.CloseTicket(1) // close sucursal=1 ticket

	open1, _ := d.GetOpenTicketsBySucursal(1)
	if len(open1) != 0 {
		t.Fatalf("sucursal=1: want 0 open, got %d", len(open1))
	}
	open2, _ := d.GetOpenTicketsBySucursal(2)
	if len(open2) != 1 {
		t.Fatalf("sucursal=2: want 1 open, got %d", len(open2))
	}
}
