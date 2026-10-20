package gatewayhistory

import (
	"testing"
	"time"
)

// TestMemStoreDailyAggregates folds deltas into per-day, per-value totals and
// returns them ordered oldest-first.
func TestMemStoreDailyAggregates(t *testing.T) {
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	s := NewMemStore(14 * 24 * time.Hour)
	s.SetNow(func() time.Time { return base.Add(24 * time.Hour) })

	deltas := []Delta{
		{Dim: DimTotal, Value: "", Spend: 1.0, Tokens: 1000, TS: base},
		{Dim: DimTotal, Value: "", Spend: 2.0, Tokens: 2000, TS: base.Add(time.Hour)},
		{Dim: DimApp, Value: "chat", Spend: 0.5, Tokens: 100, TS: base},
		{Dim: DimApp, Value: "search", Spend: 0.7, Tokens: 50, TS: base.Add(time.Hour)},
		{Dim: DimApp, Value: "chat", Spend: 0.5, Tokens: 100, TS: base.Add(24*time.Hour + time.Hour)},
	}
	if err := s.Add(deltas); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Fleet total on the first day = 3.0 USD / 3000 tokens.
	total, err := s.Daily(DimTotal, base.Add(-time.Minute))
	if err != nil {
		t.Fatalf("Daily(total): %v", err)
	}
	if len(total) != 1 {
		t.Fatalf("expected 1 total row, got %d", len(total))
	}
	if total[0].Day != "2026-10-18" || total[0].Spend != 3.0 || total[0].Tokens != 3000 {
		t.Fatalf("unexpected total row: %+v", total[0])
	}

	// App rows: chat@18, search@18, chat@19 = 3 distinct (day, value) pairs.
	apps, err := s.Daily(DimApp, base.Add(-time.Minute))
	if err != nil {
		t.Fatalf("Daily(app): %v", err)
	}
	if len(apps) != 3 {
		t.Fatalf("expected 3 app rows, got %d", len(apps))
	}
	if apps[0].Day != "2026-10-18" || apps[0].Value != "chat" || apps[0].Spend != 0.5 {
		t.Fatalf("first app row: %+v", apps[0])
	}
}

// TestMemStoreDailySinceFilter drops days older than the window.
func TestMemStoreDailySinceFilter(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	s := NewMemStore(90 * 24 * time.Hour)
	s.SetNow(func() time.Time { return base.Add(3 * 24 * time.Hour) })
	_ = s.Add([]Delta{
		{Dim: DimTeam, Value: "ml", Spend: 1, Tokens: 10, TS: base},
		{Dim: DimTeam, Value: "ml", Spend: 2, Tokens: 20, TS: base.Add(2 * 24 * time.Hour)},
	})
	rows, err := s.Daily(DimTeam, base.Add(24*time.Hour)) // since 2026-10-02
	if err != nil {
		t.Fatalf("Daily: %v", err)
	}
	if len(rows) != 1 || rows[0].Day != "2026-10-03" {
		t.Fatalf("expected only the 2026-10-03 row, got %+v", rows)
	}
}

// TestMemStorePrune drops rows older than the retention window.
func TestMemStorePrune(t *testing.T) {
	base := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	s := NewMemStore(7 * 24 * time.Hour)
	s.SetNow(func() time.Time { return base.Add(10 * 24 * time.Hour) }) // cutoff = base+3d
	_ = s.Add([]Delta{
		{Dim: DimApp, Value: "old", Spend: 1, Tokens: 1, TS: base},
		{Dim: DimApp, Value: "new", Spend: 1, Tokens: 1, TS: base.Add(9 * 24 * time.Hour)},
	})
	rows, _ := s.Daily(DimApp, base.Add(-time.Minute))
	if len(rows) != 1 || rows[0].Value != "new" {
		t.Fatalf("expected only the recent row, got %+v", rows)
	}
}

// TestMemStoreDailyInvalidDim returns empty for an unknown dimension.
func TestMemStoreDailyInvalidDim(t *testing.T) {
	s := NewMemStore(7 * 24 * time.Hour)
	rows, err := s.Daily("nope", time.Now().Add(-time.Hour))
	if err != nil || rows != nil {
		t.Fatalf("expected nil rows, got %+v / %v", rows, err)
	}
}
