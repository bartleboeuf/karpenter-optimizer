package gatewayhistory

import (
	"sort"
	"sync"
	"time"
)

// MemStore is an in-memory, time-bounded Store. It resets on process restart
// and is used when no GATEWAY_HISTORY_DSN is configured.
type MemStore struct {
	mu        sync.RWMutex
	daily     map[string]*dayAcc // key: dim|value|day
	retention time.Duration
	now       func() time.Time
}

type dayAcc struct {
	dim    string
	value  string
	day    string
	spend  float64
	tokens int64
}

// NewMemStore creates a Store retaining rows for at least window.
func NewMemStore(window time.Duration) *MemStore {
	if window <= 0 {
		window = 14 * 24 * time.Hour
	}
	return &MemStore{
		daily:     make(map[string]*dayAcc),
		retention: window,
		now:       time.Now,
	}
}

// SetNow overrides the clock (tests).
func (m *MemStore) SetNow(fn func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = fn
}

func accKey(dim, value, day string) string {
	return dim + "|" + value + "|" + day
}

// Add folds each delta into its UTC day's running total for the series.
func (m *MemStore) Add(deltas []Delta) error {
	if len(deltas) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, d := range deltas {
		if !validDim(d.Dim) {
			continue
		}
		day := dayKey(d.TS)
		key := accKey(d.Dim, d.Value, day)
		acc := m.daily[key]
		if acc == nil {
			acc = &dayAcc{dim: d.Dim, value: d.Value, day: day}
			m.daily[key] = acc
		}
		acc.spend += d.Spend
		acc.tokens += d.Tokens
	}
	m.pruneLocked(now)
	return nil
}

// Daily returns per-day totals for dim, oldest first, for days at/after since.
func (m *MemStore) Daily(dim string, since time.Time) ([]DayRow, error) {
	if !validDim(dim) {
		return nil, nil
	}
	sinceDay := dayKey(since)
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]DayRow, 0)
	for _, acc := range m.daily {
		if acc.dim != dim || acc.day < sinceDay {
			continue
		}
		out = append(out, DayRow{
			Day:    acc.day,
			DayTS:  acc.day + "T00:00:00Z",
			Value:  acc.value,
			Spend:  acc.spend,
			Tokens: acc.tokens,
		})
	}
	sortDayRows(out)
	return out, nil
}

func (m *MemStore) Close() error { return nil }

func (m *MemStore) pruneLocked(now time.Time) {
	cutoff := dayKey(now.Add(-m.retention))
	for key, acc := range m.daily {
		if acc.day < cutoff {
			delete(m.daily, key)
		}
	}
}

// sortDayRows orders rows by day ascending, then value, so the UI can plot a
// stable multi-series line chart.
func sortDayRows(rows []DayRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Day != rows[j].Day {
			return rows[i].Day < rows[j].Day
		}
		return rows[i].Value < rows[j].Value
	})
}
