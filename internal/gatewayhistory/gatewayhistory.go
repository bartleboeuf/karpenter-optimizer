// Package gatewayhistory stores per-day LiteLLM gateway cost and token usage,
// broken down by a split dimension (app / API key / team) plus a fleet total.
//
// It is fed by a sampler that, on each tick, diffs the gateway's cumulative
// spend/token counters into per-interval deltas and folds them into per-day
// running totals. Two backends implement Store: an in-memory ring (default,
// resets on process restart) and Postgres (durable, enabled via DSN).
package gatewayhistory

import (
	"time"
)

// Split dimensions the gateway cost history is tracked by. "total" is the
// fleet-wide aggregate (value is empty).
const (
	DimTotal  = "total"
	DimApp    = "app"
	DimAPIKey = "apikey"
	DimTeam   = "team"
)

// Delta is one interval's increment (since the previous sample) of spend and
// tokens for a single (dimension, value) series. The store folds deltas into
// per-day running totals; it is the unit the sampler writes.
type Delta struct {
	Dim    string  // DimTotal/DimApp/DimAPIKey/DimTeam
	Value  string  // empty for DimTotal; app/apikey/team name otherwise
	Spend  float64 // USD added in this interval
	Tokens int64   // tokens added in this interval
	TS     time.Time
}

// DayRow is one (day, dimension, value) aggregate: the total spend and tokens
// attributed to that value on that UTC day.
type DayRow struct {
	Day    string  `json:"day"` // "2006-01-02" (UTC)
	DayTS  string  `json:"dayTs"`
	Value  string  `json:"value"` // empty for DimTotal
	Spend  float64 `json:"spend"`
	Tokens int64   `json:"tokens"`
}

// Store persists per-day gateway cost/token deltas. Implementations must be
// safe for concurrent use. Add folds a batch of interval deltas into the
// per-day totals; Daily returns the (day, value) rows for one dimension within
// the window. A nil/empty error result is not expected; callers log and
// degrade (the history is an enhancement, never a blocker).
type Store interface {
	// Add folds interval deltas into per-day running totals.
	Add(deltas []Delta) error
	// Daily returns per-day, per-value totals for dim, oldest first, for days
	// at or after since (UTC).
	Daily(dim string, since time.Time) ([]DayRow, error)
	// Close releases resources (e.g. a DB pool).
	Close() error
}

// dayKey renders a UTC calendar-day key (2006-01-02) for a timestamp.
func dayKey(ts time.Time) string {
	return ts.UTC().Format("2006-01-02")
}

// validDim reports whether dim is a known split dimension.
func validDim(dim string) bool {
	switch dim {
	case DimTotal, DimApp, DimAPIKey, DimTeam:
		return true
	}
	return false
}

// ValidDim reports whether dim is a known split dimension (DimTotal, DimApp,
// DimAPIKey or DimTeam).
func ValidDim(dim string) bool { return validDim(dim) }
