// Package servingdurable provides a durable Postgres backend for per-pod LLM
// serving history. It mirrors the gatewayhistory pattern: a sampler appends
// raw samples on each tick and a retention purge drops old rows. The in-memory
// servinghistory.Store remains the fast path for short sparklines; this store
// adds multi-day retention (7d) that survives process restarts and redeploys.
package servingdurable

import (
	"time"
)

// Point is a single raw serving sample for one pod. It mirrors
// servinghistory.Point but is keyed and timestamped for durable storage.
// Pointer fields are nil when the upstream metric was absent.
type Point struct {
	PodKey         string    // "namespace/name"
	T              time.Time // sample time
	Online         bool
	KVCachePercent *float64 // 0-100
	Running        int
	Waiting        int
	Preemptions    int64
	TTFTP95Seconds *float64
	E2EP95Seconds  *float64
	TokensPerSec   *float64
	RequestsPerSec *float64
}

// Store persists raw per-pod serving samples. Implementations must be safe
// for concurrent use. Add appends a batch of samples; Query returns all pods'
// samples at or after since, grouped by pod key, oldest first per pod.
// Callers log and degrade — the durable history is an enhancement, never a
// blocker.
type Store interface {
	// Add appends raw samples (upsert on pod_key, t).
	Add(points []Point) error
	// Query returns samples for all pods at or after since, grouped by pod
	// key, each slice oldest first.
	Query(since time.Time) (map[string][]Point, error)
	// Close releases resources (e.g. a DB pool).
	Close() error
}

// NewPoint converts a servinghistory.Point plus its pod key into a durable
// Point. The caller supplies the key ("namespace/name").
func NewPoint(namespace, name string, t time.Time, online bool, kv *float64, running, waiting int, preemptions int64, ttftP95, e2eP95, tps, rps *float64) Point {
	return Point{
		PodKey:         namespace + "/" + name,
		T:              t,
		Online:         online,
		KVCachePercent: kv,
		Running:        running,
		Waiting:        waiting,
		Preemptions:    preemptions,
		TTFTP95Seconds: ttftP95,
		E2EP95Seconds:  e2eP95,
		TokensPerSec:   tps,
		RequestsPerSec: rps,
	}
}
