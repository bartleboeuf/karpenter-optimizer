package api

import (
	"context"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/karpenter-optimizer/internal/config"
	"github.com/karpenter-optimizer/internal/llmhealth"
	"github.com/karpenter-optimizer/internal/servingdurable"
	"github.com/karpenter-optimizer/internal/servinghistory"
)

// servingHistoryWindowMaxDurable is the longest serving-history window the
// API serves when the durable store is active (7 days).
const servingHistoryWindowMaxDurable = 7 * 24 * time.Hour

// initServingDurableStore returns a durable serving-history store when
// SERVING_HISTORY_DSN (or the reused gateway DSN parts) is set, else nil (the
// in-memory store alone serves the windows). A DSN that cannot be reached
// falls back to nil so serving history is never a hard dependency.
func initServingDurableStore(cfg *config.Config) servingdurable.Store {
	if cfg.ServingHistoryDSN == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ps, err := servingdurable.NewPostgres(ctx, cfg.ServingHistoryDSN, servingHistoryRetention())
	if err != nil {
		debugLog(cfg.Debug, "serving durable history: Postgres unavailable (%v); in-memory only\n", err)
		return nil
	}
	debugLog(cfg.Debug, "serving durable history: using Postgres store (retention %s)\n", servingHistoryRetention())
	return ps
}

// servingHistoryRetention reads SERVING_HISTORY_RETENTION_HOURS (default 168 = 7d).
func servingHistoryRetention() time.Duration {
	d := 7 * 24 * time.Hour
	if v := os.Getenv("SERVING_HISTORY_RETENTION_HOURS"); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			d = time.Duration(h) * time.Hour
		}
	}
	return d
}

// appendServingDurable records one sampler pass of raw serving samples into
// the durable store (no-op when it is not configured). Errors are logged and
// skipped — durable history must never break the in-memory fast path.
func (s *Server) appendServingDurable(health []llmhealth.PodHealth) {
	if s.servingDurableStore == nil {
		return
	}
	now := time.Now()
	pts := make([]servingdurable.Point, 0, len(health))
	for _, h := range health {
		pts = append(pts, servingdurable.NewPoint(
			h.Namespace, h.Name, now, h.Online, h.KVCachePercent, h.Running, h.Waiting,
			h.Preemptions, h.TTFTP95Seconds, h.E2EP95Seconds, h.TokensPerSec, h.RequestsPerSec,
		))
	}
	if err := s.servingDurableStore.Add(pts); err != nil {
		debugLog(s.config.Debug, "serving durable history: add: %v\n", err)
	}
}

// parseServingHistoryWindow parses a serving-history window (?window=).
// In-memory only, it is capped at maxHistoryWindow (24h). With the durable
// store active it accepts day-based durations (e.g. "7d") up to 7d.
func parseServingHistoryWindow(raw string, durable bool) time.Duration {
	if raw == "" {
		return defaultHistoryWindow
	}
	max := maxHistoryWindow
	if durable {
		max = servingHistoryWindowMaxDurable
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		if d > max {
			return max
		}
		return d
	}
	// Accept "Nd" day-based windows (Go's ParseDuration has no day unit).
	if len(raw) > 1 && (raw[len(raw)-1] == 'd' || raw[len(raw)-1] == 'D') {
		if days, err := strconv.Atoi(raw[:len(raw)-1]); err == nil && days > 0 {
			d := time.Duration(days) * 24 * time.Hour
			if d > max {
				return max
			}
			return d
		}
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
		d := time.Duration(secs) * time.Second
		if d > max {
			return max
		}
		return d
	}
	return defaultHistoryWindow
}

// mergeServingHistory merges the durable store's rows (closed period) with
// the in-memory series (which always carries the freshest samples) for one
// pod. On overlapping timestamps the in-memory point wins. The result is
// oldest first.
func mergeServingHistory(durable []servingdurable.Point, mem []servinghistory.Point, since time.Time) []servinghistory.Point {
	byUnix := make(map[int64]servinghistory.Point, len(durable)+len(mem))
	for _, d := range durable {
		if d.T.Before(since) {
			continue
		}
		byUnix[d.T.Unix()] = servinghistory.Point{
			T:              d.T.UTC().Format(time.RFC3339),
			Unix:           d.T.Unix(),
			Online:         d.Online,
			KVCachePercent: d.KVCachePercent,
			Running:        d.Running,
			Waiting:        d.Waiting,
			Preemptions:    d.Preemptions,
			TTFTP95Seconds: d.TTFTP95Seconds,
			E2EP95Seconds:  d.E2EP95Seconds,
			TokensPerSec:   d.TokensPerSec,
			RequestsPerSec: d.RequestsPerSec,
		}
	}
	// In-memory samples overwrite the durable copy at the same instant.
	for _, m := range mem {
		byUnix[m.Unix] = m
	}
	out := make([]servinghistory.Point, 0, len(byUnix))
	for _, p := range byUnix {
		out = append(out, p)
	}
	sortServingPoints(out)
	return out
}

func sortServingPoints(pts []servinghistory.Point) {
	sort.Slice(pts, func(i, j int) bool { return pts[i].Unix < pts[j].Unix })
}
