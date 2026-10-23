package api

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/karpenter-optimizer/internal/config"
	"github.com/karpenter-optimizer/internal/gatewayhistory"
	"github.com/karpenter-optimizer/internal/litellm"
)

// gwCounter is the cumulative (dim, value) spend + tokens snapshot the sampler
// keeps between ticks so it can diff into per-interval deltas.
type gwCounter struct {
	spend  float64
	tokens int64
}

// startGatewayHistorySampler launches a background goroutine that, on the
// history interval, probes the LiteLLM gateway, diffs the cumulative
// spend/token counters since the last tick, and folds the deltas into the
// gateway history store. A nil store or k8s client makes it a no-op.
func (s *Server) startGatewayHistorySampler(ctx context.Context) {
	if s.k8sClient == nil || s.gatewayHistoryStore == nil {
		return
	}
	interval := historyIntervalFromEnv()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			s.sampleGateway()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// sampleGateway probes the gateway and records one interval's worth of spend
// and tokens as daily deltas. Counter resets (a replica restart) clamp the
// diff to 0 rather than recording a negative/absurd value; a fresh series is
// baselined on its first sighting and starts accumulating on the next tick.
func (s *Server) sampleGateway() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pods, err := s.k8sClient.FindLiteLLMPods(ctx)
	if err != nil {
		debugLog(s.config.Debug, "gateway sampler: find pods: %v\n", err)
		return
	}
	if len(pods) == 0 {
		// No gateway in the cluster: drop the baseline so a later re-deploy
		// starts fresh rather than diffing against a stale cumulative counter.
		s.gwMu.Lock()
		s.lastGateway = nil
		s.gwMu.Unlock()
		s.recordGatewayMetrics(nil) // clear the /metrics gateway section
		return
	}

	refs := make([]litellm.PodRef, 0, len(pods))
	for _, p := range pods {
		refs = append(refs, litellm.PodRef{IP: p.IP, Port: p.Port})
	}
	h := litellmProber.ProbeAll(ctx, refs)
	// Publish the same probe to the /metrics snapshot (no-op when disabled).
	s.recordGatewayMetrics(&h)
	snap := gatewaySnapshot(&h)

	s.gwMu.Lock()
	prev := s.lastGateway
	s.lastGateway = snap
	s.gwMu.Unlock()

	deltas := diffGateway(prev, snap)
	if len(deltas) > 0 {
		if err := s.gatewayHistoryStore.Add(deltas); err != nil {
			debugLog(s.config.Debug, "gateway sampler: store add: %v\n", err)
		}
	}
}

// gatewaySnapshot flattens a GatewayHealth into cumulative (dim, value) spend
// and token counters. The total series' tokens are the sum of the app split
// (which partitions all traffic); spend is the fleet's cumulative spend.
func gatewaySnapshot(h *litellm.GatewayHealth) map[string]gwCounter {
	out := make(map[string]gwCounter)
	if h == nil {
		return out
	}
	if h.Spend != nil {
		out["total|"] = gwCounter{spend: *h.Spend, tokens: sumSplitTokens(h.Apps)}
	}
	for _, a := range h.Apps {
		if a.Spend == nil {
			continue
		}
		out[gwKey("app", a.Name)] = gwCounter{spend: *a.Spend, tokens: a.TokensTotal}
	}
	for _, k := range h.APIKeys {
		if k.Spend == nil {
			continue
		}
		out[gwKey("apikey", k.Name)] = gwCounter{spend: *k.Spend, tokens: k.TokensTotal}
	}
	for _, t := range h.Teams {
		if t.Spend == nil {
			continue
		}
		out[gwKey("team", t.Name)] = gwCounter{spend: *t.Spend, tokens: t.TokensTotal}
	}
	return out
}

func sumSplitTokens(ss []litellm.SplitStat) int64 {
	var t int64
	for _, s := range ss {
		t += s.TokensTotal
	}
	return t
}

func gwKey(dim, value string) string { return dim + "|" + value }

func gwKeyDim(key string) (dim, value string) {
	if i := strings.IndexByte(key, '|'); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}

// diffGateway turns two successive snapshots into per-series deltas, clamping
// any counter reset (negative diff) to zero. Series not present in the
// previous snapshot are baselined only (no delta), so the very first sighting
// never records the entire cumulative value.
func diffGateway(prev, cur map[string]gwCounter) []gatewayhistory.Delta {
	now := time.Now()
	var out []gatewayhistory.Delta
	for key, c := range cur {
		p, ok := prev[key]
		if !ok {
			continue
		}
		dSpend := c.spend - p.spend
		dTokens := c.tokens - p.tokens
		if dSpend < 0 {
			dSpend = 0
		}
		if dTokens < 0 {
			dTokens = 0
		}
		if dSpend == 0 && dTokens == 0 {
			continue
		}
		dim, value := gwKeyDim(key)
		out = append(out, gatewayhistory.Delta{Dim: dim, Value: value, Spend: dSpend, Tokens: dTokens, TS: now})
	}
	return out
}

// initGatewayHistoryStore returns a durable Postgres store when
// GATEWAY_HISTORY_DSN is set (CNPG), otherwise the in-memory store. A DSN that
// cannot be reached falls back to in-memory so history is never a hard
// dependency on the gateway UI.
func initGatewayHistoryStore(cfg *config.Config) (gatewayhistory.Store, string, error) {
	retention := gatewayHistoryRetention()
	if dsn := cfg.GatewayHistoryDSN; dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		ps, err := gatewayhistory.NewPostgres(ctx, dsn, retention)
		cancel()
		if err != nil {
			debugLog(cfg.Debug, "gateway history: Postgres unavailable (%v); using in-memory store\n", err)
			return gatewayhistory.NewMemStore(retention), "memory", err
		}
		debugLog(cfg.Debug, "gateway history: using Postgres store (retention %s)\n", retention)
		return ps, "postgres", nil
	}
	return gatewayhistory.NewMemStore(retention), "memory", nil
}

func gatewayHistoryRetention() time.Duration {
	d := 30 * 24 * time.Hour
	if v := os.Getenv("GATEWAY_HISTORY_RETENTION_HOURS"); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			d = time.Duration(h) * time.Hour
		}
	}
	return d
}

// getServingGatewayHistory returns per-day spend and token totals for the
// requested split dimension and window, for the Serving tab's cost chart.
//
// @Summary      LiteLLM gateway cost history
// @Description  Per-day spend and token totals for the gateway, split by the
// given dimension (total/app/apikey/team). Backed by Postgres when
// GATEWAY_HISTORY_DSN is set, otherwise an in-memory store.
// @Tags         serving
// @Produce      json
// @Success      200 {object} object "Daily gateway cost/token series"
// @Failure      503 {object} map[string]interface{} "Kubernetes client not configured"
// @Router       /serving/gateway/history [get]
func (s *Server) getServingGatewayHistory(c *gin.Context) {
	if s.gatewayHistoryStore == nil {
		c.JSON(http.StatusOK, gin.H{"window": "", "split": "", "store": "none", "days": []interface{}{}})
		return
	}
	split := c.Query("split")
	if !gatewayhistory.ValidDim(split) {
		split = gatewayhistory.DimApp
	}
	window := parseGatewayHistoryWindow(c.Query("window"))
	since := time.Now().UTC().Add(-window)

	rows, err := s.gatewayHistoryStore.Daily(split, since)
	if err != nil {
		debugLog(s.config.Debug, "gateway history: daily: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if rows == nil {
		rows = []gatewayhistory.DayRow{}
	}
	c.JSON(http.StatusOK, gin.H{
		"window": window.String(),
		"split":  split,
		"store":  s.gatewayHistoryStoreName,
		"days":   rows,
	})
}

// parseGatewayHistoryWindow parses a gateway-history window (default 14d,
// max 90d). Unlike parseHistoryWindow it accepts day-based durations (e.g.
// "7d") for multi-day cost trends.
func parseGatewayHistoryWindow(raw string) time.Duration {
	const def = 14 * 24 * time.Hour
	const max = 90 * 24 * time.Hour
	if raw == "" {
		return def
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		if d > max {
			return max
		}
		return d
	}
	// Go's ParseDuration has no day unit; accept "Nd".
	if len(raw) > 1 && (raw[len(raw)-1] == 'd' || raw[len(raw)-1] == 'D') {
		if days, err := strconv.Atoi(raw[:len(raw)-1]); err == nil && days > 0 {
			d := time.Duration(days) * 24 * time.Hour
			if d > max {
				return max
			}
			return d
		}
	}
	if days, err := strconv.Atoi(raw); err == nil && days > 0 {
		d := time.Duration(days) * 24 * time.Hour
		if d > max {
			return max
		}
		return d
	}
	return def
}
