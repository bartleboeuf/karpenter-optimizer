package api

import (
	"testing"

	"github.com/karpenter-optimizer/internal/litellm"
)

func ptr(v float64) *float64 { return &v }

// TestGatewaySnapshot flattens a health into (dim, value) cumulative counters,
// using the app-split token sum for the fleet-total series.
func TestGatewaySnapshot(t *testing.T) {
	h := &litellm.GatewayHealth{
		Spend: ptr(10.0),
		Apps: []litellm.SplitStat{
			{Name: "chat", Spend: ptr(4.0), TokensTotal: 100},
			{Name: "search", Spend: ptr(6.0), TokensTotal: 50},
		},
		APIKeys: []litellm.SplitStat{{Name: "svc-a", Spend: ptr(2.0), TokensTotal: 30}},
		Teams:   []litellm.SplitStat{{Name: "ml", Spend: ptr(9.0), TokensTotal: 120}},
	}
	snap := gatewaySnapshot(h)

	if c := snap["total|"]; c.spend != 10.0 || c.tokens != 150 {
		t.Fatalf("total series = %+v, want spend 10 tokens 150", c)
	}
	if c := snap["app|chat"]; c.spend != 4.0 || c.tokens != 100 {
		t.Fatalf("app|chat = %+v", c)
	}
	if c := snap["apikey|svc-a"]; c.spend != 2.0 {
		t.Fatalf("apikey|svc-a = %+v", c)
	}
	if c := snap["team|ml"]; c.spend != 9.0 {
		t.Fatalf("team|ml = %+v", c)
	}
	if _, ok := snap["app|missing"]; ok {
		t.Fatalf("should not contain absent series")
	}
}

// TestDiffGateway computes per-series deltas from successive snapshots.
func TestDiffGateway(t *testing.T) {
	prev := map[string]gwCounter{
		"total|":    {spend: 10, tokens: 150},
		"app|chat":  {spend: 4, tokens: 100},
		"apikey|k1": {spend: 2, tokens: 10},
		"team|ml":   {spend: 9, tokens: 120},
	}
	cur := map[string]gwCounter{
		"total|":     {spend: 12, tokens: 200},
		"app|chat":   {spend: 4, tokens: 100}, // unchanged -> no delta
		"apikey|k1":  {spend: 2, tokens: 10},  // unchanged -> no delta
		"team|ml":    {spend: 9.5, tokens: 125},
		"app|search": {spend: 1, tokens: 5}, // new series -> baselined only
	}
	deltas := diffGateway(prev, cur)

	got := map[string]gwCounter{}
	for _, d := range deltas {
		got[d.Dim+"|"+d.Value] = gwCounter{spend: d.Spend, tokens: d.Tokens}
	}
	if d := got["total|"]; d.spend != 2 || d.tokens != 50 {
		t.Fatalf("total delta = %+v, want 2/50", d)
	}
	if d := got["team|ml"]; d.spend != 0.5 || d.tokens != 5 {
		t.Fatalf("team|ml delta = %+v, want 0.5/5", d)
	}
	if _, ok := got["app|chat"]; ok {
		t.Fatalf("unchanged series should produce no delta")
	}
	if _, ok := got["app|search"]; ok {
		t.Fatalf("new series should be baselined, not diffed")
	}
}

// TestDiffGatewayClampsReset clamps a counter reset (value drops) to zero, so
// no (negative or spurious) delta is recorded for that series.
func TestDiffGatewayClampsReset(t *testing.T) {
	prev := map[string]gwCounter{"app|chat": {spend: 100, tokens: 10000}}
	cur := map[string]gwCounter{"app|chat": {spend: 0.5, tokens: 10}} // replica restarted
	deltas := diffGateway(prev, cur)
	if len(deltas) != 0 {
		t.Fatalf("expected no delta after a full reset, got %+v", deltas)
	}
	for _, d := range deltas {
		if d.Spend < 0 || d.Tokens < 0 {
			t.Fatalf("negative delta after reset: %+v", d)
		}
	}
}
