// Package alerts implements a small, opt-in alerting engine on top of the
// cluster/serving/gateway data the server already samples. It evaluates a set
// of conditions each tick, drives them through a pending -> firing -> cleared
// state machine (with a held-clear window so flapping does not spam), and
// fans the result out to configured channels (ntfy, Discord, Slack, or a
// generic JSON webhook). All outbound URLs are SSRF-validated and pinned to a
// resolved IP; channel URLs are treated as credentials and masked in the API.
package alerts

import "time"

// Severity ranks an alert for channel formatting. warning is the default;
// critical is reserved for conditions that directly impact serving.
type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Event status: what transition produced the outbound message.
const (
	StatusFiring   = "firing"
	StatusResolved = "resolved"
	StatusReminder = "reminder"
	StatusTest     = "test"
)

// Condition IDs — stable strings used as state keys and in messages.
const (
	CondCostSpike         = "cost_spike"
	CondDisruptionBlocked = "disruption_blocked"
	CondServingPressure   = "serving_pressure"
	CondGateway           = "gateway"
	CondServingOffline    = "serving_offline"
)

// Alert is one fired (or resolved) condition, included in a channel payload.
type Alert struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Message  string   `json:"message"`
	Severity Severity `json:"severity"`
	FiredAt  string   `json:"firedAt,omitempty"` // RFC3339; empty for resolved
}

// WebhookPayload is the generic JSON body POSTed to webhook channels.
type WebhookPayload struct {
	Source   string  `json:"source"`
	Status   string  `json:"status"`
	Severity string  `json:"severity"`
	Title    string  `json:"title"`
	Alerts   []Alert `json:"alerts"`
}

// ConditionConfig gates one condition. For is the duration the condition must
// hold before it fires (and, after firing, must stay clear before resolving).
type ConditionConfig struct {
	Enabled bool   `json:"enabled"`
	For     string `json:"for"` // duration, e.g. "5m"; min 30s
	// Per-condition thresholds.
	CostSpikePct         float64 `json:"costSpikePct,omitempty"`         // cost up >X% vs 24h avg
	DisruptionBlockedMin int     `json:"disruptionBlockedMin,omitempty"` // blocked >N min
	KVCachePct           float64 `json:"kvCachePct,omitempty"`           // serving KV cache >=X%
	GatewayErrorRatePct  float64 `json:"gatewayErrorRatePct,omitempty"`  // gateway error rate >X%
	GatewayRejected      int64   `json:"gatewayRejected,omitempty"`      // cumulative 503s >N
}

// ChannelConfig is one outbound channel. URL is a credential: it is never
// returned by GET /alerts/config (masked) and is treated as secret.
type ChannelConfig struct {
	Type string `json:"type"` // ntfy | discord | slack | webhook
	URL  string `json:"url"`
	// ntfy-specific
	Priority int    `json:"priority,omitempty"` // 1..5, ntfy priority
	Tags     string `json:"tags,omitempty"`     // comma-separated ntfy tags
}

// Config is the full alerting configuration.
type Config struct {
	Enabled bool `json:"enabled"`
	// RemindWhileFiring is an optional repeat interval ("1h"/"4h"/"12h"/"24h");
	// empty disables reminders.
	RemindWhileFiring string          `json:"remindWhileFiring,omitempty"`
	CostSpike         ConditionConfig `json:"costSpike"`
	DisruptionBlocked ConditionConfig `json:"disruptionBlocked"`
	ServingPressure   ConditionConfig `json:"servingPressure"`
	Gateway           ConditionConfig `json:"gateway"`
	ServingOffline    ConditionConfig `json:"servingOffline"`
	Channels          []ChannelConfig `json:"channels"`
}

// Snapshot is the point-in-time cluster state the evaluator reads each tick.
// The server fills it from values it already has (history store, disruption
// list, serving/gateway probe caches) so an alert tick adds no extra AWS or
// heavy K8s fan-out beyond the disruption list.
type Snapshot struct {
	Time time.Time

	// Cost-spike inputs.
	CostPerHour     float64 // current hourly cost
	Cost24hAverage  float64 // mean hourly cost over the trailing 24h
	HasCostBaseline bool    // true when enough history exists to compare

	// Disruption inputs.
	DisruptionBlockedFor time.Duration // longest time a node has been blocked (0 = none)

	// Serving inputs.
	ServingKVCacheMax float64       // max KV-cache % across pods (0 = none/unknown)
	ServingOfflineFor time.Duration // longest time a serving pod has been offline (0 = none)
	HasServingPods    bool

	// Gateway inputs.
	GatewayOnline        bool
	GatewayErrorRatePct  float64
	GatewayRejectedTotal int64
}

// defaultConfig returns a conservative all-disabled default.
func defaultConfig() Config {
	return Config{
		Enabled:           false,
		RemindWhileFiring: "",
		CostSpike:         ConditionConfig{For: "15m", CostSpikePct: 50},
		DisruptionBlocked: ConditionConfig{For: "5m", DisruptionBlockedMin: 10},
		ServingPressure:   ConditionConfig{For: "5m", KVCachePct: 85},
		Gateway:           ConditionConfig{For: "5m", GatewayErrorRatePct: 5, GatewayRejected: 100},
		ServingOffline:    ConditionConfig{For: "5m"},
		Channels:          nil,
	}
}

// DefaultConfig returns a conservative all-disabled default config.
func DefaultConfig() Config { return defaultConfig() }
