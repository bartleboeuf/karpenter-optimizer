package alerts

import (
	"fmt"
	"time"
)

// conditions returns the built-in condition definitions.
func conditions() []conditionDef {
	return []conditionDef{
		{
			id:       CondCostSpike,
			title:    "Cluster cost spike",
			severity: SeverityWarning,
			enabled:  func(c Config) bool { return c.Enabled && c.CostSpike.Enabled },
			threshold: func(c Config) time.Duration {
				return parseFor(c.CostSpike.For, 15*time.Minute)
			},
			predicate: func(c Config, s Snapshot) (bool, string) {
				if !s.HasCostBaseline || s.Cost24hAverage <= 0 {
					return false, ""
				}
				threshold := c.CostSpike.CostSpikePct
				if threshold <= 0 {
					threshold = 50
				}
				deltaPct := (s.CostPerHour - s.Cost24hAverage) / s.Cost24hAverage * 100
				if deltaPct > threshold {
					return true, fmt.Sprintf("%.0f%%/hr, +%.0f%% vs 24h avg (%.2f)", s.CostPerHour, deltaPct, s.Cost24hAverage)
				}
				return false, ""
			},
			message: func(id, detail string) string {
				if detail == "" {
					return "Cluster cost is above its 24h average."
				}
				return "Cluster cost " + detail + " is above its 24h average."
			},
		},
		{
			id:       CondDisruptionBlocked,
			title:    "Node disruption blocked",
			severity: SeverityWarning,
			enabled:  func(c Config) bool { return c.Enabled && c.DisruptionBlocked.Enabled },
			threshold: func(c Config) time.Duration {
				return parseFor(c.DisruptionBlocked.For, 5*time.Minute)
			},
			predicate: func(c Config, s Snapshot) (bool, string) {
				min := c.DisruptionBlocked.DisruptionBlockedMin
				if min <= 0 {
					min = 10
				}
				if s.DisruptionBlockedFor >= time.Duration(min)*time.Minute {
					return true, fmt.Sprintf("blocked for %s", s.DisruptionBlockedFor.Round(time.Minute))
				}
				return false, ""
			},
			message: func(id, detail string) string {
				if detail == "" {
					return "A node deletion is blocked."
				}
				return "A node deletion has been " + detail + "."
			},
		},
		{
			id:       CondServingPressure,
			title:    "Serving under pressure",
			severity: SeverityWarning,
			enabled:  func(c Config) bool { return c.Enabled && c.ServingPressure.Enabled },
			threshold: func(c Config) time.Duration {
				return parseFor(c.ServingPressure.For, 5*time.Minute)
			},
			predicate: func(c Config, s Snapshot) (bool, string) {
				if !s.HasServingPods {
					return false, ""
				}
				threshold := c.ServingPressure.KVCachePct
				if threshold <= 0 {
					threshold = 85
				}
				if s.ServingKVCacheMax >= threshold {
					return true, fmt.Sprintf("KV cache %.0f%%", s.ServingKVCacheMax)
				}
				return false, ""
			},
			message: func(id, detail string) string {
				if detail == "" {
					return "A serving pod is under pressure."
				}
				return "A serving pod is under pressure: " + detail + "."
			},
		},
		{
			id:       CondGateway,
			title:    "Gateway errors",
			severity: SeverityCritical,
			enabled:  func(c Config) bool { return c.Enabled && c.Gateway.Enabled },
			threshold: func(c Config) time.Duration {
				return parseFor(c.Gateway.For, 5*time.Minute)
			},
			predicate: func(c Config, s Snapshot) (bool, string) {
				if !s.GatewayOnline {
					return false, ""
				}
				errThresh := c.Gateway.GatewayErrorRatePct
				if errThresh <= 0 {
					errThresh = 5
				}
				rejThresh := c.Gateway.GatewayRejected
				if rejThresh <= 0 {
					rejThresh = 100
				}
				if s.GatewayErrorRatePct > errThresh {
					return true, fmt.Sprintf("%.1f%% errors", s.GatewayErrorRatePct)
				}
				if s.GatewayRejectedTotal > rejThresh {
					return true, fmt.Sprintf("%d rejected (503)", s.GatewayRejectedTotal)
				}
				return false, ""
			},
			message: func(id, detail string) string {
				if detail == "" {
					return "The LiteLLM gateway is reporting errors."
				}
				return "The LiteLLM gateway is reporting " + detail + "."
			},
		},
		{
			id:       CondServingOffline,
			title:    "Serving pod offline",
			severity: SeverityCritical,
			enabled:  func(c Config) bool { return c.Enabled && c.ServingOffline.Enabled },
			threshold: func(c Config) time.Duration {
				return parseFor(c.ServingOffline.For, 5*time.Minute)
			},
			predicate: func(c Config, s Snapshot) (bool, string) {
				if !s.HasServingPods {
					return false, ""
				}
				if s.ServingOfflineFor > 0 {
					return true, fmt.Sprintf("offline for %s", s.ServingOfflineFor.Round(time.Minute))
				}
				return false, ""
			},
			message: func(id, detail string) string {
				if detail == "" {
					return "A serving pod is offline."
				}
				return "A serving pod has been " + detail + "."
			},
		},
	}
}

func parseFor(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
