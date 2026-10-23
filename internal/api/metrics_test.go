package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/karpenter-optimizer/internal/kubernetes"
	"github.com/karpenter-optimizer/internal/litellm"
	"github.com/karpenter-optimizer/internal/llmhealth"
	"github.com/stretchr/testify/assert"
)

func floatPtr(f float64) *float64 { return &f }

func scrapeMetrics(t *testing.T, s *Server) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	return w.Body.String()
}

func TestMetricsDisabledByDefault(t *testing.T) {
	server := setupTestServer()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestMetricsEnabledServesUp(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()
	body := scrapeMetrics(t, server)
	assert.Contains(t, body, "# HELP karpenter_optimizer_up")
	assert.Contains(t, body, "karpenter_optimizer_up 1")
	// k8s-connected value is environment-dependent (a local kubeconfig may
	// exist); only assert the metric is present in 0/1 form.
	assert.Regexp(t, `karpenter_optimizer_kubernetes_connected [01]`, body)
	assert.Regexp(t, `karpenter_optimizer_karpenter_detected [01]`, body)
}

func TestMetricsClusterAndNodePoolSnapshot(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()

	nodes := []kubernetes.NodeInfo{
		{
			Name: "n1", NodePool: "default",
			CPUUsage:    &kubernetes.NodeUsage{Used: 4, Allocatable: 8},
			MemoryUsage: &kubernetes.NodeUsage{Used: 16, Allocatable: 32},
			PodCount:    5,
		},
		{
			Name: "n2", NodePool: "default",
			CPUUsage:    &kubernetes.NodeUsage{Used: 2, Allocatable: 8},
			MemoryUsage: &kubernetes.NodeUsage{Used: 8, Allocatable: 32},
			PodCount:    3,
		},
		{
			Name: "n3", NodePool: "gpu",
			CPUUsage:    &kubernetes.NodeUsage{Used: 1, Allocatable: 96},
			MemoryUsage: &kubernetes.NodeUsage{Used: 4, Allocatable: 768},
			PodCount:    1,
		},
	}
	poolCost := map[string]float64{"default": 5.0, "gpu": 1.74}
	server.recordClusterMetrics(nodes, 6.74, poolCost)

	body := scrapeMetrics(t, server)
	assert.Contains(t, body, "karpenter_optimizer_cluster_nodes 3")
	assert.Contains(t, body, "karpenter_optimizer_cluster_pods 9")
	assert.Contains(t, body, "karpenter_optimizer_cluster_cpu_used 7")
	assert.Contains(t, body, "karpenter_optimizer_cluster_cpu_allocatable 112")
	assert.Contains(t, body, "karpenter_optimizer_cluster_memory_used 28")
	assert.Contains(t, body, "karpenter_optimizer_cluster_memory_allocatable 832")
	assert.Contains(t, body, "karpenter_optimizer_cluster_cost_per_hour 6.74")
	assert.Contains(t, body, `karpenter_optimizer_nodepool_nodes{nodepool="default"} 2`)
	assert.Contains(t, body, `karpenter_optimizer_nodepool_cost_per_hour{nodepool="default"} 5`)
	assert.Contains(t, body, `karpenter_optimizer_nodepool_nodes{nodepool="gpu"} 1`)
}

func TestMetricsNodePoolDropsWhenDrained(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()

	server.recordClusterMetrics(
		[]kubernetes.NodeInfo{{Name: "n1", NodePool: "a", CPUUsage: &kubernetes.NodeUsage{Used: 1, Allocatable: 2}}},
		1.0, map[string]float64{"a": 1.0})

	server.recordClusterMetrics(
		[]kubernetes.NodeInfo{{Name: "n2", NodePool: "b", CPUUsage: &kubernetes.NodeUsage{Used: 1, Allocatable: 2}}},
		2.0, map[string]float64{"b": 2.0})

	body := scrapeMetrics(t, server)
	assert.NotContains(t, body, `nodepool="a"`)
	assert.Contains(t, body, `karpenter_optimizer_nodepool_nodes{nodepool="b"} 1`)
}

func TestMetricsServingPodSnapshot(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()

	server.recordServingMetrics([]llmhealth.PodHealth{{
		Namespace: "ai", Name: "vllm-0", Model: "Qwen/Qwen2.5-7B-Instruct",
		Online:         true,
		KVCachePercent: floatPtr(88.5),
		TTFTP95Seconds: floatPtr(1.1),
		E2EP95Seconds:  floatPtr(6.4),
		TokensPerSec:   floatPtr(412.7),
		RequestsPerSec: floatPtr(1.9),
	}})

	body := scrapeMetrics(t, server)
	assert.Contains(t, body, `serving_pod_online{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 1`)
	assert.Contains(t, body, `serving_kv_cache_pct{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 88.5`)
	assert.Contains(t, body, `serving_ttft_p95_seconds{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 1.1`)
	assert.Contains(t, body, `serving_e2e_p95_seconds{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 6.4`)
	assert.Contains(t, body, `serving_tokens_per_second{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 412.7`)
	assert.Contains(t, body, `serving_requests_per_second{model="Qwen/Qwen2.5-7B-Instruct",namespace="ai",pod="vllm-0"} 1.9`)
}

func TestMetricsGatewaySnapshot(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()

	gw := &litellm.GatewayHealth{
		Pods: 2, Online: true,
		InFlightRequests: 3, AdmissionQueued: 1, AdmissionRejected: 12,
		ErrorRatePct: floatPtr(1.2), RequestsPerSec: floatPtr(4.2),
		Spend: floatPtr(1234.56),
		Apps: []litellm.SplitStat{
			{Name: "web-ui", Spend: floatPtr(540.2)},
			{Name: "", Spend: floatPtr(10)},
		},
	}
	server.recordGatewayMetrics(gw)

	body := scrapeMetrics(t, server)
	assert.Contains(t, body, "karpenter_optimizer_gateway_pods 2")
	assert.Contains(t, body, "karpenter_optimizer_gateway_online 1")
	assert.Contains(t, body, "karpenter_optimizer_gateway_in_flight_requests 3")
	assert.Contains(t, body, "karpenter_optimizer_gateway_admission_queued 1")
	assert.Contains(t, body, "karpenter_optimizer_gateway_admission_rejected_total 12")
	assert.Contains(t, body, "karpenter_optimizer_gateway_error_rate_pct 1.2")
	assert.Contains(t, body, "karpenter_optimizer_gateway_requests_per_second 4.2")
	assert.Contains(t, body, "karpenter_optimizer_gateway_spend_total 1234.56")
	assert.Contains(t, body, `karpenter_optimizer_gateway_app_spend_total{app="web-ui"} 540.2`)
	// Empty app alias groups as (unattributed), matching the gateway UI.
	assert.Contains(t, body, `karpenter_optimizer_gateway_app_spend_total{app="(unattributed)"} 10`)

	// A nil probe (no LiteLLM pods) clears the gateway section.
	server.recordGatewayMetrics(nil)
	body = scrapeMetrics(t, server)
	assert.NotContains(t, body, "karpenter_optimizer_gateway_pods")
}

func TestMetricsDisruptionSnapshot(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()

	d1, d2 := 1.74, 0.5
	server.recordDisruptionMetrics([]kubernetes.NodeDisruptionInfo{
		{NodeName: "n1", NodePool: "default", IsBlocked: true, CostPerHour: &d1},
		{NodeName: "n2", NodePool: "gpu", CostPerHour: &d2},
	})

	body := scrapeMetrics(t, server)
	assert.Contains(t, body, "karpenter_optimizer_disruptions_blocked 1")
	assert.Contains(t, body, "karpenter_optimizer_disruptions_active 1")
	assert.Contains(t, body, "karpenter_optimizer_disruptions_cost_per_hour 2.24")
	assert.Contains(t, body, `karpenter_optimizer_disruption_cost_per_hour{node="n1",nodepool="default"} 1.74`)
	assert.Contains(t, body, `karpenter_optimizer_disruption_cost_per_hour{node="n2",nodepool="gpu"} 0.5`)
}

func TestMetricsPublishIsolation(t *testing.T) {
	// A snapshot clone must not share maps with the live one (no data races,
	// no cross-publish bleed).
	t.Setenv("METRICS_ENABLED", "true")
	server := setupTestServer()
	server.recordClusterMetrics(
		[]kubernetes.NodeInfo{{Name: "n1", NodePool: "a"}}, 0, nil)
	snap1 := server.snapshotMetrics()
	server.recordClusterMetrics(
		[]kubernetes.NodeInfo{{Name: "n2", NodePool: "b"}}, 0, nil)
	assert.NotContains(t, snap1.nodePools, "b")
	assert.Contains(t, snap1.nodePools, "a")
}
