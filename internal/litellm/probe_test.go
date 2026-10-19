package litellm

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/karpenter-optimizer/internal/llmhealth"
)

func splitHostPort(t *testing.T, rawURL string) (string, int32) {
	t.Helper()
	u := strings.TrimPrefix(rawURL, "http://")
	host, portStr, err := net.SplitHostPort(u)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) = %v", rawURL, err)
	}
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		t.Fatalf("ParseInt(%q) = %v", portStr, err)
	}
	return host, int32(port)
}

const replicaA = `# TYPE litellm_in_flight_requests gauge
litellm_in_flight_requests 5
# TYPE litellm_admission_queued_requests gauge
litellm_admission_queued_requests 2
# TYPE litellm_admission_rejected_requests_total counter
litellm_admission_rejected_requests_total 3
# TYPE litellm_proxy_total_requests_metric counter
# (request counter is labeled by the alias-prefixed requested_model)
litellm_proxy_total_requests_metric_total{requested_model="pre/gpt-4o"} 100
litellm_proxy_total_requests_metric_total{requested_model="pre/qwen3-30b"} 40
# TYPE litellm_proxy_failed_requests_metric counter
litellm_proxy_failed_requests_metric_total{requested_model="pre/qwen3-30b"} 4
# TYPE litellm_spend_metric counter
litellm_spend_metric_total{model="gpt-4o"} 1.25
litellm_spend_metric_total{model="qwen3-30b"} 0.5
# TYPE litellm_total_tokens_metric counter
# (tokens carry BOTH model and requested_model -> the join bridge)
litellm_total_tokens_metric_total{model="gpt-4o",requested_model="pre/gpt-4o"} 50000
litellm_total_tokens_metric_total{model="qwen3-30b",requested_model="pre/qwen3-30b"} 20000
# TYPE litellm_request_total_latency_metric histogram
litellm_request_total_latency_metric_bucket{model="gpt-4o",le="1.0"} 60
litellm_request_total_latency_metric_bucket{model="gpt-4o",le="10.0"} 100
litellm_request_total_latency_metric_bucket{model="gpt-4o",le="+Inf"} 100
litellm_request_total_latency_metric_sum{model="gpt-4o"} 900
litellm_request_total_latency_metric_count{model="gpt-4o"} 100
litellm_request_total_latency_metric_bucket{model="qwen3-30b",le="1.0"} 10
litellm_request_total_latency_metric_bucket{model="qwen3-30b",le="10.0"} 40
litellm_request_total_latency_metric_bucket{model="qwen3-30b",le="+Inf"} 40
litellm_request_total_latency_metric_sum{model="qwen3-30b"} 120
litellm_request_total_latency_metric_count{model="qwen3-30b"} 40
# TYPE litellm_llm_api_time_to_first_token_metric histogram
litellm_llm_api_time_to_first_token_metric_bucket{model="gpt-4o",le="0.5"} 50
litellm_llm_api_time_to_first_token_metric_bucket{model="gpt-4o",le="2.0"} 100
litellm_llm_api_time_to_first_token_metric_bucket{model="gpt-4o",le="+Inf"} 100
litellm_llm_api_time_to_first_token_metric_sum{model="gpt-4o"} 80
litellm_llm_api_time_to_first_token_metric_count{model="gpt-4o"} 100
# TYPE litellm_overhead_latency_metric histogram
litellm_overhead_latency_metric_bucket{le="0.01"} 120
litellm_overhead_latency_metric_bucket{le="0.05"} 139
litellm_overhead_latency_metric_bucket{le="+Inf"} 140
litellm_overhead_latency_metric_sum 5
litellm_overhead_latency_metric_count 140
# TYPE litellm_request_queue_time_seconds histogram
litellm_request_queue_time_seconds_bucket{le="0.1"} 130
litellm_request_queue_time_seconds_bucket{le="+Inf"} 140
litellm_request_queue_time_seconds_sum 3
litellm_request_queue_time_seconds_count 140
`

const replicaB = `# TYPE litellm_in_flight_requests gauge
litellm_in_flight_requests 3
# TYPE litellm_admission_queued_requests gauge
litellm_admission_queued_requests 1
# TYPE litellm_proxy_total_requests_metric counter
litellm_proxy_total_requests_metric_total{requested_model="pre/gpt-4o"} 30
# TYPE litellm_proxy_failed_requests_metric counter
litellm_proxy_failed_requests_metric_total{requested_model="pre/gpt-4o"} 1
# TYPE litellm_spend_metric counter
litellm_spend_metric_total{model="gpt-4o"} 0.3
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric_total{model="gpt-4o",requested_model="pre/gpt-4o"} 15000
`

func TestProbeAllAggregatesReplicas(t *testing.T) {
	t.Parallel()

	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, replicaA)
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, replicaB)
	}))
	defer b.Close()
	aIP, aPort := splitHostPort(t, a.URL)
	bIP, bPort := splitHostPort(t, b.URL)

	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: aIP, Port: aPort}, {IP: bIP, Port: bPort}})

	if !h.Online {
		t.Fatalf("expected online gateway, got %+v", h)
	}
	if h.Pods != 2 {
		t.Errorf("Pods = %d, want 2", h.Pods)
	}
	if h.InFlightRequests != 8 {
		t.Errorf("InFlightRequests = %d, want 8 (5+3)", h.InFlightRequests)
	}
	if h.AdmissionQueued != 3 {
		t.Errorf("AdmissionQueued = %d, want 3 (2+1)", h.AdmissionQueued)
	}
	if h.AdmissionRejected != 3 {
		t.Errorf("AdmissionRejected = %d, want 3", h.AdmissionRejected)
	}
	if h.TotalRequests != 170 {
		t.Errorf("TotalRequests = %d, want 170 (100+40+30)", h.TotalRequests)
	}
	if h.FailedRequests != 5 {
		t.Errorf("FailedRequests = %d, want 5 (4+1)", h.FailedRequests)
	}
	if h.ErrorRatePct == nil || *h.ErrorRatePct < 2.9 || *h.ErrorRatePct > 3.1 {
		t.Errorf("ErrorRatePct = %v, want ~2.94", h.ErrorRatePct)
	}
	if h.Spend == nil {
		t.Fatal("Spend = nil")
	} else if *h.Spend < 2.04 || *h.Spend > 2.06 {
		t.Errorf("Spend = %v, want ~2.05", *h.Spend)
	}
	// E2E p95 = worst across replicas: only A has the histogram.
	// gpt-4o p95: target 95 -> le=10.0 bucket (100): interpolate 1..10 at (95-60)/40
	if h.E2EP95Seconds == nil {
		t.Fatal("E2EP95Seconds = nil")
	} else if *h.E2EP95Seconds < 1.0 || *h.E2EP95Seconds >= 10.0 {
		t.Errorf("E2EP95Seconds = %v, want within [1.0, 10.0)", *h.E2EP95Seconds)
	}
	if h.TTFTP95Seconds == nil {
		t.Fatal("TTFTP95Seconds = nil")
	} else if *h.TTFTP95Seconds < 0.5 || *h.TTFTP95Seconds >= 2.0 {
		t.Errorf("TTFTP95Seconds = %v, want within [0.5, 2.0)", *h.TTFTP95Seconds)
	}
	if h.OverheadP95Seconds == nil {
		t.Fatal("OverheadP95Seconds = nil")
	} else if *h.OverheadP95Seconds < 0.01 || *h.OverheadP95Seconds > 0.05 {
		t.Errorf("OverheadP95Seconds = %v, want within (0.01, 0.05]", *h.OverheadP95Seconds)
	}
	if h.QueueTimeP95Seconds == nil {
		t.Fatal("QueueTimeP95Seconds = nil")
	} else if *h.QueueTimeP95Seconds < 0.0 || *h.QueueTimeP95Seconds > 0.1 {
		t.Errorf("QueueTimeP95Seconds = %v, want within [0, 0.1]", *h.QueueTimeP95Seconds)
	}

	// Per-model breakdown.
	if len(h.Models) != 2 {
		t.Fatalf("Models = %d, want 2: %+v", len(h.Models), h.Models)
	}
	// Models must be deterministically ordered by name (map iteration is random).
	if h.Models[0].Model != "gpt-4o" || h.Models[1].Model != "qwen3-30b" {
		t.Errorf("Models order = %q, %q; want gpt-4o, qwen3-30b (by name)", h.Models[0].Model, h.Models[1].Model)
	}
	byModel := map[string]ModelStat{}
	for _, ms := range h.Models {
		byModel[ms.Model] = ms
	}
	gpt, ok := byModel["gpt-4o"]
	if !ok {
		t.Fatal("missing model gpt-4o")
	}
	if gpt.RequestsTotal != 130 {
		t.Errorf("gpt-4o RequestsTotal = %d, want 130 (100+30)", gpt.RequestsTotal)
	}
	if gpt.FailedRequests != 1 {
		t.Errorf("gpt-4o FailedRequests = %d, want 1", gpt.FailedRequests)
	}
	if gpt.ErrorRatePct == nil || *gpt.ErrorRatePct < 0.7 || *gpt.ErrorRatePct > 0.8 {
		t.Errorf("gpt-4o ErrorRatePct = %v, want ~0.77", gpt.ErrorRatePct)
	}
	if gpt.Spend == nil || *gpt.Spend < 1.5 || *gpt.Spend > 1.6 {
		t.Errorf("gpt-4o Spend = %v, want ~1.55", gpt.Spend)
	}
	qwen, ok := byModel["qwen3-30b"]
	if !ok {
		t.Fatal("missing model qwen3-30b")
	}
	if qwen.RequestsTotal != 40 || qwen.FailedRequests != 4 {
		t.Errorf("qwen3-30b totals = %d/%d, want 40/4", qwen.RequestsTotal, qwen.FailedRequests)
	}
	if qwen.ErrorRatePct == nil || *qwen.ErrorRatePct < 9.9 || *qwen.ErrorRatePct > 10.1 {
		t.Errorf("qwen3-30b ErrorRatePct = %v, want 10", qwen.ErrorRatePct)
	}
	// qwen3-30b E2E p95: target 38 -> le=10.0 bucket (40): interpolate 1..10 at (38-10)/30
	if qwen.E2EP95Seconds == nil {
		t.Fatal("qwen3-30b E2EP95Seconds = nil")
	} else if *qwen.E2EP95Seconds <= 1.0 || *qwen.E2EP95Seconds >= 10.0 {
		t.Errorf("qwen3-30b E2EP95Seconds = %v, want within (1.0, 10.0)", *qwen.E2EP95Seconds)
	}
}

// replicaApps carries metadata_trace_name on the token/request/spend counters,
// including a "None" (unattributed) caller.
const replicaApps = `# TYPE litellm_proxy_total_requests_metric counter
litellm_proxy_total_requests_metric_total{metadata_trace_name="app-a"} 120
litellm_proxy_total_requests_metric_total{metadata_trace_name="app-b"} 40
litellm_proxy_total_requests_metric_total{metadata_trace_name="None"} 10
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric_total{metadata_trace_name="app-a"} 90000
litellm_total_tokens_metric_total{metadata_trace_name="app-b"} 30000
litellm_total_tokens_metric_total{metadata_trace_name="None"} 5000
# TYPE litellm_input_tokens_metric counter
litellm_input_tokens_metric_total{metadata_trace_name="app-a"} 80000
litellm_input_tokens_metric_total{metadata_trace_name="app-b"} 25000
litellm_input_tokens_metric_total{metadata_trace_name="None"} 4000
# TYPE litellm_output_tokens_metric counter
litellm_output_tokens_metric_total{metadata_trace_name="app-a"} 10000
litellm_output_tokens_metric_total{metadata_trace_name="app-b"} 5000
litellm_output_tokens_metric_total{metadata_trace_name="None"} 1000
# TYPE litellm_spend_metric counter
litellm_spend_metric_total{metadata_trace_name="app-a"} 3.75
litellm_spend_metric_total{metadata_trace_name="app-b"} 1.25
litellm_spend_metric_total{metadata_trace_name="None"} 0
`

func TestProbeAllAppBreakdown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, replicaApps)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})

	if !h.Online {
		t.Fatalf("expected online gateway, got %+v", h)
	}
	if len(h.Apps) != 3 {
		t.Fatalf("Apps = %d, want 3: %+v", len(h.Apps), h.Apps)
	}
	byApp := map[string]SplitStat{}
	for _, a := range h.Apps {
		byApp[a.Name] = a
	}

	a, ok := byApp["app-a"]
	if !ok {
		t.Fatal("missing app-a")
	}
	if a.RequestsTotal != 120 {
		t.Errorf("app-a RequestsTotal = %d, want 120", a.RequestsTotal)
	}
	if a.TokensTotal != 90000 || a.TokensInput != 80000 || a.TokensOutput != 10000 {
		t.Errorf("app-a tokens = %d/%d/%d, want 90000/80000/10000", a.TokensTotal, a.TokensInput, a.TokensOutput)
	}
	if a.Spend == nil || *a.Spend < 3.74 || *a.Spend > 3.76 {
		t.Errorf("app-a Spend = %v, want ~3.75", a.Spend)
	}
	// First probe: no rate yet.
	if a.TokensPerSec != nil {
		t.Errorf("app-a TokensPerSec on first probe = %v, want nil", *a.TokensPerSec)
	}

	b, ok := byApp["app-b"]
	if !ok {
		t.Fatal("missing app-b")
	}
	if b.TokensTotal != 30000 || b.TokensInput != 25000 || b.TokensOutput != 5000 {
		t.Errorf("app-b tokens = %d/%d/%d, want 30000/25000/5000", b.TokensTotal, b.TokensInput, b.TokensOutput)
	}

	// "None" is bucketed as (unattributed).
	un, ok := byApp["(unattributed)"]
	if !ok {
		t.Fatal("missing (unattributed) app")
	}
	if un.TokensTotal != 5000 {
		t.Errorf("(unattributed) TokensTotal = %d, want 5000", un.TokensTotal)
	}
	// 0.0 spend is still reported (not dropped).
	if un.Spend == nil {
		t.Errorf("(unattributed) Spend = nil, want 0")
	} else if *un.Spend != 0 {
		t.Errorf("(unattributed) Spend = %v, want 0", *un.Spend)
	}

	// Sorted by total tokens desc: app-a, app-b, (unattributed).
	if h.Apps[0].Name != "app-a" || h.Apps[1].Name != "app-b" || h.Apps[2].Name != "(unattributed)" {
		t.Errorf("app order = %q, %q, %q; want app-a, app-b, (unattributed)", h.Apps[0].Name, h.Apps[1].Name, h.Apps[2].Name)
	}
}

func TestProbeAllAppRateFromCounterDiffs(t *testing.T) {
	t.Parallel()

	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, body)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	p := NewProber("")
	body = replicaApps
	p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}}) // baseline
	time.Sleep(50 * time.Millisecond)

	// Bump app-a's token counter by 2000; the diff over ~50ms yields a rate > 0.
	body = strings.Replace(replicaApps,
		`litellm_total_tokens_metric_total{metadata_trace_name="app-a"} 90000`,
		`litellm_total_tokens_metric_total{metadata_trace_name="app-a"} 92000`, 1)
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})

	a := findApp(h.Apps, "app-a")
	if a == nil {
		t.Fatal("missing app-a")
	}
	if a.TokensPerSec == nil {
		t.Fatal("app-a TokensPerSec = nil, want a rate from the counter diff")
	}
	if *a.TokensPerSec <= 0 {
		t.Errorf("app-a TokensPerSec = %v, want > 0", *a.TokensPerSec)
	}
}

func findApp(apps []SplitStat, name string) *SplitStat {
	for i := range apps {
		if apps[i].Name == name {
			return &apps[i]
		}
	}
	return nil
}

// replicaModelApp carries both `model` and `metadata_trace_name` on the token /
// spend counters, so each (model, app) cell is derivable directly. A "None"
// trace name is included to exercise the (unattributed) bucket.
const replicaModelApp = `# TYPE litellm_proxy_total_requests_metric counter
litellm_proxy_total_requests_metric_total{requested_model="pre/m1"} 100
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric_total{model="m1",requested_model="pre/m1",metadata_trace_name="app-a"} 1000
litellm_total_tokens_metric_total{model="m1",requested_model="pre/m1",metadata_trace_name="app-b"} 300
litellm_total_tokens_metric_total{model="m1",requested_model="pre/m1",metadata_trace_name="None"} 200
# TYPE litellm_input_tokens_metric counter
litellm_input_tokens_metric_total{model="m1",metadata_trace_name="app-a"} 900
litellm_input_tokens_metric_total{model="m1",metadata_trace_name="app-b"} 250
litellm_input_tokens_metric_total{model="m1",metadata_trace_name="None"} 150
# TYPE litellm_output_tokens_metric counter
litellm_output_tokens_metric_total{model="m1",metadata_trace_name="app-a"} 100
litellm_output_tokens_metric_total{model="m1",metadata_trace_name="app-b"} 50
litellm_output_tokens_metric_total{model="m1",metadata_trace_name="None"} 50
# TYPE litellm_spend_metric counter
litellm_spend_metric_total{model="m1",metadata_trace_name="app-a"} 1.20
litellm_spend_metric_total{model="m1",metadata_trace_name="app-b"} 0.50
litellm_spend_metric_total{model="m1",metadata_trace_name="None"} 0.30
`

func findModel(models []ModelStat, name string) *ModelStat {
	for i := range models {
		if models[i].Model == name {
			return &models[i]
		}
	}
	return nil
}

func findModelApp(model ModelStat, name string) *ModelSplitStat {
	for i := range model.Apps {
		if model.Apps[i].Name == name {
			return &model.Apps[i]
		}
	}
	return nil
}

func TestProbeAllModelAppBreakdown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, replicaModelApp)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})

	m := findModel(h.Models, "m1")
	if m == nil {
		t.Fatalf("missing model m1: %+v", h.Models)
	}
	if len(m.Apps) != 3 {
		t.Fatalf("m1.Apps = %d, want 3: %+v", len(m.Apps), m.Apps)
	}

	a := findModelApp(*m, "app-a")
	if a == nil {
		t.Fatal("missing app-a under m1")
	}
	if a.TokensTotal != 1000 || a.TokensInput != 900 || a.TokensOutput != 100 {
		t.Errorf("m1/app-a tokens = %d/%d/%d, want 1000/900/100", a.TokensTotal, a.TokensInput, a.TokensOutput)
	}
	if a.Spend == nil || *a.Spend < 1.19 || *a.Spend > 1.21 {
		t.Errorf("m1/app-a Spend = %v, want ~1.20", a.Spend)
	}

	b := findModelApp(*m, "app-b")
	if b == nil {
		t.Fatal("missing app-b under m1")
	}
	if b.TokensTotal != 300 || b.TokensInput != 250 || b.TokensOutput != 50 {
		t.Errorf("m1/app-b tokens = %d/%d/%d, want 300/250/50", b.TokensTotal, b.TokensInput, b.TokensOutput)
	}

	un := findModelApp(*m, "(unattributed)")
	if un == nil {
		t.Fatal("missing (unattributed) under m1")
	}
	if un.TokensTotal != 200 {
		t.Errorf("m1/(unattributed) TokensTotal = %d, want 200", un.TokensTotal)
	}
	if un.Spend == nil || *un.Spend < 0.29 || *un.Spend > 0.31 {
		t.Errorf("m1/(unattributed) Spend = %v, want ~0.30", un.Spend)
	}

	// Sorted by total tokens desc: app-a, app-b, (unattributed).
	if m.Apps[0].Name != "app-a" || m.Apps[1].Name != "app-b" || m.Apps[2].Name != "(unattributed)" {
		t.Errorf("m1 app order = %q, %q, %q; want app-a, app-b, (unattributed)", m.Apps[0].Name, m.Apps[1].Name, m.Apps[2].Name)
	}
}

// replicaSplitDims carries model, metadata_trace_name, api_key_alias and
// team_alias all on the token/spend series, so every split dimension is
// derivable without bridging.
const replicaSplitDims = `# TYPE litellm_proxy_total_requests_metric counter
litellm_proxy_total_requests_metric_total{api_key_alias="Key A",team_alias="Backend"} 30
litellm_proxy_total_requests_metric_total{api_key_alias="Key B",team_alias="Frontend"} 20
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric_total{model="m1",metadata_trace_name="app-a",api_key_alias="Key A",team_alias="Backend"} 800
litellm_total_tokens_metric_total{model="m1",metadata_trace_name="app-b",api_key_alias="Key B",team_alias="Frontend"} 200
# TYPE litellm_input_tokens_metric counter
litellm_input_tokens_metric_total{model="m1",api_key_alias="Key A",team_alias="Backend"} 700
litellm_input_tokens_metric_total{model="m1",api_key_alias="Key B",team_alias="Frontend"} 150
# TYPE litellm_output_tokens_metric counter
litellm_output_tokens_metric_total{model="m1",api_key_alias="Key A",team_alias="Backend"} 100
litellm_output_tokens_metric_total{model="m1",api_key_alias="Key B",team_alias="Frontend"} 50
# TYPE litellm_spend_metric counter
litellm_spend_metric_total{model="m1",api_key_alias="Key A",team_alias="Backend"} 1.0
litellm_spend_metric_total{model="m1",api_key_alias="Key B",team_alias="Frontend"} 0.5
`

func TestProbeAllSplitDimensions(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, replicaSplitDims)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if !h.Online {
		t.Fatalf("expected online gateway, got %+v", h)
	}

	// Top-level: API-key dimension (Key A leads by tokens).
	if len(h.APIKeys) != 2 {
		t.Fatalf("APIKeys = %d, want 2: %+v", len(h.APIKeys), h.APIKeys)
	}
	ka := findSplit(h.APIKeys, "Key A")
	if ka == nil {
		t.Fatal("missing API key 'Key A'")
	}
	if ka.RequestsTotal != 30 || ka.TokensTotal != 800 || ka.TokensInput != 700 || ka.TokensOutput != 100 {
		t.Errorf("Key A = %+v, want req=30 tok=800 in=700 out=100", ka)
	}
	if ka.Spend == nil || *ka.Spend < 0.99 || *ka.Spend > 1.01 {
		t.Errorf("Key A spend = %v, want ~1.0", ka.Spend)
	}
	if h.APIKeys[0].Name != "Key A" || h.APIKeys[1].Name != "Key B" {
		t.Errorf("APIKeys order = %q, %q; want Key A, Key B (by tokens)", h.APIKeys[0].Name, h.APIKeys[1].Name)
	}

	// Top-level: Team dimension (Backend leads by tokens).
	if len(h.Teams) != 2 {
		t.Fatalf("Teams = %d, want 2: %+v", len(h.Teams), h.Teams)
	}
	tb := findSplit(h.Teams, "Backend")
	if tb == nil {
		t.Fatal("missing team 'Backend'")
	}
	if tb.TokensTotal != 800 || tb.TokensInput != 700 || tb.TokensOutput != 100 {
		t.Errorf("Backend tokens = %d/%d/%d, want 800/700/100", tb.TokensTotal, tb.TokensInput, tb.TokensOutput)
	}

	// Per-model: m1 must expose apiKeys[] and teams[] mirroring the top-level.
	m := findModel(h.Models, "m1")
	if m == nil {
		t.Fatal("missing model m1")
	}
	mka := findModelSplit(*m, "apiKeys", "Key A")
	if mka == nil {
		t.Fatal("missing m1 API key 'Key A'")
	}
	if mka.TokensTotal != 800 || mka.TokensInput != 700 || mka.TokensOutput != 100 {
		t.Errorf("m1/Key A tokens = %d/%d/%d, want 800/700/100", mka.TokensTotal, mka.TokensInput, mka.TokensOutput)
	}
	if mka.Spend == nil || *mka.Spend < 0.99 || *mka.Spend > 1.01 {
		t.Errorf("m1/Key A spend = %v, want ~1.0", mka.Spend)
	}
	mteam := findModelSplit(*m, "teams", "Backend")
	if mteam == nil {
		t.Fatal("missing m1 team 'Backend'")
	}
	if mteam.TokensTotal != 800 {
		t.Errorf("m1/Backend tokens = %d, want 800", mteam.TokensTotal)
	}
}

func findSplit(splits []SplitStat, name string) *SplitStat {
	for i := range splits {
		if splits[i].Name == name {
			return &splits[i]
		}
	}
	return nil
}

func findModelSplit(model ModelStat, field, name string) *ModelSplitStat {
	var rows []ModelSplitStat
	switch field {
	case "apiKeys":
		rows = model.APIKeys
	case "teams":
		rows = model.Teams
	case "apps":
		rows = model.Apps
	}
	for i := range rows {
		if rows[i].Name == name {
			return &rows[i]
		}
	}
	return nil
}

func TestProbeAllRateFromCounterDiffs(t *testing.T) {
	t.Parallel()

	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, body)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	body = "# TYPE litellm_proxy_total_requests_metric counter\nlitellm_proxy_total_requests_metric 100\n"
	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if h.RequestsPerSec != nil {
		t.Fatalf("RequestsPerSec on first probe = %v, want nil", *h.RequestsPerSec)
	}

	body = "# TYPE litellm_proxy_total_requests_metric counter\nlitellm_proxy_total_requests_metric 120\n"
	time.Sleep(50 * time.Millisecond)
	h = p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if h.RequestsPerSec == nil {
		t.Fatal("RequestsPerSec = nil after second probe")
	}
	// 20 requests in ~50ms => ~400 req/s (loose bounds for timer jitter).
	if *h.RequestsPerSec < 100 || *h.RequestsPerSec > 2000 {
		t.Errorf("RequestsPerSec = %v, want ~400", *h.RequestsPerSec)
	}
}

func TestProbeAllRateSurvivesCounterReset(t *testing.T) {
	t.Parallel()

	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, body)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	body = "# TYPE litellm_proxy_total_requests_metric counter\nlitellm_proxy_total_requests_metric 500\n"
	p := NewProber("")
	p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})

	// Counter resets (replica restart): a drop must not produce a negative rate.
	body = "# TYPE litellm_proxy_total_requests_metric counter\nlitellm_proxy_total_requests_metric 10\n"
	time.Sleep(50 * time.Millisecond)
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if h.RequestsPerSec == nil {
		t.Fatal("RequestsPerSec = nil after second probe")
	}
	if *h.RequestsPerSec != 0 {
		t.Errorf("RequestsPerSec = %v after counter reset, want 0", *h.RequestsPerSec)
	}
}

func TestProbeAllAuthRequired(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-key" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("unauthorized"))
			return
		}
		_, _ = fmt.Fprint(w, replicaA)
	}))
	defer srv.Close()
	ip, port := splitHostPort(t, srv.URL)

	// Without a key: offline with an actionable auth hint.
	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if h.Online {
		t.Fatal("expected offline when /metrics returns 401")
	}
	if !strings.Contains(h.AuthError, "LITELLM_METRICS_API_KEY") {
		t.Errorf("AuthError = %q, want a hint mentioning LITELLM_METRICS_API_KEY", h.AuthError)
	}

	// With the right key: online.
	p2 := NewProber("secret-key")
	h2 := p2.ProbeAll(context.Background(), []PodRef{{IP: ip, Port: port}})
	if !h2.Online {
		t.Fatalf("expected online with a valid key, got %+v", h2)
	}
	if h2.TotalRequests != 140 {
		t.Errorf("TotalRequests = %d, want 140", h2.TotalRequests)
	}
}

func TestProbeAllNoReplicasAndUnreachable(t *testing.T) {
	t.Parallel()

	p := NewProber("")
	if h := p.ProbeAll(context.Background(), nil); h.Online || h.Error == "" {
		t.Errorf("empty refs: got %+v, want offline with error", h)
	}

	// 127.0.0.1:1 is virtually guaranteed to refuse connections.
	h := p.ProbeAll(context.Background(), []PodRef{{IP: "127.0.0.1", Port: 1}})
	if h.Online {
		t.Error("expected offline for an unreachable replica")
	}
	if h.Error == "" {
		t.Error("expected an error message")
	}
	if h.AuthError != "" {
		t.Errorf("AuthError = %q, want empty (connection refused, not 401)", h.AuthError)
	}
}

func TestProbeAllPartialReplicas(t *testing.T) {
	t.Parallel()

	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, replicaA)
	}))
	defer a.Close()
	aIP, aPort := splitHostPort(t, a.URL)

	p := NewProber("")
	h := p.ProbeAll(context.Background(), []PodRef{
		{IP: aIP, Port: aPort},
		{IP: "127.0.0.1", Port: 1},
	})
	if !h.Online {
		t.Fatalf("expected online with one healthy replica, got %+v", h)
	}
	if h.Pods != 2 {
		t.Errorf("Pods = %d, want 2", h.Pods)
	}
	if h.TotalRequests != 140 {
		t.Errorf("TotalRequests = %d, want 140 (only replica A counted)", h.TotalRequests)
	}
}

// TestModelRequestBridge verifies that request/failed counters labeled by the
// alias-prefixed `requested_model` are joined onto the base `model` via a
// metric carrying both labels, and that unbridgeable requested_models are
// dropped from per-model rows (still counted in the fleet total).
func TestModelRequestBridge(t *testing.T) {
	t.Parallel()

	body := `# TYPE litellm_proxy_total_requests_metric counter
litellm_proxy_total_requests_metric_total{requested_model="pre/gpt-4o"} 10
litellm_proxy_total_requests_metric_total{requested_model="pre/qwen"} 5
litellm_proxy_total_requests_metric_total{requested_model="pre/orphan"} 7
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric_total{model="gpt-4o",requested_model="pre/gpt-4o"} 100
litellm_total_tokens_metric_total{model="qwen",requested_model="pre/qwen"} 200
`
	m := llmhealth.ParsePrometheus(body)

	req, failed := modelRequestValues(m)
	if req["gpt-4o"] != 10 {
		t.Errorf("gpt-4o requests = %v, want 10", req["gpt-4o"])
	}
	if req["qwen"] != 5 {
		t.Errorf("qwen requests = %v, want 5", req["qwen"])
	}
	if _, ok := req["pre/orphan"]; ok {
		t.Error("unbridgeable requested_model must not appear in per-model rows")
	}
	// The orphan's 7 requests still count toward the fleet total.
	if total, _ := m.Total("litellm_proxy_total_requests_metric"); total != 22 {
		t.Errorf("fleet total = %v, want 22 (10+5+7)", total)
	}
	if failed["gpt-4o"] != 0 {
		t.Errorf("gpt-4o failed = %v, want 0", failed["gpt-4o"])
	}
}
