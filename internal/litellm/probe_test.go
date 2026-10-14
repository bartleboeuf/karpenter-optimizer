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
litellm_proxy_total_requests_metric{model="gpt-4o",api_key="A"} 100
litellm_proxy_total_requests_metric{model="qwen3-30b",api_key="A"} 40
# TYPE litellm_proxy_failed_requests_metric counter
litellm_proxy_failed_requests_metric{model="qwen3-30b",api_key="A"} 4
# TYPE litellm_spend_metric counter
litellm_spend_metric{model="gpt-4o"} 1.25
litellm_spend_metric{model="qwen3-30b"} 0.5
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric{model="gpt-4o"} 50000
litellm_total_tokens_metric{model="qwen3-30b"} 20000
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
litellm_proxy_total_requests_metric{model="gpt-4o",api_key="A"} 30
# TYPE litellm_proxy_failed_requests_metric counter
litellm_proxy_failed_requests_metric{model="gpt-4o",api_key="A"} 1
# TYPE litellm_spend_metric counter
litellm_spend_metric{model="gpt-4o"} 0.3
# TYPE litellm_total_tokens_metric counter
litellm_total_tokens_metric{model="gpt-4o"} 15000
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
