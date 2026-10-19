// Package litellm probes in-cluster LiteLLM AI-gateway proxy pods for
// gateway-level metrics (in-flight requests, admission queue, request/error
// rates, latency split into model time vs gateway overhead, spend) and a
// per-model breakdown. It is read-only and graceful: unreachable replicas
// degrade to a "gateway unreachable" report, never a hard error.
package litellm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/karpenter-optimizer/internal/llmhealth"
)

// PodRef identifies one LiteLLM replica to scrape.
type PodRef struct {
	IP   string
	Port int32
}

// ModelSplitStat is one (model, split-value) usage cell: how many tokens and
// how much spend a given app / API key / team consumed on a given model.
// Derived from the token/spend counters, which carry both `model` and the
// split label on the same series (no bridging). Values without a real label
// (e.g. metadata_trace_name="None") are grouped as "(unattributed)".
type ModelSplitStat struct {
	Name         string   `json:"name"`
	TokensTotal  int64    `json:"tokensTotal"`
	TokensInput  int64    `json:"tokensInput"`
	TokensOutput int64    `json:"tokensOutput"`
	Spend        *float64 `json:"spend,omitempty"` // cumulative USD for this split value on this model
}

// ModelStat is the per-model breakdown of gateway traffic.
type ModelStat struct {
	Model          string           `json:"model"`
	RequestsTotal  int64            `json:"requestsTotal"`
	FailedRequests int64            `json:"failedRequests"`
	ErrorRatePct   *float64         `json:"errorRatePct,omitempty"` // failed/total, 0-100
	RequestsPerSec *float64         `json:"requestsPerSec,omitempty"`
	TokensPerSec   *float64         `json:"tokensPerSec,omitempty"`
	E2EP95Seconds  *float64         `json:"e2eP95Seconds,omitempty"`  // p95 end-to-end latency (model + gateway)
	TTFTP95Seconds *float64         `json:"ttftP95Seconds,omitempty"` // p95 time-to-first-token
	Spend          *float64         `json:"spend,omitempty"`          // cumulative USD attributed to this model
	Apps           []ModelSplitStat `json:"apps,omitempty"`           // per-calling-app (metadata_trace_name) breakdown
	APIKeys        []ModelSplitStat `json:"apiKeys,omitempty"`        // per API-key-alias breakdown
	Teams          []ModelSplitStat `json:"teams,omitempty"`          // per team-alias breakdown
}

// SplitStat is the breakdown of gateway traffic by one calling dimension —
// app (metadata_trace_name), API key (api_key_alias) or team (team_alias).
// Requests without a real label value are grouped as "(unattributed)".
type SplitStat struct {
	Name          string   `json:"name"`
	RequestsTotal int64    `json:"requestsTotal"`
	TokensTotal   int64    `json:"tokensTotal"`
	TokensInput   int64    `json:"tokensInput"`
	TokensOutput  int64    `json:"tokensOutput"`
	TokensPerSec  *float64 `json:"tokensPerSec,omitempty"`
	Spend         *float64 `json:"spend,omitempty"` // cumulative USD attributed to this split value
}

// GatewayHealth is the aggregated health of the LiteLLM gateway fleet.
// Counters and gauges are summed across replicas; quantiles take the worst
// case (max), which is conservative for latency.
type GatewayHealth struct {
	Pods   int  `json:"pods"` // replicas discovered
	Online bool `json:"online"`

	AuthError string `json:"authError,omitempty"` // 401/403 hint when /metrics needs a key
	Error     string `json:"error,omitempty"`     // all replicas unreachable, etc.

	InFlightRequests    int      `json:"inFlightRequests"`
	AdmissionQueued     int      `json:"admissionQueued"`
	AdmissionRejected   int64    `json:"admissionRejected"` // 503s rejected by admission control
	TotalRequests       int64    `json:"totalRequests"`
	FailedRequests      int64    `json:"failedRequests"`
	ErrorRatePct        *float64 `json:"errorRatePct,omitempty"`
	RequestsPerSec      *float64 `json:"requestsPerSec,omitempty"` // from counter diffs
	E2EP95Seconds       *float64 `json:"e2eP95Seconds,omitempty"`
	TTFTP95Seconds      *float64 `json:"ttftP95Seconds,omitempty"`
	QueueTimeP95Seconds *float64 `json:"queueTimeP95Seconds,omitempty"` // p95 time spent queued at the gateway
	OverheadP95Seconds  *float64 `json:"overheadP95Seconds,omitempty"`  // p95 gateway processing overhead
	Spend               *float64 `json:"spend,omitempty"`               // cumulative USD

	Models  []ModelStat `json:"models,omitempty"`
	Apps    []SplitStat `json:"apps,omitempty"`    // per calling-app (metadata_trace_name) usage
	APIKeys []SplitStat `json:"apiKeys,omitempty"` // per API-key-alias (api_key_alias) usage
	Teams   []SplitStat `json:"teams,omitempty"`   // per team-alias (team_alias) usage
}

// splitDim identifies one breakdown dimension: which label the counters are
// grouped by, the JSON field that carries its top-level table, and the opaque
// rate-state prefix (kept distinct per dimension so rates never collide).
type splitDim struct {
	label    string
	rateKey  string
	topField string // GatewayHealth field / ModelStat field the table lands in
}

var splitDims = []splitDim{
	{label: "metadata_trace_name", rateKey: "aptok", topField: "apps"},
	{label: "api_key_alias", rateKey: "aptokkey", topField: "apiKeys"},
	{label: "team_alias", rateKey: "aptokteam", topField: "teams"},
}

// Prober keeps the last sample of every counter it has seen (per replica,
// metric and model) so rates can be derived from diffs between successive
// probes, the same pattern as llmhealth.Prober.
type Prober struct {
	mu     sync.Mutex
	last   map[string]*rateState
	http   *http.Client
	apiKey string
}

type rateState struct {
	value float64
	ts    time.Time
}

// NewProber returns a Prober with a 5s HTTP timeout. apiKey, when non-empty,
// is sent as "Authorization: Bearer <key>" (LiteLLM's /metrics has required
// auth since v1.85.0).
func NewProber(apiKey string) *Prober {
	return &Prober{
		last:   make(map[string]*rateState),
		http:   &http.Client{Timeout: 5 * time.Second},
		apiKey: apiKey,
	}
}

// modelMetrics are the LiteLLM metrics that carry a base `model` label (used
// to discover the full model list). The request/failed counters are excluded
// because in current LiteLLM they are labeled with the alias-prefixed
// `requested_model` rather than `model`.
var modelMetrics = []string{
	"litellm_request_total_latency_metric",
	"litellm_llm_api_time_to_first_token_metric",
	"litellm_total_tokens_metric",
	"litellm_input_tokens_metric",
	"litellm_output_tokens_metric",
	"litellm_spend_metric",
}

// splitDimMetrics are the LiteLLM metrics that carry the split-dimension labels
// (metadata_trace_name / api_key_alias / team_alias). We union their raw label
// values to discover every split value; a given value may appear on only a
// subset of these metrics.
var splitDimMetrics = []string{
	"litellm_proxy_total_requests_metric",
	"litellm_proxy_failed_requests_metric",
	"litellm_total_tokens_metric",
	"litellm_input_tokens_metric",
	"litellm_output_tokens_metric",
	"litellm_spend_metric",
}

// requestCounterLabel is the label LiteLLM uses on its request/failed counters.
// Since v1.81.x it is `requested_model` (alias-prefixed); older releases used
// `model`. The prober detects which one is present per replica.
const (
	reqLabelRequested = "requested_model"
	reqLabelModel     = "model"
	unattributedName  = "(unattributed)"
)

// ProbeAll scrapes every replica and aggregates the results.
func (p *Prober) ProbeAll(ctx context.Context, refs []PodRef) GatewayHealth {
	h := GatewayHealth{Pods: len(refs)}

	var (
		okPods, authFails, unreach int
		fleetReqRate               float64
		hasFleetReq                bool
	)
	models := map[string]*modelAcc{}
	// One accumulator per split dimension, keyed by the raw label value.
	splits := map[string]map[string]*splitAcc{}
	for _, d := range splitDims {
		splits[d.label] = map[string]*splitAcc{}
	}

	for _, ref := range refs {
		replicaKey := fmt.Sprintf("%s:%d", ref.IP, ref.Port)
		m, status := p.fetch(ctx, ref)
		switch status {
		case statusOK:
			okPods++
		case statusAuth:
			authFails++
			continue
		default:
			unreach++
			continue
		}

		if v, ok := m.Gauge("litellm_in_flight_requests"); ok {
			h.InFlightRequests += int(v)
		}
		if v, ok := m.Gauge("litellm_admission_queued_requests"); ok {
			h.AdmissionQueued += int(v)
		}
		if v, ok := m.Total("litellm_admission_rejected_requests"); ok {
			h.AdmissionRejected += int64(v)
		}
		if v, ok := m.Total("litellm_proxy_total_requests_metric"); ok {
			h.TotalRequests += int64(v)
		}
		if v, ok := m.Total("litellm_proxy_failed_requests_metric"); ok {
			h.FailedRequests += int64(v)
		}
		e2eP95, hasE2E := m.HistogramQuantile("litellm_request_total_latency_metric", 0.95)
		mergeQuantile(&h.E2EP95Seconds, e2eP95, hasE2E)
		ttftP95, hasTTFT := m.HistogramQuantile("litellm_llm_api_time_to_first_token_metric", 0.95)
		mergeQuantile(&h.TTFTP95Seconds, ttftP95, hasTTFT)
		queueP95, hasQueue := m.HistogramQuantile("litellm_request_queue_time_seconds", 0.95)
		mergeQuantile(&h.QueueTimeP95Seconds, queueP95, hasQueue)
		overheadP95, hasOverhead := m.HistogramQuantile("litellm_overhead_latency_metric", 0.95)
		mergeQuantile(&h.OverheadP95Seconds, overheadP95, hasOverhead)
		if v, ok := m.Total("litellm_spend_metric"); ok {
			setSum(&h.Spend, v)
		}

		reqTotal, _ := m.Total(requestCounterName)
		if r, ok := p.observeRate(replicaKey+"|fleetReq", reqTotal); ok {
			fleetReqRate += r
			hasFleetReq = true
		}

		// Per-model breakdown: base models come from the `model` label (tokens /
		// latency / spend metrics). Request/failed counters are labeled by the
		// alias-prefixed `requested_model`, so they are bridged to the base model
		// (see modelRequestValues) before being attributed.
		reqByModel, failedByModel := modelRequestValues(m)
		for _, name := range modelNames(m) {
			acc := models[name]
			if acc == nil {
				acc = &modelAcc{model: name}
				models[name] = acc
			}
			acc.requests += reqByModel[name]
			acc.failed += failedByModel[name]
			if v, ok := m.TotalByLabel("litellm_spend_metric", "model", name); ok {
				setSum(&acc.spend, v)
			}
			modelE2EP95, hasModelE2E := m.HistogramQuantileByLabel("litellm_request_total_latency_metric", "model", name, 0.95)
			mergeQuantile(&acc.e2eP95, modelE2EP95, hasModelE2E)
			modelTTFTP95, hasModelTTFT := m.HistogramQuantileByLabel("litellm_llm_api_time_to_first_token_metric", "model", name, 0.95)
			mergeQuantile(&acc.ttftP95, modelTTFTP95, hasModelTTFT)
			if r, ok := p.observeRate(replicaKey+"|req|"+name, reqByModel[name]); ok {
				acc.addRate(&acc.reqRate, r)
			}
			if tok, ok := m.TotalByLabel("litellm_total_tokens_metric", "model", name); ok {
				if r, ok := p.observeRate(replicaKey+"|tok|"+name, tok); ok {
					acc.addRate(&acc.tokRate, r)
				}
			}
		}

		// Per-dimension breakdown (app / API key / team): the request, token
		// and spend metrics are all labeled directly with each split label, so
		// every cell is summed straight off the series — no bridging.
		for _, dim := range splitDims {
			bucket := splits[dim.label]
			for _, raw := range labelValuesAcross(m, dim.label) {
				key := splitKey(raw)
				acc := bucket[key]
				if acc == nil {
					acc = &splitAcc{name: key}
					bucket[key] = acc
				}
				if v, ok := m.TotalByLabel("litellm_proxy_total_requests_metric", dim.label, raw); ok {
					acc.requests += v
				}
				if v, ok := m.TotalByLabel("litellm_total_tokens_metric", dim.label, raw); ok {
					acc.tokens += v
					if r, ok := p.observeRate(replicaKey+"|"+dim.rateKey+"|"+raw, v); ok {
						acc.addRate(&acc.tokRate, r)
					}
				}
				if v, ok := m.TotalByLabel("litellm_input_tokens_metric", dim.label, raw); ok {
					acc.tokensIn += v
				}
				if v, ok := m.TotalByLabel("litellm_output_tokens_metric", dim.label, raw); ok {
					acc.tokensOut += v
				}
				if v, ok := m.TotalByLabel("litellm_spend_metric", dim.label, raw); ok {
					setSum(&acc.spend, v)
				}
			}
		}

		// Model × split cross-tab: the token/spend counters carry both `model`
		// and the split label on the same series, so each (model, value) cell is
		// summed directly. Feeds the expandable per-model detail in the UI.
		for _, dim := range splitDims {
			cellTotals := map[modelCell]float64{}
			cellInputs := map[modelCell]float64{}
			cellOutputs := map[modelCell]float64{}
			cellSpends := map[modelCell]float64{}
			cellHasSpend := map[modelCell]bool{}
			for _, pr := range m.LabelPairValues("litellm_total_tokens_metric", "model", dim.label) {
				cellTotals[modelCell{pr.ValueA, pr.ValueB}] += pr.Value
			}
			for _, pr := range m.LabelPairValues("litellm_input_tokens_metric", "model", dim.label) {
				cellInputs[modelCell{pr.ValueA, pr.ValueB}] += pr.Value
			}
			for _, pr := range m.LabelPairValues("litellm_output_tokens_metric", "model", dim.label) {
				cellOutputs[modelCell{pr.ValueA, pr.ValueB}] += pr.Value
			}
			for _, pr := range m.LabelPairValues("litellm_spend_metric", "model", dim.label) {
				k := modelCell{pr.ValueA, pr.ValueB}
				cellSpends[k] += pr.Value
				cellHasSpend[k] = true
			}
			mergeModelCells(models, cellTotals, cellInputs, cellOutputs, cellSpends, cellHasSpend, dim.topField)
		}
	}

	h.Online = okPods > 0
	if !h.Online {
		switch {
		case authFails > 0:
			h.AuthError = "LiteLLM /metrics returned 401/403 — set LITELLM_METRICS_API_KEY to the gateway's metrics admin key"
		case unreach > 0:
			h.Error = "no LiteLLM replica reachable"
		default:
			h.Error = "no LiteLLM replicas discovered"
		}
		return h
	}

	if h.TotalRequests > 0 {
		rate := float64(h.FailedRequests) / float64(h.TotalRequests) * 100
		h.ErrorRatePct = &rate
	}
	if hasFleetReq {
		h.RequestsPerSec = &fleetReqRate
	}

	stats := make([]ModelStat, 0, len(models))
	for _, acc := range models {
		stats = append(stats, acc.stat())
	}
	sortModelStats(stats)
	h.Models = stats

	// One top-level table per dimension, sorted by token volume.
	for _, dim := range splitDims {
		table := make([]SplitStat, 0, len(splits[dim.label]))
		for _, acc := range splits[dim.label] {
			table = append(table, acc.stat())
		}
		sortSplitStats(table)
		switch dim.topField {
		case "apps":
			h.Apps = table
		case "apiKeys":
			h.APIKeys = table
		case "teams":
			h.Teams = table
		}
	}
	return h
}

// modelNames is the full model list: the union of `model` label values across
// all model-scoped metrics, in first-seen order.
func modelNames(m *llmhealth.Metrics) []string {
	seen := map[string]bool{}
	var out []string
	for _, metric := range modelMetrics {
		for _, n := range m.LabelValues(metric, "model") {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	return out
}

// labelValuesAcross is the union of raw `labelKey` label values across all
// split-dimension metrics, in first-seen order.
func labelValuesAcross(m *llmhealth.Metrics, labelKey string) []string {
	seen := map[string]bool{}
	var out []string
	for _, metric := range splitDimMetrics {
		for _, n := range m.LabelValues(metric, labelKey) {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	return out
}

// modelSplitAcc is one (model, split-value) token/spend cell.
type modelSplitAcc struct {
	name      string
	tokens    float64
	tokensIn  float64
	tokensOut float64
	spend     *float64
}

type modelAcc struct {
	model    string
	requests float64
	failed   float64
	reqRate  *float64
	tokRate  *float64
	e2eP95   *float64
	ttftP95  *float64
	spend    *float64
	// splits is keyed by split-dimension top-field name ("apps"/"apiKeys"/"teams"),
	// then by the raw label value.
	splits map[string]map[string]*modelSplitAcc
}

// splitCells returns the model's cells for one dimension, or nil.
func (a *modelAcc) splitCells(field string) map[string]*modelSplitAcc {
	if a.splits == nil {
		return nil
	}
	return a.splits[field]
}

func (a *modelAcc) addRate(dst **float64, r float64) {
	if *dst == nil {
		v := r
		*dst = &v
	} else {
		**dst += r
	}
}

func (a *modelAcc) stat() ModelStat {
	stat := ModelStat{Model: a.model}
	stat.RequestsTotal = int64(a.requests)
	stat.FailedRequests = int64(a.failed)
	if a.requests > 0 {
		rate := a.failed / a.requests * 100
		stat.ErrorRatePct = &rate
	}
	stat.RequestsPerSec = a.reqRate
	stat.TokensPerSec = a.tokRate
	stat.E2EP95Seconds = a.e2eP95
	stat.TTFTP95Seconds = a.ttftP95
	stat.Spend = a.spend
	stat.Apps = modelSplitStats(a.splitCells("apps"))
	stat.APIKeys = modelSplitStats(a.splitCells("apiKeys"))
	stat.Teams = modelSplitStats(a.splitCells("teams"))
	return stat
}

// modelCell is a (model, raw split-value) key for the cross-tab aggregation.
type modelCell struct{ model, raw string }

// mergeModelCells folds one replica's (model, value) token/spend cells into the
// per-model accumulators for the given dimension field ("apps"/"apiKeys"/"teams").
func mergeModelCells(
	models map[string]*modelAcc,
	totals, inputs, outputs, spends map[modelCell]float64,
	hasSpend map[modelCell]bool,
	field string,
) {
	for k, tot := range totals {
		acc := models[k.model]
		if acc == nil {
			continue // model has no latency/spend series; nothing to attach to
		}
		if acc.splits == nil {
			acc.splits = map[string]map[string]*modelSplitAcc{}
		}
		if acc.splits[field] == nil {
			acc.splits[field] = map[string]*modelSplitAcc{}
		}
		cells := acc.splits[field]
		cell := cells[k.raw]
		if cell == nil {
			cell = &modelSplitAcc{name: splitKey(k.raw)}
			cells[k.raw] = cell
		}
		cell.tokens += tot
		cell.tokensIn += inputs[k]
		cell.tokensOut += outputs[k]
		if hasSpend[k] {
			setSum(&cell.spend, spends[k])
		}
	}
}

// modelSplitStats renders a model's cells for one dimension, merging cells that
// normalize to the same split key (e.g. raw "None" and empty both become
// "(unattributed)") and sorting by total tokens (desc) then name.
func modelSplitStats(cells map[string]*modelSplitAcc) []ModelSplitStat {
	merged := map[string]*modelSplitAcc{}
	var order []string
	for _, c := range cells {
		if existing, ok := merged[c.name]; ok {
			existing.tokens += c.tokens
			existing.tokensIn += c.tokensIn
			existing.tokensOut += c.tokensOut
			if c.spend != nil {
				setSum(&existing.spend, *c.spend)
			}
			continue
		}
		merged[c.name] = &modelSplitAcc{name: c.name, tokens: c.tokens, tokensIn: c.tokensIn, tokensOut: c.tokensOut, spend: c.spend}
		order = append(order, c.name)
	}
	out := make([]ModelSplitStat, 0, len(order))
	for _, name := range order {
		c := merged[name]
		out = append(out, ModelSplitStat{
			Name:         name,
			TokensTotal:  int64(c.tokens),
			TokensInput:  int64(c.tokensIn),
			TokensOutput: int64(c.tokensOut),
			Spend:        c.spend,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TokensTotal != out[j].TokensTotal {
			return out[i].TokensTotal > out[j].TokensTotal
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// splitKey normalizes a raw split-dimension label value into its display key.
// LiteLLM emits the literal string "None" (and empty) for unlabeled requests
// (e.g. no metadata_trace_name / no team); all such values map to one
// "(unattributed)" bucket.
func splitKey(raw string) string {
	if raw == "" || strings.EqualFold(raw, "none") {
		return unattributedName
	}
	return raw
}

type splitAcc struct {
	name      string
	requests  float64
	tokens    float64
	tokensIn  float64
	tokensOut float64
	tokRate   *float64
	spend     *float64
}

func (a *splitAcc) addRate(dst **float64, r float64) {
	if *dst == nil {
		v := r
		*dst = &v
	} else {
		**dst += r
	}
}

func (a *splitAcc) stat() SplitStat {
	return SplitStat{
		Name:          a.name,
		RequestsTotal: int64(a.requests),
		TokensTotal:   int64(a.tokens),
		TokensInput:   int64(a.tokensIn),
		TokensOutput:  int64(a.tokensOut),
		TokensPerSec:  a.tokRate,
		Spend:         a.spend,
	}
}

// sortSplitStats orders a split table by total tokens (desc), then name, so the
// largest consumers lead the table regardless of map iteration order.
func sortSplitStats(s []SplitStat) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].TokensTotal != s[j].TokensTotal {
			return s[i].TokensTotal > s[j].TokensTotal
		}
		return s[i].Name < s[j].Name
	})
}

// sortModelStats orders models alphabetically. The per-model accumulators live
// in a map, so without an explicit sort the row order would be random on every
// scrape — the UI re-renders the table every poll and the rows would appear to
// shuffle. A stable name order keeps the table readable.
func sortModelStats(s []ModelStat) {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Model < s[j].Model
	})
}

// requestCounterName is the base name of LiteLLM's total-requests counter. The
// parser matches it suffix-tolerantly, so both `litellm_proxy_total_requests_metric`
// and `litellm_proxy_total_requests_metric_total` are read.
const requestCounterName = "litellm_proxy_total_requests_metric"

// requestCounterLabel reports which label this replica's request counters use
// for the model dimension.
func requestCounterLabel(m *llmhealth.Metrics) string {
	if len(m.LabelValues(requestCounterName, reqLabelRequested)) > 0 {
		return reqLabelRequested
	}
	return reqLabelModel
}

// modelRequestValues returns per-base-model total requests and failures for one
// replica. The counters are labeled by the alias-prefixed `requested_model`, so
// each value is bridged to the base `model` (via a metric that carries both
// labels) before being keyed by base model; anything that can't be bridged is
// dropped from the per-model rows (it still counts in the fleet total).
func modelRequestValues(m *llmhealth.Metrics) (map[string]float64, map[string]float64) {
	labelKey := requestCounterLabel(m)
	bridge := m.LabelPairMap(reqLabelRequested, "model")
	return projectByBaseModel(m, "litellm_proxy_total_requests_metric", labelKey, bridge),
		projectByBaseModel(m, "litellm_proxy_failed_requests_metric", labelKey, bridge)
}

func projectByBaseModel(m *llmhealth.Metrics, metric, labelKey string, bridge map[string]string) map[string]float64 {
	out := map[string]float64{}
	for _, raw := range m.LabelValues(metric, labelKey) {
		base, ok := bridge[raw]
		if !ok {
			continue
		}
		if v, ok := m.TotalByLabel(metric, labelKey, raw); ok {
			out[base] += v
		}
	}
	return out
}

// observeRate derives a per-second rate for one cumulative counter from diffs
// between successive probes, keyed by an opaque state key. ok=false on the
// first sample for that key (no previous point). Counter resets (replica
// restart) yield 0 growth, never a negative rate.
func (p *Prober) observeRate(key string, cur float64) (float64, bool) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	prev, ok := p.last[key]
	var r float64
	hasRate := false
	if ok && now.After(prev.ts) {
		if dt := now.Sub(prev.ts).Seconds(); dt > 0 {
			r = (cur - prev.value) / dt
			if r < 0 {
				r = 0 // replica restarted / counter reset
			}
			hasRate = true
		}
	}
	p.last[key] = &rateState{value: cur, ts: now}
	return r, hasRate
}

type fetchStatus int

const (
	statusOK fetchStatus = iota
	statusAuth
	statusUnreachable
)

// fetch scrapes one replica's /metrics endpoint.
func (p *Prober) fetch(ctx context.Context, ref PodRef) (*llmhealth.Metrics, fetchStatus) {
	url := fmt.Sprintf("http://%s:%d/metrics", ref.IP, ref.Port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, statusUnreachable
	}
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, statusUnreachable
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, statusAuth
	case resp.StatusCode != http.StatusOK:
		return nil, statusUnreachable
	}
	var sb strings.Builder
	_, _ = io.Copy(&sb, io.LimitReader(resp.Body, 8<<20))
	return llmhealth.ParsePrometheus(sb.String()), statusOK
}

func mergeQuantile(dst **float64, val float64, ok bool) {
	if !ok {
		return
	}
	if *dst == nil || val > **dst {
		v := val
		*dst = &v
	}
}

func setSum(dst **float64, v float64) {
	if *dst == nil {
		x := v
		*dst = &x
	} else {
		**dst += v
	}
}
