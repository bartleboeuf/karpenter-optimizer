package llmhealth

import "strings"

// Metrics is a parsed Prometheus text exposition with query helpers. It is
// exported so other probers (e.g. the LiteLLM gateway prober) can reuse the
// same parsing without depending on the vLLM-specific PodHealth logic. Callers
// only use the methods; the underlying samples are package-internal.
type Metrics struct {
	samples    []promSample
	gauges     map[string][]namedValue
	counters   map[string][]namedValue
	histograms map[string]map[string]*histogram
}

// ParsePrometheus parses a Prometheus text exposition body into Metrics.
// Lines are grouped: `*_bucket` lines become cumulative histogram buckets,
// `*_sum`/`*_count` lines feed histogram means, `*_total` lines are counters,
// everything else is a gauge. A leading "vllm:" namespace prefix is stripped
// from names so vLLM versions with and without it are matched identically
// (harmless for other backends, which don't use the prefix).
func ParsePrometheus(text string) *Metrics {
	samples := parsePrometheus(text)
	gauges, counters, histograms := splitSamples(samples)
	return &Metrics{
		samples:    samples,
		gauges:     gauges,
		counters:   counters,
		histograms: histograms,
	}
}

// Gauge sums a gauge's value across all label variants.
func (m *Metrics) Gauge(name string) (float64, bool) {
	return gaugeValue(m.gauges, name)
}

// Counter sums a counter's value across all label variants.
func (m *Metrics) Counter(name string) (float64, bool) {
	return counterValue(m.counters, name)
}

// HistogramMean returns the weighted mean of a histogram (sum/count across
// label variants, 0 when empty).
func (m *Metrics) HistogramMean(name string) (float64, bool) {
	return histogramMean(m.histogramVariants(name))
}

// HistogramQuantile estimates a quantile of a histogram: per label variant
// the quantile is interpolated from its cumulative buckets, and the result
// is the max across variants (conservative for latency).
func (m *Metrics) HistogramQuantile(name string, q float64) (float64, bool) {
	return histogramQuantile(m.histogramVariants(name), q)
}

// histogramVariants returns all label-variant histograms of the named metric.
func (m *Metrics) histogramVariants(name string) []*histogram {
	return histogramVariants(m.histograms, name)
}

// CounterByLabel sums a counter's value for the given label (exact match),
// across all other label variants. Returns ok=false when no series carries
// that label value.
func (m *Metrics) CounterByLabel(name, labelKey, labelValue string) (float64, bool) {
	return sumByLabel(m.counters[name], labelKey, labelValue)
}

// Total returns a metric's value regardless of how the parser classified it.
// Useful for cumulative counters that lack the "_total" suffix (e.g.
// LiteLLM's litellm_proxy_failed_requests_metric), which the parser stores
// as gauges.
func (m *Metrics) Total(name string) (float64, bool) {
	c, cOK := counterValue(m.counters, name)
	g, gOK := gaugeValue(m.gauges, name)
	if !cOK && !gOK {
		return 0, false
	}
	return c + g, true
}

// TotalByLabel is Total restricted to one label value (see Total).
func (m *Metrics) TotalByLabel(name, labelKey, labelValue string) (float64, bool) {
	c, cOK := sumByLabel(m.counters[name], labelKey, labelValue)
	g, gOK := sumByLabel(m.gauges[name], labelKey, labelValue)
	if !cOK && !gOK {
		return 0, false
	}
	return c + g, true
}

// GaugeByLabel sums a gauge's value for the given label (exact match), across
// all other label variants.
func (m *Metrics) GaugeByLabel(name, labelKey, labelValue string) (float64, bool) {
	return sumByLabel(m.gauges[name], labelKey, labelValue)
}

// LabelValues returns the distinct values of labelKey across all series of
// the named metric — gauges, counters, and histograms (by base name, so
// bucket/sum/count series are matched too) — in first-seen order.
func (m *Metrics) LabelValues(name string, labelKey string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(labels map[string]string) {
		if labels == nil {
			return
		}
		if lv, ok := labels[labelKey]; ok && lv != "" && !seen[lv] {
			seen[lv] = true
			out = append(out, lv)
		}
	}
	for _, s := range m.samples {
		base := s.name
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if b, ok := strings.CutSuffix(base, suffix); ok {
				base = b
			}
		}
		if base == name {
			add(s.labels)
		}
	}
	return out
}

// HistogramQuantileByLabel estimates a quantile for the histogram series whose
// base name equals `name` and which carry labelKey=labelValue (other labels
// summed). Returns ok=false when no matching bucket series exists.
func (m *Metrics) HistogramQuantileByLabel(name, labelKey, labelValue string, q float64) (float64, bool) {
	bucketName, sumName, countName := name+"_bucket", name+"_sum", name+"_count"
	var merged histogram
	for _, s := range m.samples {
		switch s.name {
		case bucketName, sumName, countName:
		default:
			continue
		}
		if v, ok := s.labels[labelKey]; !ok || v != labelValue {
			continue
		}
		switch s.name {
		case bucketName:
			le, ok := leFromLabels(s.labels)
			if !ok {
				continue
			}
			merged.buckets = append(merged.buckets, bucket{le: le, count: s.value})
		case sumName:
			merged.sum += s.value
		case countName:
			merged.count += s.value
		}
	}
	if len(merged.buckets) == 0 {
		return 0, false
	}
	return histogramVariantQuantile(&merged, q)
}

func sumByLabel(vs []namedValue, labelKey, labelValue string) (float64, bool) {
	var total float64
	found := false
	for _, v := range vs {
		if v.labels == nil {
			continue
		}
		if lv, ok := v.labels[labelKey]; ok && lv == labelValue {
			total += v.value
			found = true
		}
	}
	return total, found
}
