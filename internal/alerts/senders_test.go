package alerts

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

// --- SSRF policy ----------------------------------------------------------

func TestPublicIP(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"198.51.100.7", true}, // TEST-NET-2 (public)
		{"2001:4860:4860::8888", true},
		{"10.0.0.1", false},        // RFC1918
		{"192.168.1.1", false},     // RFC1918
		{"172.16.0.1", false},      // RFC1918
		{"127.0.0.1", false},       // loopback
		{"169.254.169.254", false}, // cloud metadata
		{"169.254.1.1", false},     // link-local
		{"224.0.0.1", false},       // multicast
		{"0.0.0.0", false},         // unspecified
		{"fd00::1", false},         // unique-local (private)
	}
	for _, tc := range cases {
		if got := publicIP(net.ParseIP(tc.ip)); got != tc.want {
			t.Errorf("publicIP(%s) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

func TestValidateChannelURL_SchemeAndHost(t *testing.T) {
	// Non-http(s) schemes and missing hosts are rejected (no resolution).
	for _, bad := range []string{
		"file:///etc/passwd",
		"ftp://example.com/x",
		"https://",
	} {
		if _, _, err := validateChannelURL(bad); err == nil {
			t.Errorf("validateChannelURL(%q): expected error", bad)
		}
	}
}

func TestValidateChannelURL_PrivateIPRefused(t *testing.T) {
	for _, bad := range []string{
		"http://127.0.0.1:8080/hook",
		"http://10.0.0.1/hook",
		"http://169.254.169.254/latest/meta-data/",
		"https://[fd00::1]/hook",
	} {
		if _, _, err := validateChannelURL(bad); err == nil {
			t.Errorf("validateChannelURL(%q): expected SSRF refusal", bad)
		}
	}
}

func TestValidateChannelURL_PublicIPLiteral(t *testing.T) {
	sendURL, originalHost, err := validateChannelURL("https://93.184.216.34:8443/hook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sendURL != "https://93.184.216.34:8443/hook" {
		t.Errorf("sendURL = %q, want host pinned to itself", sendURL)
	}
	if originalHost != "93.184.216.34:8443" {
		t.Errorf("originalHost = %q", originalHost)
	}
}

func TestSsrfSafeHost_IPLiterals(t *testing.T) {
	if _, err := ssrfSafeHost("192.168.0.10"); err == nil {
		t.Fatalf("expected refusal for private IP literal")
	}
	if _, err := ssrfSafeHost("169.254.169.254"); err == nil {
		t.Fatalf("expected refusal for metadata IP")
	}
	if got, err := ssrfSafeHost("93.184.216.34"); err != nil || got != "93.184.216.34" {
		t.Fatalf("public IP literal: got %q, %v", got, err)
	}
}

func TestSniHost(t *testing.T) {
	if got := sniHost("ntfy.example.com:8443"); got != "ntfy.example.com" {
		t.Errorf("sniHost with port = %q", got)
	}
	if got := sniHost("ntfy.example.com"); got != "ntfy.example.com" {
		t.Errorf("sniHost bare = %q", got)
	}
}

// --- sender construction / config-time validation -------------------------

func TestSenderUnknownType(t *testing.T) {
	if _, err := NewSender(ChannelConfig{Type: "carrier-pigeon", URL: "https://x"}); err == nil {
		t.Fatalf("expected error for unknown channel type")
	}
}

func TestSenderRejectsPrivateURL(t *testing.T) {
	if _, err := NewSender(ChannelConfig{Type: "webhook", URL: "http://10.0.0.5/hook"}); err == nil {
		t.Fatalf("expected SSRF refusal for private URL at config time")
	}
}

func TestSenderRejectsDiscordWrongHost(t *testing.T) {
	// discord only allows discord.com / discordapp.com webhook hosts.
	if _, err := NewSender(ChannelConfig{Type: "discord", URL: "https://93.184.216.34/hook"}); err == nil {
		t.Fatalf("expected discord host refusal")
	}
}

// --- payload shaping (pure build(), no network) ---------------------------

func testPayload() WebhookPayload {
	return WebhookPayload{
		Source:   "karpenter-optimizer",
		Status:   StatusFiring,
		Severity: "warning",
		Title:    "Cluster cost spike",
		Alerts: []Alert{{
			ID: CondCostSpike, Title: "Cluster cost spike",
			Message: "cost 15, +60% vs avg", Severity: SeverityWarning,
			FiredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		}},
	}
}

func TestWebhookPayloadShaping(t *testing.T) {
	w := (&webhookSender{}).build(ChannelConfig{}, testPayload())
	var got WebhookPayload
	if err := json.Unmarshal(w.body, &got); err != nil {
		t.Fatalf("webhook body is not the payload JSON: %v", err)
	}
	if got.Source != "karpenter-optimizer" || got.Status != StatusFiring || len(got.Alerts) != 1 {
		t.Errorf("webhook payload not shaped correctly: %+v", got)
	}
	if got.Alerts[0].ID != CondCostSpike || got.Alerts[0].FiredAt == "" {
		t.Errorf("alert fields missing: %+v", got.Alerts[0])
	}
	if w.contentType != "application/json" {
		t.Errorf("content type = %q", w.contentType)
	}
}

func TestDiscordPayloadShaping(t *testing.T) {
	w := (&discordSender{}).build(ChannelConfig{}, testPayload())
	var got map[string]string
	if err := json.Unmarshal(w.body, &got); err != nil {
		t.Fatalf("discord body not JSON: %v", err)
	}
	if !strings.Contains(got["content"], "Cluster cost spike") {
		t.Errorf("discord content missing message: %q", got["content"])
	}
}

func TestSlackPayloadShaping(t *testing.T) {
	w := (&slackSender{}).build(ChannelConfig{}, testPayload())
	var got map[string]string
	if err := json.Unmarshal(w.body, &got); err != nil {
		t.Fatalf("slack body not JSON: %v", err)
	}
	if !strings.Contains(got["text"], "Cluster cost spike") {
		t.Errorf("slack text missing message: %q", got["text"])
	}
}

func TestNtfyBuild(t *testing.T) {
	ch := ChannelConfig{Type: "ntfy", Priority: 3, Tags: "warning,cluster"}
	w := (&ntfySender{}).build(ch, testPayload())
	if w.contentType != "text/plain; charset=utf-8" {
		t.Errorf("ntfy content type = %q", w.contentType)
	}
	if w.headers["Priority"] != "3" {
		t.Errorf("ntfy Priority header = %q, want 3", w.headers["Priority"])
	}
	if w.headers["Tags"] != "warning,cluster" {
		t.Errorf("ntfy Tags header = %q", w.headers["Tags"])
	}
	if !strings.Contains(string(w.body), "Cluster cost spike") {
		t.Errorf("ntfy body missing message: %q", string(w.body))
	}
	// Out-of-range priority is dropped (ntfy accepts 1..5).
	w2 := (&ntfySender{}).build(ChannelConfig{Priority: 9}, testPayload())
	if _, ok := w2.headers["Priority"]; ok {
		t.Errorf("out-of-range priority should be dropped, got %q", w2.headers["Priority"])
	}
}

func TestRenderText(t *testing.T) {
	text := renderText(testPayload())
	if !strings.Contains(text, "firing") || !strings.Contains(text, "Cluster cost spike") ||
		!strings.Contains(text, "warning") {
		t.Errorf("renderText missing fields: %q", text)
	}
}

// --- manager masking ------------------------------------------------------

func TestManager_MaskedURLs(t *testing.T) {
	m := NewManager(t.TempDir() + "/alerts.json")
	if err := m.UpdateConfig(Config{
		Enabled:  true,
		Channels: []ChannelConfig{{Type: "ntfy", URL: "https://ntfy.example.com/my-secret-topic"}},
	}); err != nil {
		t.Fatalf("update config: %v", err)
	}
	masked := m.MaskedURLs()
	if len(masked) != 1 {
		t.Fatalf("expected 1 masked channel, got %d", len(masked))
	}
	if strings.Contains(masked[0].URL, "my-secret-topic") {
		t.Errorf("masked URL leaks the secret path: %q", masked[0].URL)
	}
	if !strings.Contains(masked[0].URL, "ntfy.example.com") {
		t.Errorf("masked URL should keep the host: %q", masked[0].URL)
	}
}

func TestMaskURL_ParsesScheme(t *testing.T) {
	if got := maskURL("https://ntfy.example.com/topic"); !strings.HasPrefix(got, "https://ntfy.example.com/") {
		t.Errorf("maskURL = %q", got)
	}
	if got := maskURL("not a url"); got != "***" {
		t.Errorf("maskURL unparseable = %q, want ***", got)
	}
}
