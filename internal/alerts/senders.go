package alerts

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// maxResponseBytes bounds how much of a channel response body we drain (we
// never log bodies, only status codes, to avoid leaking payloads).
const maxResponseBytes = 1 << 20

// httpClient is the shared outbound client. It is a package variable so tests
// can swap in a capture transport (no real network, no DNS).
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	},
}

// Sender delivers one outbound message on a channel.
type Sender interface {
	Name() string
	Send(ctx context.Context, ch ChannelConfig, payload WebhookPayload) error
}

// NewSender returns the Sender for a channel type, or an error for unknown
// types. The URL is validated (SSRF policy) up front so a bad channel is
// rejected before any message is attempted.
func NewSender(ch ChannelConfig) (Sender, error) {
	switch strings.ToLower(ch.Type) {
	case "ntfy":
		if err := checkURL(ch.URL); err != nil {
			return nil, fmt.Errorf("ntfy: %w", err)
		}
		return &ntfySender{}, nil
	case "discord":
		if h := hostOf(ch.URL); !discordHost.MatchString(h) {
			return nil, fmt.Errorf("discord: host %q is not a discord webhook", h)
		}
		if err := checkURL(ch.URL); err != nil {
			return nil, fmt.Errorf("discord: %w", err)
		}
		return &discordSender{}, nil
	case "slack":
		if h := hostOf(ch.URL); !slackHost.MatchString(h) {
			return nil, fmt.Errorf("slack: host %q is not a slack webhook", h)
		}
		if err := checkURL(ch.URL); err != nil {
			return nil, fmt.Errorf("slack: %w", err)
		}
		return &slackSender{}, nil
	case "webhook", "":
		if err := checkURL(ch.URL); err != nil {
			return nil, fmt.Errorf("webhook: %w", err)
		}
		return &webhookSender{}, nil
	default:
		return nil, fmt.Errorf("unknown channel type %q", ch.Type)
	}
}

// checkURL validates a channel URL against the SSRF policy (used at config
// time, before any message is sent).
func checkURL(raw string) error {
	_, _, err := validateChannelURL(raw)
	return err
}

// renderText builds the human-readable body for chat-style channels.
func renderText(p WebhookPayload) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%s* %s (%s)\n", p.Status, p.Title, p.Source)
	for _, a := range p.Alerts {
		fmt.Fprintf(&b, "• [%s] %s: %s\n", a.Severity, a.Title, a.Message)
	}
	return b.String()
}

// postJSON is the single SSRF-safe transport: it validates the URL, pins the
// host to a resolved public IP, never follows redirects, sets SNI to the
// original host, and discards (bounded) the response body without logging it.
func postJSON(ctx context.Context, ch ChannelConfig, body []byte, contentType string, headers map[string]string) (int, error) {
	req, err := buildRequest(ctx, ch, body, contentType, headers)
	if err != nil {
		return 0, err
	}
	// Clone the shared client so we can set SNI to the original host for this
	// pinned-IP connection without affecting other senders.
	client := *httpClient
	client.Transport = &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSClientConfig: &tls.Config{
			// SNI = original hostname (matches the server cert); the
			// connection itself is pinned to the resolved public IP.
			ServerName: sniHost(req.Host),
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	}
	return resp.StatusCode, nil
}

// buildRequest validates the channel URL and constructs the outbound POST
// request with the host pinned to a resolved public IP, the Host header set to
// the original host, and the given body/headers. Pure (no I/O) so tests can
// assert on the exact wire request without a network.
func buildRequest(ctx context.Context, ch ChannelConfig, body []byte, contentType string, headers map[string]string) (*http.Request, error) {
	sendURL, originalHost, err := validateChannelURL(ch.URL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Host = originalHost
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "karpenter-optimizer-alerts/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// sniHost returns the original hostname (port stripped) to use as the TLS SNI
// for a pinned-IP connection.
func sniHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// --- concrete senders -----------------------------------------------------

// wire describes the outbound request a sender produces for a payload, split
// out from Send so payload shaping is testable without a network.
type wire struct {
	body        []byte
	contentType string
	headers     map[string]string
}

type ntfySender struct{}

func (n *ntfySender) Name() string { return "ntfy" }
func (n *ntfySender) build(ch ChannelConfig, p WebhookPayload) wire {
	headers := map[string]string{}
	if ch.Priority > 0 && ch.Priority <= 5 {
		headers["Priority"] = fmt.Sprintf("%d", ch.Priority)
	}
	if ch.Tags != "" {
		headers["Tags"] = ch.Tags
	}
	return wire{body: []byte(renderText(p)), contentType: "text/plain; charset=utf-8", headers: headers}
}
func (n *ntfySender) Send(ctx context.Context, ch ChannelConfig, p WebhookPayload) error {
	w := n.build(ch, p)
	code, err := postJSON(ctx, ch, w.body, w.contentType, w.headers)
	if err != nil {
		return fmt.Errorf("ntfy send: %w", err)
	}
	if code >= 400 {
		return fmt.Errorf("ntfy returned %d", code)
	}
	return nil
}

type discordSender struct{}

func (d *discordSender) Name() string { return "discord" }
func (d *discordSender) build(ch ChannelConfig, p WebhookPayload) wire {
	body, _ := json.Marshal(map[string]string{"content": renderText(p)})
	return wire{body: body, contentType: "application/json"}
}
func (d *discordSender) Send(ctx context.Context, ch ChannelConfig, p WebhookPayload) error {
	w := d.build(ch, p)
	code, err := postJSON(ctx, ch, w.body, w.contentType, w.headers)
	if err != nil {
		return fmt.Errorf("discord send: %w", err)
	}
	if code >= 400 {
		return fmt.Errorf("discord returned %d", code)
	}
	return nil
}

type slackSender struct{}

func (s *slackSender) Name() string { return "slack" }
func (s *slackSender) build(ch ChannelConfig, p WebhookPayload) wire {
	body, _ := json.Marshal(map[string]string{"text": renderText(p)})
	return wire{body: body, contentType: "application/json"}
}
func (s *slackSender) Send(ctx context.Context, ch ChannelConfig, p WebhookPayload) error {
	w := s.build(ch, p)
	code, err := postJSON(ctx, ch, w.body, w.contentType, w.headers)
	if err != nil {
		return fmt.Errorf("slack send: %w", err)
	}
	if code >= 400 {
		return fmt.Errorf("slack returned %d", code)
	}
	return nil
}

type webhookSender struct{}

func (w *webhookSender) Name() string { return "webhook" }
func (w *webhookSender) build(ch ChannelConfig, p WebhookPayload) wire {
	body, _ := json.Marshal(p)
	return wire{body: body, contentType: "application/json"}
}
func (w *webhookSender) Send(ctx context.Context, ch ChannelConfig, p WebhookPayload) error {
	wr := w.build(ch, p)
	code, err := postJSON(ctx, ch, wr.body, wr.contentType, wr.headers)
	if err != nil {
		return fmt.Errorf("webhook send: %w", err)
	}
	if code >= 400 {
		return fmt.Errorf("webhook returned %d", code)
	}
	return nil
}
