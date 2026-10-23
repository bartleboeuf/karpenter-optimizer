package alerts

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
)

// ssrfSafeHost is the allow/deny policy for outbound channel URLs. It refuses
// anything that is not a public http(s) host: link-local, loopback, metadata,
// multicast and reserved ranges. The IP is resolved and returned so the caller
// can pin the connection to it (preventing DNS-rebinding between validation and
// send).
func ssrfSafeHost(host string) (pinned string, err error) {
	// Bare host or host:port.
	name := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	}
	// Already an IP literal.
	if ip := net.ParseIP(name); ip != nil {
		if !publicIP(ip) {
			return "", fmt.Errorf("host %q is not a public address", name)
		}
		return name, nil
	}
	if name == "" {
		return "", fmt.Errorf("empty host")
	}
	var pinnedIPs []string
	ips, err := net.DefaultResolver.LookupIPAddr(context.Background(), name)
	if err != nil {
		return "", fmt.Errorf("resolve %q: %w", name, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no addresses for %q", name)
	}
	for _, ip := range ips {
		if !publicIP(ip.IP) {
			return "", fmt.Errorf("host %q resolves to non-public address", name)
		}
		pinnedIPs = append(pinnedIPs, ip.IP.String())
	}
	// Pin to the first public address (deterministic send target).
	return pinnedIPs[0], nil
}

// publicIP reports whether ip is a routable public unicast address.
func publicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	if ip.IsMulticast() || ip.IsUnspecified() || ip.IsPrivate() {
		return false
	}
	// Cloud provider metadata endpoint (IPv4).
	if v4 := ip.To4(); v4 != nil && v4[0] == 169 && v4[1] == 254 {
		return false
	}
	return true
}

// validateChannelURL validates a channel URL's scheme and host, returning the
// URL to send to (with the host pinned to a resolved public IP) and the
// original host (for the Host header / SNI).
func validateChannelURL(raw string) (sendURL string, originalHost string, err error) {
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", fmt.Errorf("parse URL: %w", perr)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", fmt.Errorf("scheme %q not allowed (http/https only)", u.Scheme)
	}
	host := u.Host
	if host == "" {
		return "", "", fmt.Errorf("missing host")
	}
	pinned, verr := ssrfSafeHost(host)
	if verr != nil {
		return "", "", verr
	}
	// Rebuild the URL with the host pinned to the resolved public IP. The
	// original Host is preserved via the request's Host header / SNI.
	send := *u
	// Keep the port from the original URL.
	if _, port, perr := net.SplitHostPort(host); perr == nil && port != "" {
		send.Host = net.JoinHostPort(pinned, port)
	} else {
		send.Host = pinned
	}
	return send.String(), host, nil
}

// isDiscord validates that a discord webhook URL points at discord.com.
var discordHost = regexp.MustCompile(`^discord(app)?\.com$`)
var slackHost = regexp.MustCompile(`^slack\.com$`)

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
