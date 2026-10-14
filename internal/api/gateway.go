package api

import (
	"context"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/karpenter-optimizer/internal/litellm"
)

// litellmProber is process-wide so counter diffs (rates) survive across
// requests, same pattern as servingProber. NewServer replaces it with a
// configured instance (metrics auth key).
var litellmProber = litellm.NewProber("")

var (
	gatewayCacheMu   sync.Mutex
	gatewayCacheLast time.Time
	gatewayCacheData litellm.GatewayHealth
	gatewayCacheOK   bool
)

// getServingGateway reports LiteLLM AI-gateway health: fleet-wide in-flight /
// queue / error / latency / spend figures plus a per-model breakdown.
// Results are cached for 15s to bound scrape load.
//
// @Summary      LiteLLM gateway health
// @Description  Aggregated gateway metrics (in-flight requests, admission queue, error rate, E2E/TTFT p95, overhead, spend) and per-model breakdown for all discovered LiteLLM proxy replicas.
// @Tags         serving
// @Produce      json
// @Success      200  {object}  object  "Gateway health"
// @Failure      503  {object}  map[string]interface{}  "Kubernetes client not configured"
// @Failure      500  {object}  map[string]interface{}  "Internal server error"
// @Router       /serving/gateway [get]
func (s *Server) getServingGateway(c *gin.Context) {
	if s.k8sClient == nil {
		c.JSON(503, gin.H{"error": "Kubernetes client not configured"})
		return
	}

	if cached, ok := gatewayCache(); ok {
		c.JSON(200, gin.H{"gateway": cached})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	pods, err := s.k8sClient.FindLiteLLMPods(ctx)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	refs := make([]litellm.PodRef, 0, len(pods))
	for _, p := range pods {
		refs = append(refs, litellm.PodRef{IP: p.IP, Port: p.Port})
	}

	// No LiteLLM proxy in the cluster: the frontend hides the gateway card.
	if len(refs) == 0 {
		c.JSON(200, gin.H{"gateway": nil})
		return
	}

	health := litellmProber.ProbeAll(ctx, refs)
	storeGatewayCache(health)
	c.JSON(200, gin.H{"gateway": health})
}

func gatewayCache() (litellm.GatewayHealth, bool) {
	gatewayCacheMu.Lock()
	defer gatewayCacheMu.Unlock()
	if gatewayCacheOK && time.Since(gatewayCacheLast) < servingCacheTTL {
		return gatewayCacheData, true
	}
	return litellm.GatewayHealth{}, false
}

func storeGatewayCache(health litellm.GatewayHealth) {
	gatewayCacheMu.Lock()
	defer gatewayCacheMu.Unlock()
	gatewayCacheLast = time.Now()
	gatewayCacheData = health
	gatewayCacheOK = true
}
