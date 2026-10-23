package servingdurable

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewPoint(t *testing.T) {
	now := time.Now()
	kv := 87.5
	pt := NewPoint("ai", "vllm-0", now, true, &kv, 4, 0, 1, nil, nil, nil, nil)
	assert.Equal(t, "ai/vllm-0", pt.PodKey)
	assert.True(t, pt.Online)
	assert.NotNil(t, pt.KVCachePercent)
	assert.Equal(t, 87.5, *pt.KVCachePercent)
	assert.Equal(t, 4, pt.Running)
	assert.Nil(t, pt.TTFTP95Seconds)
}

func TestNewPostgresRejectsUnreachableDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := NewPostgres(ctx, "postgres://user:pass@127.0.0.1:1/db?sslmode=disable", 7*24*time.Hour)
	assert.Error(t, err)
}
