package recommender

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/karpenter-optimizer/internal/awspricing"
	"github.com/karpenter-optimizer/internal/config"
	"github.com/stretchr/testify/assert"
)

type fakePricingProvider struct {
	instanceTypes []string
	listErr       error
	prices        map[string]float64
	priceErrors   map[string]error
}

func (f *fakePricingProvider) GetAvailableEC2InstanceTypes(context.Context, string) ([]string, error) {
	return f.instanceTypes, f.listErr
}

func (f *fakePricingProvider) GetProductPrice(_ context.Context, instanceType, _ string) (float64, error) {
	if err, ok := f.priceErrors[instanceType]; ok {
		return 0, err
	}
	if price, ok := f.prices[instanceType]; ok {
		return price, nil
	}
	return 0, errors.Join(awspricing.ErrNoPricingFound, errors.New("instance unavailable in region"))
}

func TestNewRecommender(t *testing.T) {
	cfg := &config.Config{}
	rec := NewRecommender(cfg)

	assert.NotNil(t, rec)
}

func TestEstimateCost(t *testing.T) {
	cfg := &config.Config{}
	rec := NewRecommender(cfg)
	ctx := context.Background()

	tests := []struct {
		name          string
		instanceTypes []string
		capacityType  string
		nodeCount     int
		expectError   bool
	}{
		{
			name:          "valid on-demand instance",
			instanceTypes: []string{"m5.large"},
			capacityType:  "on-demand",
			nodeCount:     1,
			expectError:   false,
		},
		{
			name:          "valid spot instance",
			instanceTypes: []string{"m5.large"},
			capacityType:  "spot",
			nodeCount:     1,
			expectError:   false,
		},
		{
			name:          "multiple nodes",
			instanceTypes: []string{"m5.large"},
			capacityType:  "on-demand",
			nodeCount:     3,
			expectError:   false,
		},
		{
			name:          "invalid instance type",
			instanceTypes: []string{"invalid.type"},
			capacityType:  "on-demand",
			nodeCount:     1,
			expectError:   false, // Should fallback gracefully
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := rec.EstimateCost(ctx, tt.instanceTypes, tt.capacityType, tt.nodeCount)

			if !tt.expectError {
				assert.GreaterOrEqual(t, cost, 0.0, "Cost should be non-negative")
			}
		})
	}
}

func TestEstimateInstanceCapacity(t *testing.T) {
	cfg := &config.Config{}
	rec := NewRecommender(cfg)

	tests := []struct {
		name         string
		instanceType string
		expectCPU    float64
		expectMemory float64
	}{
		{
			name:         "m5.large",
			instanceType: "m5.large",
			expectCPU:    2.0,
			expectMemory: 8.0,
		},
		{
			name:         "m5.xlarge",
			instanceType: "m5.xlarge",
			expectCPU:    4.0,
			expectMemory: 16.0,
		},
		{
			name:         "unknown instance type",
			instanceType: "unknown.type",
			expectCPU:    4.0, // Default fallback
			expectMemory: 8.0, // Default fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpu, memory := rec.estimateInstanceCapacity(tt.instanceType)

			assert.Equal(t, tt.expectCPU, cpu, "CPU should match expected value")
			assert.Equal(t, tt.expectMemory, memory, "Memory should match expected value")
		})
	}
}

func TestChinaHardcodedCandidatesExcludeAMDFamilies(t *testing.T) {
	rec := &Recommender{
		config:     &config.Config{AWSRegion: "cn-north-1"},
		awsPricing: &fakePricingProvider{listErr: errors.New("pricing index unavailable")},
	}

	got := rec.getCandidateInstanceTypes("amd64", 4, 16)
	assert.NotEmpty(t, got, "China should still use the non-AMD hardcoded fallback")
	for _, instanceType := range got {
		family, _, _ := strings.Cut(instanceType, ".")
		assert.False(t, strings.HasSuffix(strings.ToLower(family), "a"), "%s should not be an AMD family", instanceType)
	}
}

func TestChinaHardcodedCandidateHelperExcludesAMDFamilies(t *testing.T) {
	rec := &Recommender{config: &config.Config{AWSRegion: "cn-north-1"}}
	got := rec.getHardcodedCandidateInstanceTypes("amd64", 4, 16)
	assert.NotEmpty(t, got)
	for _, instanceType := range got {
		family, _, _ := strings.Cut(instanceType, ".")
		assert.False(t, strings.HasSuffix(strings.ToLower(family), "a"), "%s should not be an AMD family", instanceType)
	}
}

func TestFilterAMDInstanceTypes(t *testing.T) {
	got := filterAMDInstanceTypes([]string{"m6a.xlarge", "c6a.2xlarge", "r6a.large", "m6i.xlarge", "m7g.large", "t3a.medium"})
	assert.Equal(t, []string{"m6i.xlarge", "m7g.large"}, got)
}

func TestChinaFallbackPriceIsRegionSpecific(t *testing.T) {
	for _, tt := range []struct {
		region string
		want   float64
	}{
		{region: "cn-north-1", want: 2.02642},
		{region: "cn-northwest-1", want: 1.35551},
	} {
		t.Run(tt.region, func(t *testing.T) {
			rec := &Recommender{config: &config.Config{AWSRegion: tt.region}}
			result, sources := rec.EstimateCostWithSource(context.Background(), []string{"m6i.xlarge"}, "on-demand", 1)
			assert.Equal(t, tt.want, result.Cost)
			assert.Equal(t, "CNY", result.Currency)
			assert.Equal(t, PricingSourceRegionalFallback, result.Source)
			assert.Equal(t, PricingSourceRegionalFallback, sources["m6i.xlarge"])
		})
	}
}

func TestChinaFallbackPriceDoesNotMaskRegionalNoProduct(t *testing.T) {
	rec := &Recommender{
		config: &config.Config{AWSRegion: "cn-north-1"},
		awsPricing: &fakePricingProvider{
			priceErrors: map[string]error{"m6i.xlarge": awspricing.ErrNoPricingFound},
		},
	}
	result, _ := rec.EstimateCostWithSource(context.Background(), []string{"m6i.xlarge"}, "on-demand", 1)
	assert.Zero(t, result.Cost)
	assert.Equal(t, PricingSourceUnknown, result.Source)
}

func TestChinaFallbackSpotPriceUsesEstimateDiscount(t *testing.T) {
	rec := &Recommender{config: &config.Config{AWSRegion: "cn-north-1"}}
	result, _ := rec.EstimateCostWithSource(context.Background(), []string{"m6i.xlarge"}, "spot", 1)
	assert.InDelta(t, 2.02642*awspricing.SpotDiscountFactor, result.Cost, 0.000001)
	assert.Equal(t, PricingSourceRegionalFallback, result.Source)
}

func TestUSEast1FallbackPricesMatchPricingSnapshot(t *testing.T) {
	rec := &Recommender{config: &config.Config{AWSRegion: "us-east-1"}}
	result, sources := rec.EstimateCostWithSource(context.Background(), []string{"x8g.xlarge", "r8i.xlarge"}, "on-demand", 2)
	assert.InDelta(t, 0.3908+0.27784, result.Cost, 0.000001)
	assert.Equal(t, PricingSourceHardcoded, result.Source)
	assert.Equal(t, PricingSourceHardcoded, sources["x8g.xlarge"])
}

func TestUSEast1FallbackDoesNotOverrideRegionalNoProduct(t *testing.T) {
	rec := &Recommender{
		config: &config.Config{AWSRegion: "us-east-1"},
		awsPricing: &fakePricingProvider{
			priceErrors: map[string]error{"m6i.xlarge": awspricing.ErrNoPricingFound},
		},
	}
	result, _ := rec.EstimateCostWithSource(context.Background(), []string{"m6i.xlarge"}, "on-demand", 1)
	assert.Zero(t, result.Cost)
	assert.Equal(t, PricingSourceUnknown, result.Source)
}

func TestUSFallbackPricesAreNotUsedOutsideUSEast1(t *testing.T) {
	rec := &Recommender{config: &config.Config{AWSRegion: "eu-west-1"}}
	result, _ := rec.EstimateCostWithSource(context.Background(), []string{"m6i.xlarge"}, "on-demand", 1)
	assert.Zero(t, result.Cost, "without regional pricing, us-east-1 fallback must not be reused")
	assert.Equal(t, PricingSourceUnknown, result.Source)
}

func TestChinaOptimizationSkipsUnpricedCandidate(t *testing.T) {
	rec := &Recommender{
		config: &config.Config{AWSRegion: "cn-north-1"},
		awsPricing: &fakePricingProvider{
			instanceTypes: []string{"m6a.xlarge", "m6i.xlarge"},
			prices:        map[string]float64{"m6i.xlarge": 0.2},
		},
	}

	got, nodes, cost, _ := rec.findOptimalInstanceTypesWithCapacityType(context.Background(), 2, 4, "amd64", false, false)
	assert.Equal(t, []string{"m6i.xlarge"}, got)
	assert.Equal(t, 1, nodes)
	assert.Equal(t, 0.2, cost)
}

