package awspricing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

func TestChinaPricingRegionConfiguration(t *testing.T) {
	tests := []struct {
		region       string
		location     string
		apiEndpoint  string
		indexBaseURL string
	}{
		{
			region:       "cn-north-1",
			location:     "China (Beijing)",
			apiEndpoint:  "https://api.pricing.cn-northwest-1.amazonaws.com.cn",
			indexBaseURL: "https://pricing.cn-northwest-1.amazonaws.com.cn",
		},
		{
			region:       "cn-northwest-1",
			location:     "China (Ningxia)",
			apiEndpoint:  "https://api.pricing.cn-northwest-1.amazonaws.com.cn",
			indexBaseURL: "https://pricing.cn-northwest-1.amazonaws.com.cn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			if got := getLocationForRegion(tt.region); got != tt.location {
				t.Errorf("getLocationForRegion(%q) = %q, want %q", tt.region, got, tt.location)
			}
			if got := pricingAPIEndpoint(tt.region); got != tt.apiEndpoint {
				t.Errorf("pricingAPIEndpoint(%q) = %q, want %q", tt.region, got, tt.apiEndpoint)
			}
			if got := pricingIndexBaseURL(tt.region); got != tt.indexBaseURL {
				t.Errorf("pricingIndexBaseURL(%q) = %q, want %q", tt.region, got, tt.indexBaseURL)
			}
		})
	}
}

func TestPricingCurrencyForRegion(t *testing.T) {
	tests := []struct {
		region string
		want   string
	}{
		{region: "cn-north-1", want: "CNY"},
		{region: "cn-northwest-1", want: "CNY"},
		{region: "us-east-1", want: "USD"},
		{region: "eu-west-1", want: "USD"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			if got := PricingCurrencyForRegion(tt.region); got != tt.want {
				t.Errorf("PricingCurrencyForRegion(%q) = %q, want %q", tt.region, got, tt.want)
			}
		})
	}
}

func TestExtractPriceFromDimensionUsesRegionCurrency(t *testing.T) {
	dimension := map[string]interface{}{
		"pricePerUnit": map[string]interface{}{
			"USD": "0.12",
			"CNY": "0.84",
		},
	}
	tests := []struct {
		region string
		want   float64
	}{
		{region: "cn-north-1", want: 0.84},
		{region: "us-east-1", want: 0.12},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			client := &Client{region: tt.region}
			got, err := client.extractPriceFromDimension(dimension)
			if err != nil {
				t.Fatalf("extractPriceFromDimension() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("extractPriceFromDimension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNonChinaPricingEndpointsRemainUnchanged(t *testing.T) {
	if got := pricingAPIEndpoint("us-east-1"); got != "" {
		t.Errorf("pricingAPIEndpoint(us-east-1) = %q, want no override", got)
	}
	if got, want := pricingIndexBaseURL("us-east-1"), "https://pricing.us-east-1.amazonaws.com"; got != want {
		t.Errorf("pricingIndexBaseURL(us-east-1) = %q, want %q", got, want)
	}
}

func TestPricingAPIRegionsPreferTargetGeography(t *testing.T) {
	tests := []struct {
		region string
		want   []string
	}{
		{region: "us-west-2", want: []string{"us-east-1", "eu-central-1", "ap-south-1"}},
		{region: "eu-west-1", want: []string{"eu-central-1", "us-east-1", "ap-south-1"}},
		{region: "ap-northeast-1", want: []string{"ap-south-1", "us-east-1", "eu-central-1"}},
		{region: "cn-north-1", want: []string{"cn-northwest-1"}},
		{region: "cn-northwest-1", want: []string{"cn-northwest-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			got := pricingAPIRegions(tt.region)
			if len(got) != len(tt.want) {
				t.Fatalf("pricingAPIRegions(%q) = %v, want %v", tt.region, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("pricingAPIRegions(%q) = %v, want %v", tt.region, got, tt.want)
				}
			}
		})
	}
}

func TestIsRetryablePricingError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "throttling", err: &smithy.GenericAPIError{Code: "ThrottlingException"}, want: true},
		{name: "server error", err: &smithy.GenericAPIError{Code: "InternalFailure"}, want: true},
		{name: "access denied", err: &smithy.GenericAPIError{Code: "AccessDeniedException"}, want: false},
		{name: "invalid request", err: errors.New("validation error"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryablePricingError(tt.err); got != tt.want {
				t.Errorf("isRetryablePricingError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestGetAvailableEC2InstanceTypesFiltersTargetLocation(t *testing.T) {
	index := `{"products":{
		"beijing":{"attributes":{"productFamily":"Compute Instance","instanceType":"m6i.large","location":"China (Beijing)","processorArchitecture":"64-bit"}},
		"ningxia":{"attributes":{"productFamily":"Compute Instance","instanceType":"m6a.large","location":"China (Ningxia)","processorArchitecture":"64-bit"}},
		"other":{"attributes":{"productFamily":"Compute Instance","instanceType":"m5.large","location":"US East (N. Virginia)","processorArchitecture":"64-bit"}}
	}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(index))
	}))
	defer server.Close()

	client := &Client{
		baseURL:            server.URL,
		region:             "cn-north-1",
		longHttpClient:     server.Client(),
		instanceTypesCache: make(map[string]cachedInstanceTypes),
	}

	got, err := client.GetAvailableEC2InstanceTypes(context.Background(), "amd64")
	if err != nil {
		t.Fatalf("GetAvailableEC2InstanceTypes() error = %v", err)
	}
	if len(got) != 1 || got[0] != "m6i.large" {
		t.Fatalf("GetAvailableEC2InstanceTypes() = %v, want [m6i.large]", got)
	}
}

func TestGetProductPriceReturnsCachedRegionalMiss(t *testing.T) {
	miss := fmt.Errorf("%w for instance type m6a.xlarge", ErrNoPricingFound)
	client := &Client{
		cache: make(map[string]cachedPrice),
		cacheTTL: time.Hour,
	}
	client.cache["m6a.xlarge-on-demand"] = cachedPrice{err: miss, expiresAt: time.Now().Add(time.Hour)}

	_, err := client.GetProductPrice(context.Background(), "m6a.xlarge", "on-demand")
	if !errors.Is(err, ErrNoPricingFound) {
		t.Fatalf("GetProductPrice() error = %v, want cached regional miss", err)
	}
}
