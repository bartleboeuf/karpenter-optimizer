package awspricing

import (
	"errors"
	"testing"

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
