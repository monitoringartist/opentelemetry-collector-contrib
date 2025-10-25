// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewMetricsClient(t *testing.T) {
	client := NewMetricsClient("https://test.logicmonitor.com", "testID", "testKey", true, http.DefaultClient, zap.NewNop())
	assert.NotNil(t, client)
	assert.Equal(t, "https://test.logicmonitor.com", client.endpoint)
	assert.Equal(t, "testID", client.accessID)
	assert.Equal(t, "testKey", client.accessKey)
	assert.True(t, client.autoCreateResource)
}

func TestSendMetrics_Success(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/metric/ingest", r.URL.Path)
		assert.Equal(t, "create=true", r.URL.RawQuery)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.NotEmpty(t, r.Header.Get("Authorization"))
		assert.Contains(t, r.Header.Get("Authorization"), "LMv1")

		// Return success response
		w.WriteHeader(http.StatusAccepted)
		resp := MetricResponse{
			Success: true,
			Message: "Metric data accepted",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create client with autoCreateResource=true
	client := NewMetricsClient(server.URL, "testID", "testKey", true, server.Client(), zap.NewNop())

	// Create payload
	payload := &MetricPayload{
		ResourceName: "test-host",
		ResourceIDs: map[string]string{
			"system.displayname": "test-host",
		},
		DataSource: "TestDataSource",
		Instances: []MetricInstance{
			{
				InstanceName: "test-instance",
				DataPoints: []MetricDataPoint{
					{
						DataPointName: "test_metric",
						DataPointType: "gauge",
						Values: map[string]interface{}{
							"1234567890": 42.0,
						},
					},
				},
			},
		},
	}

	// Send metrics with a test timestamp
	ctx := context.Background()
	timestamp := time.Now().UnixMilli()
	resp, err := client.SendMetrics(ctx, payload, timestamp)

	// Verify
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "Metric data accepted", resp.Message)
}

func TestSendMetrics_Error(t *testing.T) {
	// Create test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		resp := MetricResponse{
			Success: false,
			Message: "Invalid request",
			Errors: []MetricErrorDetail{
				{
					Code:    400,
					Message: "Resource name is mandatory",
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create client with autoCreateResource=true
	client := NewMetricsClient(server.URL, "testID", "testKey", true, server.Client(), zap.NewNop())

	// Create invalid payload
	payload := &MetricPayload{
		ResourceName: "",
		Instances:    []MetricInstance{},
	}

	// Send metrics with a test timestamp
	ctx := context.Background()
	timestamp := time.Now().UnixMilli()
	resp, err := client.SendMetrics(ctx, payload, timestamp)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
	assert.NotNil(t, resp)
	assert.False(t, resp.Success)
}

func TestGenerateAuth(t *testing.T) {
	client := NewMetricsClient("https://test.logicmonitor.com", "testID", "testKey", true, http.DefaultClient, zap.NewNop())
	
	timestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	auth := client.generateAuth("POST", "/metric/ingest", `{"test":"data"}`, timestamp)
	
	// Verify format
	assert.Contains(t, auth, "LMv1")
	assert.Contains(t, auth, "testID")
	assert.Contains(t, auth, ":")
	
	// Verify it's deterministic
	auth2 := client.generateAuth("POST", "/metric/ingest", `{"test":"data"}`, timestamp)
	assert.Equal(t, auth, auth2)
}

func TestGenerateAuth_WithQueryString(t *testing.T) {
	client := NewMetricsClient("https://test.logicmonitor.com", "testID", "testKey", true, http.DefaultClient, zap.NewNop())
	
	timestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	
	// Test with query string
	authWithQuery := client.generateAuth("POST", "/metric/ingest?create=true", `{"test":"data"}`, timestamp)
	
	// Test without query string
	authWithoutQuery := client.generateAuth("POST", "/metric/ingest", `{"test":"data"}`, timestamp)
	
	// They should be different
	assert.NotEqual(t, authWithQuery, authWithoutQuery)
	
	// Both should have proper format
	assert.Contains(t, authWithQuery, "LMv1")
	assert.Contains(t, authWithoutQuery, "LMv1")
}

func TestSendMetrics_WithoutAutoCreate(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request does NOT have create query param
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/metric/ingest", r.URL.Path)
		assert.Empty(t, r.URL.RawQuery) // Should be empty when autoCreateResource=false
		
		// Return success response
		w.WriteHeader(http.StatusAccepted)
		resp := MetricResponse{
			Success: true,
			Message: "Metric data accepted",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create client with autoCreateResource=false
	client := NewMetricsClient(server.URL, "testID", "testKey", false, server.Client(), zap.NewNop())

	// Create payload
	payload := &MetricPayload{
		ResourceName: "test-host",
		ResourceIDs: map[string]string{
			"system.displayname": "test-host",
		},
		DataSource: "TestDataSource",
		Instances: []MetricInstance{
			{
				InstanceName: "test-instance",
				DataPoints: []MetricDataPoint{
					{
						DataPointName: "test_metric",
						DataPointType: "gauge",
						Values: map[string]interface{}{
							"1234567890": 42.0,
						},
					},
				},
			},
		},
	}

	// Send metrics with a test timestamp
	ctx := context.Background()
	timestamp := time.Now().UnixMilli()
	resp, err := client.SendMetrics(ctx, payload, timestamp)

	// Verify
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestMetricPayload_Marshal(t *testing.T) {
	payload := &MetricPayload{
		ResourceName: "test-host",
		ResourceIDs: map[string]string{
			"system.displayname": "test-host",
			"system.ips":         "192.168.1.1",
		},
		DataSource:            "CPU",
		DataSourceDisplayName: "CPU Metrics",
		DataSourceGroup:       "System",
		Instances: []MetricInstance{
			{
				InstanceName:        "cpu-0",
				InstanceDisplayName: "CPU 0",
				InstanceProperties: map[string]string{
					"core": "0",
				},
				DataPoints: []MetricDataPoint{
					{
						DataPointName:            "utilization",
						DataPointDescription:     "CPU Utilization",
						DataPointType:            "gauge",
						DataPointAggregationType: "avg",
						Values: map[string]interface{}{
							"1234567890": 75.5,
						},
					},
				},
			},
		},
	}

	// Marshal to JSON
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal back
	var decoded MetricPayload
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, payload.ResourceName, decoded.ResourceName)
	assert.Equal(t, payload.DataSource, decoded.DataSource)
	assert.Len(t, decoded.Instances, 1)
	assert.Equal(t, "cpu-0", decoded.Instances[0].InstanceName)
}

