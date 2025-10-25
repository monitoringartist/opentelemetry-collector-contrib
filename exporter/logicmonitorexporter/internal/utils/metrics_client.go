// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"go.uber.org/zap"
)

const (
	metricsIngestPath = "/metric/ingest"
)

// HTTPError represents an HTTP error with status code
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

// IsClientError returns true if the error is a 4xx client error (non-retryable)
// Note: 429 (Too Many Requests) is excluded as it should be retried
func (e *HTTPError) IsClientError() bool {
	return e.StatusCode >= 400 && e.StatusCode < 500 && e.StatusCode != http.StatusTooManyRequests
}

// MetricsClient is a client for sending metrics to LogicMonitor Push Metrics API
type MetricsClient struct {
	endpoint           string
	accessID           string
	accessKey          string
	autoCreateResource bool
	client             *http.Client
	logger             *zap.Logger
}

// MetricPayload represents the metric data to be sent to LogicMonitor
type MetricPayload struct {
	ResourceName          string            `json:"resourceName"`
	ResourceIDs           map[string]string `json:"resourceIds"`
	DataSource            string            `json:"dataSource,omitempty"`
	DataSourceDisplayName string            `json:"dataSourceDisplayName,omitempty"`
	DataSourceGroup       string            `json:"dataSourceGroup,omitempty"`
	Instances             []MetricInstance  `json:"instances"`
}

// MetricInstance represents an instance within a datasource
type MetricInstance struct {
	InstanceName        string            `json:"instanceName"`
	InstanceDisplayName string            `json:"instanceDisplayName,omitempty"`
	InstanceProperties  map[string]string `json:"instanceProperties,omitempty"`
	DataPoints          []MetricDataPoint `json:"dataPoints"`
}

// MetricDataPoint represents a single datapoint
type MetricDataPoint struct {
	DataPointName            string                 `json:"dataPointName"`
	DataPointDescription     string                 `json:"dataPointDescription,omitempty"`
	DataPointType            string                 `json:"dataPointType,omitempty"`
	DataPointAggregationType string                 `json:"dataPointAggregationType,omitempty"`
	PercentileValue          int                    `json:"percentileValue,omitempty"`
	Values                   map[string]interface{} `json:"values"`
}

// MetricResponse represents the response from LogicMonitor API
type MetricResponse struct {
	Success bool                `json:"success"`
	Message string              `json:"message"`
	Errors  []MetricErrorDetail `json:"errors,omitempty"`
}

// MetricErrorDetail represents error details in the response
type MetricErrorDetail struct {
	Code        int               `json:"code,omitempty"`
	Message     string            `json:"message"`
	ResourceIDs map[string]string `json:"resourceIds,omitempty"`
}

// NewMetricsClient creates a new metrics client
func NewMetricsClient(endpoint, accessID, accessKey string, autoCreateResource bool, httpClient *http.Client, logger *zap.Logger) *MetricsClient {
	return &MetricsClient{
		endpoint:           endpoint,
		accessID:           accessID,
		accessKey:          accessKey,
		autoCreateResource: autoCreateResource,
		client:             httpClient,
		logger:             logger,
	}
}

// SendMetrics sends metric payload to LogicMonitor
// timestamp should be in milliseconds and should match the timestamps in the metric data
func (c *MetricsClient) SendMetrics(ctx context.Context, payload *MetricPayload, timestamp int64) (*MetricResponse, error) {
	// Marshal payload to JSON
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Build query string if needed
	queryString := ""
	if c.autoCreateResource {
		queryString = "?create=true"
	}

	// Create HTTP request with query string
	url := c.endpoint + metricsIngestPath + queryString
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Generate authentication signature using provided timestamp
	// IMPORTANT: Per LogicMonitor docs, the signature uses ONLY the base path without query params
	// The query string is added to the URL but NOT included in the signature calculation
	// See: https://www.logicmonitor.com/support/push-metrics/ingesting-metrics-with-the-push-metrics-rest-api
	auth := c.generateAuth(http.MethodPost, metricsIngestPath, string(body), timestamp)

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", auth)

	// DEBUG: Log authentication details
	c.logger.Debug("Push Metrics Request",
		zap.String("url", url),
		zap.String("auth_header", auth),
		zap.Int64("timestamp", timestamp),
		zap.Int("body_length", len(body)))

	// Send request
	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Error("Failed to send HTTP request",
			zap.Error(err),
			zap.String("url", url))
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Log response details
	c.logger.Debug("LogicMonitor API Response",
		zap.Int("status_code", resp.StatusCode),
		zap.String("status", resp.Status),
		zap.Int("body_length", len(respBody)),
		zap.String("response_body", string(respBody)))

	// Parse response
	var metricResp MetricResponse
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &metricResp); err != nil {
			c.logger.Error("Failed to parse response",
				zap.Error(err),
				zap.Int("status_code", resp.StatusCode),
				zap.String("response_body", string(respBody)))
			return nil, fmt.Errorf("failed to parse response (status %d): %w, body: %s", resp.StatusCode, err, string(respBody))
		}
	}

	// Check for errors
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		if metricResp.Message == "" {
			metricResp.Message = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		metricResp.Success = false

		c.logger.Error("LogicMonitor API returned error",
			zap.Int("status_code", resp.StatusCode),
			zap.String("message", metricResp.Message),
			zap.Any("errors", metricResp.Errors))

		// Return HTTPError to allow caller to detect client errors (4xx) vs server errors (5xx)
		return &metricResp, &HTTPError{
			StatusCode: resp.StatusCode,
			Message:    metricResp.Message,
		}
	}

	metricResp.Success = true
	return &metricResp, nil
}

// generateAuth generates LMv1 authentication signature
// Format: LMv1 <AccessId>:<Signature>:<Timestamp>
// Signature = Base64(HMAC-SHA256(Method + Timestamp + Body + Path, AccessKey))
// IMPORTANT: Per LogicMonitor Python example, timestamp is in MILLISECONDS for signature
func (c *MetricsClient) generateAuth(method, path, body string, timestamp int64) string {
	// Create string to sign: Method + Timestamp(milliseconds) + Body + Path
	// The timestamp parameter is already in milliseconds, use it directly
	stringToSign := method + strconv.FormatInt(timestamp, 10) + body + path

	// Generate HMAC SHA256 signature
	// LogicMonitor Push Metrics API: hex encode the hash, then base64 encode the hex string
	// Per official docs: https://www.logicmonitor.com/support/push-metrics/ingesting-metrics-with-the-push-metrics-rest-api
	h := hmac.New(sha256.New, []byte(c.accessKey))
	h.Write([]byte(stringToSign))
	hash := h.Sum(nil)
	hexString := hex.EncodeToString(hash)
	signature := base64.StdEncoding.EncodeToString([]byte(hexString))

	// DEBUG: Log signature details
	c.logger.Debug("Auth signature generation",
		zap.String("method", method),
		zap.String("path", path),
		zap.Int("body_length", len(body)),
		zap.Int64("timestamp_ms", timestamp),
		zap.String("hex_sig_prefix", hexString[:40]),
		zap.String("base64_sig_prefix", signature[:40]))

	// Return formatted auth header using milliseconds timestamp
	return fmt.Sprintf("LMv1 %s:%s:%d", c.accessID, signature, timestamp)
}
