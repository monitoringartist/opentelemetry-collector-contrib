// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/logicmonitorexporter/internal/metrics"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	lmutils "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/logicmonitorexporter/internal/utils"
)

const (
	// LogicMonitor rate limits: https://www.logicmonitor.com/support/push-metrics/rate-limiting-for-push-metrics
	maxInstancesPerPayload   = 100     // Maximum instances allowed per payload
	maxPayloadSizeBytes      = 1048576 // 1 MB uncompressed payload limit
	maxCompressedPayloadSize = 104858  // ~102 KB compressed payload limit
)

type Sender struct {
	logger        *zap.Logger
	metricsClient *lmutils.MetricsClient
	batchTimeout  time.Duration
}

// metricBatch accumulates metrics for batching within LogicMonitor limits
type metricBatch struct {
	resourceName   string
	resourceIDs    map[string]string
	dataSourceName string
	instances      []lmutils.MetricInstance
	estimatedSize  int // Estimated JSON size in bytes
	instanceCount  int
	timestampStr   string    // Timestamp string from metric data (Unix seconds as string, e.g., "1760864865")
	createdAt      time.Time // Time when batch was created (for timeout tracking)
}

// NewSender creates a new Sender
func NewSender(endpoint string, client *http.Client, accessID, accessKey string, autoCreateResource bool, batchTimeout time.Duration, logger *zap.Logger) (*Sender, error) {
	metricsClient := lmutils.NewMetricsClient(endpoint, accessID, accessKey, autoCreateResource, client, logger)

	return &Sender{
		logger:        logger,
		metricsClient: metricsClient,
		batchTimeout:  batchTimeout,
	}, nil
}

func (s *Sender) SendMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Convert OpenTelemetry metrics to LogicMonitor format with batching
	// to respect LogicMonitor rate limits
	resourceMetrics := md.ResourceMetrics()

	// Track batches by resource+datasource to group metrics
	batches := make(map[string]*metricBatch)

	for i := 0; i < resourceMetrics.Len(); i++ {
		resourceMetric := resourceMetrics.At(i)
		resource := resourceMetric.Resource()

		// Extract resource information
		resourceName := getResourceName(resource.Attributes())
		resourceID := getResourceID(resource.Attributes())
		resourceProps := convertAttributes(resource.Attributes())

		// Merge resourceProps with resourceID
		mergedResourceID := make(map[string]string)
		for k, v := range resourceID {
			mergedResourceID[k] = v
		}
		for k, v := range resourceProps {
			mergedResourceID[k] = v
		}

		scopeMetrics := resourceMetric.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			scopeMetric := scopeMetrics.At(j)
			metrics := scopeMetric.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				// Create datasource name based on metric name
				dataSourceName := generateDataSourceName(metric.Name())

				// Create batch key (resource + datasource)
				batchKey := resourceName + "|" + dataSourceName

				// Get or create batch for this resource+datasource combination
				batch, exists := batches[batchKey]
				if !exists {
					batch = &metricBatch{
						resourceName:   resourceName,
						resourceIDs:    mergedResourceID,
						dataSourceName: dataSourceName,
						instances:      make([]lmutils.MetricInstance, 0, maxInstancesPerPayload),
						estimatedSize:  500, // Base size for resource and datasource structure
						instanceCount:  0,
						createdAt:      time.Now(), // Track batch creation time for timeout
					}
					batches[batchKey] = batch
				}

				// Check if batch has timed out (only if timeout > 0)
				if s.batchTimeout > 0 && time.Since(batch.createdAt) >= s.batchTimeout {
					// Flush timed-out batch
					if err := s.flushBatch(ctx, batch); err != nil {
						return err
					}
					// Create new batch
					batch = &metricBatch{
						resourceName:   resourceName,
						resourceIDs:    mergedResourceID,
						dataSourceName: dataSourceName,
						instances:      make([]lmutils.MetricInstance, 0, maxInstancesPerPayload),
						estimatedSize:  500,
						instanceCount:  0,
						createdAt:      time.Now(),
					}
					batches[batchKey] = batch
				}

				// Process different metric types and add to batch
				var err error
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					err = s.addGaugeToBatch(ctx, batch, batches, batchKey, metric)
				case pmetric.MetricTypeSum:
					err = s.addSumToBatch(ctx, batch, batches, batchKey, metric)
				case pmetric.MetricTypeHistogram:
					err = s.addHistogramToBatch(ctx, batch, batches, batchKey, metric)
				case pmetric.MetricTypeSummary:
					err = s.addSummaryToBatch(ctx, batch, batches, batchKey, metric)
				default:
					s.logger.Warn("Unsupported metric type", zap.String("type", metric.Type().String()))
					continue
				}

				if err != nil {
					return err
				}
			}
		}
	}

	// Flush all remaining batches
	for _, batch := range batches {
		if err := s.flushBatch(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// addGaugeToBatch adds gauge datapoints to the batch, flushing if needed
func (s *Sender) addGaugeToBatch(ctx context.Context, batch *metricBatch, batches map[string]*metricBatch, batchKey string, metric pmetric.Metric) error {
	gauge := metric.Gauge()
	dataPoints := gauge.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)
		instance, timestampStr := s.createMetricInstance(metric.Name(), dp.DoubleValue(), dp.Timestamp(), dp.Attributes(), "GAUGE")

		// Set batch timestamp from first instance
		if batch.instanceCount == 0 {
			batch.timestampStr = timestampStr
		}

		// Check if we can add this instance to the batch
		if !batch.canAddInstance(&instance) {
			// Flush current batch and create new one
			if err := s.flushBatch(ctx, batch); err != nil {
				return err
			}
			// Reset batch
			batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
			batch.estimatedSize = 500
			batch.instanceCount = 0
			batch.timestampStr = timestampStr // Set timestamp for new batch
			batch.createdAt = time.Now()      // Reset creation time
		}

		batch.addInstance(instance)
	}
	return nil
}

// addSumToBatch adds sum datapoints to the batch, flushing if needed
func (s *Sender) addSumToBatch(ctx context.Context, batch *metricBatch, batches map[string]*metricBatch, batchKey string, metric pmetric.Metric) error {
	sum := metric.Sum()
	dataPoints := sum.DataPoints()

	metricType := "COUNTER"
	if !sum.IsMonotonic() {
		metricType = "GAUGE"
	}

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)
		instance, timestampStr := s.createMetricInstance(metric.Name(), dp.DoubleValue(), dp.Timestamp(), dp.Attributes(), metricType)

		// Set batch timestamp from first instance
		if batch.instanceCount == 0 {
			batch.timestampStr = timestampStr
		}

		if !batch.canAddInstance(&instance) {
			if err := s.flushBatch(ctx, batch); err != nil {
				return err
			}
			batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
			batch.estimatedSize = 500
			batch.instanceCount = 0
			batch.timestampStr = timestampStr
			batch.createdAt = time.Now() // Reset creation time
		}

		batch.addInstance(instance)
	}
	return nil
}

// addHistogramToBatch adds histogram datapoints to the batch, flushing if needed
func (s *Sender) addHistogramToBatch(ctx context.Context, batch *metricBatch, batches map[string]*metricBatch, batchKey string, metric pmetric.Metric) error {
	histogram := metric.Histogram()
	dataPoints := histogram.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		// Add count
		countInstance, timestampStr := s.createMetricInstance(metric.Name()+"_count", float64(dp.Count()), dp.Timestamp(), dp.Attributes(), "COUNTER")

		// Set batch timestamp from first instance
		if batch.instanceCount == 0 {
			batch.timestampStr = timestampStr
		}

		if !batch.canAddInstance(&countInstance) {
			if err := s.flushBatch(ctx, batch); err != nil {
				return err
			}
			batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
			batch.estimatedSize = 500
			batch.instanceCount = 0
			batch.timestampStr = timestampStr
			batch.createdAt = time.Now() // Reset creation time
		}
		batch.addInstance(countInstance)

		// Add sum if available
		if dp.HasSum() {
			sumInstance, _ := s.createMetricInstance(metric.Name()+"_sum", dp.Sum(), dp.Timestamp(), dp.Attributes(), "COUNTER")
			if !batch.canAddInstance(&sumInstance) {
				if err := s.flushBatch(ctx, batch); err != nil {
					return err
				}
				batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
				batch.estimatedSize = 500
				batch.instanceCount = 0
				batch.timestampStr = timestampStr
				batch.createdAt = time.Now() // Reset creation time
			}
			batch.addInstance(sumInstance)
		}
	}
	return nil
}

// addSummaryToBatch adds summary datapoints to the batch, flushing if needed
func (s *Sender) addSummaryToBatch(ctx context.Context, batch *metricBatch, batches map[string]*metricBatch, batchKey string, metric pmetric.Metric) error {
	summary := metric.Summary()
	dataPoints := summary.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)

		// Add count
		countInstance, timestampStr := s.createMetricInstance(metric.Name()+"_count", float64(dp.Count()), dp.Timestamp(), dp.Attributes(), "COUNTER")

		// Set batch timestamp from first instance
		if batch.instanceCount == 0 {
			batch.timestampStr = timestampStr
		}

		if !batch.canAddInstance(&countInstance) {
			if err := s.flushBatch(ctx, batch); err != nil {
				return err
			}
			batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
			batch.estimatedSize = 500
			batch.instanceCount = 0
			batch.timestampStr = timestampStr
			batch.createdAt = time.Now() // Reset creation time
		}
		batch.addInstance(countInstance)

		// Add sum
		sumInstance, _ := s.createMetricInstance(metric.Name()+"_sum", dp.Sum(), dp.Timestamp(), dp.Attributes(), "COUNTER")
		if !batch.canAddInstance(&sumInstance) {
			if err := s.flushBatch(ctx, batch); err != nil {
				return err
			}
			batch.instances = make([]lmutils.MetricInstance, 0, maxInstancesPerPayload)
			batch.estimatedSize = 500
			batch.instanceCount = 0
			batch.timestampStr = timestampStr
			batch.createdAt = time.Now() // Reset creation time
		}
		batch.addInstance(sumInstance)
	}
	return nil
}

// createMetricInstance creates a MetricInstance from metric data
// Returns the instance and the timestamp string (Unix seconds)
func (s *Sender) createMetricInstance(metricName string, value float64, timestamp pcommon.Timestamp, attributes pcommon.Map, metricType string) (lmutils.MetricInstance, string) {
	// Extract instance name from Kubernetes pod label, fallback to metric name
	instanceName := getInstanceName(attributes)
	instanceProperties := convertAttributes(attributes)
	timestampStr := strconv.FormatInt(timestamp.AsTime().Unix(), 10)

	instance := lmutils.MetricInstance{
		InstanceName:        sanitizeName(instanceName),
		InstanceDisplayName: instanceName, // Keep display name readable (no sanitization)
		InstanceProperties:  instanceProperties,
		DataPoints: []lmutils.MetricDataPoint{
			{
				DataPointName:            sanitizeName(metricName),
				DataPointType:            metricType,
				DataPointAggregationType: "none",
				Values: map[string]interface{}{
					timestampStr: value,
				},
			},
		},
	}

	return instance, timestampStr
}

// estimatePayloadSize estimates the JSON size of a payload for rate limit checking
func estimatePayloadSize(payload *lmutils.MetricPayload) int {
	// Quick estimation by marshaling to JSON
	data, err := json.Marshal(payload)
	if err != nil {
		// If marshaling fails, return a conservative estimate
		return maxPayloadSizeBytes
	}
	return len(data)
}

// canAddInstance checks if adding an instance would exceed LogicMonitor limits
func (b *metricBatch) canAddInstance(instance *lmutils.MetricInstance) bool {
	// Check instance count limit
	if b.instanceCount+1 > maxInstancesPerPayload {
		return false
	}

	// Estimate size increase (conservative estimate)
	// Each instance adds: instance name, properties, datapoints
	estimatedIncrease := len(instance.InstanceName) + 500 // Conservative buffer for JSON structure
	for _, dp := range instance.DataPoints {
		estimatedIncrease += len(dp.DataPointName) + 100 // Datapoint name + value structure
	}

	// Check if adding this would exceed payload size limit (with 20% safety margin)
	// Use 80% of max size as safety limit
	safetyLimit := (maxPayloadSizeBytes * 4) / 5 // 80% of max
	if b.estimatedSize+estimatedIncrease > safetyLimit {
		return false
	}

	return true
}

// addInstance adds an instance to the batch, or merges datapoints if instance already exists
func (b *metricBatch) addInstance(instance lmutils.MetricInstance) {
	// Check if this instance already exists in the batch
	existingIndex := -1
	for i, existing := range b.instances {
		if existing.InstanceName == instance.InstanceName {
			existingIndex = i
			break
		}
	}

	if existingIndex >= 0 {
		// Instance exists - merge the datapoints
		b.instances[existingIndex].DataPoints = append(b.instances[existingIndex].DataPoints, instance.DataPoints...)

		// Update estimated size (only new datapoints)
		for _, dp := range instance.DataPoints {
			b.estimatedSize += len(dp.DataPointName) + 100
		}
	} else {
		// New instance - add it
		b.instances = append(b.instances, instance)
		b.instanceCount++

		// Update estimated size
		estimatedIncrease := len(instance.InstanceName) + 500
		for _, dp := range instance.DataPoints {
			estimatedIncrease += len(dp.DataPointName) + 100
		}
		b.estimatedSize += estimatedIncrease
	}
}

// flush sends the accumulated batch to LogicMonitor
func (s *Sender) flushBatch(ctx context.Context, batch *metricBatch) error {
	if batch == nil || batch.instanceCount == 0 {
		return nil
	}

	payload := &lmutils.MetricPayload{
		ResourceName:          batch.resourceName,
		ResourceIDs:           batch.resourceIDs,
		DataSource:            batch.dataSourceName,
		DataSourceDisplayName: batch.dataSourceName,
		DataSourceGroup:       "",
		Instances:             batch.instances,
	}

	// Final size check
	actualSize := estimatePayloadSize(payload)
	if actualSize > maxPayloadSizeBytes {
		s.logger.Warn("Payload size exceeds limit, may be rejected by LogicMonitor",
			zap.Int("payloadSize", actualSize),
			zap.Int("limit", maxPayloadSizeBytes),
			zap.Int("instanceCount", batch.instanceCount))
	}

	// Use CURRENT time for authentication signature (required by LogicMonitor API)
	// The metric data timestamps are in the payload, but auth must use current time
	authTimestampMillis := time.Now().UnixMilli()

	s.logger.Debug("Sending batched metrics",
		zap.String("resourceName", batch.resourceName),
		zap.String("dataSource", batch.dataSourceName),
		zap.Int("instanceCount", batch.instanceCount),
		zap.Int("estimatedSize", actualSize),
		zap.Int64("authTimestampMillis", authTimestampMillis))

	// Send to LogicMonitor with CURRENT time for auth
	resp, err := s.metricsClient.SendMetrics(ctx, payload, authTimestampMillis)
	if err != nil {
		// Check if this is a client error (4xx) - these should not be retried
		var httpErr *lmutils.HTTPError
		if errors.As(err, &httpErr) && httpErr.IsClientError() {
			s.logger.Warn("Dropping metrics due to client error (will not retry)",
				zap.Int("status_code", httpErr.StatusCode),
				zap.String("message", httpErr.Message),
				zap.String("resource", batch.resourceName),
				zap.String("datasource", batch.dataSourceName))
			// Return permanent error to prevent retries
			return consumererror.NewPermanent(fmt.Errorf("failed to send batched metrics: %w", err))
		}
		// Server errors (5xx) or network errors are retryable
		return fmt.Errorf("failed to send batched metrics: %w", err)
	}

	if !resp.Success && len(resp.Errors) > 0 {
		return fmt.Errorf("metric ingestion errors: %s", resp.Message)
	}

	s.logger.Info("Successfully sent metrics batch",
		zap.String("resource", batch.resourceName),
		zap.String("datasource", batch.dataSourceName),
		zap.Int("instanceCount", batch.instanceCount))

	return nil
}

// generateDataSourceName creates a DataSourceName from metric name
func generateDataSourceName(metricName string) string {
	if metricName == "" {
		return "Ungroupped"
	}

	// Find the first occurrence of either _ or .
	firstUnderscore := strings.Index(metricName, "_")
	firstDot := strings.Index(metricName, ".")

	var delimiterPos int = -1

	if firstUnderscore != -1 {
		delimiterPos = firstUnderscore
	}
	if firstDot != -1 && (delimiterPos == -1 || firstDot < delimiterPos) {
		delimiterPos = firstDot
	}

	var firstPart string
	if delimiterPos == -1 {
		if len(metricName) <= 2 {
			return "Ungroupped"
		}
		firstPart = metricName
	} else {
		firstPart = metricName[:delimiterPos]
		if len(firstPart) < 2 {
			return "Ungroupped"
		}
	}

	cleaned := cleanDataSourceName(firstPart)
	cleaned = strings.TrimSpace(cleaned)

	if len(cleaned) < 2 {
		return "Ungroupped"
	}

	result := strings.ToUpper(string(cleaned[0])) + strings.ToLower(cleaned[1:])

	if len(result) > 64 {
		result = result[:64]
	}

	return finalizeDataSourceName(result)
}

func cleanDataSourceName(input string) string {
	var result strings.Builder
	result.Grow(len(input))

	for _, r := range input {
		if isInvalidDataSourceChar(r) {
			continue
		}
		result.WriteRune(r)
	}

	return result.String()
}

func isInvalidDataSourceChar(r rune) bool {
	switch r {
	case ',', ';', '/', '*', '[', ']', '?', '\'', '"', '`', '#', '\n', '\r':
		return true
	default:
		return false
	}
}

func finalizeDataSourceName(input string) string {
	if input == "" {
		return "Ungroupped"
	}

	result := strings.TrimSpace(input)

	if strings.Contains(result, "-") {
		lastHyphenIndex := strings.LastIndex(result, "-")

		if lastHyphenIndex != len(result)-1 {
			var cleaned strings.Builder
			cleaned.Grow(len(result))

			for i, r := range result {
				if r == '-' && i != len(result)-1 {
					continue
				}
				cleaned.WriteRune(r)
			}
			result = cleaned.String()
		}

		if result == "-" || result == "" {
			return "Ungroupped"
		}

		if strings.HasSuffix(result, "-") && len(result) < 2 {
			return "Ungroupped"
		}
	}

	if len(result) < 1 {
		return "Ungroupped"
	}

	return result
}

func sanitizeName(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := re.ReplaceAllString(name, "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")

	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "metric_" + sanitized
	}

	if len(sanitized) == 0 {
		sanitized = "unknown_metric"
	}

	return sanitized
}

func getResourceName(attrs pcommon.Map) string {
	if name, exists := attrs.Get("service.name"); exists {
		return name.Str()
	}
	if name, exists := attrs.Get("host.name"); exists {
		return name.Str()
	}
	return "unknown"
}

func getResourceID(attrs pcommon.Map) map[string]string {
	resourceID := make(map[string]string)
	if name, exists := attrs.Get("service.name"); exists {
		resourceID["service.name"] = name.Str()
	}
	if name, exists := attrs.Get("host.name"); exists {
		resourceID["system.displayname"] = name.Str()
	}
	if len(resourceID) == 0 {
		resourceID["system.displayname"] = "unknown"
	}
	return resourceID
}

func getInstanceName(attrs pcommon.Map) string {
	// Try Kubernetes pod name first (from Prometheus scraping)
	if podName, exists := attrs.Get("kubernetes_pod_name"); exists {
		return podName.Str()
	}
	// Try pod label (alternative format)
	if podName, exists := attrs.Get("pod"); exists {
		return podName.Str()
	}
	// Try instance label (Prometheus target)
	if instance, exists := attrs.Get("instance"); exists {
		return instance.Str()
	}
	// Try job name as fallback
	if job, exists := attrs.Get("job"); exists {
		return job.Str()
	}
	// Ultimate fallback
	return "unknown-instance"
}

func convertAttributes(attrs pcommon.Map) map[string]string {
	return lmutils.ConvertAndNormalizeAttributesToStrings(attrs)
}
