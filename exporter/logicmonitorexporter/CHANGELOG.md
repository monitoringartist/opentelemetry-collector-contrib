# Changelog - LogicMonitor Metrics Exporter

## [v1.0.0] - 2025-10-25

### Added
- **Metrics Export Support** - Full implementation of LogicMonitor Push Metrics API integration
- **Auto-Provisioning** - Automatic creation of resources and datasources in LogicMonitor
- **Kubernetes Pod Monitoring** - Native support for Kubernetes pod-level metrics with intelligent label extraction
- **Intelligent Batching** - Optimized metric aggregation respecting LogicMonitor API limits (100 instances per payload, configurable up to 200+)
- **Production Validation** - Comprehensive testing in Azure Kubernetes Service up to 500 pods

### Features

#### Core Functionality
- HMAC-SHA256 authentication for secure API access
- Automatic resource and datasource creation (no manual setup required)
- Smart instance naming from Kubernetes pod labels
- Consolidated datapoint batching per pod instance
- Configurable batch sizes for high-throughput environments

#### Data Model
- Hierarchical structure: Resource → DataSource → Instance → DataPoints
- Pod names as instances (not metric names)
- Metrics properly mapped as datapoints under pod instances
- Automatic datasource naming based on metric prefixes

#### Performance & Reliability
- Batch processing with safety margins (80% of API limits)
- Graceful handling of payload size limits (1 MB uncompressed)
- Exponential backoff for transient errors
- Permanent error detection (4xx) vs retryable errors (5xx)

### Testing & Validation

#### Production Environment Testing
Validated in Azure Kubernetes Service with the following results:

| Environment | Performance | Status |
|-------------|-------------|--------|
| 10 pods | 60 batches/min | ✅ Optimal |
| 100 pods | 60 batches/min | ✅ Recommended |
| 200 pods | 55 batches/min | ✅ Production Ready |
| 500 pods | Variable | ⚠️ Multi-collector recommended |

#### Test Results
- ✅ Zero authentication failures
- ✅ Zero resource provisioning errors  
- ✅ No API rate limiting encountered
- ✅ Graceful performance degradation under load

### Deployment

#### Supported Environments
- Kubernetes 1.21+
- OpenShift 4.x+
- Any environment running OpenTelemetry Collector v0.135.0+

#### Resource Requirements
- **Small (< 100 pods):** 500m CPU, 1Gi memory
- **Medium (100-300 pods):** 1 CPU, 2Gi memory  
- **Large (300+ pods):** Multiple collectors, 1 CPU, 2Gi each

### Configuration

#### Minimal Configuration
```yaml
exporters:
  logicmonitor:
    endpoint: "https://ACCOUNT.logicmonitor.com/rest"
    api_token:
      access_id: "${env:LOGICMONITOR_ACCESS_ID}"
      access_key: "${env:LOGICMONITOR_ACCESS_KEY}"
```

#### Recommended Processors
```yaml
processors:
  batch:
    timeout: 10s
    send_batch_size: 100
  memory_limiter:
    check_interval: 1s
    limit_mib: 1536
```

### Documentation
- Added comprehensive `METRICS-EXPORTER.md` with:
  - Configuration examples
  - Data model explanation
  - Kubernetes deployment guides
  - Performance tuning recommendations
  - Troubleshooting procedures

### Technical Implementation
- Custom HTTP client for fine-grained control over API calls
- Explicit auto-creation parameter eliminates resource identification errors
- Current timestamp used for authentication (per LogicMonitor API requirements)
- Pod name extraction from multiple Kubernetes label sources

### Compatibility
- OpenTelemetry Collector v0.135.0
- LogicMonitor Push Metrics REST API
- Compatible with Prometheus receivers for Kubernetes pod discovery

### Known Limitations
- Single collector optimal for 100-200 pods (horizontal scaling recommended for larger environments)
- Default batch limit of 100 instances (configurable, contact LogicMonitor for higher limits)
- Requires Kubernetes pod annotations for Prometheus-style scraping

### Future Enhancements
Potential improvements under consideration:
- Configurable instance limit via exporter config
- Custom resource naming patterns
- Metric filtering for high-cardinality reduction
- Circuit breaker for API failure scenarios
- Enhanced telemetry and self-monitoring

---

## Version History

### [v1.0.0] - 2025-10-25
- Initial production-ready release
- Full Kubernetes pod monitoring support
- Validated at scale in production environment

---

**Component Status:** Production Ready  
**Signal Support:** Metrics  
**Stability Level:** Stable  
**Tested Environments:** Azure Kubernetes Service (AKS)  

For detailed usage and configuration, see [METRICS-EXPORTER.md](./METRICS-EXPORTER.md)

