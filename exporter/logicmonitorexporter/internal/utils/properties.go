// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package utils // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/logicmonitorexporter/internal/utils"

import (
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// ConvertAndNormalizeAttributes converts OpenTelemetry attributes to normalized LogicMonitor properties
func ConvertAndNormalizeAttributes(attrs pcommon.Map) map[string]interface{} {
	result := make(map[string]interface{})
	attrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsRaw()
		return true
	})
	return NormalizeResourceProperties(result)
}

// ConvertAndNormalizeAttributesToStrings converts OpenTelemetry attributes to normalized string properties
func ConvertAndNormalizeAttributesToStrings(attrs pcommon.Map) map[string]string {
	result := make(map[string]string)
	attrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return NormalizeResourcePropertiesStrings(result)
}

// NormalizeResourceProperties normalizes resource properties according to LogicMonitor specifications
// Returns map[string]interface{} for log exporter compatibility
func NormalizeResourceProperties(props map[string]interface{}) map[string]interface{} {
	normalized := make(map[string]interface{})
	
	for key, value := range props {
		// Convert value to string for processing
		var valueStr string
		if value == nil {
			continue
		}
		if s, ok := value.(string); ok {
			valueStr = s
		} else {
			valueStr = ""
		}
		
		// Skip if key is empty
		if key == "" {
			continue
		}
		
		// Normalize key and value
		normalizedKey := normalizePropertyString(key)
		normalizedValue := normalizePropertyString(valueStr)
		
		// Skip if normalization resulted in empty strings
		if normalizedKey == "" {
			continue
		}
		
		// Check if key is allowed (case insensitive)
		if !isAllowedPropertyKey(normalizedKey) {
			continue
		}
		
		// Apply length limits
		if len(normalizedKey) > 255 {
			normalizedKey = normalizedKey[:255]
		}
		if len(normalizedValue) > 24000 {
			normalizedValue = normalizedValue[:24000]
		}
		
		// Return original value type if it was preserved, otherwise use normalized string
		if normalizedValue == "" && value != nil {
			normalized[normalizedKey] = value
		} else {
			normalized[normalizedKey] = normalizedValue
		}
	}
	
	return normalized
}

// NormalizeResourcePropertiesStrings normalizes resource properties for string-only maps
func NormalizeResourcePropertiesStrings(props map[string]string) map[string]string {
	normalized := make(map[string]string)
	
	for key, value := range props {
		// Skip if key or value is empty/null
		if key == "" || value == "" {
			continue
		}
		
		// Normalize key and value
		normalizedKey := normalizePropertyString(key)
		normalizedValue := normalizePropertyString(value)
		
		// Skip if normalization resulted in empty strings
		if normalizedKey == "" || normalizedValue == "" {
			continue
		}
		
		// Check if key is allowed (case insensitive)
		if !isAllowedPropertyKey(normalizedKey) {
			continue
		}
		
		// Apply length limits
		if len(normalizedKey) > 255 {
			normalizedKey = normalizedKey[:255]
		}
		if len(normalizedValue) > 24000 {
			normalizedValue = normalizedValue[:24000]
		}
		
		normalized[normalizedKey] = normalizedValue
	}
	
	return normalized
}

// normalizePropertyString normalizes a property key or value string
func normalizePropertyString(s string) string {
	// Remove forbidden characters: , ; / * [ ] ? ' " ` ## and newline
	forbiddenChars := regexp.MustCompile(`[,;/*\[\]?'"` + "`" + `#\n\r]`)
	s = forbiddenChars.ReplaceAllString(s, "-")
	
	// Remove backslashes
	s = strings.ReplaceAll(s, "\\", "-")
	
	// Trim leading and trailing spaces
	s = strings.TrimSpace(s)
	
	return s
}

// isAllowedPropertyKey checks if a property key is allowed (case insensitive)
func isAllowedPropertyKey(key string) bool {
	keyLower := strings.ToLower(key)
	
	// System properties are not allowed (system.xxx)
	if strings.HasPrefix(keyLower, "system.") {
		return false
	}
	
	// Auto properties are not allowed (auto.xxx)  
	if strings.HasPrefix(keyLower, "auto.") {
		return false
	}
	
	// Reserved properties are not allowed (predef.xxx)
	if strings.HasPrefix(keyLower, "predef.") {
		return false
	}
	
	return true
}
