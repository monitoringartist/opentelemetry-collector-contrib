// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeResourcePropertiesStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name: "valid properties",
			input: map[string]string{
				"environment": "production",
				"region":      "us-west-2",
				"version":     "1.0.0",
			},
			expected: map[string]string{
				"environment": "production",
				"region":      "us-west-2",
				"version":     "1.0.0",
			},
		},
		{
			name: "forbidden system properties",
			input: map[string]string{
				"system.hostname": "server1",
				"auto.discovered": "true",
				"predef.category": "server",
				"valid.property": "value",
			},
			expected: map[string]string{
				"valid.property": "value",
			},
		},
		{
			name: "forbidden characters",
			input: map[string]string{
				"key,with,commas":     "value;with;semicolons",
				"key/with/slashes":    "value*with*asterisks",
				"key[with]brackets":   "value?with?questions",
				"key'with'quotes":     "value\"with\"doublequotes",
				"key`with`backticks":  "value#with#hashes",
				"key\nwith\nnewlines": "value\rwith\rcarriagereturns",
				"key\\with\\backslashes": "value\\with\\backslashes",
			},
			expected: map[string]string{
				"key-with-commas":       "value-with-semicolons",
				"key-with-slashes":      "value-with-asterisks",
				"key-with-brackets":     "value-with-questions",
				"key-with-quotes":       "value-with-doublequotes",
				"key-with-backticks":    "value-with-hashes",
				"key-with-newlines":     "value-with-carriagereturns",
				"key-with-backslashes":  "value-with-backslashes",
			},
		},
		{
			name: "leading and trailing spaces",
			input: map[string]string{
				"  key_with_spaces  ": "  value_with_spaces  ",
				"normal_key":         "normal_value",
			},
			expected: map[string]string{
				"key_with_spaces": "value_with_spaces",
				"normal_key":      "normal_value",
			},
		},
		{
			name: "empty keys and values",
			input: map[string]string{
				"":           "empty_key",
				"empty_value": "",
				"valid_key":   "valid_value",
			},
			expected: map[string]string{
				"valid_key": "valid_value",
			},
		},
		{
			name: "length limits",
			input: map[string]string{
				// Key longer than 255 characters
				string(make([]byte, 300)): "value",
				"normal_key": string(make([]byte, 25000)), // Value longer than 24000 characters
			},
			expected: map[string]string{
				string(make([]byte, 255)): "value",
				"normal_key": string(make([]byte, 24000)),
			},
		},
		{
			name: "case insensitive forbidden prefixes",
			input: map[string]string{
				"SYSTEM.hostname": "server1",
				"Auto.discovered": "true", 
				"PREDEF.category": "server",
				"System.Ip":       "192.168.1.1",
				"valid.property":  "value",
			},
			expected: map[string]string{
				"valid.property": "value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeResourcePropertiesStrings(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAllowedPropertyKey(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		{"environment", true},
		{"region", true},
		{"version", true},
		{"system.hostname", false},
		{"SYSTEM.IP", false},
		{"auto.discovered", false},
		{"AUTO.CREATED", false},
		{"predef.category", false},
		{"PREDEF.TYPE", false},
		{"valid.property", true},
		{"", true}, // Empty key is handled elsewhere
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := isAllowedPropertyKey(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizePropertyString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal string",
			input:    "normal_value",
			expected: "normal_value",
		},
		{
			name:     "forbidden characters",
			input:    "value,with;forbidden/chars*and[brackets]?and'quotes\"and`backticks#and##hashes",
			expected: "value-with-forbidden-chars-and-brackets--and-quotes-and-backticks-and--hashes",
		},
		{
			name:     "newlines and carriage returns",
			input:    "value\nwith\nnewlines\rand\rcarriage\rreturns",
			expected: "value-with-newlines-and-carriage-returns",
		},
		{
			name:     "backslashes",
			input:    "value\\with\\backslashes",
			expected: "value-with-backslashes",
		},
		{
			name:     "leading and trailing spaces",
			input:    "  value with spaces  ",
			expected: "value with spaces",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only forbidden characters",
			input:    ",;/*[]?'\"#\n\r\\",
			expected: "-------------",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizePropertyString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
