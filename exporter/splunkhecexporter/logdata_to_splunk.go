// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkhecexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter"

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	// Keys are taken from https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/overview.md#trace-context-in-legacy-formats.
	// spanIDFieldKey is the key used in log event for the span id (if any).
	spanIDFieldKey = "span_id"
	// traceIDFieldKey is the key used in the log event for the trace id (if any).
	traceIDFieldKey = "trace_id"
)

func newHecLogConverter(config *Config, exportFunc hecRequestExportFunc) *hecLogConverter {
	return &hecLogConverter{
		hecDataConverter: hecDataConverter{
			config:     config,
			exportFunc: exportFunc,
		},
		legacyUnmarshaller: &plog.ProtoUnmarshaler{},
	}
}

// hecLogConverter extends hecDataConverter for log data
type hecLogConverter struct {
	hecDataConverter
	legacyUnmarshaller *plog.ProtoUnmarshaler
}

// Unmarshal un-marshals given bytes back to hecRequest
// This method is needed for reading data from persistent queue
func (hlc *hecLogConverter) Unmarshal(data []byte) (exporterhelper.Request, error) {
	req, err := hlc.hecDataConverter.Unmarshal(data)
	// Backward compatibility issue. Before exporter batching, persistent queue defaults to use protobuf marshaller/unmarshaller
	// For a period of time, we want to attempt to unmarshal previous requests until all clients have migrated to the
	// latest version
	if err != nil {
		if logs, err := hlc.legacyUnmarshaller.UnmarshalLogs(data); err == nil {
			return newHecRequest(hlc.exportFunc, hlc.logsToSplunkEvents(logs)), nil
		} else {
			return nil, err
		}
	}
	return req, err
}

func (hlc *hecLogConverter) RequestFromLogs(_ context.Context, logs plog.Logs) (exporterhelper.Request, error) {
	data := hlc.logsToSplunkEvents(logs)
	return newHecRequest(hlc.exportFunc, data), nil
}

func (hlc *hecLogConverter) logsToSplunkEvents(logs plog.Logs) hecBatchMap {
	result := hecBatchMap{}

	logsIn := logs.ResourceLogs()
	for i := 0; i < logsIn.Len(); i++ {
		resourceLogs := logsIn.At(i)
		resources := resourceLogs.Resource()
		sls := resourceLogs.ScopeLogs()

		scopeKey := hecScope{}
		if accessToken, ok := resources.Attributes().Get(splunk.HecTokenLabel); ok {
			scopeKey.hecToken = accessToken.Str()
		}
		if index, ok := resources.Attributes().Get(splunk.DefaultIndexLabel); ok {
			scopeKey.index = index.Str()
		}
		if sls.Len() > 0 && isProfilingData(sls.At(0)) {
			scopeKey.isProfilingData = true
		}

		var events []*splunk.Event
		for j := 0; j < sls.Len(); j++ {
			logSlice := sls.At(j).LogRecords()
			numLogs := logSlice.Len()
			for k := 0; k < numLogs; k++ {
				record := logSlice.At(k)
				events = append(events, mapLogRecordToSplunkEvent(resources, record, hlc.config))
			}
		}
		result[scopeKey] = events
	}
	return result
}

func mapLogRecordToSplunkEvent(res pcommon.Resource, lr plog.LogRecord, config *Config) *splunk.Event {
	host := unknownHostName
	source := config.Source
	sourcetype := config.SourceType
	index := config.Index
	fields := map[string]any{}
	sourceKey := config.HecToOtelAttrs.Source
	sourceTypeKey := config.HecToOtelAttrs.SourceType
	indexKey := config.HecToOtelAttrs.Index
	hostKey := config.HecToOtelAttrs.Host
	severityTextKey := config.HecFields.SeverityText
	severityNumberKey := config.HecFields.SeverityNumber
	if spanID := lr.SpanID(); !spanID.IsEmpty() {
		fields[spanIDFieldKey] = hex.EncodeToString(spanID[:])
	}
	if traceID := lr.TraceID(); !traceID.IsEmpty() {
		fields[traceIDFieldKey] = hex.EncodeToString(traceID[:])
	}
	if lr.SeverityText() != "" {
		fields[severityTextKey] = lr.SeverityText()
	}
	if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
		fields[severityNumberKey] = lr.SeverityNumber()
	}

	res.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case hostKey:
			host = v.Str()
		case sourceKey:
			source = v.Str()
		case sourceTypeKey:
			sourcetype = v.Str()
		case indexKey:
			index = v.Str()
		case splunk.HecTokenLabel:
			// ignore
		default:
			mergeValue(fields, k, v.AsRaw())
		}
		return true
	})
	lr.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case hostKey:
			host = v.Str()
		case sourceKey:
			source = v.Str()
		case sourceTypeKey:
			sourcetype = v.Str()
		case indexKey:
			index = v.Str()
		case splunk.HecTokenLabel:
			// ignore
		default:
			mergeValue(fields, k, v.AsRaw())
		}
		return true
	})

	body := lr.Body().AsRaw()
	if body == nil {
		body = ""
	}

	return &splunk.Event{
		Time:       nanoTimestampToEpochMilliseconds(lr.Timestamp()),
		Host:       host,
		Source:     source,
		SourceType: sourcetype,
		Index:      index,
		Event:      body,
		Fields:     fields,
	}
}

// nanoTimestampToEpochMilliseconds transforms nanoseconds into <sec>.<ms>. For example, 1433188255.500 indicates 1433188255 seconds and 500 milliseconds after epoch.
func nanoTimestampToEpochMilliseconds(ts pcommon.Timestamp) float64 {
	return time.Duration(ts).Round(time.Millisecond).Seconds()
}

func mergeValue(dst map[string]any, k string, v any) {
	switch element := v.(type) {
	case []any:
		if isArrayFlat(element) {
			dst[k] = v
		} else {
			jsonStr, _ := jsoniter.MarshalToString(element)
			dst[k] = jsonStr
		}
	case map[string]any:
		flattenAndMergeMap(element, dst, k)
	default:
		dst[k] = v
	}

}

func isArrayFlat(array []any) bool {
	for _, v := range array {
		switch v.(type) {
		case []any, map[string]any:
			return false
		}
	}
	return true
}

func flattenAndMergeMap(src, dst map[string]any, key string) {
	for k, v := range src {
		current := fmt.Sprintf("%s.%s", key, k)
		switch element := v.(type) {
		case map[string]any:
			flattenAndMergeMap(element, dst, current)
		case []any:
			if isArrayFlat(element) {
				dst[current] = element
			} else {
				jsonStr, _ := jsoniter.MarshalToString(element)
				dst[current] = jsonStr
			}

		default:
			dst[current] = element
		}
	}
}
