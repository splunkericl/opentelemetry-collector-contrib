// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkhecexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterbatcher"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchperresourceattr"
)

const (
	defaultMaxIdleCons          = 100
	defaultHTTPTimeout          = 10 * time.Second
	defaultHTTP2ReadIdleTimeout = time.Second * 10
	defaultHTTP2PingTimeout     = time.Second * 10
	defaultIdleConnTimeout      = 10 * time.Second
	defaultSplunkAppName        = "OpenTelemetry Collector Contrib"
)

// TODO: Find a place for this to be shared.
type baseMetricsExporter struct {
	component.Component
	consumer.Metrics
}

// TODO: Find a place for this to be shared.
type baseLogsExporter struct {
	component.Component
	consumer.Logs
}

// TODO: Find a place for this to be shared.
type baseTracesExporter struct {
	component.Component
	consumer.Traces
}

// NewFactory creates a factory for Splunk HEC exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability))
}

func getExporterOptions(cfg *Config, startFunc component.StartFunc, shutdownFunc component.ShutdownFunc) []exporterhelper.Option {
	options := []exporterhelper.Option{
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithStart(startFunc),
		exporterhelper.WithShutdown(shutdownFunc),
		exporterhelper.WithBatcher(cfg.BatcherConfig),
	}

	return options
}

func createDefaultConfig() component.Config {
	defaultMaxConns := defaultMaxIdleCons
	defaultIdleConnTimeout := defaultIdleConnTimeout
	return &Config{
		LogDataEnabled:       true,
		ProfilingDataEnabled: true,
		ClientConfig: confighttp.ClientConfig{
			Timeout:              defaultHTTPTimeout,
			IdleConnTimeout:      &defaultIdleConnTimeout,
			MaxIdleConnsPerHost:  &defaultMaxConns,
			MaxIdleConns:         &defaultMaxConns,
			HTTP2ReadIdleTimeout: defaultHTTP2ReadIdleTimeout,
			HTTP2PingTimeout:     defaultHTTP2PingTimeout,
		},
		SplunkAppName: defaultSplunkAppName,
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		QueueSettings: exporterhelper.NewDefaultQueueSettings(),
		BatcherConfig: exporterbatcher.Config{
			Enabled:      false,
			FlushTimeout: 200 * time.Millisecond, // need default value to prevent validation failures
		},
		DisableCompression:      false,
		MaxContentLengthLogs:    defaultContentLengthLogsLimit,
		MaxContentLengthMetrics: defaultContentLengthMetricsLimit,
		MaxContentLengthTraces:  defaultContentLengthTracesLimit,
		MaxEventSize:            defaultMaxEventSize,
		HecToOtelAttrs: splunk.HecToOtelAttrs{
			Source:     splunk.DefaultSourceLabel,
			SourceType: splunk.DefaultSourceTypeLabel,
			Index:      splunk.DefaultIndexLabel,
			Host:       conventions.AttributeHostName,
		},
		HecFields: OtelToHecFields{
			SeverityText:   splunk.DefaultSeverityTextLabel,
			SeverityNumber: splunk.DefaultSeverityNumberLabel,
		},
		HealthPath:            splunk.DefaultHealthPath,
		HecHealthCheckEnabled: false,
		ExportRaw:             false,
		Telemetry: HecTelemetry{
			Enabled:              false,
			OverrideMetricsNames: map[string]string{},
			ExtraAttributes:      map[string]string{},
		},
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter.Traces, error) {
	cfg := config.(*Config)

	c := newTracesClient(set, cfg)

	exporterOptions := getExporterOptions(cfg, c.start, c.stop)

	e, err := exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		c.pushTraceData,
		exporterOptions...)

	if err != nil {
		return nil, err
	}

	wrapped := &baseTracesExporter{
		Component: e,
		Traces:    batchperresourceattr.NewMultiBatchPerResourceTraces([]string{splunk.HecTokenLabel, splunk.DefaultIndexLabel}, e),
	}

	return wrapped, nil
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter.Metrics, error) {
	cfg := config.(*Config)

	c := newMetricsClient(set, cfg)

	exporterOptions := getExporterOptions(cfg, c.start, c.stop)

	e, err := exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		c.pushMetricsData,
		exporterOptions...)
	if err != nil {
		return nil, err
	}

	wrapped := &baseMetricsExporter{
		Component: e,
		Metrics:   batchperresourceattr.NewMultiBatchPerResourceMetrics([]string{splunk.HecTokenLabel, splunk.DefaultIndexLabel}, e),
	}

	return wrapped, nil
}

func createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter exporter.Logs, err error) {
	cfg := config.(*Config)

	c := newLogsClient(set, cfg)

	exporterOptions := getExporterOptions(cfg, c.start, c.stop)

	logsExporter, err := exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		c.pushLogData,
		exporterOptions...)

	if err != nil {
		return nil, err
	}

	wrapped := &baseLogsExporter{
		Component: logsExporter,
		Logs: batchperresourceattr.NewMultiBatchPerResourceLogs([]string{splunk.HecTokenLabel, splunk.DefaultIndexLabel}, &perScopeBatcher{
			logsEnabled:      cfg.LogDataEnabled,
			profilingEnabled: cfg.ProfilingDataEnabled,
			logger:           set.Logger,
			next:             logsExporter,
		}),
	}

	return wrapped, nil
}
