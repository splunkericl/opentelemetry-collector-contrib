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
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchperresourceattr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter/internal/metadata"
)

const (
	defaultMaxIdleCons          = 100
	defaultHTTPTimeout          = 10 * time.Second
	defaultHTTP2ReadIdleTimeout = time.Second * 10
	defaultHTTP2PingTimeout     = time.Second * 10
	defaultIdleConnTimeout      = 10 * time.Second
	defaultSplunkAppName        = "OpenTelemetry Collector Contrib"
	defaultBatchTimeout         = 10 * time.Millisecond
	defaultS2SBatchSize         = 128
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

// NewFactory creates a factory for Splunk HEC exporter.
func NewFactory() exporter.Factory {
	return NewFactoryWithExporterBatching(false)
}

// NewFactoryWithExporterBatching creates a factory for Splunk HEC exporter with useExporterBatching option.
func NewFactoryWithExporterBatching(useExporterBatching bool) exporter.Factory {
	createLogFn := createLogsExporter
	if useExporterBatching {
		createLogFn = createLogsExporterWithExporterBatching
	}

	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogFn, metadata.LogsStability))
}

func createDefaultConfig() component.Config {
	defaultMaxConns := defaultMaxIdleCons
	defaultIdleConnTimeout := defaultIdleConnTimeout
	return &Config{
		LogDataEnabled:       true,
		ProfilingDataEnabled: true,
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout:              defaultHTTPTimeout,
			IdleConnTimeout:      &defaultIdleConnTimeout,
			MaxIdleConnsPerHost:  &defaultMaxConns,
			MaxIdleConns:         &defaultMaxConns,
			HTTP2ReadIdleTimeout: defaultHTTP2ReadIdleTimeout,
			HTTP2PingTimeout:     defaultHTTP2PingTimeout,
		},
		SplunkAppName:           defaultSplunkAppName,
		BackOffConfig:           configretry.NewDefaultBackOffConfig(),
		QueueSettings:           exporterhelper.NewDefaultQueueSettings(),
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

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		c.pushTraceData,
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithStart(c.start),
		exporterhelper.WithShutdown(c.stop))
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter.Metrics, error) {
	cfg := config.(*Config)

	c := newMetricsClient(set, cfg)

	exporter, err := exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		c.pushMetricsData,
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithStart(c.start),
		exporterhelper.WithShutdown(c.stop))
	if err != nil {
		return nil, err
	}

	wrapped := &baseMetricsExporter{
		Component: exporter,
		Metrics:   batchperresourceattr.NewBatchPerResourceMetrics(splunk.HecTokenLabel, exporter),
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

	logsExporter, err := exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		c.pushLogData,
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithStart(c.start),
		exporterhelper.WithShutdown(c.stop))

	if err != nil {
		return nil, err
	}

	wrapped := &baseLogsExporter{
		Component: logsExporter,
		Logs: batchperresourceattr.NewBatchPerResourceLogs(splunk.HecTokenLabel, &perScopeBatcher{
			logsEnabled:      cfg.LogDataEnabled,
			profilingEnabled: cfg.ProfilingDataEnabled,
			logger:           set.Logger,
			next:             logsExporter,
		}),
	}

	return wrapped, nil
}

func createLogsExporterWithExporterBatching(
	ctx context.Context,
	set exporter.CreateSettings,
	config component.Config,
) (exporter exporter.Logs, err error) {
	cfg := config.(*Config)

	c := newLogsClient(set, cfg)

	logsConverter := newHecLogConverter(cfg, c.pushHECDataInBatches)

	return exporterhelper.NewLogsRequestExporter(
		ctx,
		set,
		logsConverter.RequestFromLogs,
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithBatcher(
			exporterhelper.MergeBatcherConfig{
				Enabled: true,
				Timeout: defaultBatchTimeout,
			},
			hecMergeFunc,
			exporterhelper.WithSplitBatcher(exporterhelper.SplitBatcherConfig{
				MaxSizeItems: defaultS2SBatchSize,
			}, hecMergeSplitFunc)),
		exporterhelper.WithRequestQueue(exporterhelper.QueueConfig{
			Enabled:        cfg.QueueSettings.Enabled,
			NumConsumers:   cfg.QueueSettings.NumConsumers,
			QueueItemsSize: defaultS2SBatchSize * cfg.QueueSettings.QueueSize,
		}, exporterhelper.NewPersistentQueueFactory(cfg.QueueSettings.StorageID, logsConverter.Marshal, logsConverter.Unmarshal)),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithStart(c.start),
		exporterhelper.WithShutdown(c.stop),
	)
}
