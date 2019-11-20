package main

import (
	"C"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/version"
	"github.com/weaveworks/common/logging"
)
import "fmt"

// Map of plugin IDs to loki output plugin instances. No need for thread safety: fluent-bit initializes plugins one
// after the other (single-threaded during initialization) and no records are received before initialization completed
// (multi-read, no writes at runtime).
var plugins = map[string]*loki{}
var logger log.Logger

func init() {
	var logLevel logging.Level
	_ = logLevel.Set("info")
	logger = newLogger(logLevel)
}

type pluginConfig struct {
	ctx unsafe.Pointer
}

func (c *pluginConfig) Get(key string) string {
	return output.FLBPluginConfigKey(c.ctx, key)
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "loki", "Ship fluent-bit logs to Grafana Loki")
}

//export FLBPluginInit
// (fluentbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	conf, err := parseConfig(&pluginConfig{ctx: ctx})
	if err != nil {
		level.Error(logger).Log("[flb-go]", "failed to launch", "error", err)
		return output.FLB_ERROR
	}

	id := output.FLBPluginConfigKey(ctx, "id")
	logger := log.With(newLogger(conf.logLevel), "id", id)
	if _, ok := plugins[id]; ok {
		level.Info(logger).Log("[flb-go]", "plugin already initialized before, please set unique plugin ID")
		return output.FLB_ERROR
	}

	level.Info(logger).Log("[flb-go]", "Starting fluent-bit-go-loki", "version", version.Info())
	paramLogger := log.With(logger, "[flb-go]", "provided parameter")
	level.Info(paramLogger).Log("URL", conf.clientConfig.URL)
	level.Info(paramLogger).Log("ID", conf.id)
	level.Info(paramLogger).Log("TenantID", conf.clientConfig.TenantID)
	level.Info(paramLogger).Log("BatchWait", conf.clientConfig.BatchWait)
	level.Info(paramLogger).Log("BatchSize", conf.clientConfig.BatchSize)
	level.Info(paramLogger).Log("Labels", conf.clientConfig.ExternalLabels)
	level.Info(paramLogger).Log("LogLevel", conf.logLevel.String())
	level.Info(paramLogger).Log("RemoveKeys", fmt.Sprintf("%+v", conf.removeKeys))
	level.Info(paramLogger).Log("LabelKeys", fmt.Sprintf("%+v", conf.labelKeys))
	level.Info(paramLogger).Log("LineFormat", conf.lineFormat)
	level.Info(paramLogger).Log("DropSingleKey", conf.dropSingleKey)
	level.Info(paramLogger).Log("LabelMapPath", fmt.Sprintf("%+v", conf.labeMap))

	plugin, err := newPlugin(conf, logger)
	if err != nil {
		level.Error(logger).Log("newPlugin", err)
		return output.FLB_ERROR
	}
	output.FLBPluginSetContext(ctx, id)
	plugins[id] = plugin

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, _ *C.char) int {
	id := output.FLBPluginGetContext(ctx).(string)
	plugin, ok := plugins[id]
	if !ok {
		level.Error(logger).Log("[flb-go]", "plugin not initialized", "id", id)
		return output.FLB_ERROR
	}

	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	dec := output.NewDecoder(data, int(length))

	for {
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Get timestamp
		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			level.Warn(plugin.logger).Log("msg", "timestamp isn't known format. Use current time.")
			timestamp = time.Now()
		}

		err := plugin.sendRecord(record, timestamp)
		if err != nil {
			level.Error(plugin.logger).Log("msg", "error sending record to Loki", "error", err)
			return output.FLB_ERROR
		}
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	for _, plugin := range plugins {
		plugin.client.Stop()
	}
	return output.FLB_OK
}

func main() {}
