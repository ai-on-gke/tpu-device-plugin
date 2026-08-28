// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tpuutilization

import (
	"time"

	umpb "tpu-device-plugin/pkg/monitoring/proto/utilization_metrics_go_proto"
	"tpu-device-plugin/pkg/monitoring/tpuutilizationutil"
)

// InitOptions configures a host metrics client.
type InitOptions struct {
	// MetricsBar identifies the host metrics resource to read from.
	MetricsBar string
	// TPUType is the TPU generation the client reports for.
	TPUType tpuutilizationutil.TPUType
}

// Opts builds InitOptions for the given metrics resource and TPU generation.
func Opts(metricsBar string, tpuType tpuutilizationutil.TPUType) *InitOptions {
	return &InitOptions{
		MetricsBar: metricsBar,
		TPUType:    tpuType,
	}
}

// MetricsClient reads host-side TPU utilization metrics.
type MetricsClient struct{}

// NewMetricsClient returns a non-functional host metrics client. It never
// returns an error; the returned client is not wired to any data source.
func NewMetricsClient(opts *InitOptions) (*MetricsClient, error) {
	return &MetricsClient{}, nil
}

// UtilizationPercentagePerDevice returns per-device utilization percentages for
// the given metric type over the given window. The stub returns an empty map.
func (c *MetricsClient) UtilizationPercentagePerDevice(since time.Duration, metricType umpb.UtilizationMetricType) (map[string]float64, error) {
	return map[string]float64{}, nil
}

// ChipIdentifierPerDevice returns a per-device chip identifier map. The stub
// returns an empty map.
func (c *MetricsClient) ChipIdentifierPerDevice() map[string]string {
	return map[string]string{}
}
