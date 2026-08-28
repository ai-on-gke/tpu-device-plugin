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

package utilization_metrics_go_proto

// UtilizationMetricType enumerates the kinds of host-side TPU utilization
// metrics that can be reported per device.
type UtilizationMetricType int32

const (
	UtilizationMetricType_UNSPECIFIED_UTILIZATION UtilizationMetricType = 0
	UtilizationMetricType_TENSORCORE_UTILIZATION  UtilizationMetricType = 1
	UtilizationMetricType_HBM_UTILIZATION         UtilizationMetricType = 2
	UtilizationMetricType_ICI_UTILIZATION         UtilizationMetricType = 3
	UtilizationMetricType_SPARSECORE_UTILIZATION  UtilizationMetricType = 4
)

// String returns the enum name for the utilization metric type.
func (t UtilizationMetricType) String() string {
	switch t {
	case UtilizationMetricType_TENSORCORE_UTILIZATION:
		return "TENSORCORE_UTILIZATION"
	case UtilizationMetricType_HBM_UTILIZATION:
		return "HBM_UTILIZATION"
	case UtilizationMetricType_ICI_UTILIZATION:
		return "ICI_UTILIZATION"
	case UtilizationMetricType_SPARSECORE_UTILIZATION:
		return "SPARSECORE_UTILIZATION"
	default:
		return "UNSPECIFIED_UTILIZATION"
	}
}
