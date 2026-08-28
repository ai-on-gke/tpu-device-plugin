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

package util

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"tpu-device-plugin/pkg/monitoring/tpuutilizationutil"
)

func TestInitEnvs(t *testing.T) {
	os.Setenv("NODE_IP", "localhost")
	cases := []struct {
		desc                  string
		nodeLabels            map[string]string
		subSliceTopology      string
		requestedChipCount    int
		enableDeviceSpreading bool
		isPrivileged          bool
		visibleChipIds        []string
		disableVbarUds        bool // inverted naming for testing as vbar UDS should be default
		numaNodeIds           []string
		want                  map[string]string
		wantErr               bool
	}{
		{
			desc: "v3 device 2x2 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v3-device",
				TopologyLabel:         "2x2",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v3-8",
				"TPU_TOPOLOGY":              "2x2",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v3 slice 4x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v3-slice",
				TopologyLabel:         "4x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v3-32",
				"TPU_TOPOLOGY":              "4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,2,1",
				"TPU_HOST_BOUNDS":           "2,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v3 slice 4x8 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v3-slice",
				TopologyLabel:         "4x8",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v3-64",
				"TPU_TOPOLOGY":              "4x8",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,4,1",
				"TPU_HOST_BOUNDS":           "2,4,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v3 slice 8x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v3-slice",
				TopologyLabel:         "8x16",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v3-256",
				"TPU_TOPOLOGY":              "8x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "4,8,1",
				"TPU_HOST_BOUNDS":           "4,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v3 slice 16x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v3-slice",
				TopologyLabel:         "16x16",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v3-512",
				"TPU_TOPOLOGY":              "16x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "8,8,1",
				"TPU_HOST_BOUNDS":           "8,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v4 podslice 2x2x2 topology, 8 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "2x2x2",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-16",
				"TPU_TOPOLOGY":              "2x2x2",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,2",
				"TPU_HOST_BOUNDS":           "1,1,2",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v4 podslice 2x2x4",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "2x2x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-32",
				"TPU_TOPOLOGY":              "2x2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,4",
				"TPU_HOST_BOUNDS":           "1,1,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v4 podslice 2x2x1",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "2x2x1",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-8",
				"TPU_TOPOLOGY":              "2x2x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu-v4-lite-podslice (does not exist)",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-lite-podslice",
				AcceleratorCountLabel: "4",
				TopologyLabel:         "2x2x2",
			},
			wantErr: true,
		},
		{
			desc: "v4 podslice 4x4x4 w/o uds enabled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
			},
			disableVbarUds: true,
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "true",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "localhost:8353",
			},
		},
		{
			desc: "v4 podslice 4x4x4",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "true",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc:       "missing node labels",
			nodeLabels: map[string]string{},
			wantErr:    true,
		},
		{
			desc: "invalid accelerator type",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "invalid-accelerator-type",
				AcceleratorCountLabel: "4",
				TopologyLabel:         "2x2",
			},
			wantErr: true,
		},
		{
			desc: "invalid topology",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-lite-podslice",
				AcceleratorCountLabel: "4",
				TopologyLabel:         "2x2",
			},
			wantErr: true,
		},
		{
			desc: "invalid accelerator count",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-lite-podslice",
				TopologyLabel:         "2x2",
				AcceleratorCountLabel: "6",
			},
			wantErr: true,
		},
		{
			desc: "v5 lite podslice 1x1 topology, 1 chip per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "1x1",
				AcceleratorCountLabel: "1",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-1",
				"TPU_TOPOLOGY":              "1x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "1,1,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "1,1,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 2x2 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "2x2",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-4",
				"TPU_TOPOLOGY":              "2x2",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 2x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-8",
				"TPU_TOPOLOGY":              "2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,2,1",
				"TPU_HOST_BOUNDS":           "1,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 2x4 topology, 8 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "8",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-8",
				"TPU_TOPOLOGY":              "2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,4,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,4,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 4x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "4x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-16",
				"TPU_TOPOLOGY":              "4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,2,1",
				"TPU_HOST_BOUNDS":           "2,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 4x8 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "4x8",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-32",
				"TPU_TOPOLOGY":              "4x8",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,4,1",
				"TPU_HOST_BOUNDS":           "2,4,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 8x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "8x16",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-128",
				"TPU_TOPOLOGY":              "8x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,true,false",
				"WRAP":                      "false,true,false",
				"HOST_BOUNDS":               "4,8,1",
				"TPU_HOST_BOUNDS":           "4,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5 lite podslice 16x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "16x16",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-256",
				"TPU_TOPOLOGY":              "16x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,false",
				"WRAP":                      "true,true,false",
				"HOST_BOUNDS":               "8,8,1",
				"TPU_HOST_BOUNDS":           "8,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5p slice 2x2x4",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-slice",
				TopologyLabel:         "2x2x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5p-32",
				"TPU_TOPOLOGY":              "2x2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,4",
				"TPU_HOST_BOUNDS":           "1,1,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5p slice 2x2x1",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-slice",
				TopologyLabel:         "2x2x1",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5p-8",
				"TPU_TOPOLOGY":              "2x2x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5p slice 4x4x4",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-slice",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5p-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "true",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "invalid v5p accelerator type",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-lite-podslice",
				AcceleratorCountLabel: "4",
				TopologyLabel:         "2x2x2",
			},
			wantErr: true,
		},
		{
			desc: "v6e slice 1x1 topology, 1 chip per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "1x1",
				AcceleratorCountLabel: "1",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-1",
				"TPU_TOPOLOGY":              "1x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "1,1,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "1,1,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 2x2 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "2x2",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-4",
				"TPU_TOPOLOGY":              "2x2",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 2x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-8",
				"TPU_TOPOLOGY":              "2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,2,1",
				"TPU_HOST_BOUNDS":           "1,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 2x4 topology, 8 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "8",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-8",
				"TPU_TOPOLOGY":              "2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,4,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,4,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 4x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "4x4",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-16",
				"TPU_TOPOLOGY":              "4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,2,1",
				"TPU_HOST_BOUNDS":           "2,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 4x8 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "4x8",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-32",
				"TPU_TOPOLOGY":              "4x8",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,4,1",
				"TPU_HOST_BOUNDS":           "2,4,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 8x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "8x16",
				AcceleratorCountLabel: "4",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-128",
				"TPU_TOPOLOGY":              "8x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,true,false",
				"WRAP":                      "false,true,false",
				"HOST_BOUNDS":               "4,8,1",
				"TPU_HOST_BOUNDS":           "4,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v6e slice 16x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "16x16",
				AcceleratorCountLabel: "4",
			},
			numaNodeIds: []string{},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-256",
				"TPU_TOPOLOGY":              "16x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,false",
				"WRAP":                      "true,true,false",
				"HOST_BOUNDS":               "8,8,1",
				"TPU_HOST_BOUNDS":           "8,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu7x slice 1x1x1 topology, 1 chip per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "1x1x1",
				AcceleratorCountLabel: "1",
			},
			enableDeviceSpreading: true,
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "tpu7x-2",
				"TPU_TOPOLOGY":              "1x1x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "1,1,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "1,1,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu7x slice 2x2x1 topology, 2 chips per node, numa aligned",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "2x2x1",
				AcceleratorCountLabel: "4",
			},
			requestedChipCount:    2,
			enableDeviceSpreading: true,
			isPrivileged:          true,
			numaNodeIds:           []string{"0"},
			visibleChipIds:        []string{"0", "1"},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":        "tpu7x-8",
				"TPU_TOPOLOGY":                "2x2x1",
				"TPU_TOPOLOGY_ALT":            "false",
				"ALT":                         "false",
				"TPU_TOPOLOGY_WRAP":           "false,false,false",
				"WRAP":                        "false,false,false",
				"HOST_BOUNDS":                 "2,1,1",
				"TPU_HOST_BOUNDS":             "2,1,1",
				"CHIPS_PER_HOST_BOUNDS":       "1,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS":   "1,2,1",
				"TPU_SKIP_MDS_QUERY":          "true",
				"TPU_RUNTIME_METRICS_PORTS":   "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":    "unix:///var/run/tpu-plugin/vbar.sock",
				"TPU_VISIBLE_CHIPS":           "0,1",
				"WORKLOAD_NIC_PREFERRED_NUMA": "0",
			},
		},
		{
			desc: "tpu7x slice 2x2x1 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "2x2x1",
				AcceleratorCountLabel: "4",
			},
			enableDeviceSpreading: true,
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "tpu7x-8",
				"TPU_TOPOLOGY":              "2x2x1",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu7x slice 2x2x2 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "2x2x2",
				AcceleratorCountLabel: "4",
			},
			numaNodeIds:           []string{"0", "1"},
			enableDeviceSpreading: true,
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":        "tpu7x-16",
				"TPU_TOPOLOGY":                "2x2x2",
				"TPU_TOPOLOGY_ALT":            "false",
				"ALT":                         "false",
				"TPU_TOPOLOGY_WRAP":           "false,false,false",
				"WRAP":                        "false,false,false",
				"HOST_BOUNDS":                 "1,1,2",
				"TPU_HOST_BOUNDS":             "1,1,2",
				"CHIPS_PER_HOST_BOUNDS":       "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS":   "2,2,1",
				"TPU_SKIP_MDS_QUERY":          "true",
				"TPU_RUNTIME_METRICS_PORTS":   "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":    "unix:///var/run/tpu-plugin/vbar.sock",
				"WORKLOAD_NIC_PREFERRED_NUMA": "0,1",
			},
		},
		{
			desc: "tpu7x slice 4x4x4 topology, 4 chips per node; ICI resilency should be enabled by default",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
			},
			enableDeviceSpreading: true,
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "tpu7x-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "true",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu7x slice 4x4x4 topology, 4 chips per node, with ICI resilency disabled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "False",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "tpu7x-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "false",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "tpu7x slice 12x24x24 topology, 4 chips per node; ICI resilency should be enabled by default",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "12x24x24",
				AcceleratorCountLabel: "4",
			},
			enableDeviceSpreading: true,
			isPrivileged:          true,
			numaNodeIds:           []string{"0", "1"},
			visibleChipIds:        []string{"0", "1", "2", "3"},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":        "tpu7x-13824",
				"TPU_TOPOLOGY":                "12x24x24",
				"TPU_TOPOLOGY_ALT":            "false",
				"ALT":                         "false",
				"TPU_TOPOLOGY_WRAP":           "true,true,true",
				"WRAP":                        "true,true,true",
				"HOST_BOUNDS":                 "6,12,24",
				"TPU_HOST_BOUNDS":             "6,12,24",
				"CHIPS_PER_HOST_BOUNDS":       "2,2,1",
				"ENABLE_ICI_RESILIENCY":       "true",
				"TPU_CHIPS_PER_HOST_BOUNDS":   "2,2,1",
				"TPU_SKIP_MDS_QUERY":          "true",
				"TPU_RUNTIME_METRICS_PORTS":   "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":    "unix:///var/run/tpu-plugin/vbar.sock",
				"TPU_VISIBLE_CHIPS":           "0,1,2,3",
				"WORKLOAD_NIC_PREFERRED_NUMA": "0,1",
			},
		},
		{
			desc: "tpu7x slice 12x24x24 topology, 4 chips per node; ICI resilency disabled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu7x",
				TopologyLabel:         "12x24x24",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "False",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "tpu7x-13824",
				"TPU_TOPOLOGY":              "12x24x24",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "6,12,24",
				"TPU_HOST_BOUNDS":           "6,12,24",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "false",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434,8435,8436,8437,8438",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v4 podslice 4x4x4 with ICI resilency disabled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v4-podslice",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "False",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v4-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "false",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5p slice 4x4x4 with ICI resilency disabled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-slice",
				TopologyLabel:         "4x4x4",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "false",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5p-128",
				"TPU_TOPOLOGY":              "4x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,true",
				"WRAP":                      "true,true,true",
				"HOST_BOUNDS":               "2,2,4",
				"TPU_HOST_BOUNDS":           "2,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"ENABLE_ICI_RESILIENCY":     "false",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5p slice 2x4x4 with ICI resilency disabled should not add ICI env var",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5p-slice",
				TopologyLabel:         "2x4x4",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "false",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5p-64",
				"TPU_TOPOLOGY":              "2x4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,2,4",
				"TPU_HOST_BOUNDS":           "1,2,4",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "v5e 16x16 topology should not have ICI env var even labeled",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "16x16",
				AcceleratorCountLabel: "4",
				ICIResiliency:         "false",
			},
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-256",
				"TPU_TOPOLOGY":              "16x16",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "true,true,false",
				"WRAP":                      "true,true,false",
				"HOST_BOUNDS":               "8,8,1",
				"TPU_HOST_BOUNDS":           "8,8,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "Subslicing 4x4 in v6e slice 16x16 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v6e-slice",
				TopologyLabel:         "16x16",
				AcceleratorCountLabel: "4",
			},
			subSliceTopology: "4x4",
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v6e-16",
				"TPU_TOPOLOGY":              "4x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "2,2,1",
				"TPU_HOST_BOUNDS":           "2,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "Subslicing 2x2 in v5 lite podslice 2x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "4",
			},
			subSliceTopology: "2x2",
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-4",
				"TPU_TOPOLOGY":              "2x2",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,1,1",
				"TPU_HOST_BOUNDS":           "1,1,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_WORKER_ID":             "0",
				"TPU_WORKER_HOSTNAMES":      "localhost",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
		{
			desc: "Subslicing invalid 2x3 in v5 lite podslice 2x4 topology, 4 chips per node",
			nodeLabels: map[string]string{
				AcceleratorLabel:      "tpu-v5-lite-podslice",
				TopologyLabel:         "2x4",
				AcceleratorCountLabel: "4",
			},
			subSliceTopology: "2x3",
			want: map[string]string{
				"TPU_ACCELERATOR_TYPE":      "v5litepod-8",
				"TPU_TOPOLOGY":              "2x4",
				"TPU_TOPOLOGY_ALT":          "false",
				"ALT":                       "false",
				"TPU_TOPOLOGY_WRAP":         "false,false,false",
				"WRAP":                      "false,false,false",
				"HOST_BOUNDS":               "1,2,1",
				"TPU_HOST_BOUNDS":           "1,2,1",
				"CHIPS_PER_HOST_BOUNDS":     "2,2,1",
				"TPU_CHIPS_PER_HOST_BOUNDS": "2,2,1",
				"TPU_SKIP_MDS_QUERY":        "true",
				"TPU_RUNTIME_METRICS_PORTS": "8431,8432,8433,8434",
				"VBAR_CONTROL_SERVICE_URL":  "unix:///var/run/tpu-plugin/vbar.sock",
			},
		},
	}
	for _, tc := range cases {
		accelerator := tc.nodeLabels[AcceleratorLabel]
		chipCountNodeLabelValue := tc.nodeLabels[AcceleratorCountLabel]
		topology := tc.nodeLabels[TopologyLabel]
		enableICIResiliency := tc.nodeLabels[ICIResiliency]
		chipCount, err := ChipCount(chipCountNodeLabelValue)
		acceleratorCount := chipCount
		if SupportsTensorNode(accelerator) {
			acceleratorCount = acceleratorCount * 2
		}

		requestedChipCount := chipCount
		if tc.requestedChipCount != 0 {
			requestedChipCount = tc.requestedChipCount
		}

		if err != nil {
			if tc.wantErr {
				continue
			}
			t.Fatalf("%q: error converting chip count to int: %v", tc.desc, err)
		}

		got, err := InitEnvs(InitEnvOptions{
			Accelerator:           accelerator,
			Topology:              topology,
			ChipCount:             chipCount,
			AcceleratorCount:      acceleratorCount,
			EnableICIResiliency:   enableICIResiliency,
			SubSliceTopology:      tc.subSliceTopology,
			RequestedChipCount:    requestedChipCount,
			VisibleChipIds:        tc.visibleChipIds,
			IsPrivileged:          tc.isPrivileged,
			EnableDeviceSpreading: tc.enableDeviceSpreading,
			NumaNodeIds:           tc.numaNodeIds,
			EnableVbarUds:         !tc.disableVbarUds,
		})
		if err != nil {
			if tc.wantErr {
				continue
			}
			t.Errorf("%q: error initializing environment variables: %v", tc.desc, err)
		}
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("%q: incorrect environment variables (+got/-want): %s", tc.desc, diff)
		}
	}
}

func TestIsValidSubSliceTopology(t *testing.T) {
	cases := []struct {
		desc        string
		topology    string
		subTopology string
		expected    bool
		wantErr     bool
	}{
		{
			desc:        "Invalid subslice topology: subSliceTopology is an invalid combination",
			topology:    "2x4",
			subTopology: "2x3",
			expected:    false,
			wantErr:     true,
		},
		{
			desc:        "Invalid subslice topology: subSliceTopology is larger than the topology",
			topology:    "2x4",
			subTopology: "4x4",
			expected:    false,
			wantErr:     true,
		},
		{
			desc:        "Invalid subslice topology: subSliceTopology dimension is wrong",
			topology:    "2x4",
			subTopology: "2x2x1",
			expected:    false,
			wantErr:     true,
		},
		{
			desc:        "No subslice topology",
			topology:    "2x4",
			subTopology: "",
			expected:    false,
			wantErr:     false,
		},
		{
			desc:        "Valid subslice topology",
			topology:    "2x4",
			subTopology: "2x2",
			expected:    true,
			wantErr:     false,
		},
	}
	for _, tc := range cases {
		valid, err := IsValidSubSliceTopology(tc.topology, tc.subTopology)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: wantErr is %v, getting err %v", tc.desc, tc.wantErr, err)
		}
		if valid != tc.expected {
			t.Errorf("%q: valid is %v, expecting %v", tc.desc, valid, tc.expected)
		}
	}
}

func TestNodeLabels(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	cases := []struct {
		desc    string
		server  *httptest.Server
		wantErr bool
	}{
		{
			desc: "valid labels for podslice",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := fmt.Fprintf(w, `%s=tpu-v4-podslice,%s=2x2x2,%s=4`, AcceleratorLabel, TopologyLabel, AcceleratorCountLabel)
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: false,
		},
		{
			desc: "invalid labels for podslice (podslice must have topology)",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := fmt.Fprintf(w, `%s=tpu-v4-podslice`, AcceleratorLabel)
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: true,
		},
		{
			desc: "valid labels for v5p slice",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := fmt.Fprintf(w, `%s=tpu-v5-slice,%s=2x2x2,%s=4`, AcceleratorLabel, TopologyLabel, AcceleratorCountLabel)
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: false,
		},
		{
			desc: "valid labels for provision only mode (topology not required)",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := fmt.Fprintf(w, `%s=tpu-v4-podslice,%s=PROVISION_ONLY,%s=4`, AcceleratorLabel, AcceleratorTopologyModeLabel, AcceleratorCountLabel)
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: false,
		},
		{
			desc: "valid labels for static provisioning mode (topology required)",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := fmt.Fprintf(w, `%s=tpu-v4-podslice,%s=AUTO_CONNECT,%s=4`, AcceleratorLabel, AcceleratorTopologyModeLabel, AcceleratorCountLabel)
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: true,
		},
		{
			desc: "404 Not Found from server",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			})),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		_, err := NodeLabels(tc.server.URL)
		if err != nil && !tc.wantErr {
			t.Errorf("%q: error getting node labels: %v", tc.desc, err)
		}
	}
}

func TestGetCreatedByMIG(t *testing.T) {
	cases := []struct {
		desc    string
		server  *httptest.Server
		want    string
		wantErr bool
	}{
		{
			desc: "valid MIG response",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("mig-12345"))
			})),
			want:    "mig-12345",
			wantErr: false,
		},
		{
			desc: "404 Not Found from server returns empty string and nil error",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("<!DOCTYPE html><html><head><title>Error 404 (Not Found)</title></head></html>"))
			})),
			want:    "",
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := GetCreatedByMIG(tc.server.URL)
			if (err != nil) != tc.wantErr {
				t.Errorf("%q: GetCreatedByMIG() error = %v, wantErr %v", tc.desc, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("%q: GetCreatedByMIG() got = %q, want %q", tc.desc, got, tc.want)
			}
		})
	}
}

func TestNodeInstanceID(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	cases := []struct {
		desc    string
		server  *httptest.Server
		wantErr bool
	}{
		{
			desc: "get instance ID successfully",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`instanceID0`))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: false,
		},
		{
			desc: "does not get instance ID",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, err := w.Write([]byte(""))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			})),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		_, err := NodeInstanceID(tc.server.URL)
		if err != nil && !tc.wantErr {
			t.Errorf("%q: error getting Node instanceID: %v", tc.desc, err)
		}
	}
}

func TestCallHTTPServer(t *testing.T) {
	cases := []struct {
		desc    string
		server  *httptest.Server
		wantErr error
	}{
		{
			desc: "404 Not Found returns ErrMetadataNotFound",
			server: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("<!DOCTYPE html><html><head><title>Error 404 (Not Found)</title></head></html>"))
			})),
			wantErr: ErrMetadataNotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := CallHTTPServer(tc.server.URL, true, 50*time.Millisecond, true)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%q: CallHTTPServer() error = %v, want errors.Is(err, %v)", tc.desc, err, tc.wantErr)
			}
		})
	}
}

func TestGetEnvName(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	cases := []struct {
		desc     string
		envName  string
		nodeName string
		wantErr  bool
	}{
		{
			desc:     "get Node Name",
			envName:  "NODE_NAME",
			nodeName: "nodename1",
			wantErr:  false,
		},
		{
			desc:     "does not get Node Name",
			envName:  "NODE_NAME",
			nodeName: "",
			wantErr:  true,
		},
		{
			desc:     "get Project ID",
			envName:  "CLUSTER_PROJECT",
			nodeName: "1234567",
			wantErr:  true,
		},
		{
			desc:     "get cluster location",
			envName:  "CLUSTER_LOCATION",
			nodeName: "us-west1-b",
			wantErr:  true,
		},
		{
			desc:     "does not get node ip",
			envName:  "NODE_IP",
			nodeName: "",
			wantErr:  true,
		},
	}
	for _, tc := range cases {
		t.Setenv(tc.envName, tc.nodeName)
		_, err := GetEnvName(tc.envName)
		if err != nil && !tc.wantErr {
			t.Errorf("%q: error getting nodeName: %v", tc.desc, err)
		}
	}
}

func TestGetTPURunningContainerInfo(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cases := []struct {
		desc    string
		want    []TPUContainerInfo
		podList []any
		wantErr bool
	}{
		{
			desc: "Only TPU pod and running container",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)},
					map[string]string{SubSliceTopologyAnnotation: "2x2"}),
			},
			want: []TPUContainerInfo{
				{
					Pod:              "tpu-pod",
					Namespace:        "default",
					Container:        "tpu-c",
					PodIP:            "10.10.10.10",
					SubSliceTopology: "2x2",
					RequestedTPU:     4,
				},
			},
			wantErr: false,
		},
		{
			desc: "Only TPU pod and running privileged container",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, true),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)},
					map[string]string{SubSliceTopologyAnnotation: "2x2"}),
			},
			want: []TPUContainerInfo{
				{
					Pod:              "tpu-pod",
					Namespace:        "default",
					Container:        "tpu-c",
					PodIP:            "10.10.10.10",
					SubSliceTopology: "2x2",
					RequestedTPU:     4,
					IsPrivileged:     true,
				},
			},
			wantErr: false,
		},
		{
			desc: "Multiple TPU pods and only one running",
			podList: []any{
				getPod("tpu-pod-2", "default", "10.10.10.12", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", false)}, map[string]string{}),
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)}, map[string]string{}),
				getPod("tpu-pod-1", "default", "10.10.10.11", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", false)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod",
					Namespace:    "default",
					Container:    "tpu-c",
					PodIP:        "10.10.10.10",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "Multiple running TPU containers in multiple pods",
			podList: []any{
				// Pod 1: running TPU container
				getPod("tpu-pod-1", "default", "10.0.0.1",
					[]v1.Container{},
					[]v1.Container{tpuContainer("tpu-c-1", true, true, false)},
					[]v1.ContainerStatus{},
					[]v1.ContainerStatus{containerStatus("tpu-c-1", true)},
					map[string]string{}),
				// Pod 2: running TPU container
				getPod("tpu-pod-2", "default", "10.0.0.3",
					[]v1.Container{tpuContainer("tpu-c-2", true, true, false)},
					[]v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c-2", true)},
					[]v1.ContainerStatus{},
					map[string]string{}),
				// Pod 3: non-running TPU container
				getPod("tpu-pod-3", "default", "10.0.0.4",
					[]v1.Container{},
					[]v1.Container{tpuContainer("tpu-c-3", true, true, false)},
					[]v1.ContainerStatus{},
					[]v1.ContainerStatus{containerStatus("tpu-c-3", false)},
					map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod-1",
					Namespace:    "default",
					Container:    "tpu-c-1",
					PodIP:        "10.0.0.1",
					RequestedTPU: 4,
				},
				{
					Pod:          "tpu-pod-2",
					Namespace:    "default",
					Container:    "tpu-c-2",
					PodIP:        "10.0.0.3",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "Multiple running privileged TPU containers in multiple pods",
			podList: []any{
				// Pod 1: running TPU container
				getPod("tpu-pod-1", "default", "10.0.0.1",
					[]v1.Container{},
					[]v1.Container{tpuContainer("tpu-c-1", true, true, true)},
					[]v1.ContainerStatus{},
					[]v1.ContainerStatus{containerStatus("tpu-c-1", true)},
					map[string]string{}),
				// Pod 2: running TPU container
				getPod("tpu-pod-2", "default", "10.0.0.3",
					[]v1.Container{tpuContainer("tpu-c-2", true, true, true)},
					[]v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c-2", true)},
					[]v1.ContainerStatus{},
					map[string]string{}),
				// Pod 3: non-running TPU container
				getPod("tpu-pod-3", "default", "10.0.0.4",
					[]v1.Container{},
					[]v1.Container{tpuContainer("tpu-c-3", true, true, true)},
					[]v1.ContainerStatus{},
					[]v1.ContainerStatus{containerStatus("tpu-c-3", false)},
					map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod-1",
					Namespace:    "default",
					Container:    "tpu-c-1",
					PodIP:        "10.0.0.1",
					RequestedTPU: 4,
					IsPrivileged: true,
				},
				{
					Pod:          "tpu-pod-2",
					Namespace:    "default",
					Container:    "tpu-c-2",
					PodIP:        "10.0.0.3",
					RequestedTPU: 4,
					IsPrivileged: true,
				},
			},
			wantErr: false,
		},

		{
			desc: "TPU pod with running container + non-TPU pod",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)}, map[string]string{}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("non-tpu-c", true)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod",
					Namespace:    "default",
					Container:    "tpu-c",
					PodIP:        "10.10.10.10",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "TPU pod (init Container running) + non-TPU pod",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{tpuContainer("tpu-c", true, true, false)}, []v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c", true)}, []v1.ContainerStatus{}, map[string]string{}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("non-tpu-c", true)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod",
					Namespace:    "default",
					Container:    "tpu-c",
					PodIP:        "10.10.10.10",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "TPU pod (init Container running), no IP + non-TPU pod",
			podList: []any{
				getPod("tpu-pod", "default", "", []v1.Container{tpuContainer("tpu-c", true, true, false)}, []v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c", true)}, []v1.ContainerStatus{}, map[string]string{}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod",
					Namespace:    "default",
					Container:    "tpu-c",
					PodIP:        "",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "Error parsing",
			podList: []any{
				getPod("tpu-pod", "default", "", []v1.Container{tpuContainer("tpu-c", true, true, false)}, []v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c", true)}, []v1.ContainerStatus{}, map[string]string{}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:          "tpu-pod",
					Namespace:    "default",
					Container:    "tpu-c",
					PodIP:        "",
					RequestedTPU: 4,
				},
			},
			wantErr: true,
		},
		{
			desc: "TPU pod (init Container not running) + non-TPU pod",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{tpuContainer("tpu-c", true, true, false)}, []v1.Container{},
					[]v1.ContainerStatus{containerStatus("tpu-c", false)}, []v1.ContainerStatus{}, map[string]string{}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{}, map[string]string{}),
			},
			want:    []TPUContainerInfo{},
			wantErr: false,
		},
		{
			desc: "TPU pod (init Container not running) + main container running",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10",
					[]v1.Container{tpuContainer("tpu-c-init", true, true, false)},
					[]v1.Container{tpuContainer("tpu-c", true, true, false)},
					[]v1.ContainerStatus{containerStatus("tpu-c-init", false)},
					[]v1.ContainerStatus{containerStatus("tpu-c", true)},
					map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Namespace:    "default",
					Pod:          "tpu-pod",
					Container:    "tpu-c",
					PodIP:        "10.10.10.10",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
		{
			desc: "non-TPU pod with running container",
			podList: []any{
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("non-tpu-c", true)}, map[string]string{}),
			},
			want:    []TPUContainerInfo{},
			wantErr: false,
		},
	}
	containerInfoExtractor := ContainerInfoExtractor{}
	for _, tc := range cases {
		tpuContainerInfo := containerInfoExtractor.GetTPUContainerInfo(ctx, GetPodsFromInformer(tc.podList), IsContainerRunning)
		assert.Equalf(t, tc.want, tpuContainerInfo, "%s, TPUContainerInfo differ", tc.desc)
	}
}

func TestGetTPUUnscheduledContainerInfo(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cases := []struct {
		desc    string
		want    []TPUContainerInfo
		podList []any
		wantErr bool
	}{
		{
			desc: "Only TPU pod and running container",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)},
					map[string]string{}),
			},
			want:    []TPUContainerInfo{},
			wantErr: false,
		},
		{
			desc: "Multiple TPU pods and only one waiting",
			podList: []any{
				getPod("tpu-pod-2", "default", "10.10.10.12", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)}, map[string]string{}),
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{},
					map[string]string{SubSliceTopologyAnnotation: "4x4"}),
				getPod("tpu-pod-1", "default", "10.10.10.11", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("tpu-c", true)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:              "tpu-pod",
					Namespace:        "default",
					Container:        "tpu-c",
					PodIP:            "10.10.10.10",
					SubSliceTopology: "4x4",
					RequestedTPU:     4,
				},
			},
			wantErr: false,
		},
		{
			desc: "TPU pod with waiting container + non-TPU pod",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
					tpuContainer("tpu-c", true, true, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{},
					map[string]string{SubSliceTopologyAnnotation: "4x4"}),
				getPod("non-tpu-pod", "default", "20.20.20.20", []v1.Container{}, []v1.Container{
					tpuContainer("non-tpu-c", false, false, false),
				}, []v1.ContainerStatus{}, []v1.ContainerStatus{containerStatus("non-tpu-c", false)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Pod:              "tpu-pod",
					Namespace:        "default",
					Container:        "tpu-c",
					PodIP:            "10.10.10.10",
					SubSliceTopology: "4x4",
					RequestedTPU:     4,
				},
			},
			wantErr: false,
		},
		{
			// Shouldn't happen and should be blocked
			desc: "TPU pod (init Container not running) + main container running",
			podList: []any{
				getPod("tpu-pod", "default", "10.10.10.10",
					[]v1.Container{tpuContainer("tpu-c-init", true, true, false)},
					[]v1.Container{tpuContainer("tpu-c", true, true, false)},
					[]v1.ContainerStatus{},
					[]v1.ContainerStatus{containerStatus("tpu-c", true)}, map[string]string{}),
			},
			want: []TPUContainerInfo{
				{
					Namespace:    "default",
					Pod:          "tpu-pod",
					Container:    "tpu-c-init",
					PodIP:        "10.10.10.10",
					RequestedTPU: 4,
				},
			},
			wantErr: false,
		},
	}
	containerInfoExtractor := ContainerInfoExtractor{}
	for _, tc := range cases {
		tpuContainerInfo := containerInfoExtractor.GetTPUContainerInfo(ctx, GetPodsFromInformer(tc.podList), IsContainerStatusEmpty)
		assert.Equalf(t, tc.want, tpuContainerInfo, "%s, TPUContainerInfo differ", tc.desc)
	}
}

func tpuContainer(name string, limits, requests bool, privileged bool) v1.Container {
	container := v1.Container{
		Name:  name,
		Image: "k8s.gcr.io/pause",
	}
	if limits {
		container.Resources.Requests = v1.ResourceList{
			v1.ResourceName("google.com/tpu"): resource.MustParse("4"),
		}
	}
	if requests {
		container.Resources.Limits = v1.ResourceList{
			v1.ResourceName("google.com/tpu"): resource.MustParse("4"),
		}
	}
	if privileged {
		p := true
		container.SecurityContext = &v1.SecurityContext{Privileged: &p}
	}
	return container
}

func containerStatus(name string, running bool) v1.ContainerStatus {
	containerStatus := v1.ContainerStatus{
		Name: name,
		State: v1.ContainerState{
			Waiting: &v1.ContainerStateWaiting{Reason: "pending to scheule"},
		},
	}
	if running {
		containerStatus = v1.ContainerStatus{
			Name: name,
			State: v1.ContainerState{
				Running: &v1.ContainerStateRunning{StartedAt: metav1.Time{Time: time.Now()}},
			},
		}
	}

	return containerStatus
}

func getPod(name, namespace, podIP string, initC, c []v1.Container, initCS []v1.ContainerStatus, cs []v1.ContainerStatus, annotations map[string]string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Status: v1.PodStatus{
			PodIP:                 podIP,
			ContainerStatuses:     cs,
			InitContainerStatuses: initCS,
		},
		Spec: v1.PodSpec{
			Containers:     c,
			InitContainers: initC,
		},
	}
}

func TestGetEnvInfo(t *testing.T) {
	// Output map is same as returned by fetchFn, so we are just testing
	// the label checks.
	cases := []struct {
		desc    string
		want    EnvInfo
		setEnv  map[string]string
		wantErr bool
	}{
		{
			desc: "Get Env Info successfully",
			want: EnvInfo{
				ClusterName:      "tputest",
				ClusterProjectID: "1234567",
				ClusterLocation:  "us-west1-b",
				PodNamespace:     "kube-system",
				PodName:          "tpu-device-plugin-axjs",
				ContainerName:    "tpu-device-plugin",
			},
			setEnv: map[string]string{
				ClusterNameEnv:     "tputest",
				ClusterProjectEnv:  "1234567",
				ClusterLocationEnv: "us-west1-b",
				PodNamespaceEnv:    "kube-system",
				PodNameEnv:         "tpu-device-plugin-axjs",
				ContainerNameEnv:   "tpu-device-plugin",
			},
			wantErr: false,
		},
		{
			desc: "Miss ClusterProjectID, and get error ",
			want: EnvInfo{},
			setEnv: map[string]string{
				ClusterNameEnv:     "tputest",
				ClusterProjectEnv:  "",
				ClusterLocationEnv: "us-west1-b",
				PodNamespaceEnv:    "kube-system",
				PodNameEnv:         "tpu-device-plugin-axjs",
				ContainerNameEnv:   "tpu-device-plugin",
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		for k, v := range tc.setEnv {
			t.Setenv(k, v)
		}
		envInfo, err := GetEnvInfo()

		if tc.wantErr {
			assert.NotEqualf(t, nil, err, "Expect to have error here but didn't catch any error")
		} else {
			assert.Equalf(t, nil, err, "Expect to not have error here but caught an error")
			assert.Equalf(t, envInfo, tc.want, "EnvInfo differ")
		}
	}
}

func TestTPUUtilizationUtilType(t *testing.T) {
	cases := []struct {
		desc    string
		tpuGen  string
		want    tpuutilizationutil.TPUType
		wantErr bool
	}{
		{
			desc:    "test v4",
			tpuGen:  "v4",
			want:    tpuutilizationutil.V4,
			wantErr: false,
		},
		{
			desc:    "test v5litepod",
			tpuGen:  "v5litepod",
			want:    tpuutilizationutil.V5lite,
			wantErr: false,
		},
		{
			desc:    "test v5p",
			tpuGen:  "v5p",
			want:    tpuutilizationutil.V5,
			wantErr: false,
		},
		{
			desc:    "test v6e",
			tpuGen:  "v6e",
			want:    tpuutilizationutil.V6E,
			wantErr: false,
		},
		{
			desc:    "test tpu7x",
			tpuGen:  "tpu7x",
			want:    tpuutilizationutil.TPU7x,
			wantErr: false,
		},
	}
	for _, tc := range cases {
		tputilizationUtilType, err := TPUUtilizationUtilType(tc.tpuGen)
		if tc.wantErr {
			assert.NotEqualf(t, nil, err, "Expect to have error here but didn't catch any error")
		} else {
			assert.Equalf(t, tputilizationUtilType, tc.want, "TPUUtilizationUtilType differ")
		}
	}
}

func TestApplyNetworkSettings(t *testing.T) {
	tempDir := t.TempDir()
	for _, setting := range networkSettings {
		fileDir := filepath.Dir(filepath.Join(tempDir, setting.FilePath))
		err := os.MkdirAll(fileDir, 0777)
		if err != nil {
			t.Errorf("Failed to create dir %v: %v", fileDir, err)
		}
	}
	err := applyNetworkSettings(tempDir)
	assert.NoError(t, err, "Expect not to have error here but got error")
	for _, setting := range networkSettings {
		filePath := filepath.Join(tempDir, setting.FilePath)
		value, err := os.ReadFile(filePath)
		assert.NoError(t, err, "Expect to not have error here but caught an error on reading file")
		assert.Equalf(t, setting.Value, string(value), "setting value differ")
	}
}

func TestApplyNetworkSettingsNonFatalError(t *testing.T) {
	successfulSettings := []SystemSetting{
		{FilePath: "proc/sys/net/ipv4/tcp_slow_start_after_idle", Value: "0"},
		{FilePath: "proc/sys/net/ipv4/tcp_no_metrics_save", Value: "1"},
		{FilePath: "proc/sys/net/core/somaxconn", Value: "4096"},
		{FilePath: "proc/sys/net/ipv4/tcp_max_syn_backlog", Value: "4096"},
		{FilePath: "proc/sys/net/ipv4/tcp_mtu_probing", Value: "0"},
		{FilePath: "proc/sys/net/core/optmem_max", Value: "131072"},
	}
	tempDir := t.TempDir()
	for _, setting := range successfulSettings {
		fileDir := filepath.Dir(filepath.Join(tempDir, setting.FilePath))
		err := os.MkdirAll(fileDir, 0777)

		if err != nil {
			t.Errorf("Failed to create dir %v: %v", fileDir, err)
		}
	}
	err := applyNetworkSettings(tempDir)
	expectedErrorMsg := filepath.Join(tempDir, "sys/module/tcp_cubic/parameters/hystart_detect")
	assert.EqualErrorf(t, err, expectedErrorMsg, "Error should be: %v, got: %v", expectedErrorMsg, err)
	for _, setting := range successfulSettings {
		filePath := filepath.Join(tempDir, setting.FilePath)
		value, err := os.ReadFile(filePath)
		assert.NoError(t, err, "Expect to not have error here but caught an error on reading file")
		assert.Equalf(t, setting.Value, string(value), "setting value differ")
	}
}

func TestGetSubsliceLabels(t *testing.T) {
	cases := []struct {
		desc                string
		serverHandler       http.HandlerFunc
		nodeTopology        string
		expectedLabels      map[string]string
		gen                 string
		enableFullHierarchy bool
		wantErr             bool
	}{
		{
			desc:         "Valid 2D response for 16x16",
			nodeTopology: "16x16",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"4x4": "hash-a", "2x4": "hash-b", "2x2": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v6e",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-4x4-id": "hash-a",
				"cloud.google.com/gke-tpu-slice-2x4-id": "hash-b",
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-c",
			},
			wantErr: false,
		},
		{
			desc:         "Valid 2D response with smaller topologies",
			nodeTopology: "4x4",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"4x4": "hash-a", "2x4": "hash-b", "2x2": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v6e",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-4x4-id": "hash-a",
				"cloud.google.com/gke-tpu-slice-2x4-id": "hash-b",
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-c",
			},
			wantErr: false,
		},
		{
			desc:         "Valid 3D response with smaller topologies < V7X",
			nodeTopology: "4x4x4",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"4x4x4": "hash-a", "2x4x4": "hash-b", "2x2x4": "hash-c", "2x2x2": "hash-d", "2x2x1": "hash-e"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v4",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-4x4x4-id": "hash-a",
				"cloud.google.com/gke-tpu-slice-2x4x4-id": "hash-b",
				"cloud.google.com/gke-tpu-slice-2x2x4-id": "hash-c",
				"cloud.google.com/gke-tpu-slice-2x2x2-id": "hash-d",
				"cloud.google.com/gke-tpu-slice-2x2x1-id": "hash-e",
			},
			wantErr: false,
		},
		{
			desc:         "Valid 3D response with smaller topologies V7X",
			nodeTopology: "4x4x4",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"4x4x4": "hash-a", "2x4x4": "hash-b", "2x2x4": "hash-c", "2x2x2": "hash-d", "2x2x1": "hash-e"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "tpu7x",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-4x4x4-id": "hash-a",
				"cloud.google.com/gke-tpu-partition-2x4x4-id": "hash-b",
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "hash-c",
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "hash-d",
				"cloud.google.com/gke-tpu-partition-2x2x1-id": "hash-e",
			},
			wantErr: false,
		},
		{
			desc:         "Response with larger 2D topologies should be skipped",
			nodeTopology: "2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "4x4": "hash-b", "2x4": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v6e",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "Response with larger 3D topologies should be skipped v7x",
			nodeTopology: "2x2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b", "2x2x4": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "tpu7x",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "No nodeTopology, shouldn't be skipped v7x",
			nodeTopology: "",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b", "2x2x4": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "tpu7x",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "hash-a",
				"cloud.google.com/gke-tpu-partition-4x4x4-id": "hash-b",
				"cloud.google.com/gke-tpu-partition-2x2x4-id": "hash-c",
			},
			wantErr: false,
		},
		{
			desc:         "Response with larger 3D topologies should be skipped v4",
			nodeTopology: "2x2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b", "2x2x4": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v4",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "Response with different dimensions should be skipped",
			nodeTopology: "2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "2x2x2": "hash-b"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen: "v6e",
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc: "Empty response (not gSC or allowlist)",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			expectedLabels: map[string]string{},
			wantErr:        false,
		},
		{
			desc: "Non-200 HTTP status from server",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}),
			expectedLabels: map[string]string{},
			wantErr:        true,
		},
		{
			desc: "404 Not Found from server",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			}),
			expectedLabels: map[string]string{},
			wantErr:        true,
		},
		{
			desc:         "Hierarchy labels enabled, some topologies larger than node topology",
			nodeTopology: "2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "4x4": "hash-b", "32x32": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v6e",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-a",
				"cloud.google.com/gke-tpu-slice-4x4-id": "hash-b",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, but skipped for v4",
			nodeTopology: "2x2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v4",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, but skipped for v5p",
			nodeTopology: "2x2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v5p",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, partition template for tpu7x",
			nodeTopology: "2x2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2x2": "hash-a", "4x4x4": "hash-b"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "tpu7x",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-partition-2x2x2-id": "hash-a",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, topologies up to 16x16 populated for v5litepod",
			nodeTopology: "2x2",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "4x4": "hash-b", "32x32": "hash-c"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v5litepod",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id": "hash-a",
				"cloud.google.com/gke-tpu-slice-4x4-id": "hash-b",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, node topology 8x8, v6e",
			nodeTopology: "8x8",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "4x4": "hash-b", "8x8": "hash-c", "16x16": "hash-d", "32x32": "hash-e"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v6e",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id":   "hash-a",
				"cloud.google.com/gke-tpu-slice-4x4-id":   "hash-b",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "hash-c",
				"cloud.google.com/gke-tpu-slice-16x16-id": "hash-d",
			},
			wantErr: false,
		},
		{
			desc:         "Hierarchy labels enabled, node topology 32x32 (over 16x16), v6e",
			nodeTopology: "32x32",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				jsonResponse := `{"2x2": "hash-a", "4x4": "hash-b", "16x16": "hash-c", "32x32": "hash-d", "64x64": "hash-e"}`
				_, err := w.Write([]byte(jsonResponse))
				if err != nil {
					t.Errorf("error writing response: %v", err)
				}
			}),
			gen:                 "v6e",
			enableFullHierarchy: true,
			expectedLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-2x2-id":   "hash-a",
				"cloud.google.com/gke-tpu-slice-4x4-id":   "hash-b",
				"cloud.google.com/gke-tpu-slice-16x16-id": "hash-c",
				"cloud.google.com/gke-tpu-slice-32x32-id": "hash-d",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			server := httptest.NewServer(tc.serverHandler)
			defer server.Close()
			gotLabels, err := GetSubsliceLabels(server.URL, tc.nodeTopology, tc.gen, tc.enableFullHierarchy)

			if (err != nil) != tc.wantErr {
				t.Errorf("GetSubsliceLabels() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr {
				if !reflect.DeepEqual(gotLabels, tc.expectedLabels) {
					t.Errorf("GetSubsliceLabels() gotLabels = %v, want %v", gotLabels, tc.expectedLabels)
				}
			}
		})
	}

}

func TestParseTopology(t *testing.T) {
	cases := []struct {
		desc     string
		topology string
		want     []int
		wantErr  bool
	}{
		{"Valid 2D topology", "2x2", []int{2, 2}, false},
		{"Valid 3D topology", "4x4x4", []int{4, 4, 4}, false},
		{"Valid 3D topology", "2x2x1", []int{2, 2, 1}, false},
		{"Invalid format - no 'x'", "16", nil, true},
		{"Invalid format - non-integer value", "8xabc", nil, true},
		{"Invalid format - too many dimensions", "2x2x2x2", nil, true},
		{"Invalid format - empty string", "", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := parseTopology(tc.topology)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseTopology() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr {
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("parseTopology() got = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestWaitForPartitionLabels_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block long enough for cancellation to occur.
		time.Sleep(200 * time.Millisecond)
		// Return 200 with invalid JSON to avoid retryablehttp retries (which happen on 500s)
		// but ensure GetSubsliceLabels returns an error.
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("invalid-json")); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel the context after a short delay.
	time.AfterFunc(100*time.Millisecond, cancel)

	_, err := WaitForPartitionLabels(ctx, server.URL, "4x4x4", "tpu7x", false, 1*time.Second, 10*time.Millisecond)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("WaitForPartitionLabels() error = %v, want %v", err, context.Canceled)
	}
}

func TestWaitForPartitionLabels(t *testing.T) {
	jsonResponse := `{
		"2x2x1": "ac9dce457d4206170433b18ba2e604ba",
		"2x2x2": "997ca394242dcc507baa22f476bf7fad",
		"2x2x4": "888841721cdfd95426a165958b0a4683",
		"2x4x4": "d7816ebbd77ebaaa689a732007cf0351",
		"4x4x4": "fba785f80d18552357dcdef6d3d16c27"
	}`
	expectedLabels := map[string]string{
		"cloud.google.com/gke-tpu-partition-2x2x1-id": "ac9dce457d4206170433b18ba2e604ba",
		"cloud.google.com/gke-tpu-partition-2x2x2-id": "997ca394242dcc507baa22f476bf7fad",
		"cloud.google.com/gke-tpu-partition-2x2x4-id": "888841721cdfd95426a165958b0a4683",
		"cloud.google.com/gke-tpu-partition-2x4x4-id": "d7816ebbd77ebaaa689a732007cf0351",
		"cloud.google.com/gke-tpu-partition-4x4x4-id": "fba785f80d18552357dcdef6d3d16c27",
	}

	cases := []struct {
		desc           string
		handler        func(w http.ResponseWriter, r *http.Request)
		timeout        time.Duration
		interval       time.Duration
		expectedLabels map[string]string
		wantErr        bool
		tpuGen         string
		nodeTopology   string
	}{
		{
			desc: "Success on first try",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				if _, err := w.Write([]byte(jsonResponse)); err != nil {
					t.Errorf("failed to write response: %v", err)
				}
			},
			timeout:        1 * time.Second,
			interval:       10 * time.Millisecond,
			expectedLabels: expectedLabels,
			wantErr:        false,
			tpuGen:         "tpu7x",
			nodeTopology:   "4x4x4",
		},
		{
			desc: "Success after retries",
			handler: func() func(w http.ResponseWriter, r *http.Request) {
				count := 0
				return func(w http.ResponseWriter, r *http.Request) {
					count++
					if count < 3 {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					w.WriteHeader(http.StatusOK)
					if _, err := w.Write([]byte(jsonResponse)); err != nil {
						t.Errorf("failed to write response: %v", err)
					}
				}
			}(),
			timeout:        1 * time.Second,
			interval:       10 * time.Millisecond,
			expectedLabels: expectedLabels,
			wantErr:        false,
			tpuGen:         "tpu7x",
			nodeTopology:   "4x4x4",
		},
		{
			desc: "Timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			timeout:      50 * time.Millisecond,
			interval:     10 * time.Millisecond,
			wantErr:      true,
			tpuGen:       "tpu7x",
			nodeTopology: "4x4x4",
		},
		{
			desc: "Timeout on 404 returns empty map and nil error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			timeout:        50 * time.Millisecond,
			interval:       10 * time.Millisecond,
			expectedLabels: map[string]string{},
			wantErr:        false,
			tpuGen:         "tpu7x",
			nodeTopology:   "4x4x4",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.handler))
			defer server.Close()

			got, err := WaitForPartitionLabels(context.Background(), server.URL, tc.nodeTopology, tc.tpuGen, false, tc.timeout, tc.interval)
			if (err != nil) != tc.wantErr {
				t.Errorf("WaitForPartitionLabels() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr && !reflect.DeepEqual(got, tc.expectedLabels) {
				t.Errorf("WaitForPartitionLabels() got = %v, want %v", got, tc.expectedLabels)
			}
		})
	}
}

func TestSetSubsliceLabels(t *testing.T) {
	const testNodeName = "test-node"

	for _, tt := range []struct {
		desc            string
		initialLabels   map[string]string
		partitionLabels map[string]string
		wantLabels      map[string]string
		wantErr         bool
	}{
		{
			desc:            "Empty Partition Label Map",
			initialLabels:   map[string]string{"existing-label": "value"},
			partitionLabels: map[string]string{},
			wantLabels:      map[string]string{"existing-label": "value"},
			wantErr:         false,
		},
		{
			desc:          "Apply new labels to a node with no labels",
			initialLabels: map[string]string{},
			partitionLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantErr: false,
		},
		{
			desc: "Apply new labels alongside existing ones",
			initialLabels: map[string]string{
				"existing-label": "value",
			},
			partitionLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantLabels: map[string]string{
				"existing-label": "value",
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantErr: false,
		},
		{
			desc: "Update existing partition labels with new hashes",
			initialLabels: map[string]string{
				"existing-label": "value",
				"cloud.google.com/gke-tpu-slice-16x16-id": "jb23j1k2b3j1k23",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "askldnaslkdn1",
			},
			partitionLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantLabels: map[string]string{
				"existing-label": "value",
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantErr: false,
		},
		{
			desc: "No update if matching label/hash exists",
			initialLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
			},
			partitionLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
			},
			wantLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
			},
			wantErr: false,
		},
		{
			desc: "Existing partition labels remain if new partition map from MDS is empty",
			initialLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			partitionLabels: map[string]string{},
			wantLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdi1h1ih2pi12h3",
			},
			wantErr: false,
		},
		{
			desc: "Existing partition labels without new entry remain, but other topologies get updated",
			initialLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdbjabsjdkb1jb1",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdhaijsdhajsdhp",
			},
			partitionLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdi1h1ih2pi12h3",
			},
			wantLabels: map[string]string{
				"cloud.google.com/gke-tpu-slice-16x16-id": "asdi1h1ih2pi12h3",
				"cloud.google.com/gke-tpu-slice-8x8-id":   "asdhaijsdhajsdhp",
			},
			wantErr: false,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			node := makeNode(tt.initialLabels, nil)
			clientset := fake.NewSimpleClientset(&v1.NodeList{Items: []v1.Node{node}})
			labelHandler := NewNodeMetadataHandler(testNodeName, clientset)

			gotErr := SetSubsliceLabels(context.Background(), labelHandler, tt.partitionLabels)
			if gotErr != nil && !tt.wantErr {
				t.Errorf("SetSubsliceLabels() returned unexpected error: %v, wantErr: %v", gotErr, tt.wantErr)
			}
			if gotErr == nil && tt.wantErr {
				t.Errorf("SetSubsliceLabels() expected error but got nil")
			}

			updatedNode, _ := clientset.CoreV1().Nodes().Get(context.Background(), "test-node", metav1.GetOptions{})
			if diff := cmp.Diff(tt.wantLabels, updatedNode.GetObjectMeta().GetLabels()); diff != "" {
				t.Errorf("RemoveLabel() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestApplyNodeAnnotation(t *testing.T) {
	const testNodeName = "test-node"
	const testKey = "testKey"
	const testValue = "testValue"

	for _, tt := range []struct {
		desc               string
		initialAnnotations map[string]string
		applyKey           string
		applyValue         string
		wantAnnotations    map[string]string
		wantErr            bool
	}{
		{
			desc:               "Apply new annotation to a node with no annotations",
			initialAnnotations: map[string]string{},
			applyKey:           testKey,
			applyValue:         testValue,
			wantAnnotations:    map[string]string{testKey: testValue},
			wantErr:            false,
		},
		{
			desc: "Apply new annotation alongside existing ones",
			initialAnnotations: map[string]string{
				"existing-annotation": "value",
			},
			applyKey:   testKey,
			applyValue: testValue,
			wantAnnotations: map[string]string{
				"existing-annotation": "value",
				testKey:               testValue,
			},
			wantErr: false,
		},
		{
			desc: "Update existing annotation with a new value",
			initialAnnotations: map[string]string{
				"existing-annotation": "value",
				testKey:               "old-mig-name",
			},
			applyKey:   testKey,
			applyValue: testValue,
			wantAnnotations: map[string]string{
				"existing-annotation": "value",
				testKey:               testValue,
			},
			wantErr: false,
		},
		{
			desc:               "Error case: Node does not exist in the fake client",
			initialAnnotations: nil, // This signals we won't pre-load the node
			applyKey:           testKey,
			applyValue:         testValue,
			wantErr:            true,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			var clientset *fake.Clientset

			if tt.initialAnnotations != nil {
				node := makeNode(nil, tt.initialAnnotations)
				clientset = fake.NewSimpleClientset(&v1.NodeList{Items: []v1.Node{node}})
			} else {
				clientset = fake.NewSimpleClientset()
			}

			gotErr := ApplyNodeAnnotation(context.Background(), clientset, testNodeName, tt.applyKey, tt.applyValue)

			if (gotErr != nil) != tt.wantErr {
				t.Errorf("ApplyNodeAnnotation() error mismatch: got error %v, want error %v", gotErr, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			updatedNode, err := clientset.CoreV1().Nodes().Get(context.Background(), testNodeName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Failed to get updated node: %v", err)
			}

			if diff := cmp.Diff(tt.wantAnnotations, updatedNode.GetObjectMeta().GetAnnotations()); diff != "" {
				t.Errorf("ApplyNodeAnnotation() annotations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIsLegacyTPU(t *testing.T) {
	tests := []struct {
		tpuGen string
		want   bool
	}{
		{"v3", true},
		{"v4", true},
		{"v5p", false},
		{"v6e", false},
		{"tpu7x", false},
	}
	for _, tc := range tests {
		t.Run(tc.tpuGen, func(t *testing.T) {
			if got := IsLegacyTPU(tc.tpuGen); got != tc.want {
				t.Errorf("IsLegacyTPU(%q) = %v, want %v", tc.tpuGen, got, tc.want)
			}
		})
	}
}

func TestGetDeviceDirectory(t *testing.T) {
	tests := []struct {
		tpuGen string
		want   string
	}{
		{"v3", "/dev"},
		{"v4", "/dev"},
		{"v5p", "/dev/vfio"},
		{"v6e", "/dev/vfio"},
		{"tpu7x", "/dev/vfio"},
	}
	for _, tc := range tests {
		t.Run(tc.tpuGen, func(t *testing.T) {
			if got := GetDeviceDirectory(tc.tpuGen); got != tc.want {
				t.Errorf("GetDeviceDirectory(%q, \"/dev\", \"/dev/vfio\") = %q, want %q", tc.tpuGen, got, tc.want)
			}
		})
	}
}
