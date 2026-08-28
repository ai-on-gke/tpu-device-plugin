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

package healthcheck

import (
	"os"
	"path"
	"testing"
	"time"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type Pair struct {
	elem1 string
	elem2 string
}

func TestTpuHealthChecker(t *testing.T) {

	cases := []struct {
		name                 string
		createDevices        []string
		initialDevices       map[string]*pluginapi.Device
		createDeleteOps      []Pair
		expectedDeviceStates []Pair
		tpuGen               string
		pciSlotToDeviceIds   map[string][]string
	}{
		{
			name:          "Test marking as unhealthy",
			createDevices: []string{"accel0", "accel1"},
			initialDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
			},
			createDeleteOps: []Pair{
				{"accel0", "delete"},
			},
			expectedDeviceStates: []Pair{
				{"accel0", pluginapi.Unhealthy},
			},
		},
		{
			name:          "Test marking multiple as unhealthy",
			createDevices: []string{"accel0", "accel1"},
			initialDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
			},
			createDeleteOps: []Pair{
				{"accel0", "delete"},
				{"accel1", "delete"},
			},
			expectedDeviceStates: []Pair{
				{"accel0", pluginapi.Unhealthy},
				{"accel1", pluginapi.Unhealthy},
			},
		},
		{
			name:          "Test marking healthy",
			createDevices: []string{"accel1"},
			initialDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Unhealthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
			},
			createDeleteOps: []Pair{
				{"accel0", "create"},
			},
			expectedDeviceStates: []Pair{
				{"accel0", pluginapi.Healthy},
			},
		},
		{
			name:          "Test marking device healthy - TPU7x",
			createDevices: []string{"0", "2", "3", "4", "5", "6", "7"},
			initialDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Unhealthy,
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
				},
			},
			tpuGen: "tpu7x",
			pciSlotToDeviceIds: map[string][]string{
				"0000:00:01": {"0", "1"},
				"0000:00:02": {"2", "3"},
				"0000:00:03": {"4", "5"},
				"0000:00:04": {"6", "7"},
			},
			createDeleteOps: []Pair{
				{"1", "create"},
			},
			expectedDeviceStates: []Pair{
				{"0000:00:01", pluginapi.Healthy},
			},
		},
		{
			name:          "Test both physical devices are healthy - TPU7x",
			createDevices: []string{"2", "3", "4", "5", "6", "7"},
			initialDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Unhealthy,
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
				},
			},
			tpuGen: "tpu7x",
			pciSlotToDeviceIds: map[string][]string{
				"0000:00:01": {"0", "1"},
				"0000:00:02": {"2", "3"},
				"0000:00:03": {"4", "5"},
				"0000:00:04": {"6", "7"},
			},
			createDeleteOps: []Pair{
				{"1", "create"},
			},
			expectedDeviceStates: []Pair{},
		},
		{
			name:          "Test already healthy doesn't send signal",
			createDevices: []string{"accel0", "accel1"},
			initialDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
			},
			createDeleteOps:      []Pair{},
			expectedDeviceStates: []Pair{},
		},
		{
			name:          "Test already unhealthy doesn't send signal",
			createDevices: []string{"accel1"},
			initialDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Unhealthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
			},
			createDeleteOps:      []Pair{},
			expectedDeviceStates: []Pair{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.createDeleteOps) == 0 {
				tc.createDeleteOps = []Pair{{"dummy", "no-op"}}
			}

			testDevDir, _ := os.MkdirTemp("", "dev")
			defer os.RemoveAll(testDevDir)

			// Create TPU device nodes
			for _, device := range tc.createDevices {
				s := path.Join(testDevDir, device)
				_, err := os.Create(s)
				if err != nil {
					t.Errorf("Failed to create TPU device node %v: %v", s, err)
				}
			}
			// health = make(chan v1beta1.Device, 1)
			health := make(chan *pluginapi.Device, 1)

			testTPUHealthChecker := NewTPUHealthChecker(tc.initialDevices, health, testDevDir, tc.tpuGen, tc.pciSlotToDeviceIds)
			// healthcheck.tpuCheckInterval = 1 * time.Second
			err := testTPUHealthChecker.Start()

			if err != nil {
				t.Errorf("Failed start tpu health checker: %v", err)
			}

			for i := range tc.createDeleteOps {
				ID := tc.createDeleteOps[i].elem1
				op := tc.createDeleteOps[i].elem2
				s := path.Join(testDevDir, ID)
				switch op {
				case "delete":
					err = os.Remove(s)
					if err != nil {
						t.Errorf("Failed to delete TPU device %v: %v", s, err)
					}
				case "create":
					_, err = os.Create(s)
					if err != nil {
						t.Errorf("Failed to create TPU device %v: %v", s, err)
					}
				}
				// Wait for 11 second for the monitorDevDir to have run
				time.Sleep(11 * time.Second)

				if len(tc.expectedDeviceStates) == 0 {
					if len(testTPUHealthChecker.health) != 0 {
						testTPUHealthChecker.Stop()
						t.Errorf("Expected empty channel signal, received %v", <-testTPUHealthChecker.health)
					}
				} else {
					if len(testTPUHealthChecker.health) == 0 {
						testTPUHealthChecker.Stop()
						t.Errorf("Expected a non-empty channel signal")
					}
					gotDevice := <-testTPUHealthChecker.health
					gotDeviceState := Pair{gotDevice.GetID(), gotDevice.GetHealth()}
					if tc.expectedDeviceStates[i] != gotDeviceState {
						testTPUHealthChecker.Stop()
						t.Errorf("unexpected device in chan (-expected, +got) = %v, %v", tc.expectedDeviceStates[i], gotDeviceState)
					}
				}
			}
			testTPUHealthChecker.Stop()
		})
	}
}
