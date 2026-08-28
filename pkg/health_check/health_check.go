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
	"fmt"
	"os"
	"path/filepath"
	"time"
	"tpu-device-plugin/pkg/tpu/util"

	"github.com/golang/glog"
	"google.golang.org/protobuf/proto"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	tpuCheckInterval = 10 * time.Second
)

// TPUHealthChecker checks the health of TPUs. Currently the health checker is limit
// to just checking for the existence of device in the `dev` directory
type TPUHealthChecker struct {
	devices            map[string]*pluginapi.Device
	devDirectory       string
	health             chan *pluginapi.Device
	stop               chan bool
	tpuGen             string
	pciSlotToDeviceIds map[string][]string
}

// NewTPUHealthChecker returns a TPUHealthChecker object for a given device name
func NewTPUHealthChecker(devices map[string]*pluginapi.Device, health chan *pluginapi.Device, devDirectory string, tpuGen string, pciSlotToDeviceIds map[string][]string) *TPUHealthChecker {
	hc := &TPUHealthChecker{
		devices:            make(map[string]*pluginapi.Device),
		health:             health,
		devDirectory:       devDirectory,
		stop:               make(chan bool),
		tpuGen:             tpuGen,
		pciSlotToDeviceIds: pciSlotToDeviceIds,
	}

	// Cloning the device map to avoid interfering with the device manager
	for id, d := range devices {
		hc.devices[id] = proto.CloneOf(d)
	}
	return hc
}

// Start creates a goRoutine that monitors the devDir for changes
func (hc *TPUHealthChecker) Start() error {
	glog.Info("Starting TPU Health Checker")

	for name, device := range hc.devices {
		glog.Infof("Healthchecker receives device %s, device %v+", name, device)
	}

	go func() {
		if err := hc.monitorDevDir(); err != nil {
			glog.Errorf("TPUHealthChecker monitorDevDir error: %v", err)
		}
	}()

	return nil
}

func (hc *TPUHealthChecker) deviceExists(deviceId string) (bool, error) {
	var deviceIDs []string
	if util.SupportsTensorNode(hc.tpuGen) {
		ids, ok := hc.pciSlotToDeviceIds[deviceId]
		if !ok {
			return false, fmt.Errorf("invalid PCI slot %s", deviceId)
		}
		deviceIDs = ids
	} else {
		// For older generations, the id is the single physical device ID.
		deviceIDs = []string{deviceId}
	}

	for _, id := range deviceIDs {
		if _, err := os.Stat(filepath.Join(hc.devDirectory, id)); err == nil {
			continue
		} else if os.IsNotExist(err) {
			glog.Errorf("device %s does not exist", id)
			return false, nil
		} else {
			glog.Errorf("deviceExists check unexpected error: %v", err)
			return false, nil
		}
	}

	return true, nil
}

// monitorDevDir monitors the dev/ directory to see if all the TPU devices are present.
func (hc *TPUHealthChecker) monitorDevDir() error {
	ticker := time.NewTicker(tpuCheckInterval)
	for {
		select {
		case <-hc.stop:
			close(hc.stop)
			ticker.Stop()
			return nil
		case <-ticker.C:
			for id, d := range hc.devices {
				val, _ := hc.deviceExists(id)
				if val && d.GetHealth() == pluginapi.Unhealthy {
					updated := proto.CloneOf(d)
					updated.Health = pluginapi.Healthy
					hc.devices[id] = updated
					hc.health <- proto.CloneOf(updated)
				} else if !val && d.GetHealth() == pluginapi.Healthy {
					updated := proto.CloneOf(d)
					updated.Health = pluginapi.Unhealthy
					hc.devices[id] = updated
					hc.health <- proto.CloneOf(updated)
				}
			}
		}
	}
}

// Stop the listening go routine
func (hc *TPUHealthChecker) Stop() {
	hc.stop <- true
	<-hc.stop
}
