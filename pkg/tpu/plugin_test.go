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

package tpu

import (
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	SubSliceTopologyAnnotation = "cloud.google.com/gke-tpu-slice-topology"
)

type KubeletStub struct {
	pluginapi.UnimplementedRegistrationServer
	sync.Mutex
	socket         string
	pluginEndpoint string
	server         *grpc.Server
}

type PCIDevice struct {
	slotID      string
	functionNum string
	iommuGroup  string
	numaNode    string
	vendorId    string
	deviceId    string
}

// NewKubeletStub returns an initialized KubeletStub for testing purpose.
func NewKubeletStub(socket string) *KubeletStub {
	return &KubeletStub{
		socket: socket,
	}
}

func (k *KubeletStub) Register(ctx context.Context, r *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
	k.Lock()
	defer k.Unlock()
	k.pluginEndpoint = r.Endpoint
	return &pluginapi.Empty{}, nil
}

func (k *KubeletStub) Start() error {
	os.Remove(k.socket)
	s, err := net.Listen("unix", k.socket)
	if err != nil {
		fmt.Printf("Can't listen at the socket: %+v", err)
		return err
	}

	k.server = grpc.NewServer([]grpc.ServerOption{}...)

	pluginapi.RegisterRegistrationServer(k.server, k)
	go func() {
		err := k.server.Serve(s)
		if err != nil {
			fmt.Printf("Can't start server: %+v", err)
		}
	}()
	return nil
}

func TestTPUManagerBetaAPI(t *testing.T) {
	os.Setenv("NODE_IP", "localhost")
	cases := []struct {
		name                   string
		nodeLabels             map[string]string
		wantUserExposedDevices map[string]*pluginapi.Device
		wantAllocatedDevices   []string
		validRequests          []*pluginapi.ContainerAllocateRequest
		invalidRequests        []*pluginapi.ContainerAllocateRequest
		wantErr                bool
		subSliceTopology       string
		createPCIDevices       []*PCIDevice
		enableDeviceSpreading  bool
		disableInformer        bool
		disableVbarUds         bool // inverted naming for testing as vbar UDS should be default
	}{
		{
			name: "TPU manager v3",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v3-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
				"accel2": {
					ID:     "accel2",
					Health: pluginapi.Healthy,
				},
				"accel3": {
					ID:     "accel3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"accel0", "accel1", "accel2", "accel3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2", "accel3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2"}},
			},
		},
		{
			name: "TPU manager v3 w/ UDS disabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v3-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			disableVbarUds: true,
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
				"accel2": {
					ID:     "accel2",
					Health: pluginapi.Healthy,
				},
				"accel3": {
					ID:     "accel3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"accel0", "accel1", "accel2", "accel3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2", "accel3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2"}},
			},
		},
		{
			name: "TPU manager v4",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v4-podslice",
				"cloud.google.com/gke-tpu-topology":      "2x2x2",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"accel0": {
					ID:     "accel0",
					Health: pluginapi.Healthy,
				},
				"accel1": {
					ID:     "accel1",
					Health: pluginapi.Healthy,
				},
				"accel2": {
					ID:     "accel2",
					Health: pluginapi.Healthy,
				},
				"accel3": {
					ID:     "accel3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"accel0", "accel1", "accel2", "accel3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2", "accel3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"accel0", "accel1", "accel2"}},
			},
		},
		{
			name: "TPU manager v5",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v5-lite-podslice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2"}},
			},
		},
		{
			name: "TPU manager v5p",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v5p-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2"}},
			},
		},
		{
			name: "TPU manager v5p w/ UDS disabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v5p-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			disableVbarUds: true,
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2"}},
			},
		},
		{
			name: "TPU manager v6e",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v6e-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2"}},
			},
		},
		{
			name: "TPU manager TPU7x - device spreading disabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu7x",
				"cloud.google.com/gke-tpu-topology":      "4x4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			disableVbarUds: true,
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: false,
			wantAllocatedDevices:  []string{"0", "1", "2", "3", "4", "5", "6", "7"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01", "0000:00:02", "0000:00:03", "0000:00:04"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
		},
		{
			name: "TPU manager TPU7x - device spreading disabled - provision only",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":           "tpu7x",
				"cloud.google.com/gke-accelerator-count":         "4",
				"cloud.google.com/gke-accelerator-topology-mode": "PROVISION_ONLY",
			},
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: false,
			wantAllocatedDevices:  []string{"0", "1", "2", "3", "4", "5", "6", "7"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01", "0000:00:02", "0000:00:03", "0000:00:04"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
		},
		{
			name: "TPU manager TPU7x - device spreading enabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu7x",
				"cloud.google.com/gke-tpu-topology":      "4x4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: true,
			wantAllocatedDevices:  []string{"0", "1"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{}},
				{DevicesIds: []string{"0000:00:01", "0000:00:03"}}, // not NUMA-aligned
			},
		},
		{
			name: "TPU manager TPU7x - device spreading enabled - provision only",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":           "tpu7x",
				"cloud.google.com/gke-accelerator-count":         "4",
				"cloud.google.com/gke-accelerator-topology-mode": "PROVISION_ONLY",
			},
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: true,
			wantAllocatedDevices:  []string{"0", "1"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{}},
				{DevicesIds: []string{"0000:00:01", "0000:00:03"}}, // not NUMA-aligned
			},
		},
		{
			name: "TPU manager TPU7x - device spreading enabled - informer disabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu7x",
				"cloud.google.com/gke-tpu-topology":      "4x4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: true,
			wantAllocatedDevices:  []string{"0", "1"},
			disableInformer:       true,
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{}},
				{DevicesIds: []string{"0000:00:01", "0000:00:03"}}, // not NUMA-aligned
			},
		},
		{
			name: "TPU manager TPU7x - device spreading enabled - informer disabled - provision only",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":           "tpu7x",
				"cloud.google.com/gke-accelerator-count":         "4",
				"cloud.google.com/gke-accelerator-topology-mode": "PROVISION_ONLY",
			},
			createPCIDevices: []*PCIDevice{
				{
					slotID:      "0000:00:01",
					functionNum: "0",
					iommuGroup:  "0",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:01",
					functionNum: "1",
					iommuGroup:  "1",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "0",
					iommuGroup:  "2",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:02",
					functionNum: "1",
					iommuGroup:  "3",
					numaNode:    "0",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "0",
					iommuGroup:  "4",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:03",
					functionNum: "1",
					iommuGroup:  "5",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "0",
					iommuGroup:  "6",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
				{
					slotID:      "0000:00:04",
					functionNum: "1",
					iommuGroup:  "7",
					numaNode:    "1",
					vendorId:    "0x1ae0",
					deviceId:    "0x0076",
				},
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0000:00:01": {
					ID:     "0000:00:01",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:02": {
					ID:     "0000:00:02",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 0,
							},
						},
					},
				},
				"0000:00:03": {
					ID:     "0000:00:03",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
				"0000:00:04": {
					ID:     "0000:00:04",
					Health: pluginapi.Healthy,
					Topology: &pluginapi.TopologyInfo{
						Nodes: []*pluginapi.NUMANode{
							{
								ID: 1,
							},
						},
					},
				},
			},
			enableDeviceSpreading: true,
			wantAllocatedDevices:  []string{"0", "1"},
			disableInformer:       true,
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0000:00:01"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{}},
				{DevicesIds: []string{"0000:00:01", "0000:00:03"}}, // not NUMA-aligned
			},
		},
		{
			name: "SubSlice TPU manager v6e",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v6e-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1"}},
			},
			subSliceTopology: "2x4",
		},
		{
			name: "SubSlice TPU manager v6e w/ vbar uds disabled",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v6e-slice",
				"cloud.google.com/gke-tpu-topology":      "4x4",
				"cloud.google.com/gke-accelerator-count": "4",
			},
			disableVbarUds: true,
			wantUserExposedDevices: map[string]*pluginapi.Device{
				"0": {
					ID:     "0",
					Health: pluginapi.Healthy,
				},
				"1": {
					ID:     "1",
					Health: pluginapi.Healthy,
				},
				"2": {
					ID:     "2",
					Health: pluginapi.Healthy,
				},
				"3": {
					ID:     "3",
					Health: pluginapi.Healthy,
				},
			},
			wantAllocatedDevices: []string{"0", "1", "2", "3"},
			validRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1", "2", "3"}},
			},
			invalidRequests: []*pluginapi.ContainerAllocateRequest{
				{DevicesIds: []string{"0", "1"}},
			},
			subSliceTopology: "2x4",
		},
		{
			name: "TPU manager with invalid node labels",
			nodeLabels: map[string]string{
				"cloud.google.com/gke-tpu-accelerator":   "tpu-v4-podslice",
				"cloud.google.com/gke-tpu-topology":      "2x2x2",
				"cloud.google.com/gke-accelerator-count": "6",
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		testDevDir, _ := os.MkdirTemp("", "dev")
		defer os.RemoveAll(testDevDir)

		var wantAllocateResponseDevices []*pluginapi.DeviceSpec
		devices := []string{}
		for _, device := range tc.wantAllocatedDevices {
			devices = append(devices, device)
		}
		sort.Strings(devices)
		for _, device := range devices {
			devicePath := path.Join(testDevDir, device)
			_, err := os.Create(devicePath)
			if err != nil {
				t.Errorf("failed to create device path %v: %v", devicePath, err)
			}
			deviceSpec := pluginapi.DeviceSpec{
				ContainerPath: devicePath,
				HostPath:      devicePath,
				Permissions:   "mrw",
			}
			wantAllocateResponseDevices = append(wantAllocateResponseDevices, &deviceSpec)
		}

		if len(tc.createPCIDevices) > 0 {
			for _, device := range tc.createPCIDevices {
				// create the fake sysfs entries for NUMA and IOMMU
				pciAddress := fmt.Sprintf("%s.%s", device.slotID, device.functionNum)

				// create path for the fake PCI device
				fakePciDevicePath := filepath.Join(testDevDir, "sys/bus/pci/devices", pciAddress)
				if err := os.MkdirAll(fakePciDevicePath, 0755); err != nil {
					t.Errorf("failed to create fake PCI device dir: %v", err)
				}

				numaNodeFile := filepath.Join(fakePciDevicePath, "numa_node")
				numaNodeContent := fmt.Sprintf("%s\n", device.numaNode)
				if err := os.WriteFile(numaNodeFile, []byte(numaNodeContent), 0644); err != nil {
					t.Errorf("failed to write fake numa_node file: %v", err)
				}

				vendorFile := filepath.Join(fakePciDevicePath, "vendor")
				vendorContent := fmt.Sprintf("%s\n", device.vendorId)
				if err := os.WriteFile(vendorFile, []byte(vendorContent), 0644); err != nil {
					t.Errorf("failed to write fake vendor file: %v", err)
				}

				deviceFile := filepath.Join(fakePciDevicePath, "device")
				deviceContent := fmt.Sprintf("%s\n", device.deviceId)
				if err := os.WriteFile(deviceFile, []byte(deviceContent), 0644); err != nil {
					t.Errorf("failed to write fake device file: %v", err)
				}

				// create fake `iommu_group` symlink
				iommuGroupPath := filepath.Join("/tmp/sys/kernel/iommu_groups", device.iommuGroup)
				if err := os.MkdirAll(iommuGroupPath, 0755); err != nil {
					t.Errorf("failed to create fake IOMMU group dir: %v", err)
				}
				symlinkPath := filepath.Join(fakePciDevicePath, "iommu_group")
				symlinkTarget := fmt.Sprintf("../../../../kernel/iommu_groups/%s", device.iommuGroup)
				if err := os.Symlink(symlinkTarget, symlinkPath); err != nil {
					t.Errorf("failed to create IOMMU symlink: %v", err)
				}
			}
		}

		if val, found := tc.nodeLabels["cloud.google.com/gke-tpu-accelerator"]; found && (strings.HasPrefix(val, "tpu-v5") || strings.HasPrefix(val, "tpu-v6e") || strings.HasPrefix(val, "tpu7x")) {
			devicePath := path.Join(testDevDir, "vfio")
			_, err := os.Create(devicePath)
			if err != nil {
				t.Errorf("failed to create device path %v: %v", devicePath, err)
			}
			deviceSpec := pluginapi.DeviceSpec{
				ContainerPath: devicePath,
				HostPath:      devicePath,
				Permissions:   "mrw",
			}
			wantAllocateResponseDevices = append(wantAllocateResponseDevices, &deviceSpec)
		}

		t.Run(tc.name, func(t *testing.T) {
			err := testTPUManagerBetaAPI(testDevDir, tc.nodeLabels, tc.wantUserExposedDevices, tc.validRequests, tc.invalidRequests, tc.subSliceTopology, wantAllocateResponseDevices, tc.enableDeviceSpreading, tc.disableInformer, !tc.disableVbarUds)
			if err != nil && !tc.wantErr {
				t.Error("unexpected error: ", err)
			}
		})
	}
}

func testTPUManagerBetaAPI(testDevDir string, nodeLabels map[string]string, wantUserExposedDevices map[string]*pluginapi.Device, validRequests []*pluginapi.ContainerAllocateRequest, invalidRequests []*pluginapi.ContainerAllocateRequest, subSliceTopology string, wantAllocateResponseDevices []*pluginapi.DeviceSpec, enableDeviceSpreading bool, disableInformer bool, enableVbarUds bool) error {
	nodeName := "test-node"

	var podList []*v1.Pod
	podList = []*v1.Pod{
		getPod("tpu-pod", "default", "10.10.10.10", []v1.Container{}, []v1.Container{
			tpuContainer("tpu-c", true, true),
		}, []v1.ContainerStatus{}, []v1.ContainerStatus{},
			map[string]string{}, nodeName),
	}

	if subSliceTopology != "" {
		podList[0].ObjectMeta.Annotations[SubSliceTopologyAnnotation] = subSliceTopology
	}

	objects := make([]runtime.Object, 0)
	for _, pod := range podList {
		objects = append(objects, pod)
	}
	mockKubeClient := fake.NewSimpleClientset(objects...)

	mockCache := &mockInformerCache{[]*v1.Pod{}}
	if !disableInformer {
		mockCache = &mockInformerCache{podList}
	}
	fakePciDevicesDir := filepath.Join(testDevDir, "sys/bus/pci/devices")

	testTpuManager, err := NewTPUManager(nodeLabels, testDevDir, []pluginapi.Mount{}, mockCache, nodeName, mockKubeClient, fakePciDevicesDir, enableDeviceSpreading, enableVbarUds)

	// change vbar socket to be within /tmp folder
	if enableVbarUds {
		testTpuManager.vbarSocket = path.Join("tmp", testTpuManager.vbarSocket)

		directoryPermissions := os.FileMode(0755)
		socketFilePermissions := os.FileMode(0700)
		err = createFileWithIntermediateDirs(testTpuManager.vbarSocket, nil, socketFilePermissions, directoryPermissions)
		if err != nil {
			return fmt.Errorf("error creating vbar socket file %s: %w", testTpuManager.vbarSocket, err)
		}
		defer os.Remove(testTpuManager.vbarSocket)
	}

	if err != nil {
		return fmt.Errorf("error initializing TPU manager: %w", err)
	}
	if testTpuManager == nil {
		return fmt.Errorf("failed to initilize a TPU manager")
	}

	// Start TPU manager.
	if err := testTpuManager.Start(); err != nil {
		return fmt.Errorf("unable to start tpu manager: %w", err)
	}

	// Tests discoverTPUs()
	discoverErr := testTpuManager.discoverTPUs()
	if discoverErr != nil {
		return discoverErr
	}
	tpus := reflect.ValueOf(testTpuManager).Elem().FieldByName("devices").Len()
	if tpus != testTpuManager.tpuChipCount {
		return fmt.Errorf("unable to discover right number of TPU devices expected: %d, received: %d ", testTpuManager.tpuChipCount, tpus)
	}

	testdir, err := os.MkdirTemp("", "tpu_device_plugin")
	if err != nil {
		return fmt.Errorf("error for creating temp dir tpu_device_plugin: %w", err)
	}
	defer os.RemoveAll(testdir)

	kubeletEndpoint := path.Join(testdir, "kubelet.sock")
	kubeletStub := NewKubeletStub(kubeletEndpoint)
	err = kubeletStub.Start()
	if err != nil {
		return fmt.Errorf("error for starting kubelet stub: %w", err)
	}
	defer kubeletStub.server.Stop()

	go func() {
		testTpuManager.Serve(testdir, "kubelet.sock", "plugin.sock")
	}()

	time.Sleep(5 * time.Second)
	devicePluginSock := path.Join(testdir, "plugin.sock")
	defer func() {
		err = testTpuManager.Stop()
		if err != nil {
			fmt.Printf("Error stopping tpu manager: %v", err)
		}
	}()
	// Verifies the grpcServer is ready to serve services.
	conn, err := grpc.Dial(devicePluginSock, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	if err != nil {
		return fmt.Errorf("error for creating grpc connection: %w", err)
	}
	defer conn.Close()

	client := pluginapi.NewDevicePluginClient(conn)

	// Tests ListAndWatch
	stream, err := client.ListAndWatch(context.Background(), &pluginapi.Empty{})
	if err != nil {
		return fmt.Errorf("error for making list and watch action: %w", err)
	}
	devs, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("error for recieving stream: %w", err)
	}
	devices := make(map[string]*pluginapi.Device)
	for _, d := range devs.Devices {
		devices[d.GetID()] = d
	}

	if diff := cmp.Diff(wantUserExposedDevices, devices, protocmp.Transform()); diff != "" {
		return fmt.Errorf("unexpected devices (-want, +got) = %s", diff)
	}

	// Tests Allocate
	err = createTestLogsinTpuLogsDir(testTpuManager.tpuLogDir)
	if err != nil {
		return fmt.Errorf("error on creating test logs in %s: %w", testTpuManager.tpuLogDir, err)
	}

	resp, err := client.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: validRequests})

	if err != nil {
		return fmt.Errorf("error for allocating a valid request: %w", err)
	}

	if subSliceTopology != "" && resp.ContainerResponses[0].Envs["TPU_TOPOLOGY"] != subSliceTopology {
		return fmt.Errorf("unexpected Envs in resp want %s got %s", subSliceTopology, resp.ContainerResponses[0].Envs["TPU_TOPOLOGY"])
	}

	vbarControlServiceURL := ""
	if enableVbarUds {
		vbarControlServiceURL = "unix:///var/run/tpu-plugin/vbar.sock"
	} else {
		vbarControlServiceURL = "localhost:8353"

	}
	if resp.ContainerResponses[0].Envs["VBAR_CONTROL_SERVICE_URL"] != vbarControlServiceURL {
		return fmt.Errorf("unexpected Envs in resp want %s got %s", vbarControlServiceURL, resp.ContainerResponses[0].Envs["VBAR_CONTROL_SERVICE_URL"])
	}

	if diff := cmp.Diff(wantAllocateResponseDevices, resp.ContainerResponses[0].Devices, protocmp.Transform()); diff != "" {
		return fmt.Errorf("unexpected devices in resp (-want, +got) = %s", diff)
	}

	expectedMounts := []*pluginapi.Mount{
		{
			ContainerPath: testTpuManager.tpuLogDir,
			HostPath:      testTpuManager.tpuLogDir,
			ReadOnly:      false,
		},
	}
	if enableVbarUds {
		// test changes vbar socket file to be under /tmp/, however, that is after we add
		// the vbar mount within tpu manager creation.
		vbarSocketMountPath := filepath.Dir(testTpuManager.vbarSocket)
		// Validate non-"/tmp/" file path is mounted.
		originalVbarSocketMountPath := strings.TrimPrefix(vbarSocketMountPath, "tmp")
		expectedMounts = append([]*pluginapi.Mount{
			{
				ContainerPath: originalVbarSocketMountPath,
				HostPath:      originalVbarSocketMountPath,
				ReadOnly:      false,
			},
		}, expectedMounts...)
	}
	if diff := cmp.Diff(expectedMounts, resp.ContainerResponses[0].Mounts, protocmp.Transform()); diff != "" {
		return fmt.Errorf("unexpected mounts in resp (-want, +got) = %s", diff)
	}

	if _, err := os.ReadDir(testTpuManager.tpuLogDir); err != nil {
		return fmt.Errorf("Expected tpuLogDir to be empty after Allocate, but it's not: %s", err)
	}

	if enableVbarUds {
		vbarPerm, _ := getFilePermissions(testTpuManager.vbarSocket)
		if diff := cmp.Diff(os.FileMode(0666), vbarPerm); diff != "" {
			return fmt.Errorf("Expected vbar socket file to have r+w permissions after Allocate, (-want, +got) = %s", diff)
		}
	}

	// Tests Allocate without having files in tpuLogsDir
	os.RemoveAll(testTpuManager.tpuLogDir)
	defer os.RemoveAll(testTpuManager.tpuLogDir)
	err = os.Mkdir(testTpuManager.tpuLogDir, 0755)
	if err != nil {
		return fmt.Errorf("Error creating log dir %v: %v", testTpuManager.tpuLogDir, err)
	}
	_, err = client.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: validRequests})
	if err != nil {
		return fmt.Errorf("error for allocating a valid request: %w", err)
	}
	if _, err := os.ReadDir(testTpuManager.tpuLogDir); err != nil {
		return fmt.Errorf("Expected tpuLogDir to be empty after Allocate, but it's not: %s", err)
	}
	fileInfo, _ := os.Stat(testTpuManager.tpuLogDir)
	perm := fileInfo.Mode().Perm().String()
	if perm != "-rwxrwxrwx" {
		return fmt.Errorf("UnExpected file permissionms in tpuLogDir: %s (expected: -rwxrwxr-x)", perm)
	}

	resp, err = client.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: invalidRequests})
	if resp != nil {
		return fmt.Errorf("non-nil resp when allocating an invalid request: %v", resp)
	}
	if err == nil {
		return fmt.Errorf("nil err when allocating an invalid request")
	}

	return nil
}

func createTestLogsinTpuLogsDir(tpuLogDir string) error {
	testFiles := []string{"file1.log", "file2.log"}
	for _, filename := range testFiles {
		if err := createFileWithIntermediateDirs(fmt.Sprintf("%s/%s", tpuLogDir, filename), nil, os.FileMode(0644), os.FileMode(0755)); err != nil {
			return fmt.Errorf("Error creating test file: %v", err)
		}
	}
	return nil
}

func createFileWithIntermediateDirs(filePath string, fileData []byte, filePerm os.FileMode, dirPerm os.FileMode) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create parent directories '%s': %w", dir, err)
	}

	if err := os.WriteFile(filePath, fileData, filePerm); err != nil {
		return fmt.Errorf("failed to write file '%s': %w", filePath, err)
	}

	return nil
}

// Helper function to get permissions bits (e.g., 0644, 0700)
func getFilePermissions(filePath string) (os.FileMode, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	return fileInfo.Mode().Perm(), nil
}

// Mock podInformer
type mockInformerCache struct {
	podList []*v1.Pod
}

func (pi *mockInformerCache) GetStore() cache.Store {
	return &mockStore{pi.podList}
}
func (pi *mockInformerCache) AddIndexers(indexers cache.Indexers) error { return nil }
func (pi *mockInformerCache) GetIndexer() cache.Indexer                 { return nil }
func (pi *mockInformerCache) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}
func (pi *mockInformerCache) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}
func (pi *mockInformerCache) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}
func (pi *mockInformerCache) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}
func (pi *mockInformerCache) GetController() cache.Controller                            { return nil }
func (pi *mockInformerCache) Run(stopCh <-chan struct{})                                 {}
func (pi *mockInformerCache) RunWithContext(ctx context.Context)                         {}
func (pi *mockInformerCache) HasSynced() bool                                            { return true }
func (pi *mockInformerCache) HasSyncedChecker() cache.DoneChecker                        { return nil }
func (pi *mockInformerCache) LastSyncResourceVersion() string                            { return "" }
func (pi *mockInformerCache) SetWatchErrorHandler(handler cache.WatchErrorHandler) error { return nil }
func (pi *mockInformerCache) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	return nil
}
func (pi *mockInformerCache) SetTransform(handler cache.TransformFunc) error { return nil }
func (pi *mockInformerCache) IsStopped() bool                                { return false }

// Mock local store
type mockStore struct {
	podList []*v1.Pod
}

func (s *mockStore) List() []interface{} {
	result := make([]interface{}, len(s.podList))
	for i, pod := range s.podList {
		result[i] = pod
	}
	return result
}
func (s *mockStore) Add(obj interface{}) error            { return nil }
func (s *mockStore) Update(obj interface{}) error         { return nil }
func (s *mockStore) Delete(obj interface{}) error         { return nil }
func (s *mockStore) ListKeys() []string                   { return []string{} }
func (s *mockStore) LastStoreSyncResourceVersion() string { return "" }
func (s *mockStore) Bookmark(rv string)                   {}
func (s *mockStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	return nil, true, nil
}
func (s *mockStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	return nil, true, nil
}
func (s *mockStore) Replace([]interface{}, string) error { return nil }
func (s *mockStore) Resync() error                       { return nil }
func getPod(name, namespace, podIP string, initC, c []v1.Container, initCS []v1.ContainerStatus, cs []v1.ContainerStatus, annotations map[string]string, nodeName string) *v1.Pod {
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
			NodeName:       nodeName,
			Containers:     c,
			InitContainers: initC,
		},
	}
}

func tpuContainer(name string, limits, requests bool) v1.Container {
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
	return container
}
