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
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"tpu-device-plugin/pkg/tpu/util"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type pluginServiceV1Beta1 struct {
	pluginapi.UnimplementedDevicePluginServer
	tm *tpuManager
}

func (s *pluginServiceV1Beta1) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (s *pluginServiceV1Beta1) ListAndWatch(emtpy *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	glog.Infoln("device-plugin: ListAndWatch start")
	if err := s.sendDevices(stream); err != nil {
		return err
	}
	for d := range s.tm.Health {
		glog.Infof("device-plugin: %s device marked as %s", d.GetID(), d.GetHealth())
		s.tm.SetDeviceHealth(d.GetID(), d.GetHealth(), d.GetTopology())
		if err := s.sendDevices(stream); err != nil {
			return err
		}
	}

	return nil
}

func (s *pluginServiceV1Beta1) Allocate(ctx context.Context, requests *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resps := new(pluginapi.AllocateResponse)
	var ce util.ContainerInfoExtractor

	pods := s.tm.podInformer.GetStore().List()
	tpuContainers := ce.GetTPUContainerInfo(ctx, util.GetPodsFromInformer(pods), util.IsContainerStatusEmpty)

	// For multi-container TPU generations (tpu7x), if the informer cache is empty,
	// it's possible that the informer hasn't synced yet. In this case, we directly list pods
	// from the API server to ensure we have the necessary pod information for allocation.
	if len(tpuContainers) == 0 && util.SupportsMultiContainer(s.tm.TpuGen) {
		pods, err := s.tm.KubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", s.tm.nodeName),
		})
		if err != nil {
			glog.Errorf("error retrieving pods: %w", err)
			return nil, err
		}

		podPointers := make([]*v1.Pod, len(pods.Items))
		for i := range pods.Items {
			podPointers[i] = &pods.Items[i]
		}
		tpuContainers = ce.GetTPUContainerInfo(ctx, podPointers, util.IsContainerStatusEmpty)
	}

	for _, rqt := range requests.ContainerRequests {
		if err := s.tm.ValidateTpuRequest(rqt.DevicesIds); err != nil {
			return nil, err
		}
		resp := new(pluginapi.ContainerAllocateResponse)
		// Add all requested devices to Allocate Response
		for _, id := range rqt.DevicesIds {
			devices, err := s.tm.DeviceSpec(id)
			if err != nil {
				return nil, err
			}

			for i := range devices {
				resp.Devices = append(resp.Devices, devices[i])
			}
		}
		defaultDevice := s.tm.DefaultDeviceSpec()
		if defaultDevice != nil {
			resp.Devices = append(resp.Devices, defaultDevice)
		}

		// remove all files in "/tmp/tpu_logs"
		if err := util.RemoveDirContents(s.tm.tpuLogDir); err != nil {
			err = fmt.Errorf("containerAllocate: Error deleting files in %s: %w", s.tm.tpuLogDir, err)
			glog.Error(err)
			return nil, err
		}

		// Change file permission of directory
		if err := os.Chmod(s.tm.tpuLogDir, 0777); err != nil {
			err = fmt.Errorf("error changing permissions on dir %s: %w", s.tm.tpuLogDir, err)
			glog.Error(err)
		}

		// Change file permission of vbar socket file to r+w for User, Group, and Others
		if err := os.Chmod(s.tm.vbarSocket, 0666); err != nil {
			err = fmt.Errorf("error changing permissions on vbar socket file %s: %w", s.tm.vbarSocket, err)
			glog.Error(err)
		}

		for i := range s.tm.mountPaths {
			resp.Mounts = append(resp.Mounts, &s.tm.mountPaths[i])
		}

		requestedChipCount := len(rqt.DevicesIds)

		// used for TPU_VISIBLE_CHIPS env within privileged containers
		var visibleChipIds []string
		if s.tm.enableDeviceSpreading {
			for _, deviceId := range rqt.DevicesIds {
				chipId, ok := s.tm.PciSlotToChipNum[deviceId]
				if !ok {
					return nil, fmt.Errorf("invalid PCI slot %s", deviceId)
				}
				visibleChipIds = append(visibleChipIds, chipId)
			}
		}

		// used to set WORKLOAD_NIC_PREFERRED_NUMA
		numaNodeIds, err := s.tm.GetUniqueNumaNodesForDevices(rqt.DevicesIds)
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		// Get subslice topology based on the devices requested. If multiple containers request
		// the same number of devices, select the first one found.
		var subSliceTopology string
		var isPrivileged bool
		numDevicesInRequest := len(rqt.DevicesIds)
		for _, tc := range tpuContainers {
			if numDevicesInRequest == int(tc.RequestedTPU) {
				subSliceTopology = tc.SubSliceTopology
				isPrivileged = tc.IsPrivileged
				break
			}
		}

		computedEnvs, err := s.tm.Envs(subSliceTopology, requestedChipCount, isPrivileged, visibleChipIds, s.tm.enableDeviceSpreading, numaNodeIds)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		resp.Envs = computedEnvs
		resps.ContainerResponses = append(resps.ContainerResponses, resp)
	}
	return resps, nil
}

func (s *pluginServiceV1Beta1) PreStartContainer(ctx context.Context, r *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	glog.Errorf("device-plugin: PreStart should NOT be called for GKE TPU device plugin\n")
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (s *pluginServiceV1Beta1) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	glog.Errorf("device-plugin: GetPreferredAllocation should NOT be called for GKE TPU device plugin\n")
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (s *pluginServiceV1Beta1) RegisterService() {
	pluginapi.RegisterDevicePluginServer(s.tm.grpcServer, s)
}

// TODO: remove this function once we move to probe based registration.
func RegisterWithV1Beta1Kubelet(kubeletEndpoint, pluginEndpoint, resourceName string) error {
	conn, err := grpc.Dial(kubeletEndpoint, grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	if err != nil {
		return fmt.Errorf("device-plugin: cannot connect to kubelet service: %v", err)
	}
	defer conn.Close()
	client := pluginapi.NewRegistrationClient(conn)

	request := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     pluginEndpoint,
		ResourceName: resourceName,
	}

	if _, err = client.Register(context.Background(), request); err != nil {
		return fmt.Errorf("device-plugin: cannot register to kubelet service: %v", err)
	}
	return nil
}

func (s *pluginServiceV1Beta1) sendDevices(stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	resp := new(pluginapi.ListAndWatchResponse)

	for _, dev := range s.tm.ListDevices() {
		resp.Devices = append(resp.Devices, &pluginapi.Device{ID: dev.GetID(), Health: dev.GetHealth(), Topology: dev.GetTopology()})
	}
	glog.Infof("ListAndWatch: send devices %v\n", resp)
	if err := stream.Send(resp); err != nil {
		glog.Errorf("device-plugin: cannot update device states: %v\n", err)
		s.tm.grpcServer.Stop()
		return err
	}
	return nil
}
