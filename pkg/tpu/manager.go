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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"tpu-device-plugin/pkg/tpu/util"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	tpuV4DeviceRegex          = `^accel[0-9]*$`
	tpuDeviceNumericalRegex   = `^(\d+)$`
	pluginSocketCheckInterval = 1 * time.Second
	resourceName              = "google.com/tpu"
	defaultDeviceID           = "vfio"
	libtpuLogDir              = "/tmp/tpu_logs"
	pciSlotIDRegexp           = `^(.+)\.\d+$`
	googlePCIVendorID         = "0x1ae0"
	defaultVbarSocketPath     = "/var/run/tpu-plugin/vbar.sock"
)

// tpuDeviceInfo holds validated information for a single TPU VFIO device.
type tpuDeviceInfo struct {
	pciAddr    string
	devicePath string
	iommuGroup string
	pciSlotId  string
}

// tpuManager manages google tpu devices.
type tpuManager struct {
	DevDirectory          string
	TpuGen                string
	mountPaths            []pluginapi.Mount
	devices               map[string]*pluginapi.Device
	grpcServer            *grpc.Server
	socket                string
	stop                  chan bool
	devicesMutex          sync.Mutex
	Health                chan *pluginapi.Device
	tpuChipCount          int
	tpuAcceleratorCount   int
	nodeLabels            map[string]string
	tpuLogDir             string
	podInformer           cache.SharedIndexInformer
	pciDevicesDir         string
	KubeClient            kubernetes.Interface
	PciSlotToDeviceIds    map[string][]string // PCI Slot ID to the device IDs that make up that physical device (TPU7x+)
	PciSlotToNumaNode     map[string]int64
	PciSlotToChipNum      map[string]string
	nodeName              string
	enableDeviceSpreading bool
	vbarSocket            string
}

func NewTPUManager(nodeLabels map[string]string, devDirectory string, mountPaths []pluginapi.Mount, podInformer cache.SharedIndexInformer, nodeName string, kubeClient kubernetes.Interface, pciDevicesDir string, enableDeviceSpreading bool, enableVbarUds bool) (*tpuManager, error) {
	chipCountNodeLabelValue := nodeLabels[util.AcceleratorCountLabel]

	// Get TPU generation (v4, v5, etc.) and chips per node count from node labels.
	tpuGen, err := util.AcceleratorGen(nodeLabels[util.AcceleratorLabel])
	if err != nil {
		return nil, err
	}

	chipCount, err := util.ChipCount(chipCountNodeLabelValue)
	// To support TensorNode, accelerator count is 2 * the "chip count"
	acceleratorCount := chipCount
	if util.SupportsTensorNode(tpuGen) {
		acceleratorCount = acceleratorCount * 2
	}
	if err != nil {
		return nil, err
	}

	var vbarSocketPath string
	if enableVbarUds {
		vbarSocketPath = defaultVbarSocketPath
		vbarSocketMountPath := filepath.Dir(vbarSocketPath)
		mountPaths = append(mountPaths, pluginapi.Mount{
			ContainerPath: vbarSocketMountPath,
			HostPath:      vbarSocketMountPath,
			ReadOnly:      false,
		})
	}

	return &tpuManager{
		TpuGen:                tpuGen,
		DevDirectory:          devDirectory,
		mountPaths:            mountPaths,
		devices:               make(map[string]*pluginapi.Device),
		stop:                  make(chan bool),
		Health:                make(chan *pluginapi.Device),
		tpuChipCount:          chipCount,
		tpuAcceleratorCount:   acceleratorCount,
		nodeLabels:            nodeLabels,
		KubeClient:            kubeClient,
		tpuLogDir:             libtpuLogDir,
		podInformer:           podInformer,
		pciDevicesDir:         pciDevicesDir,
		nodeName:              nodeName,
		enableDeviceSpreading: enableDeviceSpreading,
		vbarSocket:            vbarSocketPath,
	}, nil
}

// ListDevices lists all physical TPU devices available on this node.
func (tm *tpuManager) ListDevices() map[string]*pluginapi.Device {
	return tm.devices
}

// DeviceSpec returns the device spec that inclues list of devices to allocate for a deviceID.
func (tm *tpuManager) DeviceSpec(deviceID string) ([]*pluginapi.DeviceSpec, error) {
	dev, ok := tm.devices[deviceID]
	if !ok {
		return nil, fmt.Errorf("invalid allocation request with non-existing device %s", deviceID)
	}

	if dev.GetHealth() != pluginapi.Healthy {
		return nil, fmt.Errorf("invalid allocation request with unhealthy device %s", deviceID)
	}

	var deviceIDs []string
	if util.SupportsTensorNode(tm.TpuGen) {
		ids, ok := tm.PciSlotToDeviceIds[deviceID]
		if !ok {
			return nil, fmt.Errorf("invalid allocation request with non-existing PCI slot %s", deviceID)
		}
		deviceIDs = ids
	} else {
		// For older generations, the ID is the single physical device ID.
		deviceIDs = []string{deviceID}
	}

	deviceSpecs := make([]*pluginapi.DeviceSpec, 0, len(deviceIDs))
	for _, id := range deviceIDs {
		devicePath := path.Join(tm.DevDirectory, id)
		spec := &pluginapi.DeviceSpec{
			HostPath:      devicePath,
			ContainerPath: devicePath,
			Permissions:   "mrw",
		}
		deviceSpecs = append(deviceSpecs, spec)
	}

	return deviceSpecs, nil
}

// Currently v5, v6e, tpu7x devices have this extra device that needs to be included.
func (tm *tpuManager) DefaultDeviceSpec() *pluginapi.DeviceSpec {
	if strings.HasPrefix(tm.TpuGen, "v5") ||
		strings.HasPrefix(tm.TpuGen, "v6e") ||
		strings.HasPrefix(tm.TpuGen, "tpu7x") {
		return &pluginapi.DeviceSpec{
			HostPath:      path.Join(tm.DevDirectory, defaultDeviceID),
			ContainerPath: path.Join(tm.DevDirectory, defaultDeviceID),
			Permissions:   "mrw",
		}
	}
	return nil
}

// Discovers all TPU devices available on the local node by walking tpuManager's devDirectory.
// If a family that supports TensorNode, map TPU devices to their PCI Slot ID.
func (tm *tpuManager) discoverTPUs() error {
	var err error
	if util.SupportsTensorNode(tm.TpuGen) {
		devices, err := tm.findAllTPUDevices()
		if err != nil {
			return err
		}
		tm.PciSlotToDeviceIds, err = tm.buildPciSlotIdToDeviceIdsMap(devices)
		if err != nil {
			return err
		}

		tm.PciSlotToNumaNode, err = tm.buildPciSlotIdToNumaNodeMap(devices)
		if err != nil {
			return err
		}

		tm.PciSlotToChipNum, err = tm.buildPciSlotIdToChipNumMap()
		if err != nil {
			return err
		}

		for pciSlot := range tm.PciSlotToDeviceIds {
			glog.V(3).Infof("Found Google TPU %q\n", pciSlot)

			numaNode, ok := tm.PciSlotToNumaNode[pciSlot]
			if !ok {
				return fmt.Errorf("Could not get NUMA Node for PCI Device: %s", pciSlot)
			}

			topologyInfo := &pluginapi.TopologyInfo{
				Nodes: []*pluginapi.NUMANode{
					{
						ID: numaNode,
					},
				},
			}

			tm.SetDeviceHealth(pciSlot, pluginapi.Healthy, topologyInfo)
		}
		return nil
	}

	tpuDeviceRegex := tpuDeviceNumericalRegex
	if util.IsLegacyTPU(tm.TpuGen) {
		tpuDeviceRegex = tpuV4DeviceRegex
	}
	reg := regexp.MustCompile(tpuDeviceRegex)
	files, err := os.ReadDir(tm.DevDirectory)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if reg.MatchString(f.Name()) {
			glog.V(3).Infof("Found Google TPU %q\n", f.Name())
			tm.SetDeviceHealth(f.Name(), pluginapi.Healthy, nil)
		}
	}
	return nil
}

func (tm *tpuManager) Envs(subSliceToplogy string, requestedChipCount int, isPrivileged bool, visibleChipIds []string, enableDeviceSpreading bool, numaNodeIds []string) (map[string]string, error) {
	accelerator := tm.nodeLabels[util.AcceleratorLabel]
	topology := tm.nodeLabels[util.TopologyLabel]
	enableICIResiliency := tm.nodeLabels[util.ICIResiliency]
	enableVbarUds := tm.vbarSocket != ""

	// Initialize environment variables that will be set in containers requesting TPU resources.
	envs, err := util.InitEnvs(util.InitEnvOptions{
		Accelerator:           accelerator,
		Topology:              topology,
		ChipCount:             tm.tpuChipCount,
		AcceleratorCount:      tm.tpuAcceleratorCount,
		RequestedChipCount:    requestedChipCount,
		EnableICIResiliency:   enableICIResiliency,
		SubSliceTopology:      subSliceToplogy,
		IsPrivileged:          isPrivileged,
		VisibleChipIds:        visibleChipIds,
		EnableDeviceSpreading: enableDeviceSpreading,
		NumaNodeIds:           numaNodeIds,
		EnableVbarUds:         enableVbarUds,
	})
	if err != nil {
		return nil, fmt.Errorf("error initializing environment variables: %w", err)
	}
	return envs, nil
}

// Validate the container requesting for TPUs.
func (tm *tpuManager) ValidateTpuRequest(requestDeviceIds []string) error {
	if tm.enableDeviceSpreading {
		numChipsRequested := len(requestDeviceIds)
		if !(0 < numChipsRequested && numChipsRequested <= tm.tpuChipCount) {
			return fmt.Errorf("invalid TPU chip count request: got %d, but must request between 1 and %d chips", numChipsRequested, tm.tpuChipCount)
		}
		if len(requestDeviceIds) == 2 {
			devicesNumaAligned, err := tm.AreDevicesNumaAligned(requestDeviceIds)
			if err != nil {
				return fmt.Errorf("could not determine if devices are NUMA aligned: %w", err)
			}
			if !devicesNumaAligned {
				return fmt.Errorf("invalid 2-chip request: requested devices are not NUMA aligned. 2-chip requests require topology-aware scheduling to be enabled on the node. Please review the Kubelet Topology Manager policies (e.g., 'single-numa-node')")
			}
		}
		return nil
	}
	if len(requestDeviceIds) != tm.tpuChipCount {
		return fmt.Errorf("invalid TPU chip count request, you must request all %d chips on this node together", tm.tpuChipCount)
	}
	return nil
}

// SetDeviceHealth sets the health status for a TPU device
func (tm *tpuManager) SetDeviceHealth(name string, health string, topologyInfo *pluginapi.TopologyInfo) {
	tm.devicesMutex.Lock()
	defer tm.devicesMutex.Unlock()
	tm.devices[name] = &pluginapi.Device{ID: name, Health: health, Topology: topologyInfo}
}

// Discovers Google TPUs devices and sets up device access environment.
func (tm *tpuManager) Start() error {
	if err := tm.discoverTPUs(); err != nil {
		return err
	}

	if len(tm.devices) != tm.tpuChipCount {
		return fmt.Errorf("not all TPU chips found on the node. Found %d and expected %d", len(tm.devices), tm.tpuChipCount)
	}
	tm.mountPaths = append(tm.mountPaths, pluginapi.Mount{HostPath: tm.tpuLogDir, ContainerPath: tm.tpuLogDir, ReadOnly: false})
	return nil
}

// ensureFilePermissions verifies and, if necessary, updates the permissions
// of the file specified by `filePath` to match `targetPerm`.
func ensureFilePermissions(filePath string, targetPerm os.FileMode) {
	fileInfo, err := os.Lstat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			glog.Warningf("File %s does not exist.", filePath)
		} else {
			glog.Warningf("Error stat-ing file %s: %v.", filePath, err)
		}
		return
	}

	currentPerm := fileInfo.Mode().Perm()
	if currentPerm != targetPerm {
		err := os.Chmod(filePath, targetPerm)
		if err != nil {
			glog.Warningf("Failed to chmod file %s to %o: %v", filePath, targetPerm, err)
		} else {
			glog.Infof("Changed %s permissions to %o", filePath, targetPerm)
		}
	}
}

func (tm *tpuManager) Serve(pMountPath, kEndpoint, pluginEndpoint string) {
	registerWithKubelet := false
	if _, err := os.Stat(path.Join(pMountPath, kEndpoint)); err == nil {
		glog.Infof("will use alpha API\n")
		registerWithKubelet = true
	} else {
		glog.Infof("will use beta API\n")
	}

	for {
		select {
		case <-tm.stop:
			close(tm.stop)
			return
		default:
			{
				pluginEndpointPath := path.Join(pMountPath, pluginEndpoint)
				glog.Infof("starting device-plugin server at: %s\n", pluginEndpointPath)
				lis, err := net.Listen("unix", pluginEndpointPath)
				if err != nil {
					glog.Fatalf("starting device-plugin server failed: %v", err)
				}
				tm.socket = pluginEndpointPath
				tm.grpcServer = grpc.NewServer()

				// Registers the supported versions of service.
				pluginbeta := &pluginServiceV1Beta1{tm: tm}
				pluginbeta.RegisterService()

				var wg sync.WaitGroup
				wg.Add(1)
				// Starts device plugin service.
				go func() {
					defer wg.Done()
					// Blocking call to accept incoming connections.
					err := tm.grpcServer.Serve(lis)
					glog.Errorf("device-plugin server stopped serving: %v", err)
				}()

				if registerWithKubelet {
					// Wait till the grpcServer is ready to serve services.
					for len(tm.grpcServer.GetServiceInfo()) <= 0 {
						time.Sleep(1 * time.Second)
					}
					glog.Infoln("device-plugin server started serving")
					// Registers with Kubelet.
					err = RegisterWithV1Beta1Kubelet(path.Join(pMountPath, kEndpoint), pluginEndpoint, resourceName)
					if err != nil {
						tm.grpcServer.Stop()
						wg.Wait()
						glog.Fatal(err)
					}
					glog.Infoln("device-plugin registered with the kubelet")
				}

				// This is checking if the plugin socket was deleted
				pluginSocketCheck := time.NewTicker(pluginSocketCheckInterval)
				defer pluginSocketCheck.Stop()
			statusCheck:
				for range pluginSocketCheck.C {
					if tm.vbarSocket != "" {
						// Ensure that the vbar socket file has the correct r+w permissions.
						// The socket is created by the vbar-control-agent and if the agent
						// restarts, the file gets re-created with root permissions because the
						// agent is running as root. If the workload is not running as root,
						// it will be unable to connect to the vbar-control-agent.
						ensureFilePermissions(tm.vbarSocket, os.FileMode(0666))
					}

					if _, err := os.Lstat(pluginEndpointPath); err != nil {
						glog.Infof("stopping device-plugin server at: %s\n", pluginEndpointPath)
						glog.Errorln(err)
						tm.grpcServer.Stop()
						break statusCheck
					}
				}
				wg.Wait()
			}
		}
	}
}

func (tm *tpuManager) Stop() error {
	glog.Infof("removing device plugin socket %s\n", tm.socket)
	if err := os.Remove(tm.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	tm.stop <- true
	<-tm.stop
	close(tm.Health)
	return nil
}

// findAllTPUDevices iterates through the PCI directory, validates each entry,
// and returns a slice of structs containing information about valid TPU devices.
func (tm *tpuManager) findAllTPUDevices() ([]tpuDeviceInfo, error) {
	devices := []tpuDeviceInfo{}

	dirs, err := os.ReadDir(tm.pciDevicesDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read PCI devices directory %s: %w", tm.pciDevicesDir, err)
	}

	for _, d := range dirs {
		pciAddr := d.Name()
		devicePath := filepath.Join(tm.pciDevicesDir, pciAddr)

		if fileInfo, err := os.Stat(devicePath); err != nil || !fileInfo.IsDir() {
			continue
		}
		if isTpu, err := tm.isTPUDevice(devicePath); err != nil || !isTpu {
			continue
		}

		// IOMMU group validation.
		iommuGroupSymlink := filepath.Join(devicePath, "iommu_group")
		if _, err := os.Lstat(iommuGroupSymlink); err != nil {
			glog.Warningf("iommu group symlink doesn't exist for %s, skipping", pciAddr)
			continue
		}
		iommuGroup, err := tm.getVFIOIOMMUGroup(iommuGroupSymlink)
		if err != nil {
			glog.Warningf("could not get IOMMU group for %s, skipping: %v", pciAddr, err)
			continue
		}

		// extract the PCI slot ID (e.g., "0000:c0:02.1" -> "0000:c0:02")
		reg := regexp.MustCompile(pciSlotIDRegexp)
		matches := reg.FindStringSubmatch(pciAddr)
		if len(matches) < 2 {
			glog.Warningf("PCI address %s does not match expected format, skipping", pciAddr)
			continue
		}
		slotID := matches[1]

		devices = append(devices, tpuDeviceInfo{
			pciAddr:    pciAddr,
			devicePath: devicePath,
			iommuGroup: iommuGroup,
			pciSlotId:  slotID,
		})
	}

	return devices, nil
}

// buildPciSlotIdToDeviceIdsMap creates a map from the PCI slot ID
// to a list of current Device IDs (IOMMU group IDs) within that PCI slot.
// The grouping is formed by PCI devices that share the same slot but have
// different virtual function number. It expects each PCI slot to
// be composed of exactly two virtual functions
func (tm *tpuManager) buildPciSlotIdToDeviceIdsMap(devices []tpuDeviceInfo) (map[string][]string, error) {
	deviceIdsByPciSlot := make(map[string][]string)
	for _, device := range devices {
		deviceIdsByPciSlot[device.pciSlotId] = append(deviceIdsByPciSlot[device.pciSlotId], device.iommuGroup)
	}

	for slotID, deviceIds := range deviceIdsByPciSlot {
		if len(deviceIds) != 2 {
			return nil, fmt.Errorf("TPU device slot %s has an unexpected number of virtual functions/devices; expected 2, found %d", slotID, len(deviceIds))
		}
	}

	glog.Infof("map of devices by PCI slot: %+v", deviceIdsByPciSlot)
	return deviceIdsByPciSlot, nil
}

// buildTPUDeviceIdToNumaNodeMap builds a map from PCI Slot ID to its NUMA node.
func (tm *tpuManager) buildPciSlotIdToNumaNodeMap(devices []tpuDeviceInfo) (map[string]int64, error) {
	pciSlotNumaMap := make(map[string]int64)
	for _, device := range devices {
		if _, exists := pciSlotNumaMap[device.pciSlotId]; exists {
			continue
		}

		numaNodeFile := filepath.Join(device.devicePath, "numa_node")
		if _, err := os.Stat(numaNodeFile); err != nil {
			return nil, fmt.Errorf("numa_node file not found for device %s, skipping: %v", device.pciAddr, err)
		}

		numaNode, err := tm.readNumaNode(numaNodeFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read NUMA node for device %s: %v", device.pciAddr, err)
		}
		pciSlotNumaMap[device.pciSlotId] = numaNode
	}

	return pciSlotNumaMap, nil
}

// isTPUDevice checks if a PCI device is Google TPU
func (tm *tpuManager) isTPUDevice(pciAddress string) (bool, error) {
	devicePath := filepath.Join(pciAddress, "device")
	deviceBytes, err := os.ReadFile(devicePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read device file %s: %v", devicePath, err)
	}
	deviceID := strings.TrimSpace(string(deviceBytes))

	vendorPath := filepath.Join(pciAddress, "vendor")
	vendorBytes, err := os.ReadFile(vendorPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read vendor file %s: %v", vendorPath, err)
	}
	vendorId := strings.TrimSpace(string(vendorBytes))

	if deviceID == tm.googleID() && vendorId == googlePCIVendorID {
		return true, nil
	}

	return false, nil
}

// getVFIOIOMMUGroup takes a /sys/bus/pci/devices/{pci_addr}/iommu_group symlink path
// and returns the IOMMU group ID as a string
func (tm *tpuManager) getVFIOIOMMUGroup(symlinkPath string) (string, error) {
	target, err := os.Readlink(symlinkPath)
	if err != nil {
		return "", fmt.Errorf("failed to read symlink %s: %w", symlinkPath, err)
	}
	return filepath.Base(target), nil
}

// readNumaNode reads the content of the numa_node file and returns the integer value
func (tm *tpuManager) readNumaNode(filePath string) (int64, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read numa_node file %s: %w", filePath, err)
	}
	s := strings.TrimSpace(string(content))
	node, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("failed to parse numa_node '%s' as integer: %w", s, err)
	}
	return int64(node), nil
}

// Mapping of TPU type to the corresponding google ID.
func (tm *tpuManager) googleID() string {
	switch tm.TpuGen {
	case "tpu7x":
		return "0x0076"
	}
	return "undefined"
}

func (tm *tpuManager) GetUniqueNumaNodesForDevices(requestedDeviceIDs []string) ([]string, error) {
	if !util.SupportsTensorNode(tm.TpuGen) || len(requestedDeviceIDs) <= 1 {
		return nil, nil
	}

	deviceNumaNodeSet := make(map[int]struct{})
	for _, deviceID := range requestedDeviceIDs {
		deviceNumaNode, ok := tm.PciSlotToNumaNode[deviceID]
		if !ok {
			return nil, fmt.Errorf("could not find NUMA node information for requested device: %s", deviceID)
		}
		deviceNumaNodeSet[int(deviceNumaNode)] = struct{}{}
	}

	uniqueNumaNodes := make([]string, 0, len(deviceNumaNodeSet))
	for numaNode := range deviceNumaNodeSet {
		uniqueNumaNodes = append(uniqueNumaNodes, strconv.Itoa(numaNode))
	}
	slices.Sort(uniqueNumaNodes)

	return uniqueNumaNodes, nil
}

func (tm *tpuManager) AreDevicesNumaAligned(requestedDeviceIDs []string) (bool, error) {
	if !util.SupportsMultiContainer(tm.TpuGen) || len(requestedDeviceIDs) <= 1 {
		return false, nil
	}

	uniqueNumaNodes, err := tm.GetUniqueNumaNodesForDevices(requestedDeviceIDs)
	if err != nil {
		return false, err
	}

	// if there is more than one unique NUMA node, the devices are not aligned
	return len(uniqueNumaNodes) == 1, nil
}

// buildPciSlotIdToChipNumMap generates the definitive mapping from a PCI slot ID to its chip ID.
func (tm *tpuManager) buildPciSlotIdToChipNumMap() (map[string]string, error) {
	numaNodeToPciSlots := make(map[int64][]string)
	for slot, node := range tm.PciSlotToNumaNode {
		numaNodeToPciSlots[node] = append(numaNodeToPciSlots[node], slot)
	}

	// sort by NUMA node id (e.g., PCI slots in numa node 0 are numbered before PCI slots in numa node 1).
	var numaNodes []int64
	for node := range numaNodeToPciSlots {
		numaNodes = append(numaNodes, node)
	}
	slices.Sort(numaNodes)

	pciSlotToChipMap := make(map[string]string)
	chipCounter := 0
	for _, node := range numaNodes {
		slotsIds := numaNodeToPciSlots[node]
		// sort the PCI slots within the numa node lexicographically
		sort.Strings(slotsIds)

		for _, slot := range slotsIds {
			pciSlotToChipMap[slot] = strconv.Itoa(chipCounter)
			chipCounter++
		}
	}

	return pciSlotToChipMap, nil
}
