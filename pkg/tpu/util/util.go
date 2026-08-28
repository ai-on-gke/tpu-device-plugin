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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
	"tpu-device-plugin/pkg/monitoring/tpuutilizationutil"

	"golang.org/x/sys/unix"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	AcceleratorLabel               = "cloud.google.com/gke-tpu-accelerator"
	AcceleratorCountLabel          = "cloud.google.com/gke-accelerator-count"
	AcceleratorTopologyModeLabel   = "cloud.google.com/gke-accelerator-topology-mode"
	TopologyLabel                  = "cloud.google.com/gke-tpu-topology"
	SubSliceTopologyAnnotation     = "cloud.google.com/gke-tpu-slice-topology"
	SubSliceTopologyLabelTemplate  = "cloud.google.com/gke-tpu-slice-{{.topology}}-id"
	SubslicePartitionLabelTemplate = "cloud.google.com/gke-tpu-partition-{{.topology}}-id"
	ICIResiliency                  = "cloud.google.com/gke-tpu-ici-resiliency"
	mdsRetryTimeout                = 30 * time.Second
	KubeletRetryTimeout            = 10 * time.Second
	twist                          = "false"
	vlpMaxTopologyDim              = 16
	tpuResourceName                = "google.com/tpu"
	ClusterProjectEnv              = "CLUSTER_PROJECT"
	ClusterLocationEnv             = "CLUSTER_LOCATION"
	ClusterNameEnv                 = "CLUSTER_NAME"
	PodNamespaceEnv                = "POD_NAMESPACE"
	NodeNameEnv                    = "NODE_NAME"
	NodeIPEnv                      = "NODE_IP"
	PodNameEnv                     = "POD_NAME"
	ContainerNameEnv               = "CONTAINER_NAME"
	ICIResiliencyEnv               = "ENABLE_ICI_RESILIENCY"
	ProvisionOnlyTopologyMode      = "PROVISION_ONLY"
	RootDirectory                  = "/"
	DevDirectory                   = "/dev"
	DevDirectoryVfio               = "/dev/vfio"
)

// ErrMetadataNotFound indicates that the metadata service returned an HTTP 404 status.
var ErrMetadataNotFound = errors.New("MDS returned HTTP 404 Not Found")

var (
	acceleratorRegex     = regexp.MustCompile(`^tpu\d+[a-z]?$`)
	pastAcceleratorRegex = regexp.MustCompile(`^tpu-v\d+([ep]?[a]?-slice)?((?:-lite)?-(device|podslice))?$`)

	//The tpugen's value should follow the format of CloudTPU "TPU_ACCELERATOR_TYPE" : relative ascending order of release
	validTPUGenerations = map[string]int{
		"v3":        0,
		"v4":        1,
		"v5litepod": 4,
		"v5p":       5,
		"v6e":       6,
		"tpu7x":     8,
	}
	// chips per node -> chips per dimension
	// accelerators per node -> metrics port env variable
	acceleratorCountToMetricsPorts = map[int]string{
		1: "8431",
		2: "8431,8432",
		4: "8431,8432,8433,8434",
		8: "8431,8432,8433,8434,8435,8436,8437,8438",
	}
	networkSettings = []SystemSetting{
		{FilePath: "proc/sys/net/ipv4/tcp_slow_start_after_idle", Value: "0"},
		{FilePath: "proc/sys/net/ipv4/tcp_no_metrics_save", Value: "1"},
		{FilePath: "sys/module/tcp_cubic/parameters/hystart_detect", Value: "2"},
		{FilePath: "proc/sys/net/core/somaxconn", Value: "4096"},
		{FilePath: "proc/sys/net/ipv4/tcp_max_syn_backlog", Value: "4096"},
		{FilePath: "proc/sys/net/ipv4/tcp_mtu_probing", Value: "0"},
		{FilePath: "proc/sys/net/core/optmem_max", Value: "131072"},
	}

	validSubsliceTopologySet = map[string]bool{
		"1x1":   true,
		"2x2":   true,
		"2x4":   true,
		"4x4":   true,
		"4x8":   true,
		"8x8":   true,
		"8x16":  true,
		"16x16": true,
		"4x4x4": true,
		"2x4x4": true,
		"2x2x4": true,
		"2x2x2": true,
		"2x2x1": true,
	}
)

// TPUContainerInfo contains information about container that is using TPU
// resouce on the node.
type TPUContainerInfo struct {
	Namespace        string
	Pod              string
	Container        string
	PodIP            string
	SubSliceTopology string
	IsPrivileged     bool
	RequestedTPU     int64
}

// EnvInfo contains information about cluster/pod info from Env variable
// on tpu-device-plugin daemonset.
type EnvInfo struct {
	ClusterName      string
	ClusterProjectID string
	ClusterLocation  string
	PodNamespace     string
	PodName          string
	ContainerName    string
}

// InitEnvOptions contains fields that are required to initializing the
// environment variables used by tpu-device-plugin.
type InitEnvOptions struct {
	Accelerator           string
	Topology              string
	EnableICIResiliency   string
	ChipCount             int
	AcceleratorCount      int
	RequestedChipCount    int
	SubSliceTopology      string
	IsPrivileged          bool
	VisibleChipIds        []string
	EnableDeviceSpreading bool
	NumaNodeIds           []string
	EnableVbarUds         bool
}

// SystemSetting contains filePath and its setting value.
type SystemSetting struct {
	FilePath string
	Value    string
}

func RemoveDirContents(dirPath string) error {
	dir, _ := os.ReadDir(dirPath)
	for _, d := range dir {
		if err := os.RemoveAll(path.Join([]string{dirPath, d.Name()}...)); err != nil {
			return fmt.Errorf("failed deleting: %s, error %w", d.Name(), err)
		}
	}
	return nil
}

// Helper to check if topology is within 16x16 cap
func isTopologyUpTo16x16(dims []int) bool {
	for _, dim := range dims {
		if dim > 16 {
			return false
		}
	}
	return true
}

// Helper to check if generation uses slice template
func isSliceTemplateGen(tpuGen string) bool {
	return tpuGen == "v4" || tpuGen == "v5litepod" || tpuGen == "v5p" || tpuGen == "v6e"
}

// GetSubsliceLabels polls the MDS for partition labels.
// Returns a map of the labels alongside any error that may have arised
func GetSubsliceLabels(url string, nodeTopology string, tpuGen string, enableFullHierarchyLabels bool) (map[string]string, error) {
	labels := make(map[string]string)
	partitionLabelsJSON, err := CallHTTPServer(url, true, mdsRetryTimeout, true)
	supportsFullHierarchyLabels := enableFullHierarchyLabels && (tpuGen == "v5litepod" || tpuGen == "v6e")
	if err != nil {
		if errors.Is(err, ErrMetadataNotFound) {
			glog.Infof("Sub-slice topology labels not found on MDS (HTTP 404 for %s)", url)
			return labels, err
		}
		glog.Errorf("error reading response body for partition ID endpoint %s: %v", url, err)
		return labels, err
	}

	var partitionLabelsMap map[string]string
	err = json.Unmarshal([]byte(partitionLabelsJSON), &partitionLabelsMap)
	if err != nil {
		glog.Errorf("Error unmarshalling JSON response for partition IDs: %v", err)
		return labels, err
	}

	nodeDims, err := parseTopology(nodeTopology)
	checkDims := true
	if nodeTopology == "" || err != nil {
		glog.Errorf("Couldn't parse node topology: %s, ignoring valid topology check", nodeTopology)
		checkDims = false
	}

	var isLarger bool
	for topologyId, hash := range partitionLabelsMap {
		if checkDims {
			idDims, err := parseTopology(topologyId)
			if err != nil {
				glog.Errorf("Skipping invalid topology ID: %s", topologyId)
				continue
			}

			if len(idDims) != len(nodeDims) {
				glog.Infof("Skipping topology ID: %s due to dimension mismatch with node topology: %s", topologyId, nodeTopology)
				continue
			}

			isLarger = false
			for i := range idDims {
				if idDims[i] > nodeDims[i] {
					isLarger = true
					break
				}
			}

			// We only allow larger topologies if they are valid hierarchy labels (supported and <= 16x16)
			isAllowedHierarchy := supportsFullHierarchyLabels && isTopologyUpTo16x16(idDims)

			if isLarger && !isAllowedHierarchy {
				glog.Infof("Skipping unneeded topology ID: %s", topologyId)
				continue
			}
		}

		// Select label template
		var labelKey string
		if isSliceTemplateGen(tpuGen) {
			labelKey = strings.ReplaceAll(SubSliceTopologyLabelTemplate, "{{.topology}}", topologyId)
		} else {
			labelKey = strings.ReplaceAll(SubslicePartitionLabelTemplate, "{{.topology}}", topologyId)
		}
		labels[labelKey] = hash
	}

	return labels, nil
}

// WaitForPartitionLabels polls the MDS for partition labels with retries until success, timeout, or context cancellation.
func WaitForPartitionLabels(ctx context.Context, url, nodeTopology, tpuGen string, enableFullHierarchyLabels bool, timeoutDuration, interval time.Duration) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	for {
		partitionLabels, err := GetSubsliceLabels(url, nodeTopology, tpuGen, enableFullHierarchyLabels)
		if err == nil {
			return partitionLabels, nil
		}
		if errors.Is(err, ErrMetadataNotFound) {
			glog.Infof("Partition labels not found on MDS yet (HTTP 404 for %s), retrying...", url)
		} else {
			glog.Errorf("error fetching partition labels: %v", err)
		}

		select {
		case <-ctx.Done():
			if errors.Is(err, ErrMetadataNotFound) {
				glog.Infof("Partition labels not found on MDS after timeout (HTTP 404 for %s); assuming non-subsliced capacity mode", url)
				return make(map[string]string), nil
			}
			return nil, fmt.Errorf("timed out or context cancelled while waiting for partition labels: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// parseTopology handles both 2D and 3D topologies and returns an array of integers.
func parseTopology(topology string) ([]int, error) {
	parts := strings.Split(topology, "x")
	if len(parts) < 2 || len(parts) > 3 {
		return nil, fmt.Errorf("invalid topology format: %s", topology)
	}

	dims := make([]int, len(parts))
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		dims[i] = n
	}

	return dims, nil
}

// NodeLabels queries the MDS to get the node labels and verifies the labels
// contain the key/value pairs containing required TPU info.
func NodeLabels(url string) (map[string]string, error) {
	labels := make(map[string]string)
	kubeLabels, err := CallHTTPServer(url, true, mdsRetryTimeout, true)
	if err != nil {
		if errors.Is(err, ErrMetadataNotFound) {
			glog.Infof("Node labels not found on MDS (HTTP 404 for %s)", url)
		} else {
			glog.Errorf("error reading response body for node labels from %s: %v", url, err)
		}
		return labels, err
	}
	// Parse node labels into map.
	kubeLabelsArr := strings.Split(string(kubeLabels[:]), ",")

	for _, pair := range kubeLabelsArr {
		parts := strings.Split(pair, "=")
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]
		labels[key] = val
	}

	// Verify we have valid values for accelerator type, topology, and accelerator count.
	_, exists := labels[AcceleratorLabel]
	if !exists {
		return nil, fmt.Errorf("node label %s must be set", AcceleratorLabel)
	}

	// Only verify the topology if incremental vm provisioning is used.
	if val, ok := labels[AcceleratorTopologyModeLabel]; !ok || val != ProvisionOnlyTopologyMode {
		_, exists = labels[TopologyLabel]
		if !exists {
			return nil, fmt.Errorf("node label %s must be set", TopologyLabel)
		}
	}
	_, exists = labels[AcceleratorCountLabel]
	if !exists {
		return nil, fmt.Errorf("node label %s must be set", AcceleratorCountLabel)
	}
	return labels, nil
}

func GetCreatedByMIG(url string) (string, error) {
	body, err := CallHTTPServer(url, true, mdsRetryTimeout, false)
	if err != nil {
		if errors.Is(err, ErrMetadataNotFound) {
			glog.Infof("Created by MIG attribute not found on MDS (HTTP 404 for %s)", url)
			return "", nil
		}
		return "", fmt.Errorf("metadata service call to %s failed when attempting to get mig: %w", url, err)
	}

	return string(body), nil
}

func ApplyNodeAnnotation(ctx context.Context, clientset kubernetes.Interface, nodeName, key, value string) error {
	payload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				key: value,
			},
		},
	}

	// Marshal the payload into a JSON byte slice.
	patch, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch for node %q: %w", nodeName, err)
	}

	_, err = clientset.CoreV1().Nodes().Patch(
		ctx,
		nodeName,
		types.StrategicMergePatchType,
		patch,
		metav1.PatchOptions{},
	)

	if err != nil {
		return fmt.Errorf("failed to update node annotation %q on node %q: %w", key, nodeName, err)
	}

	return nil
}

func IsValidSubSliceTopology(topology string, subSliceTopology string) (bool, error) {
	// subSliceTopology not specified
	if subSliceTopology == "" {
		return false, nil
	}
	// subSliceTopology not valid
	if _, ok := validSubsliceTopologySet[subSliceTopology]; !ok {
		return false, fmt.Errorf("invalid value for subSliceTopology: %s", subSliceTopology)
	}
	// subSliceTopology mismatch with node topology
	dims := strings.Split(topology, "x")
	subDims := strings.Split(subSliceTopology, "x")
	if len(dims) != len(subDims) {
		return false, fmt.Errorf("invalid value for subSliceTopology: %s, dimension mismatches", subSliceTopology)
	}
	for i := range dims {
		d1, _ := strconv.Atoi(dims[i])      // From control plane
		d2, err := strconv.Atoi(subDims[i]) // From user input
		if err != nil {
			return false, fmt.Errorf("invalid value for subSliceTopology: %s", subSliceTopology)
		}
		if d2 > d1 {
			return false, fmt.Errorf("invalid value for subSliceTopology: %s, subSliceTopology shouldn't be larger than topology", subSliceTopology)
		}
	}
	return true, nil
}

// InitEnvs initializes a map of environment variables containing required
// metadata values for TPU workloads to run.
func InitEnvs(opts InitEnvOptions) (map[string]string, error) {
	// Get accelerator generation (v4, v5, etc.) and topology dimensions from node labels.
	tpuGen, err := AcceleratorGen(opts.Accelerator)
	if err != nil {
		return nil, err
	}
	var topology string
	valid, err := IsValidSubSliceTopology(opts.Topology, opts.SubSliceTopology)
	if valid {
		topology = opts.SubSliceTopology
	} else {
		topology = opts.Topology
	}
	if err != nil {
		glog.Errorf("Invalid subSliceTopology: %v, setting the topology env as %s.", err, topology)
	}

	var topologyDims []int64
	var acceleratorTypeConverted string
	// Metrics ports
	ports, exists := acceleratorCountToMetricsPorts[opts.AcceleratorCount]
	if !exists {
		return nil, fmt.Errorf("invalid value for acceleratorCount: %d", opts.AcceleratorCount)
	}
	// TODO: Update env vars with prefixes decided by Cloud TPU
	// team once they are supported in libtpu, Pytorch etc.
	envs := map[string]string{
		"TPU_SKIP_MDS_QUERY":        "true",
		"TPU_RUNTIME_METRICS_PORTS": ports,
	}

	if topology != "" {
		topologyDims, err = getTopologyDims(topology)
		if err != nil {
			return nil, err
		}
		// Convert accelerator type to <tpuGeneration>-<numCores> format used by GKE.
		acceleratorTypeConverted, err = convertAcceleratorType(tpuGen, topologyDims)
		if err != nil {
			return nil, err
		}
		envs["TPU_TOPOLOGY"] = topology
		envs["TPU_ACCELERATOR_TYPE"] = acceleratorTypeConverted
	}

	// socket defined in tpunetd service
	if opts.EnableVbarUds {
		envs["VBAR_CONTROL_SERVICE_URL"] = "unix:///var/run/tpu-plugin/vbar.sock"
	} else {
		nodeIp, err := GetEnvName(NodeIPEnv)
		if err != nil {
			glog.Infof("$NODE_IP is not set in env")
		} else {
			envs["VBAR_CONTROL_SERVICE_URL"] = nodeIp + ":8353"
		}
	}

	if opts.EnableDeviceSpreading && opts.IsPrivileged && len(opts.VisibleChipIds) > 0 {
		envs["TPU_VISIBLE_CHIPS"] = strings.Join(opts.VisibleChipIds, ",")
	}

	if len(opts.NumaNodeIds) > 0 {
		envs["WORKLOAD_NIC_PREFERRED_NUMA"] = strings.Join(opts.NumaNodeIds, ",")
	}

	// Set metadata specific to TPU podslices, TPU slice or TPU devices.
	if isPodslice(opts.Accelerator) && topology != "" {
		if err := addPodsliceOrSliceEnvs(tpuGen, opts.EnableICIResiliency, opts.RequestedChipCount, topologyDims, envs); err != nil {
			return nil, err
		}
	}

	// For single host we can add additional env vars to further reduce
	// configuration required by user.
	// This is skipped for tpu7x +, as responsibility for these variables shifts to GCW
	// that has the multi-container context required to set these within a single
	// host environment.
	if isSingleHost(opts.ChipCount, topologyDims) && isEalierGeneration(tpuGen, "tpu7x") {
		addSingleHostEnvs(envs)
	}
	return envs, nil
}

func ChipCount(chipCount string) (int, error) {
	count, err := strconv.Atoi(chipCount)
	if err != nil {
		return -1, err
	}
	return count, nil
}

// TPU accelerator type from node label should be in format:
// "tpu-<gen>-<device/podslice/slice>".
// We convert it to "<gen>-<# of cores>" for consumption by
// libtpu, frameworks, etc.
func convertAcceleratorType(tpuGen string, topologyDims []int64) (string, error) {
	cores, err := numCores(tpuGen, topologyDims)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d", tpuGen, cores), nil
}

// AcceleratorGen obtains the generation (v3, v4, v5litepod, etc.)
// from the accelerator type label.
// accelerator: tpu-v3-device or tpu-v3-slice; return: v3
// accelerator: tpu-v4-podslice; return: v4
// accelerator: tpu-v5-lite-podslice; return: v5litepod
// accelerator: tpu-v5p-slice; return: v5p
// accelerator: tpu-v6e-slice; return: v6e
// accelerator: tpu7x; return: tpu7x
func AcceleratorGen(accelerator string) (string, error) {
	// For >= TPU7x, the new accelerator label will be the generation
	if acceleratorRegex.MatchString(accelerator) {
		return accelerator, nil
	}
	if !pastAcceleratorRegex.MatchString(accelerator) {
		return "", fmt.Errorf("invalid accelerator type: %v", accelerator)
	}

	// Edge cases that match regex but are not valid accelerator types.
	if accelerator == "tpu-v4-device" || accelerator == "tpu-v4-lite-podslice" {
		return "", fmt.Errorf("no such accelerator type: %s", accelerator)
	}

	// v = v2, v3, v4, v5, v5p, v6e
	v := strings.Split(accelerator, "-")[1]

	// append 'lite' to lite device and lite podslice
	if strings.Contains(accelerator, "lite") {
		v = fmt.Sprintf("%slite", v)
	}

	// append 'pod' to v5 lite podslices
	if strings.HasPrefix(v, "v5") && strings.Contains(accelerator, "podslice") {
		v = fmt.Sprintf("%spod", v)
	}
	if _, exists := validTPUGenerations[v]; !exists {
		return "", fmt.Errorf("invalid TPU generation: %s", v)
	}
	return v, nil
}

// IsLegacyTPU returns true if the generation uses character devices in /dev/accel* (v3, v4).
func IsLegacyTPU(tpuGen string) bool {
	return tpuGen == "v3" || tpuGen == "v4"
}

// GetDeviceDirectory determines the host device directory based on the TPU generation.
// Legacy character devices (v3, v4) use DevDirectory (/dev), while all
// modern and future TPU generations default to VFIO passthrough (/dev/vfio).
func GetDeviceDirectory(tpuGen string) string {
	if IsLegacyTPU(tpuGen) {
		return DevDirectory
	}
	return DevDirectoryVfio
}

// numCores calculates the number of cores based on the topology.
// Lite = 1 core per chip
// Non-lite = 2 cores per chip
func numCores(tpuGen string, topologyDims []int64) (int, error) {
	// Calculate total chips in the podslice.
	totalChips := calculateTotalChips(topologyDims)

	// lite-device and lite-podslice have 1 core per chip.
	// v6e is also "lite"
	if strings.Contains(tpuGen, "lite") || tpuGen == "v6e" {
		return totalChips, nil
	}
	// device and podslice have 2 cores per chip.
	return totalChips * 2, nil
}

func getChipsPerDim(tpuGen string, requestedChipCount int) ([]int64, error) {
	switch requestedChipCount {
	case 1:
		return []int64{1, 1, 1}, nil
	case 2:
		return []int64{1, 2, 1}, nil
	case 4:
		return []int64{2, 2, 1}, nil
	case 8:
		return []int64{2, 4, 1}, nil
	default:
		return nil, fmt.Errorf("invalid chip count for %s: %d", tpuGen, requestedChipCount)
	}
}

func calculateHostBounds(tpuGen string, requestedChipCount int, topologyDims []int64) (string, error) {
	// Get chips per dimension from the chipCount.
	trayChipNumPerDim, err := getChipsPerDim(tpuGen, requestedChipCount)
	if err != nil {
		return "", err
	}

	// Calculate host bounds using topology dimensions and chips per dimension.
	var hostBounds []string
	for dim, trayChipNum := range trayChipNumPerDim {
		hostBounds = append(hostBounds, strconv.FormatInt(topologyDims[dim]/trayChipNum, 10))
	}
	return strings.Join(hostBounds, ","), nil
}

func calculateTotalChips(topologyDims []int64) int {
	totalChips := 1
	for _, chips := range topologyDims {
		totalChips *= int(chips)
	}
	return totalChips
}

func getTopologyDims(topology string) ([]int64, error) {
	var topologyDims []int64
	topologyDimStrs := strings.Split(topology, "x")
	for _, s := range topologyDimStrs {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		topologyDims = append(topologyDims, int64(n))
	}

	// Add 3rd dimension of 1 to 2D topologies (e.g. 2x2 -> 2x2x1).
	if len(topologyDims) == 2 {
		topologyDims = append(topologyDims, int64(1))
	}
	return topologyDims, nil
}

func getChipsPerHostBounds(tpuGen string, requestedChipCount int) (string, error) {
	chipsPerDim, err := getChipsPerDim(tpuGen, requestedChipCount)
	if err != nil {
		return "", err
	}
	var tmp []string
	for _, chips := range chipsPerDim {
		tmp = append(tmp, strconv.Itoa(int(chips)))
	}
	return strings.Join(tmp, ","), nil
}

// Add podslice or slice Envs
func addPodsliceOrSliceEnvs(tpuGen, enableICIResiliency string, requestedChipCount int, topologyDims []int64, envs map[string]string) error {
	hostBounds, err := calculateHostBounds(tpuGen, requestedChipCount, topologyDims)
	if err != nil {
		return err
	}
	chipsPerHostBounds, err := getChipsPerHostBounds(tpuGen, requestedChipCount)
	if err != nil {
		return err
	}

	wrapVal, err := wrap(tpuGen, topologyDims)
	if err != nil {
		return err
	}

	// Enable ICI resiliency on for v4 / v5p / tpu7x topologies >= 4x4x4.
	if strings.HasPrefix(tpuGen, "v4") || strings.HasPrefix(tpuGen, "v5p") || strings.HasPrefix(tpuGen, "tpu7x") {
		if cubeOrLarger(topologyDims) {
			envs[ICIResiliencyEnv] = "true"
			if strings.ToLower(enableICIResiliency) == "false" {
				envs[ICIResiliencyEnv] = "false"
			}
		}
	}

	envs["TPU_TOPOLOGY_ALT"] = twist
	envs["ALT"] = twist
	envs["TPU_TOPOLOGY_WRAP"] = wrapVal
	envs["WRAP"] = wrapVal
	envs["HOST_BOUNDS"] = hostBounds
	envs["TPU_HOST_BOUNDS"] = hostBounds
	envs["CHIPS_PER_HOST_BOUNDS"] = chipsPerHostBounds
	envs["TPU_CHIPS_PER_HOST_BOUNDS"] = chipsPerHostBounds
	return nil
}

func addSingleHostEnvs(envs map[string]string) {
	envs["TPU_WORKER_ID"] = "0"
	envs["TPU_WORKER_HOSTNAMES"] = "localhost"
}

func isSingleHost(chipCount int, topologyDims []int64) bool {
	// If multiplication of topology dimensions == chip count on this node,
	// this is a single host.
	return chipCount == calculateTotalChips(topologyDims)
}

func wrap(tpuGen string, topologyDims []int64) (string, error) {
	switch tpuGen {
	case "v3", "v4", "v5p", "tpu7x":
		return wrapVersion(topologyDims), nil
	case "v5litepod", "v6e":
		return wrapLitePod(topologyDims), nil
	}
	return "", fmt.Errorf("invalid TPU generation: %s", tpuGen)
}

func wrapVersion(topologyDims []int64) string {
	// v4 does wraparound for v4 cube (4x4x4) or larger.
	if cubeOrLarger(topologyDims) {
		return "true,true,true"
	}
	return "false,false,false"
}

func wrapLitePod(topologyDims []int64) string {
	val := []string{"false", "false", "false"}
	for i, dim := range topologyDims {
		if dim == vlpMaxTopologyDim {
			val[i] = "true"
		}
	}
	return strings.Join(val, ",")
}

func isPodslice(accelerator string) bool {
	return strings.HasSuffix(accelerator, "slice") ||
		strings.Contains(accelerator, "tpu7x")
}

func cubeOrLarger(topologyDims []int64) bool {
	// v4 cube is 4x4x4.
	for _, dim := range topologyDims {
		if dim < 4 {
			return false
		}
	}
	return true
}

func NodeInstanceID(url string) (string, error) {
	body, err := CallHTTPServer(url, true, mdsRetryTimeout, true)
	return string(body), err
}

func GetEnvName(envName string) (string, error) {
	env := os.Getenv(envName)
	if len(env) == 0 {
		return "", fmt.Errorf("empty %s environment variable", envName)
	}
	return env, nil
}

// CallHTTPServer queries the url with header
func CallHTTPServer(url string, header bool, retryTimeout time.Duration, logger bool) ([]byte, error) {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryWaitMax = retryTimeout
	if !logger {
		retryClient.Logger = nil
	}
	client := retryClient.StandardClient()

	// Fetch node instance id from MDS.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		glog.Errorf("error creating request for %s: %v", url, err)
		return []byte(""), err
	}
	if header {
		req.Header.Add("Metadata-Flavor", "Google")
	}
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("error sending request to %s: %v", url, err)
		return []byte(""), err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w for endpoint %s", ErrMetadataNotFound, url)
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("unexpected HTTP status code %d for endpoint %s", resp.StatusCode, url)
		glog.Errorf("%v", err)
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("error reading response body from %s: %v", url, err)
		return []byte(""), err
	}
	return body, nil
}

func GetPodsFromInformer(objects []any) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(objects))
	for i, obj := range objects {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			glog.Errorf("Object at index %d is not a *v1.Pod, but a %T. Skipping.", i, obj)
			continue
		}
		pods = append(pods, pod)
	}
	return pods
}

type CheckContainerStatus func(containerName string, containerStatuses []v1.ContainerStatus) bool

// implements TpuContainerInfo interface defined in metrics.go
type ContainerInfoExtractor struct{}

func (ca ContainerInfoExtractor) GetTPUContainerInfo(ctx context.Context, pods []*v1.Pod, op CheckContainerStatus) []TPUContainerInfo {
	return ExtendedResourceTpuContainers(ctx, pods, op)
}

// ExtendedResourceTpuContainer returns TpuContainerInfo if the container has TPU extended resources request and check
// container status based on op.
func ExtendedResourceTpuContainers(ctx context.Context, pods []*v1.Pod, op CheckContainerStatus) []TPUContainerInfo {
	foundContainers := make([]TPUContainerInfo, 0)

	for _, pod := range pods {
		for _, container := range pod.Spec.InitContainers {
			isPrivileged := false
			if container.SecurityContext != nil && container.SecurityContext.Privileged != nil {
				isPrivileged = *container.SecurityContext.Privileged
			}

			requestTPU, count := ContainersRequestTPUs(container)
			if requestTPU && op(container.Name, pod.Status.InitContainerStatuses) {
				info := TPUContainerInfo{
					Container:        container.Name,
					Pod:              pod.Name,
					Namespace:        pod.Namespace,
					PodIP:            pod.Status.PodIP,
					SubSliceTopology: pod.Annotations[SubSliceTopologyAnnotation],
					IsPrivileged:     isPrivileged,
					RequestedTPU:     count,
				}
				foundContainers = append(foundContainers, info)
			}
		}

		for _, container := range pod.Spec.Containers {
			isPrivileged := false
			if container.SecurityContext != nil && container.SecurityContext.Privileged != nil {
				isPrivileged = *container.SecurityContext.Privileged
			}

			requestTPU, count := ContainersRequestTPUs(container)
			if requestTPU && op(container.Name, pod.Status.ContainerStatuses) {
				info := TPUContainerInfo{
					Container:        container.Name,
					Pod:              pod.Name,
					Namespace:        pod.Namespace,
					PodIP:            pod.Status.PodIP,
					SubSliceTopology: pod.Annotations[SubSliceTopologyAnnotation],
					IsPrivileged:     isPrivileged,
					RequestedTPU:     count,
				}
				foundContainers = append(foundContainers, info)
			}
		}
	}
	return foundContainers
}

func IsContainerRunning(containerName string, ContainerStatuses []v1.ContainerStatus) bool {
	for _, container := range ContainerStatuses {
		if container.Name == containerName {
			if container.State.Running != nil {
				return true
			}
		}
	}
	return false
}

func IsContainerStatusEmpty(containerName string, ContainerStatuses []v1.ContainerStatus) bool {
	return len(ContainerStatuses) == 0
}

func numTPUsRequested(container v1.Container) int64 {
	if l := container.Resources.Limits; l != nil {
		if resource := l[tpuResourceName]; !resource.IsZero() {
			return resource.Value()
		}
	}
	if r := container.Resources.Requests; r != nil {
		if resource := r[tpuResourceName]; !resource.IsZero() {
			return resource.Value()
		}
	}
	return 0
}

// containersRequestTPUs returns true if any container has TPU requests or limits.
func ContainersRequestTPUs(containers ...v1.Container) (bool, int64) {
	for _, container := range containers {
		tpuCount := numTPUsRequested(container)
		if tpuCount != 0 {
			return true, tpuCount
		}
	}
	return false, 0
}

// GetEnvInfo is to get env from tpu-device-plugin daemonset
func GetEnvInfo() (EnvInfo, error) {
	clusterName, err := GetEnvName(ClusterNameEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get cluster Name: %v", err)
	}
	clusterProjectID, err := GetEnvName(ClusterProjectEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get cluster project ID: %v", err)
	}
	clusterLocation, err := GetEnvName(ClusterLocationEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get cluster location: %v", err)
	}
	podNamespace, err := GetEnvName(PodNamespaceEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get pod namespace: %v", err)
	}
	podName, err := GetEnvName(PodNameEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get pod name: %v", err)
	}
	containerName, err := GetEnvName(ContainerNameEnv)
	if err != nil {
		return EnvInfo{}, fmt.Errorf("failed to get container name: %v", err)
	}

	return EnvInfo{
		ClusterName:      clusterName,
		ClusterProjectID: clusterProjectID,
		ClusterLocation:  clusterLocation,
		PodNamespace:     podNamespace,
		PodName:          podName,
		ContainerName:    containerName,
	}, nil

}

func TPUUtilizationUtilType(tpuGen string) (tpuutilizationutil.TPUType, error) {

	switch tpuGen {
	case "v3":
		return tpuutilizationutil.V3, nil
	case "v4":
		return tpuutilizationutil.V4, nil
	case "v5p":
		return tpuutilizationutil.V5, nil
	case "v5litepod":
		return tpuutilizationutil.V5lite, nil
	case "v6e":
		return tpuutilizationutil.V6E, nil
	case "tpu7x":
		return tpuutilizationutil.TPU7x, nil
		// TPU8i is not supported by the tpu-metrics lib
		// Metrics logic will be removed from the TPU Device Plugin binary before GA
	}

	return "", fmt.Errorf("unknown device type: %s", tpuGen)
}

// ApplyNetworkSettings iterates through a predefined list of network settings
// and applies them by writing to the corresponding system file. After writing,
// it reads back the value to verify the update and logs the results.
// It traverses the entire list before returning, even if an error is encountered.
func ApplyNetworkSettings() error {
	return applyNetworkSettings(RootDirectory)
}

// An implementation for ApplyNetworkSettings but taking parent directory for unit test purpose.
func applyNetworkSettings(parentDir string) error {
	var errs []string
	for _, setting := range networkSettings {
		filePath := filepath.Join(parentDir, setting.FilePath)
		err := os.WriteFile(filePath, []byte(setting.Value), 0644)
		if err != nil {
			glog.Errorf("Error writing to %s: %v", filePath, err)
			errs = append(errs, filePath)
			continue
		}
		value, err := os.ReadFile(filePath)
		if err != nil {
			glog.Errorf("Error reading from %s: %v", filePath, err)
			errs = append(errs, filePath)
			continue
		}
		glog.Infof("Current value of %s: %s", filePath, value)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func BuildKubeClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Errorf("failed to get kube config. Error: %v", err)
		return nil, err
	}
	config.ContentType = runtime.ContentTypeProtobuf

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Errorf("failed to get kube client. Error: %v", err)
		return nil, err
	}

	return kubeClient, nil
}

func SetupPodInformer(kubeClient *kubernetes.Clientset, nodeName string) cache.SharedIndexInformer {
	// Create an informer for Pods bound to the node
	fieldSelector := fields.ParseSelectorOrDie("spec.nodeName=" + nodeName)
	factory := informers.NewSharedInformerFactoryWithOptions(kubeClient, time.Second*10, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.FieldSelector = fieldSelector.String()
	}))
	podInformer := factory.Core().V1().Pods().Informer()

	// Start the informer
	glog.Infoln("Starting pod informer on the node")
	factory.Start(wait.NeverStop)
	factory.WaitForCacheSync(wait.NeverStop)

	return podInformer
}

// Copyied from k8s.io/kubernetes/pkg/util/flock
// Acquire acquires a lock on a file for the duration of the process. This method
// is reentrant.
func Acquire(path string) error {
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}

	// We don't need to close the fd since we should hold
	// it until the process exits.

	return unix.Flock(fd, unix.LOCK_EX)
}

// Using the labels passed in,
// Apply the partition labels to the node
func SetSubsliceLabels(ctx context.Context, metadataHandler NodeMetadataHandler, partitionLabels map[string]string) error {
	if len(partitionLabels) == 0 {
		glog.Info("No partition labels to apply to the node.")
		return nil
	}

	var errs []string
	for label, hash := range partitionLabels {
		glog.Infof("Applying label: %s = %s", label, hash)
		if err := metadataHandler.ApplyLabel(ctx, label, hash); err != nil {
			errMsg := fmt.Sprintf("failed to apply partition label %s=%s: %v", label, hash, err)
			glog.Error(errMsg)
			errs = append(errs, errMsg)
		}
	}

	return nil
}

// SupportsTensorNode checks if the TPU generation supports Tensor Node feature.
// Currently, tpu7x supports Tensor Node.
func SupportsTensorNode(tpuGen string) bool {
	switch tpuGen {
	case "tpu7x":
		return true
	default:
		return false
	}
}

func SupportsMultiContainer(tpuGen string) bool {
	switch tpuGen {
	case "tpu7x":
		return true
	default:
		return false
	}
}

// isEalierGeneration compares two TPU generations and returns true if gen1 is earlier than gen2.
func isEalierGeneration(gen1, gen2 string) bool {
	rank1, rank2 := validTPUGenerations[gen1], validTPUGenerations[gen2]
	return rank1 < rank2
}
