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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	healthcheck "tpu-device-plugin/pkg/health_check"
	"tpu-device-plugin/pkg/metrics"
	tpumanager "tpu-device-plugin/pkg/tpu"
	"tpu-device-plugin/pkg/tpu/util"

	"github.com/golang/glog"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	// Device plugin settings.
	kubeletEndpoint      = "kubelet.sock"
	pluginEndpointPrefix = "tpu"

	// Metadata server URLs.
	GetMIGURL        = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/created-by"
	KubeLabelsURL    = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/kube-labels"
	TopologyLabelURL = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/accelerator_topology_id"
	InstanceIDURL    = "http://metadata.google.internal/computeMetadata/v1/instance/id"

	nodeNameEnv                = "NODE_NAME"
	runtimeMetricsPort         = "8431"
	promPort                   = 2112
	promPath                   = "/metrics"
	pciDevicesDir              = "/sys/bus/pci/devices"
	createdByMIGAnnotationName = "node.gke.io/created-by-mig"
	mdsPollingTimeout          = 1 * time.Hour
	mdsPollingInterval         = 10 * time.Second
)

var (
	pluginMountPath                  = flag.String("plugin-directory", "/device-plugin", "The directory path to create plugin socket")
	enableHealthMonitoring           = flag.Bool("enable-health-monitoring", true, "If true, the device plugin will detect if the /dev/acccel* is not available")
	enableRuntimeMetrics             = flag.Bool("enable-runtime-metrics", true, "If true, the device plugin will expose TPU runtime metrics")
	enableHostMetrics                = flag.Bool("enable-host-metrics", true, "If true, the device plugin will expose TPU host metrics")
	enablePromMetrics                = flag.Bool("enable-prom-metrics", true, "If true, the device plugin will expose metrics in prometheus endpoint")
	_                                = flag.Bool("enable-alloc-wait", false, "If true, the device plugin will wait until the old device plugin register allocatable as 0")
	enableVbarUds                    = flag.Bool("enable-vbar-uds", false, "If true, the device plugin will setup the vbar-control-agent and libtpu to communicate over UDS")
	enableFlockWait                  = flag.Bool("enable-flock-wait", false, "If true, the device plugin will wait until the old device plugin release the lock")
	enableSubsliceApplication        = flag.Bool("enable-subslice-application", true, "If true, the device plugin will apply subslice labels retrieved from MDS.")
	enableDeviceSpreading            = flag.Bool("enable-device-spreading", true, "If true, the device plugin allows a single workload to allocate available TPU chips across multiple containers (TPU7, TPU7x, and TPU8i only)")
	runtimeMetricsCollectionInterval = flag.Duration("runtime-metrics-collection-interval", 10*time.Second, `Runtime collection interval (in seconds) for container/node TPU metrics`)
	hostMetricsCollectionInterval    = flag.Duration("host-metrics-collection-interval", 10*time.Second, `Host Collection interval (in seconds) for container/node TPU metrics`)
	gcmExportInterval                = flag.Duration("gcm-export-interval", 50*time.Second, `Export interval (in seconds) to Google Cloud Monitoring`)
	partitionLabelsPollingTimeout    = flag.Duration("partition-labels-polling-timeout", mdsPollingTimeout, "Timeout for polling partition labels from MDS.")
	partitionLabelsPollingInterval   = flag.Duration("partition-labels-polling-interval", mdsPollingInterval, "Interval for polling partition labels from MDS.")
	enableFullHierarchyLabels        = flag.Bool("enable-full-hierarchy-labels", false, "If true, the device plugin will populate all TPU hardware hierarchy labels")
)

func main() {
	flag.Parse()
	glog.Infoln("device-plugin started")
	mountPaths := []pluginapi.Mount{}

	err := util.ApplyNetworkSettings()
	if err != nil {
		glog.Errorf("error apllying network settings: %w", err)
	}

	// Set up podInformer for tpuManager and metricServer
	kubeClient, err := util.BuildKubeClient()
	if err != nil {
		panic(err)
	}

	// Get the Node name from environment
	// Setup the Pod Informer using node name and kubeClient
	nodeName, err := util.GetEnvName(nodeNameEnv)
	if err != nil {
		glog.Error("failed to get Node Name: %v\n", err)
	}
	podInformer := util.SetupPodInformer(kubeClient, nodeName)

	// Fetch node labels from MDS.
	nodeLabels, err := util.NodeLabels(KubeLabelsURL)
	if err != nil {
		panic(fmt.Errorf("error fetching metadata: %w", err))
	}

	model := nodeLabels[util.AcceleratorLabel]
	tpuGen, err := util.AcceleratorGen(model)
	if err != nil {
		panic(err)
	}
	tpuTopology := ""
	if val, ok := nodeLabels[util.TopologyLabel]; ok {
		tpuTopology = val
		glog.Infof("Successfully retrieved the tpu topology: %v", tpuTopology)
	} else {
		glog.Info("No tpu topology is found in the node labels.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure the context is canceled when main exits

	// Add CreatedByMIG annotation to node.
	createdByMIG, err := util.GetCreatedByMIG(GetMIGURL)
	if err != nil {
		panic(fmt.Errorf("error fetching metadata: %w", err))
	}

	if err := util.ApplyNodeAnnotation(ctx, kubeClient, nodeName, createdByMIGAnnotationName, createdByMIG); err != nil {
		glog.Errorf("error applying node annotation %s: %v", createdByMIGAnnotationName, err)
	}

	// TODO: need a deterministic way to figure out whether a node is gSC or not
	if *enableSubsliceApplication {
		go func(nodeName string) { // Fetch partition labels from MDS.
			// This logic is under the assumption that the hash will presist across node lifecycle.
			partitionLabels, err := util.WaitForPartitionLabels(ctx, TopologyLabelURL, tpuTopology, tpuGen, *enableFullHierarchyLabels, *partitionLabelsPollingTimeout, *partitionLabelsPollingInterval)
			if err != nil {
				glog.Errorf("failed to get partition labels: %v", err)
				return
			}
			glog.Infof("applying partition labels: %v", partitionLabels)

			// Create nodeMetadataHandler to apply partition labels to the nodes
			nodeMetadataHandler := util.NewNodeMetadataHandler(nodeName, kubeClient)
			err = util.SetSubsliceLabels(ctx, nodeMetadataHandler, partitionLabels)
			if err != nil {
				glog.Errorf("error applying partition labels: %v", err)
			}
		}(nodeName)
	}

	// Determine device directory based on TPU generation.
	devDir := util.GetDeviceDirectory(tpuGen)

	// device spreading is not allowed for TPU generations less than TPU7x
	enableDeviceSpreading := *enableDeviceSpreading
	if !util.SupportsMultiContainer(tpuGen) {
		enableDeviceSpreading = false
	}

	nodeName, err = util.GetEnvName(nodeNameEnv)
	if err != nil {
		glog.Error("failed to get Node Name: %v\n", err)
	}

	tm, err := tpumanager.NewTPUManager(nodeLabels, devDir, mountPaths, podInformer, nodeName, kubeClient, pciDevicesDir, enableDeviceSpreading, *enableVbarUds)
	if err != nil {
		panic(err)
	}

	for {
		err := tm.Start()
		if err == nil {
			break
		}

		glog.Errorf("failed to start TPU device manager: %v", err)
		time.Sleep(5 * time.Second)
	}

	if *enableRuntimeMetrics || *enableHostMetrics {
		glog.Infof("enable TPU Metrics")
		instanceID, err := util.NodeInstanceID(InstanceIDURL)
		if err != nil {
			glog.Error("failed to get instance ID from the metadata server: %v\n", err)
		}
		envInfo, err := util.GetEnvInfo()
		if err != nil {
			glog.Errorf("failed to create env info: %v", err)
		}
		metricServer := metrics.NewMetricServer(
			*hostMetricsCollectionInterval,
			*runtimeMetricsCollectionInterval,
			*gcmExportInterval,
			nodeName,
			instanceID,
			model,
			runtimeMetricsPort,
			tpuGen,
			tpuTopology,
			util.ContainerInfoExtractor{},
			promPath,
			promPort,
			*enableRuntimeMetrics,
			*enableHostMetrics,
			*enablePromMetrics,
			envInfo,
			podInformer,
			kubeClient,
		)
		metricServer.Start()
		defer metricServer.Stop()
	}

	if *enableHealthMonitoring {
		hc := healthcheck.NewTPUHealthChecker(tm.ListDevices(), tm.Health, tm.DevDirectory, tm.TpuGen, tm.PciSlotToDeviceIds)
		if err := hc.Start(); err != nil {
			glog.Infof("failed to start TPU Health Checker: %v", err)
			return
		}
		defer hc.Stop()
	}

	if *enableFlockWait {
		lockFile := "/device-plugin/tpu-device-plugin.lock"
		if err := util.Acquire(lockFile); err != nil {
			glog.Errorf("Failed to acquire lock exiting... %v", err)
			os.Exit(1)
		}
	}

	tm.Serve(*pluginMountPath, kubeletEndpoint, fmt.Sprintf("%s-%d.sock", pluginEndpointPrefix, time.Now().Unix()))
}
