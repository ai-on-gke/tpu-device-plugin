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

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"tpu-device-plugin/pkg/tpu/util"

	"golang.org/x/time/rate"

	"go.uber.org/zap"

	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	umpb "tpu-device-plugin/pkg/monitoring/proto/utilization_metrics_go_proto"
	pb "tpu-device-plugin/pkg/monitoring/runtime/proto/tpu_metric_service_go_proto"
	"tpu-device-plugin/pkg/monitoring/tpuutilization"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	timeout = 1 * time.Second
	// LINT.IfChange
	memoryTotalMetricsName                         = "tpu.runtime.hbm.memory.total.bytes"
	memoryUsedMetricsName                          = "tpu.runtime.hbm.memory.usage.bytes"
	dutyCycleMetricsName                           = "tpu.runtime.tensorcore.dutycycle.percent"
	dcnTransferLatenciesName                       = "megascale.dcn_transfer_latencies.microsecond.cumulative.distribution"
	dcnInboundTransferLatenciesName                = "megascale.dcn_inbound_transfer_latencies.microsecond.cumulative.distribution"
	mxlaComputeLatenciesName                       = "megascale.mxla_compute_latencies.microsecond.cumulative.distribution"
	grpcClientCallLatenciesMetricName              = "megascale.grpc_client_call_latencies.microsecond.cumulative.distribution"
	grpcServerCallLatenciesMetricName              = "megascale.grpc_server_call_latencies.microsecond.cumulative.distribution"
	grpcTCPMinRttMetricName                        = "megascale.grpc_tcp_min_rtt.microsecond.cumulative.distribution"
	grpcTCPDeliveryRateMetricName                  = "megascale.grpc_tcp_delivery_rate.Mbps.cumulative.distribution"
	grpcTCPPacketsSentMetricName                   = "megascale.grpc_tcp_packets_sent.cumulative.count"
	grpcTCPPacketsRetransmittedMetricName          = "megascale.grpc_tcp_packets_retransmitted.cumulative.count"
	grpcTCPPacketsSpuriousRetransmittedMetricName  = "megascale.grpc_tcp_packets_spurious_retransmitted.cumulative.count"
	grpcTCPRecurringRetransmitsCollectorMetricName = "megascale.grpc_tcp_recurring_retransmits.cumulative.count"
	grpcTCPBytesSentCollectorMetricName            = "megascale.grpc_tcp_bytes_sent.cumulative.count"
	grpcTCPBytesRetransmittedCollectorMetricName   = "megascale.grpc_tcp_bytes_retransmitted.cumulative.count"
	grpcTCPSyscallWritesCollectorMetricName        = "megascale.grpc_tcp_syscall_writes.cumulative.count"
	grpcTCPSyscallReadsCollectorMetricName         = "megascale.grpc_tcp_syscall_reads.cumulative.count"
	grpcTCPWriteSizeCollectorMetricName            = "megascale.grpc_tcp_write_size.cumulative.distribution"
	grpcTCPReadSizeCollectorMetricName             = "megascale.grpc_tcp_read_size.cumulative.distribution"
	grpcTCPSenderLatencyCollectorMetricName        = "megascale.grpc_tcp_sender_latency.microsecond.cumulative.distribution"
	grpcTCPTransferLatencyCollectorMetricName      = "megascale.grpc_tcp_transfer_latency.microsecond.cumulative.distribution"
	megascaleBamm2BitsSentMetricName               = "megascale.bamm2_bits_sent.cumulative.count"
	megascaleBamm2BitsReceivedMetricName           = "megascale.bamm2_bits_received.cumulative.count"
	megascaleBamm2BitsReadMetricName               = "megascale.bamm2_bits_read.cumulative.count"
	megascaleBamm2BitsWrittenMetricName            = "megascale.bamm2_bits_written.cumulative.count"
	megascaleBamm2BitsWrittenWithImmMetricName     = "megascale.bamm2_bits_written_with_imm.cumulative.count"
	collectiveLatenciesName                        = "megascale.collective_end_to_end_latencies.microsecond.cumulative.distribution"
	hostToDeviceTransferLatenciesName              = "megascale.host_to_device_transfer_latencies.microsecond.cumulative.distribution"
	deviceToHostTransferLatenciesName              = "megascale.device_to_host_transfer_latencies.microsecond.cumulative.distribution"
	dcnInboundTransferSizesCollectorMetricName     = "megascale.dcn_inbound_transfer_size.bytes.cumulative.distribution"
	dcnTransferSizesCollectorMetricName            = "megascale.dcn_transfer_size.bytes.cumulative.distribution"
	mxlaComputeOperandSizeCollectorMetricName      = "megascale.mxla_compute_operand_size.bytes.cumulative.distribution"
	collectiveInputSizesCollectorMetricName        = "megascale.collective_input_size.bytes.cumulative.distribution"
	deviceToHostTransferSizesCollectorMetricName   = "megascale.device_to_host_transfer_size.bytes.cumulative.distribution"
	hostToDeviceTransferSizesCollectorMetricName   = "megascale.host_to_device_transfer_size.bytes.cumulative.distribution"
	mlRuntimeUptimeMetricsName                     = "tpu.runtime.uptime.seconds.gauge"
	megascaleErrorDetectedMetricsName              = "megascale.error.detected.gauge"
	sliceErrorDetectedMetricsName                  = "slice.error.detected.gauge"
	// LINT.ThenChange(pkg/metrics/prom_metrics.go)
	brand                           = "cloud-tpu"
	maxNumBuckets                   = 200
	fieldManager                    = "tpu-device-plugin"
	tpuChipIDsAnnotationKey         = "node.gke.io/tpu-chip-ids"
	hangConditionType               = "MegascaleHangDetected"
	sliceErrorConditionType         = "SliceErrorDetected"
	hangErrorType                   = "HANG_DETECTED"
	hangExpirePeriod                = 30 * time.Minute
	sliceErrorConditionExpirePeriod = 30 * time.Minute
)

type CumulativeCounterInfo struct {
	start_time time.Time
	end_time   time.Time
	value      int64
}

type DistributionData struct {
	Mean                  float64
	Count                 int64
	BucketCounts          []int64
	Max                   float64
	Min                   float64
	SumOfSquaredDeviation float64
}

type DistributionInfo struct {
	start_time        time.Time
	data              DistributionData
	bucketUpperBounds []float64
	attributes        map[string]string
}

type KvlistAttributesMetricInfo struct {
	attributes map[string]string
	value      int64
}

type NodeConditionCheckInfo struct {
	conditionType          string
	logMessage             string
	lastHeartbeatTimeCache *time.Time
	expirePeriod           time.Duration
	detectionFunc          func() (bool, string)
}

type runtimeMetricsInfo struct {
	runtimeDutyCycle                    map[string]float64
	memoryTotal                         map[string]int64
	memoryUsed                          map[string]int64
	dcnTransferLatencies                []DistributionInfo
	mxlaComputeLatencies                []DistributionInfo
	dcnInboundTransferLatencies         []DistributionInfo
	grpcClientCallLatencies             []DistributionInfo
	grpcServerCallLatencies             []DistributionInfo
	grpcTCPMinRtt                       []DistributionInfo
	grpcTCPDeliveryRate                 []DistributionInfo
	grpcTCPPacketsSent                  map[string]CumulativeCounterInfo
	grpcTCPPacketsRetransmitted         map[string]CumulativeCounterInfo
	grpcTCPPacketsSpuriousRetransmitted map[string]CumulativeCounterInfo
	grpcTCPRecurringRetransmits         map[string]CumulativeCounterInfo
	grpcTCPBytesSent                    map[string]CumulativeCounterInfo
	grpcTCPBytesRetransmitted           map[string]CumulativeCounterInfo
	grpcTCPSyscallWrites                map[string]CumulativeCounterInfo
	grpcTCPSyscallReads                 map[string]CumulativeCounterInfo
	megascaleBamm2BitsSent              map[string]CumulativeCounterInfo
	megascaleBamm2BitsReceived          map[string]CumulativeCounterInfo
	megascaleBamm2BitsRead              map[string]CumulativeCounterInfo
	megascaleBamm2BitsWritten           map[string]CumulativeCounterInfo
	megascaleBamm2BitsWrittenWithImm    map[string]CumulativeCounterInfo
	grpcTCPWriteSize                    []DistributionInfo
	grpcTCPReadSize                     []DistributionInfo
	grpcTCPSenderLatency                []DistributionInfo
	grpcTCPTransferLatency              []DistributionInfo
	collectiveLatencies                 []DistributionInfo
	hostToDeviceTransferLatencies       []DistributionInfo
	deviceToHostTransferLatencies       []DistributionInfo
	dcnInboundTransferSizes             []DistributionInfo
	dcnTransferSizes                    []DistributionInfo
	mxlaComputeOperandSize              []DistributionInfo
	collectiveInputSizes                []DistributionInfo
	deviceToHostTransferSizes           []DistributionInfo
	hostToDeviceTransferSizes           []DistributionInfo
	mlRuntimeUptimeKvlist               []KvlistAttributesMetricInfo
	megascaleErrorDetectedKvlist        []KvlistAttributesMetricInfo
	sliceErrorDetectedKvlist            []KvlistAttributesMetricInfo
}

type hostMetricsInfo struct {
	tensorcoreUtilization      map[string]float64
	memoryBandwidthUtilization map[string]float64
}

// Returns upper bounds of exponential buckets.
func exponentialBuckets(scale, factor float64, count int) []float64 {
	buckets := []float64{}
	for i := 0; i <= count; i++ {
		buckets = append(buckets, scale)
		scale *= factor
	}
	return append(buckets, math.Inf(1))
}

type TpuContainerInfoExtractor interface {
	GetTPUContainerInfo(ctx context.Context, pods []*v1.Pod, op util.CheckContainerStatus) []util.TPUContainerInfo
}

// MetricServer exposes TPU metrics for all TPU containers and nodes
type MetricServer struct {
	hostCollectionInterval           time.Duration
	runtimeCollectionInterval        time.Duration
	gcmExportInterval                time.Duration
	lastTimeGCMExport                time.Time
	nodeName                         string
	instanceID                       string
	model                            string
	runtimeMetricsPort               string
	envInfo                          util.EnvInfo
	tpuGen                           string
	tpuTopology                      string
	podInformer                      cache.SharedIndexInformer
	kubeClient                       kubernetes.Interface
	containerInfoExtractor           TpuContainerInfoExtractor
	promPath                         string
	promPort                         int
	enableRuntimeMetrics             bool
	enableHostMetrics                bool
	enablePromMetrics                bool
	tpuChipIDsCache                  string
	hangLastHeartbeatTimeCache       time.Time
	sliceErrorLastHeartbeatTimeCache time.Time
	warningRateLimiters              map[string]*rate.Limiter
}

func NewMetricServer(hostCollectionInterval, runtimeCollectionInterval, gcmExportInterval time.Duration, nodeName, instanceID, model, runtimeMetricsPort, tpuGen, tpuTopology string, containerInfoExtractor TpuContainerInfoExtractor, promPath string, promPort int, enableRuntimeMetrics, enableHostMetrics, enablePromMetrics bool, envInfo util.EnvInfo, podInformer cache.SharedIndexInformer, kubeClient kubernetes.Interface) *MetricServer {
	return &MetricServer{
		hostCollectionInterval:           hostCollectionInterval,
		runtimeCollectionInterval:        runtimeCollectionInterval,
		gcmExportInterval:                gcmExportInterval,
		lastTimeGCMExport:                time.Now().Add(-gcmExportInterval),
		nodeName:                         nodeName,
		instanceID:                       instanceID,
		model:                            model,
		runtimeMetricsPort:               runtimeMetricsPort,
		envInfo:                          envInfo,
		tpuGen:                           tpuGen,
		tpuTopology:                      tpuTopology,
		podInformer:                      podInformer,
		kubeClient:                       kubeClient,
		containerInfoExtractor:           containerInfoExtractor,
		promPath:                         promPath,
		promPort:                         promPort,
		enableRuntimeMetrics:             enableRuntimeMetrics,
		enableHostMetrics:                enableHostMetrics,
		enablePromMetrics:                enablePromMetrics,
		tpuChipIDsCache:                  "",
		hangLastHeartbeatTimeCache:       time.Time{},
		sliceErrorLastHeartbeatTimeCache: time.Time{},
		warningRateLimiters:              make(map[string]*rate.Limiter),
	}
}

type RealTimeProvider interface {
	Now() time.Time
}

type RealTime struct {
}

func (rt RealTime) Now() time.Time {
	return time.Now()
}

// Start performs necessary initializations and starts the metric server.
func (m *MetricServer) Start() {
	logger, err := zap.NewProduction()
	if err != nil {
		glog.Errorf("failed to create a zap logger: %v", err)
		return
	}
	ctx := context.Background()

	// Build Prom Metrics Server object if it is enabled
	var pms *PromMetricsServer
	if m.enablePromMetrics {
		pms = NewPromMetricsServer(m.promPort, m.promPath, m.instanceID, brand, m.model, m.tpuTopology)
	}

	// Build host metric client
	var hmc *tpuutilization.MetricsClient
	if m.enableHostMetrics {
		hmc = m.buildHostMetricsClient()
	}

	// Build real time provider for mocking time.Now()
	realTime := RealTime{}

	// Start states to recover from restarts
	m.startNodeConditionCaches(ctx)

	go m.collectMetrics(ctx, logger, hmc, pms, realTime)
}

func (m *MetricServer) startNodeConditionCaches(ctx context.Context) {
	node, err := m.kubeClient.CoreV1().Nodes().Get(ctx, m.nodeName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("failed to start node condition caches, could not get node object: %v", err)
		return
	}
	for _, condition := range node.Status.Conditions {
		if condition.Status == v1.ConditionTrue {
			switch condition.Type {
			case hangConditionType:
				m.hangLastHeartbeatTimeCache = condition.LastHeartbeatTime.Time
			case sliceErrorConditionType:
				m.sliceErrorLastHeartbeatTimeCache = condition.LastHeartbeatTime.Time
			}
		}
	}
}

func (m *MetricServer) collectMetrics(ctx context.Context, logger *zap.Logger, hmc *tpuutilization.MetricsClient, pms *PromMetricsServer, realTime RealTimeProvider) {
	// Start Prometheus Metrics server
	if pms != nil {
		err := pms.Start()
		if err != nil {
			glog.Errorf("failed to start Prometheus metrics server: %w", err)
			return
		}
		defer pms.Stop()
	}

	t := time.NewTicker(m.runtimeCollectionInterval)
	defer t.Stop()
	// Tracks containers that successfully returned metrics in the previous cycle.
	// Used for stateful cleanup to avoid flip-flops.
	var lastActiveCIs []containerInfo

pushLoop:
	for {
		select {
		case <-ctx.Done():
			break pushLoop
		case <-t.C:
			// Run cleanup at the start of the cycle. This ensures that if multiple
			// containers share a TPU (device spreading), an exiting container won't
			// permanently delete shared node metrics of surviving containers.
			if m.enableRuntimeMetrics && pms != nil {
				pms.CleanupStaleMetrics(lastActiveCIs, 6)
			}
			lastActiveCIs = nil
			// call host Metrics even there is no tpu container. Only export node metrics
			var hmi hostMetricsInfo
			var err error
			if m.enableHostMetrics && hmc != nil {
				hmi, err = m.hostMetrics(hmc)
				if err != nil {
					glog.Errorf("failed to update Host Metrics: %v", err)
					continue
				}
				// Update host metrics on the Prometheus server
				if pms != nil {
					pms.UpdateHostMetrics(containerInfo{}, hmi, false)
				}
			}
			pods := m.podInformer.GetStore().List()
			tpuContainers := m.containerInfoExtractor.GetTPUContainerInfo(ctx, util.GetPodsFromInformer(pods), util.IsContainerRunning)

			// Loop over each running TPU container found on the node.
			for _, tpuContainer := range tpuContainers {
				// Export Runtime Metric only if client pod/container is running with an IP.
				if tpuContainer.PodIP == "" {
					continue
				}

				ci := containerInfo{
					Namespace: tpuContainer.Namespace,
					Pod:       tpuContainer.Pod,
					Container: tpuContainer.Container,
				}

				// export host metrics with running tpu container
				if m.enableHostMetrics {
					// Update host metrics on the Prometheus server
					if pms != nil {
						pms.UpdateHostMetrics(ci, hmi, true)
					}
				}

				if m.enableRuntimeMetrics {
					mi, err := m.runtimeMetrics(tpuContainer.PodIP, realTime)
					if err == nil {
						// Only consider container active if we successfully collected metrics.
						// This prevents zombie metrics if collection continuously fails.
						lastActiveCIs = append(lastActiveCIs, ci)
						// Update runtime metrics on the Prometheus server
						if pms != nil {
							pms.UpdateRuntimeMetrics(ci, mi)
							pms.UpdateAcceleratorRequest(ci, tpuContainer.RequestedTPU)
						}
						// Update node conditions if detected the following condition types
						nodeConditionUpdateChecks := []NodeConditionCheckInfo{
							{
								conditionType:          hangConditionType,
								logMessage:             "Failed to update node condition for hang issue: %v",
								lastHeartbeatTimeCache: &m.hangLastHeartbeatTimeCache,
								detectionFunc:          func() (bool, string) { return hangDetected(mi.megascaleErrorDetectedKvlist) },
							},
							{
								conditionType:          sliceErrorConditionType,
								logMessage:             "Failed to update node condition for slice error issue: %v",
								lastHeartbeatTimeCache: &m.sliceErrorLastHeartbeatTimeCache,
								detectionFunc:          func() (bool, string) { return sliceErrorDetected(mi.sliceErrorDetectedKvlist) },
							},
						}

						for _, check := range nodeConditionUpdateChecks {
							detected, message := check.detectionFunc()
							if detected {
								glog.Warningf("%s detected on node %s: %s", check.conditionType, m.nodeName, message)
							}
							newHeartbeatTime, err := m.updateNodeCondition(ctx, check.conditionType, detected, message, *check.lastHeartbeatTimeCache, realTime)
							if err != nil {
								glog.Errorf(check.logMessage, err)
							} else {
								*check.lastHeartbeatTimeCache = newHeartbeatTime
							}
						}
					} else {
						glog.Errorf("failed to update runtime Metrics: %v", err)
					}
				}
			}

			// Update node annotation for TPU chip IDs
			if hmc != nil {
				err := m.updateTPUChipIDsAnnotation(ctx, hmc)
				if err != nil {
					glog.Errorf("failed to update node annotation for TPU Chip GUIDs: %v", err)
				}
			}

			// Remove the following node conditions if they are expired
			nodeConditionRemovalChecks := []NodeConditionCheckInfo{
				{
					conditionType:          hangConditionType,
					logMessage:             "failed to remove expired node condition for hang issue: %v",
					lastHeartbeatTimeCache: &m.hangLastHeartbeatTimeCache,
					expirePeriod:           hangExpirePeriod,
				},
				{
					conditionType:          sliceErrorConditionType,
					logMessage:             "failed to remove expired node condition for slice error issue: %v",
					lastHeartbeatTimeCache: &m.sliceErrorLastHeartbeatTimeCache,
					expirePeriod:           sliceErrorConditionExpirePeriod,
				},
			}

			for _, check := range nodeConditionRemovalChecks {
				newHeartbeatTime, err := m.removeExpiredNodeCondition(ctx, check.conditionType, *check.lastHeartbeatTimeCache, check.expirePeriod, realTime)
				if err != nil {
					glog.Errorf(check.logMessage, err)
				} else {
					*check.lastHeartbeatTimeCache = newHeartbeatTime
				}
			}
		}
	}
}

func (m *MetricServer) updateNodeCondition(ctx context.Context, conditionType string, detected bool, message string, lastHeartbeatTimeCache time.Time, realTime RealTimeProvider) (time.Time, error) {
	if !detected {
		return lastHeartbeatTimeCache, nil
	}

	now := realTime.Now()
	var patch map[string]any
	// If cache is zero, it means the condition doesn't exist, and we create it with a lastTransitionTime.
	if lastHeartbeatTimeCache.IsZero() {
		patch = map[string]any{
			"status": map[string]any{
				"conditions": []any{
					map[string]any{
						"type":               conditionType,
						"status":             v1.ConditionTrue,
						"lastHeartbeatTime":  now,
						"lastTransitionTime": now,
						"message":            message,
					},
				},
			},
		}
	} else {
		// If the condition already exists, we only update its lastHeartbeatTime.
		patch = map[string]any{
			"status": map[string]any{
				"conditions": []any{
					map[string]any{
						"type":              conditionType,
						"status":            v1.ConditionTrue,
						"lastHeartbeatTime": now,
						"message":           message,
					},
				},
			},
		}
	}

	data, err := json.Marshal(patch)
	if err != nil {
		return lastHeartbeatTimeCache, err
	}
	_, err = m.kubeClient.CoreV1().Nodes().Patch(ctx, m.nodeName, types.StrategicMergePatchType, data, metav1.PatchOptions{}, "status")
	if err != nil {
		return lastHeartbeatTimeCache, err
	}

	return now, nil
}

func (m *MetricServer) removeExpiredNodeCondition(ctx context.Context, conditionType string, lastHeartbeatTime time.Time, expirePeriod time.Duration, realTime RealTimeProvider) (time.Time, error) {
	// If cache is zero, it means the condition doesn't exist (because it never existed or was deleted)
	if lastHeartbeatTime.IsZero() {
		return lastHeartbeatTime, nil
	}
	// Check if the cache is still valid
	if lastHeartbeatTime.After(realTime.Now().Add(-expirePeriod)) {
		return lastHeartbeatTime, nil
	}

	glog.Infof("%s condition has expired, removing...", conditionType)
	node, err := m.kubeClient.CoreV1().Nodes().Get(ctx, m.nodeName, metav1.GetOptions{})
	if err != nil {
		return lastHeartbeatTime, fmt.Errorf("failed to remove expired %s condition, could not get node object: %w", conditionType, err)
	}
	var newConditions []v1.NodeCondition
	for _, condition := range node.Status.Conditions {
		if condition.Type != v1.NodeConditionType(conditionType) {
			newConditions = append(newConditions, condition)
		}
	}
	node.Status.Conditions = newConditions
	_, err = m.kubeClient.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		return lastHeartbeatTime, fmt.Errorf("failed to remove expired %s condition, could not update node status: %w", conditionType, err)
	}

	return time.Time{}, nil
}

func (m *MetricServer) updateTPUChipIDsAnnotation(ctx context.Context, hostMetricsClient HostMetricsClient) error {
	tpuChipIDs := m.getTPUChipIDs(hostMetricsClient)
	if tpuChipIDs == "" {
		return nil
	}
	if tpuChipIDs == m.tpuChipIDsCache {
		return nil
	}

	glog.Infof("updating TPU chip IDs cache. TPU chip IDs: %v", tpuChipIDs)
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				tpuChipIDsAnnotationKey: tpuChipIDs,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		glog.Errorf("failed to build patch data: %v", err)
		return err
	}

	_, err = m.kubeClient.CoreV1().Nodes().Patch(ctx, m.nodeName, types.MergePatchType, data, metav1.PatchOptions{FieldManager: fieldManager})
	if err != nil {
		glog.Errorf("failed to apply TPU chip IDs annotation, could not patch node object: %v", err)
		return err
	}
	m.tpuChipIDsCache = tpuChipIDs
	return nil
}

func (m *MetricServer) getTPUChipIDs(hostMetricsClient HostMetricsClient) string {
	rawTPUChipIDs := hostMetricsClient.ChipIdentifierPerDevice()
	var values []string
	for _, v := range rawTPUChipIDs {
		if v != "" {
			values = append(values, v)
		}
	}
	sort.Strings(values)
	parsedTPUChipIDs := strings.Join(values, ",")

	return parsedTPUChipIDs
}

// Stop performs cleanup operations and stops the metric server.
func (m *MetricServer) Stop() {
}

// Use alias variable to enable mocking dependency for runtime metrics
var grpcDialFunctionAlias = grpc.Dial
var pbNewRuntimeMetricServiceClientFunctionAlias = generateRuntimeClient

type RuntimeClient interface {
	GetRuntimeMetric(ctx context.Context, in *pb.MetricRequest, opts ...grpc.CallOption) (*pb.MetricResponse, error)
	ListSupportedMetrics(ctx context.Context, in *pb.ListSupportedMetricsRequest, opts ...grpc.CallOption) (*pb.ListSupportedMetricsResponse, error)
}

func generateRuntimeClient(cc grpc.ClientConnInterface) RuntimeClient {
	return pb.NewRuntimeMetricServiceClient(cc)
}

func (m *MetricServer) getMetricIfSupported(ctx context.Context, client RuntimeClient, metricName string, supportedMetrics map[string]bool) *pb.MetricResponse {
	if !supportedMetrics[metricName] {
		return &pb.MetricResponse{} // Not supported, return empty response
	}
	response, err := client.GetRuntimeMetric(ctx, &pb.MetricRequest{MetricName: metricName}, grpc.WaitForReady(true))
	if err != nil {
		limiter, ok := m.warningRateLimiters[metricName]
		if !ok {
			limiter = rate.NewLimiter(rate.Every(59*time.Second), 1) // buffer of 1 second accounts for network latency
			m.warningRateLimiters[metricName] = limiter
		}
		if limiter.Allow() {
			glog.Warningf("Error received while calling GetRuntimeMetric for %s: %v", metricName, err)
		}
		return &pb.MetricResponse{} // Error occurred, return empty response
	}
	return response
}

func (m *MetricServer) runtimeMetrics(podIP string, realTime RealTimeProvider) (runtimeMetricsInfo, error) {
	port := m.runtimeMetricsPort
	podInfo := []string{podIP, port}
	data := strings.Join(podInfo, ":")
	now := realTime.Now()
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpcDialFunctionAlias(data, dialOpts...)

	if err != nil {
		glog.Error("Fail to dial: %v", err)
	}
	defer conn.Close()
	client := pbNewRuntimeMetricServiceClientFunctionAlias(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	supportedMetrics := make(map[string]bool)

	supportedMetricsResponse, err := client.ListSupportedMetrics(ctx, &pb.ListSupportedMetricsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		glog.Warningf("Error received while calling ListSupportedMetrics: %v", err)
		// If we can't list supported metrics, we don't proceed
		return runtimeMetricsInfo{}, err
	}

	for _, metric := range supportedMetricsResponse.GetSupportedMetric() {
		supportedMetrics[metric.MetricName] = true
	}

	runtimeTotalMemoryDevices := collectRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, memoryTotalMetricsName, supportedMetrics))
	runtimeMemoryUsedDevices := collectRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, memoryUsedMetricsName, supportedMetrics))
	runtimeDutyCycleDevices := collectRuntimeMetricsDouble(m.getMetricIfSupported(ctx, client, dutyCycleMetricsName, supportedMetrics))

	dcnTransferLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, dcnTransferLatenciesName, supportedMetrics), now)
	for i := range dcnTransferLatencies {
		err = validateDistributionInfo(&dcnTransferLatencies[i])
		if err != nil {
			dcnTransferLatencies = []DistributionInfo{}
			glog.Warningf("Invalid dcnTransferLatencies metric: %v", err)
			break
		}
	}

	mxlaComputeLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, mxlaComputeLatenciesName, supportedMetrics), now)
	for i := range mxlaComputeLatencies {
		err = validateDistributionInfo(&mxlaComputeLatencies[i])
		if err != nil {
			mxlaComputeLatencies = []DistributionInfo{}
			glog.Warningf("Invalid mxlaComputeLatencies metric: %v", err)
			break
		}
	}

	dcnInboundTransferLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, dcnInboundTransferLatenciesName, supportedMetrics), now)
	for i := range dcnInboundTransferLatencies {
		err = validateDistributionInfo(&dcnInboundTransferLatencies[i])
		if err != nil {
			dcnInboundTransferLatencies = []DistributionInfo{}
			glog.Warningf("Invalid dcnInboundTransferLatencies metric: %v", err)
			break
		}
	}

	grpcClientCallLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcClientCallLatenciesMetricName, supportedMetrics), now)
	for i := range grpcClientCallLatencies {
		err = validateDistributionInfo(&grpcClientCallLatencies[i])
		if err != nil {
			grpcClientCallLatencies = []DistributionInfo{}
			glog.Warningf("Invalid grpcClientCallLatencies metric: %v", err)
			break
		}
	}

	grpcServerCallLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcServerCallLatenciesMetricName, supportedMetrics), now)
	for i := range grpcServerCallLatencies {
		err = validateDistributionInfo(&grpcServerCallLatencies[i])
		if err != nil {
			grpcServerCallLatencies = []DistributionInfo{}
			glog.Warningf("Invalid grpcServerCallLatencies metric: %v", err)
			break
		}
	}

	grpcTCPMinRtt := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPMinRttMetricName, supportedMetrics), now)
	for i := range grpcTCPMinRtt {
		err = validateDistributionInfo(&grpcTCPMinRtt[i])
		if err != nil {
			grpcTCPMinRtt = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPMinRtt metric: %v", err)
			break
		}
	}

	grpcTCPDeliveryRate := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPDeliveryRateMetricName, supportedMetrics), now)
	for i := range grpcTCPDeliveryRate {
		err = validateDistributionInfo(&grpcTCPDeliveryRate[i])
		if err != nil {
			grpcTCPDeliveryRate = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPDeliveryRate metric: %v", err)
			break
		}
	}

	grpcTCPPacketsSent := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPPacketsSentMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPPacketsRetransmitted := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPPacketsRetransmittedMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPPacketsSpuriousRetransmitted := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPPacketsSpuriousRetransmittedMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPRecurringRetransmits := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPRecurringRetransmitsCollectorMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPBytesSent := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPBytesSentCollectorMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPBytesRetransmitted := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPBytesRetransmittedCollectorMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPSyscallWrites := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPSyscallWritesCollectorMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	grpcTCPSyscallReads := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, grpcTCPSyscallReadsCollectorMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	megascaleBamm2BitsSent := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, megascaleBamm2BitsSentMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	megascaleBamm2BitsReceived := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, megascaleBamm2BitsReceivedMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	megascaleBamm2BitsRead := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, megascaleBamm2BitsReadMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	megascaleBamm2BitsWritten := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, megascaleBamm2BitsWrittenMetricName, supportedMetrics), m.lastTimeGCMExport, now)
	megascaleBamm2BitsWrittenWithImm := collectCumulativeRuntimeMetricsInt64(m.getMetricIfSupported(ctx, client, megascaleBamm2BitsWrittenWithImmMetricName, supportedMetrics), m.lastTimeGCMExport, now)

	grpcTCPWriteSize := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPWriteSizeCollectorMetricName, supportedMetrics), now)
	for i := range grpcTCPWriteSize {
		err = validateDistributionInfo(&grpcTCPWriteSize[i])
		if err != nil {
			grpcTCPWriteSize = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPWriteSize metric: %v", err)
			break
		}
	}

	grpcTCPReadSize := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPReadSizeCollectorMetricName, supportedMetrics), now)
	for i := range grpcTCPReadSize {
		err = validateDistributionInfo(&grpcTCPReadSize[i])
		if err != nil {
			grpcTCPReadSize = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPReadSize metric: %v", err)
			break
		}
	}

	grpcTCPSenderLatency := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPSenderLatencyCollectorMetricName, supportedMetrics), now)
	for i := range grpcTCPSenderLatency {
		err = validateDistributionInfo(&grpcTCPSenderLatency[i])
		if err != nil {
			grpcTCPSenderLatency = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPSenderLatency metric: %v", err)
			break
		}
	}

	grpcTCPTransferLatency := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, grpcTCPTransferLatencyCollectorMetricName, supportedMetrics), now)
	for i := range grpcTCPTransferLatency {
		err = validateDistributionInfo(&grpcTCPTransferLatency[i])
		if err != nil {
			grpcTCPTransferLatency = []DistributionInfo{}
			glog.Warningf("Invalid grpcTCPTransferLatency metric: %v", err)
			break
		}
	}

	collectiveLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, collectiveLatenciesName, supportedMetrics), now)
	for i := range collectiveLatencies {
		// TODO: Plumb execution_type to the rest of our metrics system.
		// TODO: Figure out how to make this metric pipeline resilient
		// and detect these breakages much earlier.
		err = validateDistributionInfo(&collectiveLatencies[i])
		if err != nil {
			errThreeAttributes := validateDistributionInfo(&collectiveLatencies[i])
			if errThreeAttributes != nil {
				collectiveLatencies = []DistributionInfo{}
				glog.Warningf("Invalid collectiveLatencies metric: %v", err)
				break
			}
		}
	}

	hostToDeviceTransferLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, hostToDeviceTransferLatenciesName, supportedMetrics), now)
	for i := range hostToDeviceTransferLatencies {
		err = validateDistributionInfo(&hostToDeviceTransferLatencies[i])
		if err != nil {
			hostToDeviceTransferLatencies = []DistributionInfo{}
			glog.Warningf("Invalid hostToDeviceTransferLatencies metric: %v", err)
			break
		}
	}

	deviceToHostTransferLatencies := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, deviceToHostTransferLatenciesName, supportedMetrics), now)
	for i := range deviceToHostTransferLatencies {
		err = validateDistributionInfo(&deviceToHostTransferLatencies[i])
		if err != nil {
			deviceToHostTransferLatencies = []DistributionInfo{}
			glog.Warningf("Invalid deviceToHostTransferLatencies metric: %v", err)
			break
		}
	}

	dcnInboundTransferSizes := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, dcnInboundTransferSizesCollectorMetricName, supportedMetrics), now)
	for i := range dcnInboundTransferSizes {
		err = validateDistributionInfo(&dcnInboundTransferSizes[i])
		if err != nil {
			dcnInboundTransferSizes = []DistributionInfo{}
			glog.Warningf("Invalid dcnInboundTransferSizes metric: %v", err)
			break
		}
	}

	dcnTransferSizes := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, dcnTransferSizesCollectorMetricName, supportedMetrics), now)
	for i := range dcnTransferSizes {
		err = validateDistributionInfo(&dcnTransferSizes[i])
		if err != nil {
			dcnTransferSizes = []DistributionInfo{}
			glog.Warningf("Invalid dcnTransferSizes metric: %v", err)
			break
		}
	}

	mxlaComputeOperandSize := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, mxlaComputeOperandSizeCollectorMetricName, supportedMetrics), now)
	for i := range mxlaComputeOperandSize {
		err = validateDistributionInfo(&mxlaComputeOperandSize[i])
		if err != nil {
			mxlaComputeOperandSize = []DistributionInfo{}
			glog.Warningf("Invalid mxlaComputeOperandSize metric: %v", err)
			break
		}
	}

	collectiveInputSizes := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, collectiveInputSizesCollectorMetricName, supportedMetrics), now)
	for i := range collectiveInputSizes {
		err = validateDistributionInfo(&collectiveInputSizes[i])
		if err != nil {
			collectiveInputSizes = []DistributionInfo{}
			glog.Warningf("Invalid collectiveInputSizes metric: %v", err)
			break
		}
	}

	deviceToHostTransferSizes := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, deviceToHostTransferSizesCollectorMetricName, supportedMetrics), now)
	for i := range deviceToHostTransferSizes {
		err = validateDistributionInfo(&deviceToHostTransferSizes[i])
		if err != nil {
			deviceToHostTransferSizes = []DistributionInfo{}
			glog.Warningf("Invalid deviceToHostTransferSizes metric: %v", err)
			break
		}
	}

	hostToDeviceTransferSizes := collectRuntimeMetricsDistribution(m.getMetricIfSupported(ctx, client, hostToDeviceTransferSizesCollectorMetricName, supportedMetrics), now)
	for i := range hostToDeviceTransferSizes {
		err = validateDistributionInfo(&hostToDeviceTransferSizes[i])
		if err != nil {
			hostToDeviceTransferSizes = []DistributionInfo{}
			glog.Warningf("Invalid hostToDeviceTransferSizes metric: %v", err)
			break
		}
	}

	expectedMlRuntimeUptimeAttributeKeys := []string{"ml_framework_name", "ml_framework_version"}
	mlRuntimeUptimeMetricsWithKvlistAttributes := collectRuntimeMetricsInt64WithKvlistAttributes(m.getMetricIfSupported(ctx, client, mlRuntimeUptimeMetricsName, supportedMetrics), expectedMlRuntimeUptimeAttributeKeys)
	megascaleErrorDetectedMetricsWithKvlistAttributes := collectRuntimeMetricsInt64WithKvlistAttributes(m.getMetricIfSupported(ctx, client, megascaleErrorDetectedMetricsName, supportedMetrics), []string{})
	sliceErrorDetectedMetricsWithKvlistAttributes := collectRuntimeMetricsInt64WithKvlistAttributes(m.getMetricIfSupported(ctx, client, sliceErrorDetectedMetricsName, supportedMetrics), []string{})

	return runtimeMetricsInfo{
		runtimeDutyCycle:                    runtimeDutyCycleDevices,
		memoryTotal:                         runtimeTotalMemoryDevices,
		memoryUsed:                          runtimeMemoryUsedDevices,
		dcnTransferLatencies:                dcnTransferLatencies,
		mxlaComputeLatencies:                mxlaComputeLatencies,
		dcnInboundTransferLatencies:         dcnInboundTransferLatencies,
		grpcClientCallLatencies:             grpcClientCallLatencies,
		grpcServerCallLatencies:             grpcServerCallLatencies,
		grpcTCPMinRtt:                       grpcTCPMinRtt,
		grpcTCPDeliveryRate:                 grpcTCPDeliveryRate,
		grpcTCPPacketsSent:                  grpcTCPPacketsSent,
		grpcTCPPacketsRetransmitted:         grpcTCPPacketsRetransmitted,
		grpcTCPPacketsSpuriousRetransmitted: grpcTCPPacketsSpuriousRetransmitted,
		grpcTCPRecurringRetransmits:         grpcTCPRecurringRetransmits,
		grpcTCPBytesSent:                    grpcTCPBytesSent,
		grpcTCPBytesRetransmitted:           grpcTCPBytesRetransmitted,
		grpcTCPSyscallWrites:                grpcTCPSyscallWrites,
		grpcTCPSyscallReads:                 grpcTCPSyscallReads,
		megascaleBamm2BitsSent:              megascaleBamm2BitsSent,
		megascaleBamm2BitsReceived:          megascaleBamm2BitsReceived,
		megascaleBamm2BitsRead:              megascaleBamm2BitsRead,
		megascaleBamm2BitsWritten:           megascaleBamm2BitsWritten,
		megascaleBamm2BitsWrittenWithImm:    megascaleBamm2BitsWrittenWithImm,
		grpcTCPWriteSize:                    grpcTCPWriteSize,
		grpcTCPReadSize:                     grpcTCPReadSize,
		grpcTCPSenderLatency:                grpcTCPSenderLatency,
		grpcTCPTransferLatency:              grpcTCPTransferLatency,
		collectiveLatencies:                 collectiveLatencies,
		hostToDeviceTransferLatencies:       hostToDeviceTransferLatencies,
		deviceToHostTransferLatencies:       deviceToHostTransferLatencies,
		mlRuntimeUptimeKvlist:               mlRuntimeUptimeMetricsWithKvlistAttributes,
		megascaleErrorDetectedKvlist:        megascaleErrorDetectedMetricsWithKvlistAttributes,
		sliceErrorDetectedKvlist:            sliceErrorDetectedMetricsWithKvlistAttributes,
		dcnInboundTransferSizes:             dcnInboundTransferSizes,
		dcnTransferSizes:                    dcnTransferSizes,
		mxlaComputeOperandSize:              mxlaComputeOperandSize,
		collectiveInputSizes:                collectiveInputSizes,
		deviceToHostTransferSizes:           deviceToHostTransferSizes,
		hostToDeviceTransferSizes:           hostToDeviceTransferSizes,
	}, nil
}

func collectRuntimeMetricsInt64(response *pb.MetricResponse) map[string]int64 {
	runtimeMetricsInt64 := make(map[string]int64)
	for _, metric := range response.GetMetric().GetMetrics() {
		stringdeviceid := metric.GetAttribute().GetValue().GetIntAttr()
		id := strconv.FormatInt(stringdeviceid, 10)
		runtimeMetricsInt64[id] = metric.GetGauge().GetAsInt()
	}
	return runtimeMetricsInt64
}

func collectCumulativeRuntimeMetricsInt64(response *pb.MetricResponse, lastTimeGCMExport time.Time, now time.Time) map[string]CumulativeCounterInfo {
	runtimeMetricsInt64 := make(map[string]CumulativeCounterInfo)
	for _, metric := range response.GetMetric().GetMetrics() {
		stringdeviceid := metric.GetAttribute().GetValue().GetIntAttr()
		id := strconv.FormatInt(stringdeviceid, 10)
		start_time := lastTimeGCMExport
		end_time := now
		if metric.GetStartTimestamp() != nil {
			start_time = metric.GetStartTimestamp().AsTime()
			end_time = metric.GetTimestamp().AsTime()
		}
		runtimeMetricsInt64[id] = CumulativeCounterInfo{start_time, end_time, int64(metric.GetCounter().GetAsInt())}
	}
	return runtimeMetricsInt64
}

func collectRuntimeMetricsInt64WithKvlistAttributes(response *pb.MetricResponse, expectedKeys []string) []KvlistAttributesMetricInfo {
	runtimeMetricsInt64WithKvlistAttributes := []KvlistAttributesMetricInfo{}
	expectedKeysInSingleString := ""
	if len(expectedKeys) != 0 {
		expectedKeysInSingleString = strings.Join(expectedKeys, ":")
	}
	for _, metric := range response.GetMetric().GetMetrics() {
		attrs := make(map[string]string)
		for _, attr := range metric.GetAttribute().GetValue().GetKvlistAttr().GetAttributes() {
			if expectedKeysInSingleString == "" || strings.Contains(expectedKeysInSingleString, attr.GetKey()) {
				attrs[attr.GetKey()] = getAttrValueAsString(attr.GetValue())
			} else {
				glog.Warningf("Unexpected attribute key is present in the Metric: %v", attr.GetKey())
			}
		}
		if len(attrs) > 0 {
			runtimeMetricsInt64WithKvlistAttributes = append(runtimeMetricsInt64WithKvlistAttributes, KvlistAttributesMetricInfo{attrs, metric.GetGauge().GetAsInt()})
		}
	}
	return runtimeMetricsInt64WithKvlistAttributes
}

func collectRuntimeMetricsDouble(response *pb.MetricResponse) map[string]float64 {
	runtimeMetricsDouble := make(map[string]float64)
	for _, metric := range response.GetMetric().GetMetrics() {
		stringdeviceid := metric.GetAttribute().GetValue().GetIntAttr()
		id := strconv.FormatInt(stringdeviceid, 10)
		runtimeMetricsDouble[id] = metric.GetGauge().GetAsDouble()
	}
	return runtimeMetricsDouble
}

func collectRuntimeMetricsDistribution(response *pb.MetricResponse, now time.Time) []DistributionInfo {
	runtimeMetricsDist := []DistributionInfo{}
	var ub []float64

	for _, metric := range response.GetMetric().GetMetrics() {
		attributes := make(map[string]string)
		dist := DistributionData{}
		attrs := metric.GetAttribute().GetValue().GetKvlistAttr()
		for _, attr := range attrs.GetAttributes() {
			attributes[attr.GetKey()] = getAttrValueAsString(attr.GetValue())
		}
		dist.BucketCounts = metric.GetDistribution().GetBucketCounts()
		dist.Count = metric.GetDistribution().GetCount()
		dist.Max = metric.GetDistribution().GetMax()
		dist.Mean = metric.GetDistribution().GetMean()
		dist.Min = metric.GetDistribution().GetMin()
		dist.SumOfSquaredDeviation = metric.GetDistribution().GetSumOfSquaredDeviation()
		opts := metric.GetDistribution().BucketOptions.GetExponentialBuckets()
		ub = exponentialBuckets(opts.Scale, opts.GrowthFactor, int(opts.NumFiniteBuckets))
		start_time := now.Add(-1 * time.Microsecond)
		if metric.GetStartTimestamp() != nil {
			start_time = metric.GetStartTimestamp().AsTime()
		}
		runtimeMetricsDist = append(runtimeMetricsDist, DistributionInfo{start_time, dist, ub, attributes})
	}
	return runtimeMetricsDist
}

func validateDistributionInfo(di *DistributionInfo) error {
	if len(di.bucketUpperBounds) != len(di.data.BucketCounts) {
		return fmt.Errorf("bucketCounts is different (%v) than expected %v", len(di.data.BucketCounts), len(di.bucketUpperBounds))
	}
	if len(di.bucketUpperBounds) >= maxNumBuckets {
		return fmt.Errorf("# of buckets %v exceed the max %v", len(di.bucketUpperBounds), maxNumBuckets)
	}
	return nil
}

func hangDetected(metricInfo []KvlistAttributesMetricInfo) (bool, string) {
	for _, info := range metricInfo {
		if errorType, ok := info.attributes["error_type"]; ok && errorType == hangErrorType {
			return info.value != 0, ""
		}
	}
	return false, ""
}

func sliceErrorDetected(metricInfo []KvlistAttributesMetricInfo) (bool, string) {
	for _, info := range metricInfo {
		requiredKeys := []string{"error_message", "session_id", "type", "topology"}
		allKeysPresent := true

		for _, key := range requiredKeys {
			if _, ok := info.attributes[key]; !ok {
				allKeysPresent = false
				break
			}
		}

		// If all keys were found and the value is non-zero, we have a match
		if allKeysPresent && info.value != 0 {
			return true, info.attributes["error_message"]
		}
	}

	return false, ""
}

type HostMetricsClient interface {
	UtilizationPercentagePerDevice(since time.Duration, metricType umpb.UtilizationMetricType) (map[string]float64, error)
	ChipIdentifierPerDevice() map[string]string
}

func (m *MetricServer) hostMetrics(client HostMetricsClient) (hostMetricsInfo, error) {
	tensorcoreVal, error := client.UtilizationPercentagePerDevice(m.hostCollectionInterval, umpb.UtilizationMetricType_TENSORCORE_UTILIZATION)
	if error != nil {
		glog.Errorf("failed to get tensorevalue for per Device: %v", error)
	}
	tensorcoreUtilziationDevices := make(map[string]float64)
	for deviceID, utilizationPercentage := range tensorcoreVal {
		tensorcoreUtilziationDevices[deviceID] = utilizationPercentage
	}

	hbmVal, error := client.UtilizationPercentagePerDevice(m.hostCollectionInterval, umpb.UtilizationMetricType_HBM_UTILIZATION)
	if error != nil {
		glog.Errorf("failed to HBM_UTILIZATION : %v", error)
	}
	hbmUtilziationDevices := make(map[string]float64)
	for deviceID, utilizationPercentage := range hbmVal {
		hbmUtilziationDevices[deviceID] = utilizationPercentage
	}

	return hostMetricsInfo{
		tensorcoreUtilization:      tensorcoreUtilziationDevices,
		memoryBandwidthUtilization: hbmUtilziationDevices,
	}, nil

}

func (m *MetricServer) buildHostMetricsClient() *tpuutilization.MetricsClient {
	tpuGenUtil, err := util.TPUUtilizationUtilType(m.tpuGen)
	if err != nil {
		glog.Errorf("failed to get host metrics client: %v", err)
		return nil
	}
	// Initialize Client options
	opts := tpuutilization.Opts("resource4" /*metrics-bar*/, tpuGenUtil)

	hmc, err := tpuutilization.NewMetricsClient(opts)
	if err != nil {
		glog.Errorf("failed to get host metrics client: %v", err)
		hmc = nil
	}
	return hmc
}

func buildAccelID(instanceID, deviceID string) string {
	return strings.Join([]string{instanceID, deviceID}, "-")
}

// getAttrValueAsString converts an attribute value to a string representation.
// Note: GKE Metrics Library currently only supports string attributes, so other types
// are converted to their string representation here.
func getAttrValueAsString(val *pb.AttrValue) string {
	if val == nil {
		return ""
	}
	switch attr := val.GetAttr().(type) {
	case *pb.AttrValue_StringAttr:
		return attr.StringAttr
	case *pb.AttrValue_IntAttr:
		return strconv.FormatInt(attr.IntAttr, 10)
	case *pb.AttrValue_BoolAttr:
		return strconv.FormatBool(attr.BoolAttr)
	case *pb.AttrValue_DoubleAttr:
		return strconv.FormatFloat(attr.DoubleAttr, 'f', -1, 64)
	default:
		return "unknown"
	}
}
