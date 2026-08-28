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
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	DutyCycleNodeProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "duty_cycle_node",
			Help: "Percent of time when the TPU was actively processing",
		},
		[]string{"make", "accelerator_id", "model", "tpu_topology"})
	MemoryTotalNodeProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_total_node",
			Help: "Total memory available on the TPU in bytes",
		},
		[]string{"make", "accelerator_id", "model", "tpu_topology"})
	MemoryUsedNodeProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_used_node",
			Help: "Allocated TPU memory in bytes",
		},
		[]string{"make", "accelerator_id", "model", "tpu_topology"})
	TensorCoreUtilizationNodeProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tensorcore_utilization_node",
			Help: "Tensorcore percent utilization of the TPU device per node",
		},
		[]string{"make", "accelerator_id", "model", "tpu_topology"})
	MemoryBandwidthUtilizationNodeProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_bandwidth_utilization_node",
			Help: "Memory bandwidth utilization of the TPU device per node",
		},
		[]string{"make", "accelerator_id", "model", "tpu_topology"})
	DutyCycleProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "duty_cycle",
			Help: "Percent of time when the TPU was actively processing",
		},
		[]string{"namespace", "pod", "container", "make", "accelerator_id", "model", "tpu_topology"})
	MemoryTotalProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_total",
			Help: "Total memory available on the TPU in bytes",
		},
		[]string{"namespace", "pod", "container", "make", "accelerator_id", "model", "tpu_topology"})
	MemoryUsedProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_used",
			Help: "Allocated TPU memory in bytes",
		},
		[]string{"namespace", "pod", "container", "make", "accelerator_id", "model", "tpu_topology"})
	TensorCoreUtilizationProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tensorcore_utilization",
			Help: "Tensorcore percent utilization of the TPU device",
		},
		[]string{"namespace", "pod", "container", "make", "accelerator_id", "model", "tpu_topology"})
	MemoryBandwidthUtilizationProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memory_bandwidth_utilization",
			Help: "Memory bandwidth utilization of the TPU device",
		},
		[]string{"namespace", "pod", "container", "make", "accelerator_id", "model", "tpu_topology"})
	GrpcTCPPacketsSentProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_packets_sent_count",
			Help: "Total count of packets TCP sends",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPPacketsRetransmittedProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_packets_retransmitted_count",
			Help: "Total count of packets TCP retransmits",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPPacketsSpuriousRetransmittedProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_packets_spurious_retransmitted_count",
			Help: "Total count of packets TCP spurious retransmits",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPRecurringRetransmitsProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_recurring_retransmits_count",
			Help: "Total count of packets TCP recurring retransmits",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPBytesSentProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_bytes_sent_count",
			Help: "Total count of bytes TCP sends",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPBytesRetransmittedProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_bytes_retransmitted_count",
			Help: "Total count of bytes TCP retransmits",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPSyscallWritesProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_syscall_writes_count",
			Help: "Total count of TCP syscall writes",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	GrpcTCPSyscallReadsProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_tcp_syscall_reads_count",
			Help: "Total count of TCP syscall reads",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	Bamm2BitsSentProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bamm2_bits_sent_count",
			Help: "Total count of bits sent by BAMM2",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	Bamm2BitsReceivedProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bamm2_bits_received_count",
			Help: "Total count of bits received by BAMM2",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	Bamm2BitsReadProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bamm2_bits_read_count",
			Help: "Total count of bits read by BAMM2",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	Bamm2BitsWrittenProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bamm2_bits_written_count",
			Help: "Total count of bits written by BAMM2",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	Bamm2BitsWrittenWithImmProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bamm2_bits_written_with_imm_count",
			Help: "Total count of bits written with imm by BAMM2",
		},
		[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"})
	AcceleratorRequestProm = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "accelerator_request",
			Help: "Requested TPU chip count",
		},
		[]string{"namespace", "pod", "container", "resource_name"})

	NodeTokenBrokerStatusProm = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "node_token_broker_status",
			Help: "Count of token source requests, broken down by status and mode.",
		},
		[]string{"status", "mode"})
)

const metricsResetInterval = time.Minute

type PromMetricsServer struct {
	port                     int
	metricsEndpointPath      string
	instanceID               string
	brand                    string
	model                    string
	tpuTopology              string
	lastHostMetricsResetTime time.Time
	hc                       *histCollector
	// Cache of last seen metrics per container to allow selective deletion.
	lastRuntimeMetrics map[containerInfo]runtimeMetricsInfo
	// Tracks consecutive collection misses for each container.
	missCounts map[containerInfo]int
}

type containerInfo struct {
	Namespace string
	Pod       string
	Container string
}

func NewPromMetricsServer(port int, metricsEndpointPath, instanceID, brand, model, tpuTopology string) *PromMetricsServer {
	return &PromMetricsServer{
		port:                     port,
		metricsEndpointPath:      metricsEndpointPath,
		instanceID:               instanceID,
		brand:                    brand,
		model:                    model,
		tpuTopology:              tpuTopology,
		lastHostMetricsResetTime: time.Time{},
		hc:                       newHistCollector(),
		lastRuntimeMetrics:       make(map[containerInfo]runtimeMetricsInfo),
		missCounts:               make(map[containerInfo]int),
	}
}

func (m *PromMetricsServer) Start() error {
	glog.Infoln("Starting Prometheus Metrics server")
	prometheus.MustRegister(m.hc)
	go func() {
		http.Handle(m.metricsEndpointPath, promhttp.Handler())
		for {
			err := http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil)
			if err == nil {
				glog.Infoln("Prometheus Metrics server started successfully")
				break
			}
			glog.Errorf("Failed to start metric server: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
		}
	}()
	return nil
}

func (m *PromMetricsServer) UpdateRuntimeMetrics(ci containerInfo, mi runtimeMetricsInfo) {
	if ci == (containerInfo{}) {
		return
	}
	// Cache the latest metrics and reset miss count.
	m.lastRuntimeMetrics[ci] = mi
	m.missCounts[ci] = 0
	containerLabels := []string{ci.Namespace, ci.Pod, ci.Container}
	for deviceID, value := range mi.runtimeDutyCycle {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		DutyCycleProm.WithLabelValues(append(containerLabels, labels...)...).Set(value)
		DutyCycleNodeProm.WithLabelValues(labels...).Set(value)
	}
	for deviceID, value := range mi.memoryUsed {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		MemoryUsedProm.WithLabelValues(append(containerLabels, labels...)...).Set(float64(value))
		MemoryUsedNodeProm.WithLabelValues(labels...).Set(float64(value))
	}
	for deviceID, value := range mi.memoryTotal {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		MemoryTotalProm.WithLabelValues(append(containerLabels, labels...)...).Set(float64(value))
		MemoryTotalNodeProm.WithLabelValues(labels...).Set(float64(value))
	}
	m.hc.UpdateValues(mi, append(containerLabels, m.brand, m.model, m.tpuTopology))
	for _, metric := range mi.grpcTCPPacketsSent {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPPacketsSentProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPPacketsRetransmitted {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPPacketsRetransmittedProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPPacketsSpuriousRetransmitted {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPPacketsSpuriousRetransmittedProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPRecurringRetransmits {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPRecurringRetransmitsProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPBytesSent {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPBytesSentProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPBytesRetransmitted {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPBytesRetransmittedProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPSyscallWrites {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPSyscallWritesProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.grpcTCPSyscallReads {
		labels := []string{m.brand, m.model, m.tpuTopology}
		GrpcTCPSyscallReadsProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.megascaleBamm2BitsSent {
		labels := []string{m.brand, m.model, m.tpuTopology}
		Bamm2BitsSentProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.megascaleBamm2BitsReceived {
		labels := []string{m.brand, m.model, m.tpuTopology}
		Bamm2BitsReceivedProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.megascaleBamm2BitsRead {
		labels := []string{m.brand, m.model, m.tpuTopology}
		Bamm2BitsReadProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.megascaleBamm2BitsWritten {
		labels := []string{m.brand, m.model, m.tpuTopology}
		Bamm2BitsWrittenProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}
	for _, metric := range mi.megascaleBamm2BitsWrittenWithImm {
		labels := []string{m.brand, m.model, m.tpuTopology}
		Bamm2BitsWrittenWithImmProm.WithLabelValues(append(containerLabels, labels...)...).Add(float64(metric.value))
	}

}

func (m *PromMetricsServer) UpdateAcceleratorRequest(ci containerInfo, requestedTPU int64) {
	if ci == (containerInfo{}) {
		return
	}
	containerLabels := []string{ci.Namespace, ci.Pod, ci.Container}
	AcceleratorRequestProm.WithLabelValues(append(containerLabels, "google.com/tpu")...).Set(float64(requestedTPU))
}

func (m *PromMetricsServer) UpdateNodeTokenBrokerStatus(status, mode string) {
	NodeTokenBrokerStatusProm.WithLabelValues(status, mode).Inc()
}

func (m *PromMetricsServer) UpdateHostMetrics(ci containerInfo, mi hostMetricsInfo, skipUpdatingNodemetric bool) {
	m.resetHostMetricsIfNeeded()
	var containerLabels []string
	if ci != (containerInfo{}) {
		containerLabels = []string{ci.Namespace, ci.Pod, ci.Container}
	}
	for deviceID, value := range mi.tensorcoreUtilization {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		if !skipUpdatingNodemetric {
			TensorCoreUtilizationNodeProm.WithLabelValues(labels...).Set(value)
		}
		if len(containerLabels) > 0 {
			TensorCoreUtilizationProm.WithLabelValues(append(containerLabels, labels...)...).Set(value)
		}
	}
	for deviceID, value := range mi.memoryBandwidthUtilization {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		if !skipUpdatingNodemetric {
			MemoryBandwidthUtilizationNodeProm.WithLabelValues(labels...).Set(value)
		}
		if len(containerLabels) > 0 {
			MemoryBandwidthUtilizationProm.WithLabelValues(append(containerLabels, labels...)...).Set(value)
		}
	}
}

// deleteRuntimeMetrics explicitly removes all Prometheus gauges and counters
// associated with a specific container to prevent stale data.
func (m *PromMetricsServer) deleteRuntimeMetrics(ci containerInfo, mi runtimeMetricsInfo) {
	containerLabels := []string{ci.Namespace, ci.Pod, ci.Container}
	for deviceID := range mi.runtimeDutyCycle {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		DutyCycleProm.DeleteLabelValues(append(containerLabels, labels...)...)
		DutyCycleNodeProm.DeleteLabelValues(labels...)
	}
	for deviceID := range mi.memoryUsed {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		MemoryUsedProm.DeleteLabelValues(append(containerLabels, labels...)...)
		MemoryUsedNodeProm.DeleteLabelValues(labels...)
	}
	for deviceID := range mi.memoryTotal {
		acceleratorID := buildAccelID(m.instanceID, deviceID)
		labels := []string{m.brand, acceleratorID, m.model, m.tpuTopology}
		MemoryTotalProm.DeleteLabelValues(append(containerLabels, labels...)...)
		MemoryTotalNodeProm.DeleteLabelValues(labels...)
	}
	labels := []string{m.brand, m.model, m.tpuTopology}
	GrpcTCPPacketsSentProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPPacketsRetransmittedProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPPacketsSpuriousRetransmittedProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPRecurringRetransmitsProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPBytesSentProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPBytesRetransmittedProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPSyscallWritesProm.DeleteLabelValues(append(containerLabels, labels...)...)
	GrpcTCPSyscallReadsProm.DeleteLabelValues(append(containerLabels, labels...)...)
	Bamm2BitsSentProm.DeleteLabelValues(append(containerLabels, labels...)...)
	Bamm2BitsReceivedProm.DeleteLabelValues(append(containerLabels, labels...)...)
	Bamm2BitsReadProm.DeleteLabelValues(append(containerLabels, labels...)...)
	Bamm2BitsWrittenProm.DeleteLabelValues(append(containerLabels, labels...)...)
	Bamm2BitsWrittenWithImmProm.DeleteLabelValues(append(containerLabels, labels...)...)

	// Delete AcceleratorRequestProm
	AcceleratorRequestProm.DeleteLabelValues(append(containerLabels, "google.com/tpu")...)

}

// CleanupStaleMetrics compares active containers against the cache and deletes
// metrics for containers that have exceeded the miss threshold.
func (m *PromMetricsServer) CleanupStaleMetrics(activeContainers []containerInfo, missThreshold int) {
	activeMap := make(map[containerInfo]bool)
	for _, ci := range activeContainers {
		activeMap[ci] = true
	}

	if len(activeMap) == 0 {
		m.hc.UpdateValues(runtimeMetricsInfo{}, []string{})
	}

	for ci, mi := range m.lastRuntimeMetrics {
		if !activeMap[ci] {
			m.missCounts[ci]++
			if m.missCounts[ci] >= missThreshold {
				m.deleteRuntimeMetrics(ci, mi)
				delete(m.lastRuntimeMetrics, ci)
				delete(m.missCounts, ci)
			}
		}
	}
}
func (m *PromMetricsServer) resetHostMetricsIfNeeded() {
	if time.Now().After(m.lastHostMetricsResetTime.Add(metricsResetInterval)) {
		TensorCoreUtilizationProm.Reset()
		MemoryBandwidthUtilizationProm.Reset()
		TensorCoreUtilizationNodeProm.Reset()
		MemoryBandwidthUtilizationNodeProm.Reset()
		m.lastHostMetricsResetTime = time.Now()
	}
}

// Stop performs cleanup operations and stops the metric server.
func (m *PromMetricsServer) Stop() {
}

type promHistInfo struct {
	desc *prometheus.Desc
	data []promHistData
}

type promHistData struct {
	count       uint64
	sum         float64
	buckets     map[float64]uint64
	labelValues []string
}

type histCollector struct {
	dcnTransferLatenciesMetric          promHistInfo
	mxlaComputeLatenciesMetric          promHistInfo
	dcnInboundTransferLatenciesMetric   promHistInfo
	grpcClientCallLatenciesMetric       promHistInfo
	grpcServerCallLatenciesMetric       promHistInfo
	grpcTCPMinRttMetric                 promHistInfo
	grpcTCPDeliveryRateMetric           promHistInfo
	collectiveLatenciesMetric           promHistInfo
	hostToDeviceTransferLatenciesMetric promHistInfo
	deviceToHostTransferLatenciesMetric promHistInfo
	grpcTCPWriteSizesMetric             promHistInfo
	grpcTCPReadSizesMetric              promHistInfo
	grpcTCPSenderLatenciesMetric        promHistInfo
	grpcTCPTransferLatenciesMetric      promHistInfo
	dcnInboundTransferSizesMetric       promHistInfo
	dcnTransferSizesMetric              promHistInfo
	mxlaComputeOperandSizeMetric        promHistInfo
	collectiveInputSizesMetric          promHistInfo
	deviceToHostTransferSizesMetric     promHistInfo
	hostToDeviceTransferSizesMetric     promHistInfo
}

func newHistCollector() *histCollector {
	return &histCollector{
		dcnTransferLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"dcn_transfer_latencies_microsecond",
				"Distribution of network-transfer latencies for multislice traffic",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size", "type"},
				nil,
			),
			[]promHistData{},
		},
		mxlaComputeLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"compute_latencies_microsecond",
				"Host compute latency in microseconds. Measures the time it takes to compute a reduction operation in microseconds.",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size"},
				nil,
			),
			[]promHistData{},
		},
		dcnInboundTransferLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"dcn_inbound_transfer_latencies_microsecond",
				"Distribution of network-transfer latencies for inbound multislice traffic",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size", "type"},
				nil,
			),
			[]promHistData{},
		},
		grpcClientCallLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_client_call_latencies_microsecond",
				"Distribution of network-transfer latencies for the gRPC library, measuring the time it takes to complete an RPC from the application's perspective",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size"},
				nil,
			),
			[]promHistData{},
		},
		grpcServerCallLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_server_call_latencies_microsecond",
				"Distribution of network-transfer latencies for gRPC server to complete an RPC on transport’s perspective",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPMinRttMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_min_round_trip_times_microsecond",
				"Distribution of minimum network-transfer latencies per TCP connection",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPDeliveryRateMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_delivery_rates_Mbps",
				"Distribution of the TCP connections’ data transfer rates",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		collectiveLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"collective_end_to_end_latencies_microsecond",
				"Distribution of end to end collective latency for multislice traffic",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "input_size", "collective_type"},
				nil,
			),
			[]promHistData{},
		},
		hostToDeviceTransferLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"host_to_device_transfer_latencies_microsecond",
				"Distribution of host to device transfer latency for each chunk of data for multislice traffic",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size"},
				nil,
			),
			[]promHistData{},
		},
		deviceToHostTransferLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"device_to_host_transfer_latencies_microsecond",
				"Distribution of device to host transfer latency for each chunk of data for multislice traffic",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "buffer_size"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPWriteSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_write_sizes_bytes",
				"Distribution of TCP write sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPReadSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_read_sizes_bytes",
				"Distribution of TCP read sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPSenderLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_sender_latencies_microsecond",
				"Distribution of TCP sender latencies",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		grpcTCPTransferLatenciesMetric: promHistInfo{
			prometheus.NewDesc(
				"grpc_tcp_transfer_latencies_microsecond",
				"Distribution of TCP transfer latencies",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "transfer_size"},
				nil,
			),
			[]promHistData{},
		},
		dcnInboundTransferSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"dcn_inbound_transfer_sizes_bytes",
				"Distribution of network inbound transfer sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "type"},
				nil,
			),
			[]promHistData{},
		},
		dcnTransferSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"dcn_transfer_sizes_bytes",
				"Distribution of network transfer sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "type"},
				nil,
			),
			[]promHistData{},
		},
		mxlaComputeOperandSizeMetric: promHistInfo{
			prometheus.NewDesc(
				"mxla_compute_operand_sizes_bytes",
				"Distribution of compute operand size",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},

		collectiveInputSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"collective_input_sizes_bytes",
				"Distribution of collective input sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology", "collective_type", "execution_type"},
				nil,
			),
			[]promHistData{},
		},
		deviceToHostTransferSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"device_to_host_transfer_sizes_bytes",
				"Distribution of device to host transfer sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
		hostToDeviceTransferSizesMetric: promHistInfo{
			prometheus.NewDesc(
				"host_to_device_transfer_sizes_bytes",
				"Distribution of host to device transfer sizes",
				[]string{"namespace", "pod", "container", "make", "model", "tpu_topology"},
				nil,
			),
			[]promHistData{},
		},
	}
}

func (c *histCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.dcnTransferLatenciesMetric.desc
	ch <- c.mxlaComputeLatenciesMetric.desc
	ch <- c.dcnInboundTransferLatenciesMetric.desc
	ch <- c.grpcClientCallLatenciesMetric.desc
	ch <- c.grpcServerCallLatenciesMetric.desc
	ch <- c.grpcTCPMinRttMetric.desc
	ch <- c.grpcTCPDeliveryRateMetric.desc
	ch <- c.collectiveLatenciesMetric.desc
	ch <- c.hostToDeviceTransferLatenciesMetric.desc
	ch <- c.deviceToHostTransferLatenciesMetric.desc
	ch <- c.grpcTCPWriteSizesMetric.desc
	ch <- c.grpcTCPReadSizesMetric.desc
	ch <- c.grpcTCPSenderLatenciesMetric.desc
	ch <- c.grpcTCPTransferLatenciesMetric.desc
	ch <- c.dcnInboundTransferSizesMetric.desc
	ch <- c.dcnTransferSizesMetric.desc
	ch <- c.mxlaComputeOperandSizeMetric.desc
	ch <- c.collectiveInputSizesMetric.desc
	ch <- c.deviceToHostTransferSizesMetric.desc
	ch <- c.hostToDeviceTransferSizesMetric.desc
}

// Collect implements required collect function for all prometheus collectors
func (c *histCollector) Collect(ch chan<- prometheus.Metric) {
	for _, data := range c.dcnTransferLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.dcnTransferLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.mxlaComputeLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.mxlaComputeLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.dcnInboundTransferLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.dcnInboundTransferLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcClientCallLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcClientCallLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcServerCallLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcServerCallLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPMinRttMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPMinRttMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPDeliveryRateMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPDeliveryRateMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.collectiveLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.collectiveLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.hostToDeviceTransferLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.hostToDeviceTransferLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.deviceToHostTransferLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.deviceToHostTransferLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPWriteSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPWriteSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPReadSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPReadSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPSenderLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPSenderLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.grpcTCPTransferLatenciesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.grpcTCPTransferLatenciesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.dcnInboundTransferSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.dcnInboundTransferSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.dcnTransferSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.dcnTransferSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.mxlaComputeOperandSizeMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.mxlaComputeOperandSizeMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.collectiveInputSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.collectiveInputSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.deviceToHostTransferSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.deviceToHostTransferSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
	for _, data := range c.hostToDeviceTransferSizesMetric.data {
		ch <- prometheus.MustNewConstHistogram(c.hostToDeviceTransferSizesMetric.desc, data.count, data.sum, data.buckets, data.labelValues...)
	}
}

func (c *histCollector) UpdateValues(mi runtimeMetricsInfo, labelValues []string) {
	phds := []promHistData{}
	for _, di := range mi.dcnTransferLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"], di.attributes["type"])))
	}
	c.dcnTransferLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.mxlaComputeLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"])))
	}
	c.mxlaComputeLatenciesMetric.data = phds
	phds = []promHistData{}
	for _, di := range mi.dcnInboundTransferLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"], di.attributes["type"])))
	}
	c.dcnInboundTransferLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcClientCallLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"])))
	}
	c.grpcClientCallLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcServerCallLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"])))
	}
	c.grpcServerCallLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPMinRtt {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.grpcTCPMinRttMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPDeliveryRate {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.grpcTCPDeliveryRateMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.collectiveLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["input_size"], di.attributes["collective_type"])))
	}
	c.collectiveLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.hostToDeviceTransferLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"])))
	}
	c.hostToDeviceTransferLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.deviceToHostTransferLatencies {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["buffer_size"])))
	}
	c.deviceToHostTransferLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPWriteSize {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.grpcTCPWriteSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPReadSize {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.grpcTCPReadSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPSenderLatency {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.grpcTCPSenderLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.grpcTCPTransferLatency {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["transfer_size"])))
	}
	c.grpcTCPTransferLatenciesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.dcnInboundTransferSizes {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["type"])))
	}
	c.dcnInboundTransferSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.dcnTransferSizes {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["type"])))
	}
	c.dcnTransferSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.mxlaComputeOperandSize {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.mxlaComputeOperandSizeMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.collectiveInputSizes {
		phds = append(phds, toPromHistData(di, append(labelValues, di.attributes["collective_type"], di.attributes["execution_type"])))
	}
	c.collectiveInputSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.deviceToHostTransferSizes {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.deviceToHostTransferSizesMetric.data = phds

	phds = []promHistData{}
	for _, di := range mi.hostToDeviceTransferSizes {
		phds = append(phds, toPromHistData(di, labelValues))
	}
	c.hostToDeviceTransferSizesMetric.data = phds
}

func toPromHistData(di DistributionInfo, labelValues []string) promHistData {
	phd := promHistData{}
	phd.labelValues = labelValues
	phd.sum = di.data.Mean * float64(di.data.Count)
	phd.buckets = make(map[float64]uint64)
	var accumCount uint64
	for i := range di.bucketUpperBounds {
		accumCount += uint64(di.data.BucketCounts[i])
		phd.buckets[di.bucketUpperBounds[i]] = accumCount
	}
	phd.count = uint64(di.data.Count)
	return phd
}
