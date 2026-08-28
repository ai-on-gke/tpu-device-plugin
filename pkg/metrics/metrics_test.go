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
	"errors"
	"math"
	"slices"
	"sort"
	"testing"
	"time"
	"tpu-device-plugin/pkg/mocks"
	"tpu-device-plugin/pkg/tpu/util"

	"golang.org/x/time/rate"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	umpb "tpu-device-plugin/pkg/monitoring/proto/utilization_metrics_go_proto"
	pb "tpu-device-plugin/pkg/monitoring/runtime/proto/tpu_metric_service_go_proto"
)

var (
	mockMetricServer = NewMetricServer(
		time.Duration(10*time.Second),
		time.Duration(30*time.Second),
		time.Duration(30*time.Second),
		"test-node",
		"1234567890123456789",
		"tpu-v4-device",
		"runtime-metrics-port",
		"v4",
		"2x2x2",
		util.ContainerInfoExtractor{},
		"/metrics",
		2112,
		true,
		true,
		true,
		util.EnvInfo{
			ClusterName:      "cluster-name",
			ClusterProjectID: "1010101",
			ClusterLocation:  "us-central1-a",
			PodNamespace:     "kube-system",
			PodName:          "tpu-device-plugin",
			ContainerName:    "tpu-device-plugin",
		},
		nil,
		nil,
	)
)

func TestHostMetrics(t *testing.T) {
	testCases := []struct {
		desc                                  string
		mockTensorcoreUtilizationReponse      map[string]float64
		mockMemoryBandwidthUtilizationReponse map[string]float64
		wantErr                               bool
		wantHostMetricsInfo                   hostMetricsInfo
	}{
		{
			desc:                                  "Normal host metrics",
			mockTensorcoreUtilizationReponse:      map[string]float64{"1": float64(0)},
			mockMemoryBandwidthUtilizationReponse: map[string]float64{"1": float64(2)},
			wantErr:                               false,
			wantHostMetricsInfo: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{"1": 0},
				memoryBandwidthUtilization: map[string]float64{"1": 2},
			},
		},
		{
			desc:                                  "Empty runtime metrics",
			mockTensorcoreUtilizationReponse:      nil,
			mockMemoryBandwidthUtilizationReponse: nil,
			wantErr:                               false,
			wantHostMetricsInfo: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{},
				memoryBandwidthUtilization: map[string]float64{},
			},
		},
	}
	for _, tc := range testCases {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mocks.NewMockHostMetricsClient(ctrl)

		mockClient.EXPECT().UtilizationPercentagePerDevice(gomock.Any(), umpb.UtilizationMetricType_TENSORCORE_UTILIZATION).Return(tc.mockTensorcoreUtilizationReponse, nil)
		mockClient.EXPECT().UtilizationPercentagePerDevice(gomock.Any(), umpb.UtilizationMetricType_HBM_UTILIZATION).Return(tc.mockMemoryBandwidthUtilizationReponse, nil)

		hostMetricsInfo, err := mockMetricServer.hostMetrics(mockClient)

		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: Wanted error but didn't get any", tc.desc)
			}
		} else {
			if err != nil {
				t.Errorf("%q: Unexpected error: %v", tc.desc, err)
			} else {
				assert.Equal(t, hostMetricsInfo.memoryBandwidthUtilization, tc.wantHostMetricsInfo.memoryBandwidthUtilization)
				assert.Equal(t, hostMetricsInfo.tensorcoreUtilization, tc.wantHostMetricsInfo.tensorcoreUtilization)
			}
		}
	}
}

func TestGetMetricIfSupported(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetricServer := buildMetricServer("test-node", nil)
	ctx := context.Background()
	supportedMetrics := map[string]bool{"metric1": true, "metric2": true}
	metricName1 := "metric1"
	metricName2 := "metric2"
	expectedResponse := &pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: metricName1}}}
	fakeError := errors.New("fake error")

	testCases := []struct {
		desc         string
		metricName   string
		mockResponse *pb.MetricResponse
		mockError    error
		wantResponse *pb.MetricResponse
		wantLimiter  bool
	}{
		{
			desc:         "successful metric fetch",
			metricName:   metricName1,
			mockResponse: expectedResponse,
			mockError:    nil,
			wantResponse: expectedResponse,
			wantLimiter:  false,
		},
		{
			desc:         "failed metric fetch",
			metricName:   metricName1,
			mockResponse: nil,
			mockError:    fakeError,
			wantResponse: &pb.MetricResponse{},
			wantLimiter:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mockRuntimeClient := mocks.NewMockRuntimeClient(ctrl)
			mockMetricServer.warningRateLimiters = make(map[string]*rate.Limiter)
			mockRuntimeClient.EXPECT().GetRuntimeMetric(gomock.Any(), gomock.Any(), gomock.Any()).Return(tc.mockResponse, tc.mockError)

			response := mockMetricServer.getMetricIfSupported(ctx, mockRuntimeClient, tc.metricName, supportedMetrics)

			assert.Equal(t, tc.wantResponse, response)
			_, exists := mockMetricServer.warningRateLimiters[tc.metricName]
			assert.Equal(t, tc.wantLimiter, exists)
			if tc.wantLimiter {
				limiter := mockMetricServer.warningRateLimiters[tc.metricName]
				assert.False(t, limiter.Allow(), "subsequent call to Allow should be false")
			}
		})
	}

	t.Run("subsequent failed metric fetches within rate limit do not log warnings", func(t *testing.T) {
		mockRuntimeClient := mocks.NewMockRuntimeClient(ctrl)
		mockMetricServer.warningRateLimiters = make(map[string]*rate.Limiter)
		mockRuntimeClient.EXPECT().GetRuntimeMetric(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fakeError).Times(2)

		// First call, should log (limiter allows)
		mockMetricServer.getMetricIfSupported(ctx, mockRuntimeClient, metricName1, supportedMetrics)
		limiter, exists := mockMetricServer.warningRateLimiters[metricName1]
		assert.True(t, exists)

		// Second call, should not log (limiter denies)
		mockMetricServer.getMetricIfSupported(ctx, mockRuntimeClient, metricName1, supportedMetrics)

		// The limiter's Allow() should still be false.
		assert.False(t, limiter.Allow(), "subsequent call to Allow should be false")
	})

	t.Run("different failing metrics are rate limited independently", func(t *testing.T) {
		mockRuntimeClient := mocks.NewMockRuntimeClient(ctrl)
		mockMetricServer.warningRateLimiters = make(map[string]*rate.Limiter)
		mockRuntimeClient.EXPECT().GetRuntimeMetric(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fakeError).Times(2)

		// First metric fails
		mockMetricServer.getMetricIfSupported(ctx, mockRuntimeClient, metricName1, supportedMetrics)
		// Second metric fails
		mockMetricServer.getMetricIfSupported(ctx, mockRuntimeClient, metricName2, supportedMetrics)

		limiter1, exists1 := mockMetricServer.warningRateLimiters[metricName1]
		assert.True(t, exists1)
		assert.False(t, limiter1.Allow(), "subsequent call to Allow for metric1 should be false")

		limiter2, exists2 := mockMetricServer.warningRateLimiters[metricName2]
		assert.True(t, exists2)
		assert.False(t, limiter2.Allow(), "subsequent call to Allow for metric2 should be false")
	})
}

func TestRuntimeMetrics(t *testing.T) {
	orderedMetricNames := []string{
		memoryTotalMetricsName,
		memoryUsedMetricsName,
		dutyCycleMetricsName,
		dcnTransferLatenciesName,
		mxlaComputeLatenciesName,
		dcnInboundTransferLatenciesName,
		grpcClientCallLatenciesMetricName,
		grpcServerCallLatenciesMetricName,
		grpcTCPMinRttMetricName,
		grpcTCPDeliveryRateMetricName,
		grpcTCPPacketsSentMetricName,
		grpcTCPPacketsRetransmittedMetricName,
		grpcTCPPacketsSpuriousRetransmittedMetricName,
		grpcTCPRecurringRetransmitsCollectorMetricName,
		grpcTCPBytesSentCollectorMetricName,
		grpcTCPBytesRetransmittedCollectorMetricName,
		grpcTCPSyscallWritesCollectorMetricName,
		grpcTCPSyscallReadsCollectorMetricName,
		megascaleBamm2BitsSentMetricName,
		megascaleBamm2BitsReceivedMetricName,
		megascaleBamm2BitsReadMetricName,
		megascaleBamm2BitsWrittenMetricName,
		megascaleBamm2BitsWrittenWithImmMetricName,
		grpcTCPWriteSizeCollectorMetricName,
		grpcTCPReadSizeCollectorMetricName,
		grpcTCPSenderLatencyCollectorMetricName,
		grpcTCPTransferLatencyCollectorMetricName,
		collectiveLatenciesName,
		hostToDeviceTransferLatenciesName,
		deviceToHostTransferLatenciesName,
		dcnInboundTransferSizesCollectorMetricName,
		dcnTransferSizesCollectorMetricName,
		mxlaComputeOperandSizeCollectorMetricName,
		collectiveInputSizesCollectorMetricName,
		deviceToHostTransferSizesCollectorMetricName,
		hostToDeviceTransferSizesCollectorMetricName,
		mlRuntimeUptimeMetricsName,
		megascaleErrorDetectedMetricsName,
		sliceErrorDetectedMetricsName,
	}
	start_timestamp_pb := timestamppb.Now()
	start_timestamp := start_timestamp_pb.AsTime()
	timestamp := start_timestamp.Add(time.Second)

	nowTime := time.Now()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRealTimeProvider := mocks.NewMockRealTimeProvider(ctrl)
	mockRealTimeProvider.EXPECT().Now().AnyTimes().Return(nowTime)

	testCases := []struct {
		desc                                           string
		mockListSupportedMetricsReponse                []string
		mockMemoryTotalMetrics                         []*pb.Metric
		mockMemoryUsedMetrics                          []*pb.Metric
		mockRuntimeDutyCycleMetrics                    []*pb.Metric
		mockDcnTransferLatenciesMetrics                []*pb.Metric
		mockMxlaComputeLatenciesMetrics                []*pb.Metric
		mockDcnInboundTransferLatenciesMetrics         []*pb.Metric
		mockGrpcClientCallLatenciesMetrics             []*pb.Metric
		mockGrpcServerCallLatenciesMetrics             []*pb.Metric
		mockGrpcTCPMinRttMetrics                       []*pb.Metric
		mockGrpcTCPDeliveryRateMetrics                 []*pb.Metric
		mockGrpcTCPPacketsSentMetrics                  []*pb.Metric
		mockGrpcTCPPacketsRetransmittedMetrics         []*pb.Metric
		mockGrpcTCPPacketsSpuriousRetransmittedMetrics []*pb.Metric
		mockGrpcTCPRecurringRetransmitsMetrics         []*pb.Metric
		mockGrpcTCPBytesSentMetrics                    []*pb.Metric
		mockBamm2BitsSentMetrics                       []*pb.Metric
		mockBamm2BitsReceivedMetrics                   []*pb.Metric
		mockBamm2BitsReadMetrics                       []*pb.Metric
		mockBamm2BitsWrittenMetrics                    []*pb.Metric
		mockBamm2BitsWrittenWithImmMetrics             []*pb.Metric
		mockGrpcTCPBytesRetransmittedMetrics           []*pb.Metric
		mockGrpcTCPSyscallWritesMetrics                []*pb.Metric
		mockGrpcTCPSyscallReadsMetrics                 []*pb.Metric
		mockGrpcTCPWriteSizeMetrics                    []*pb.Metric
		mockGrpcTCPReadSizeMetrics                     []*pb.Metric
		mockGrpcTCPSenderLatencyMetrics                []*pb.Metric
		mockGrpcTCPTransferLatencyMetrics              []*pb.Metric
		mockCollectiveLatenciesMetrics                 []*pb.Metric
		mockDeviceToHostTransferLatenciesMetrics       []*pb.Metric
		mockHostToDeviceTransferLatenciesMetrics       []*pb.Metric
		mockMlRuntimeUptimeMetrics                     []*pb.Metric
		mockMegascaleErrorDetectedMetrics              []*pb.Metric
		mockSliceErrorDetectionMetrics                 []*pb.Metric
		mockDcnInboundTransferSizesMetrics             []*pb.Metric
		mockDcnTransferSizesMetrics                    []*pb.Metric
		mockMxlaComputeOperandSizeMetrics              []*pb.Metric
		mockCollectiveInputSizesMetrics                []*pb.Metric
		mockDeviceToHostTransferSizesMetrics           []*pb.Metric
		mockHostToDeviceTransferSizesMetrics           []*pb.Metric
		wantErr                                        bool
		wantRuntimeInfo                                runtimeMetricsInfo
	}{
		{
			desc: "Happy path",
			mockListSupportedMetricsReponse: []string{
				"tpu.runtime.hbm.memory.total.bytes",
				"tpu.runtime.hbm.memory.usage.bytes",
				"tpu.runtime.tensorcore.dutycycle.percent",
				"megascale.dcn_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.mxla_compute_latencies.microsecond.cumulative.distribution",
				"megascale.dcn_inbound_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.grpc_client_call_latencies.microsecond.cumulative.distribution",
				"megascale.grpc_server_call_latencies.microsecond.cumulative.distribution",
				"megascale.grpc_tcp_min_rtt.microsecond.cumulative.distribution",
				"megascale.grpc_tcp_delivery_rate.Mbps.cumulative.distribution",
				"megascale.grpc_tcp_packets_sent.cumulative.count",
				"megascale.grpc_tcp_packets_retransmitted.cumulative.count",
				"megascale.grpc_tcp_packets_spurious_retransmitted.cumulative.count",
				"megascale.grpc_tcp_recurring_retransmits.cumulative.count",
				"megascale.grpc_tcp_bytes_sent.cumulative.count",
				"megascale.bamm2_bits_sent.cumulative.count",
				"megascale.bamm2_bits_received.cumulative.count",
				"megascale.bamm2_bits_read.cumulative.count",
				"megascale.bamm2_bits_written.cumulative.count",
				"megascale.bamm2_bits_written_with_imm.cumulative.count",
				"megascale.grpc_tcp_bytes_retransmitted.cumulative.count",
				"megascale.grpc_tcp_syscall_writes.cumulative.count",
				"megascale.grpc_tcp_syscall_reads.cumulative.count",
				"megascale.grpc_tcp_write_size.cumulative.distribution",
				"megascale.grpc_tcp_read_size.cumulative.distribution",
				"megascale.grpc_tcp_sender_latency.microsecond.cumulative.distribution",
				"megascale.grpc_tcp_transfer_latency.microsecond.cumulative.distribution",
				"megascale.collective_end_to_end_latencies.microsecond.cumulative.distribution",
				"megascale.host_to_device_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.device_to_host_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.dcn_inbound_transfer_size.bytes.cumulative.distribution",
				"megascale.dcn_transfer_size.bytes.cumulative.distribution",
				"megascale.mxla_compute_operand_size.bytes.cumulative.distribution",
				"megascale.collective_input_size.bytes.cumulative.distribution",
				"megascale.device_to_host_transfer_size.bytes.cumulative.distribution",
				"megascale.host_to_device_transfer_size.bytes.cumulative.distribution",
				"tpu.runtime.uptime.seconds.gauge",
				"megascale.error.detected.gauge",
				"slice.error.detected.gauge",
			},
			mockMemoryTotalMetrics:      buildRuntimeResponse(1, 2),
			mockMemoryUsedMetrics:       buildRuntimeResponse(1, 2),
			mockRuntimeDutyCycleMetrics: buildRuntimeResponse(1, 0),
			mockDcnTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+", "type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+", "type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+", "type": "grpc"}),
				},
			},
			mockMxlaComputeLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+"}),
				},
			},
			mockDcnInboundTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+", "type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+", "type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+", "type": "grpc"}),
				},
			},
			mockGrpcClientCallLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+"}),
				},
			},
			mockGrpcServerCallLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+"}),
				},
			},
			mockGrpcTCPMinRttMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPDeliveryRateMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPPacketsSentMetrics:                  buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPPacketsRetransmittedMetrics:         buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPPacketsSpuriousRetransmittedMetrics: buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPRecurringRetransmitsMetrics:         buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPBytesSentMetrics:                    buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockBamm2BitsSentMetrics:                       buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockBamm2BitsReceivedMetrics:                   buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockBamm2BitsReadMetrics:                       buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockBamm2BitsWrittenMetrics:                    buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockBamm2BitsWrittenWithImmMetrics:             buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPBytesRetransmittedMetrics:           buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPSyscallWritesMetrics:                buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPSyscallReadsMetrics:                 buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPWriteSizeMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPReadSizeMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPSenderLatencyMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPTransferLatencyMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttributeWithInts(map[string]string{}, map[string]int64{"transfer_size": 512}),
				},
			},
			mockCollectiveLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6}, 4),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "512B+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "4MB+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "8MB+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "16MB+", "collective_type": "grpc"}),
				},
			},
			mockDeviceToHostTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6, 7}, 5),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
			},
			mockHostToDeviceTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6, 7, 8}, 6),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
			},

			mockDcnInboundTransferSizesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"type": "grpc"}),
				},
			},
			mockDcnTransferSizesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"type": "grpc"}),
				},
			},

			mockMxlaComputeOperandSizeMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},

			mockCollectiveInputSizesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"collective_type": "grpc", "execution_type": "th"}),
				},
			},

			mockDeviceToHostTransferSizesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},

			mockHostToDeviceTransferSizesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},

			mockMlRuntimeUptimeMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(map[string]string{"ml_framework_name": "fake_framework", "ml_framework_version": "x.y.z"}, 10),
				buildMlRuntimeUptimeResponse(map[string]string{"ml_framework_version": "x.y.z"}, 10),
				buildMlRuntimeUptimeResponse(map[string]string{"ml_framework_name": "fake_framework"}, 10),
			},
			mockMegascaleErrorDetectedMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(
					map[string]string{
						"error_type": "HANG_DETECTED",
						"host_name":  "gke-tpu-abc-123",
						"launch_id":  "123",
						"start_time": "2025-01-01 01:02:03 UTC",
					},
					1,
				),
			},
			mockSliceErrorDetectionMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(
					map[string]string{
						"error_message": "slice error",
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
					},
					1,
				),
			},
			wantErr: false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:      map[string]int64{"1": 2},
				memoryUsed:       map[string]int64{"1": 2},
				runtimeDutyCycle: map[string]float64{"1": 0},
				dcnTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+", "type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+", "type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "2MB+", "type": "grpc"},
					},
				},
				mxlaComputeLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "2MB+"},
					},
				},
				dcnInboundTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+", "type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+", "type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "2MB+", "type": "grpc"},
					},
				},
				grpcClientCallLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "2MB+"},
					},
				},
				grpcServerCallLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "2MB+"},
					},
				},
				grpcTCPMinRtt: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				grpcTCPDeliveryRate: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPRecurringRetransmits:         map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPBytesSent:                    map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				megascaleBamm2BitsSent:              map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				megascaleBamm2BitsReceived:          map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				megascaleBamm2BitsRead:              map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				megascaleBamm2BitsWritten:           map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				megascaleBamm2BitsWrittenWithImm:    map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPBytesRetransmitted:           map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPSyscallWrites:                map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPSyscallReads:                 map[string]CumulativeCounterInfo{"1": {start_timestamp, timestamp, 2}},
				grpcTCPWriteSize: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				grpcTCPReadSize: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				grpcTCPSenderLatency: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				grpcTCPTransferLatency: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"transfer_size": "512"},
					},
				},

				dcnInboundTransferSizes: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"type": "grpc"},
					},
				},
				dcnTransferSizes: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"type": "grpc"},
					},
				},

				mxlaComputeOperandSize: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},

				collectiveInputSizes: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"collective_type": "grpc", "execution_type": "th"},
					},
				},

				deviceToHostTransferSizes: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},

				hostToDeviceTransferSizes: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{},
					},
				},
				collectiveLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, math.Inf(1)},
						attributes:        map[string]string{"input_size": "512B+", "collective_type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"input_size": "4MB+", "collective_type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"input_size": "8MB+", "collective_type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"input_size": "16MB+", "collective_type": "grpc"},
					},
				},
				deviceToHostTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6, 7},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, 32, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+"},
					},
				},
				hostToDeviceTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6, 7, 8},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, 32, 64, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "512B+"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1MB+"},
					},
				},
				mlRuntimeUptimeKvlist: []KvlistAttributesMetricInfo{
					{
						attributes: map[string]string{"ml_framework_name": "fake_framework", "ml_framework_version": "x.y.z"},
						value:      10,
					},
					{
						attributes: map[string]string{"ml_framework_version": "x.y.z"},
						value:      10,
					},
					{
						attributes: map[string]string{"ml_framework_name": "fake_framework"},
						value:      10,
					},
				},
				megascaleErrorDetectedKvlist: []KvlistAttributesMetricInfo{
					{
						attributes: map[string]string{
							"error_type": "HANG_DETECTED",
							"host_name":  "gke-tpu-abc-123",
							"launch_id":  "123",
							"start_time": "2025-01-01 01:02:03 UTC",
						},
						value: 1,
					},
				},
				sliceErrorDetectedKvlist: []KvlistAttributesMetricInfo{
					{
						attributes: map[string]string{
							"error_message": "slice error",
							"session_id":    "123",
							"type":          "test",
							"topology":      "2x2",
						},
						value: 1,
					},
				},
			},
		},
		{
			desc: "Collective latencies with mixed attributes",
			mockListSupportedMetricsReponse: []string{
				"megascale.collective_end_to_end_latencies.microsecond.cumulative.distribution",
			},
			mockCollectiveLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6}, 4),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "512B+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "4MB+", "collective_type": "grpc", "execution_type": "test"}),
				},
			},
			wantErr: false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{},
				memoryUsed:                          map[string]int64{},
				runtimeDutyCycle:                    map[string]float64{},
				dcnTransferLatencies:                []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, math.Inf(1)},
						attributes:        map[string]string{"input_size": "512B+", "collective_type": "grpc"},
					},
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"input_size": "4MB+", "collective_type": "grpc", "execution_type": "test"},
					},
				},
				hostToDeviceTransferLatencies: []DistributionInfo{},
				deviceToHostTransferLatencies: []DistributionInfo{},
				mlRuntimeUptimeKvlist:         []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:  []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:      []KvlistAttributesMetricInfo{},
			},
		},
		{
			desc:                            "Bad attribute settings",
			mockListSupportedMetricsReponse: []string{},
			mockMemoryTotalMetrics:          buildRuntimeResponse(1, 2),
			mockMemoryUsedMetrics:           buildRuntimeResponse(1, 2),
			mockRuntimeDutyCycleMetrics:     buildRuntimeResponse(1, 0),
			mockDcnTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+", "type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2MB+", "type": "grpc"}),
				},
			},
			mockGrpcClientCallLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcServerCallLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPMinRttMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "2B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
			},
			mockGrpcTCPDeliveryRateMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "512B+"}),
				},
			},
			mockGrpcTCPPacketsSentMetrics:                  buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPPacketsRetransmittedMetrics:         buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPPacketsSpuriousRetransmittedMetrics: buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPRecurringRetransmitsMetrics:         buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPBytesSentMetrics:                    buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPBytesRetransmittedMetrics:           buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPSyscallWritesMetrics:                buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPSyscallReadsMetrics:                 buildCumulativeRuntimeResponse(1, 2, start_timestamp, timestamp),
			mockGrpcTCPWriteSizeMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPReadSizeMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPSenderLatencyMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttribute(map[string]string{}),
				},
			},
			mockGrpcTCPTransferLatencyMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5}, 3),
					Attribute:      buildMXLAAttributeWithInts(map[string]string{}, map[string]int64{"transfer_size": 512}),
				},
			},
			mockCollectiveLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6}, 4),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "512B+"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "4MB+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "8MB+", "collective_type": "grpc"}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2}, 0),
					Attribute:      buildMXLAAttribute(map[string]string{"input_size": "16MB+", "collective_type": "grpc"}),
				},
			},
			mockDeviceToHostTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6, 7}, 5),
					Attribute:      buildMXLAAttribute(map[string]string{"type": "grpc"}),
				},
			},
			mockHostToDeviceTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4, 5, 6, 7, 8}, 6),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": ""}),
				},
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{1, 2, 3, 4}, 2),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1MB+"}),
				},
			},
			mockMlRuntimeUptimeMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(map[string]string{"ml_framework_name_bad_key": "fake_framework", "ml_framework_version_bad_key": "x.y.z"}, 10),
			},
			mockMegascaleErrorDetectedMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(map[string]string{}, 1),
			},
			mockSliceErrorDetectionMetrics: []*pb.Metric{
				buildMlRuntimeUptimeResponse(map[string]string{}, 1),
			},
			wantErr: false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{},
				memoryUsed:                          map[string]int64{},
				runtimeDutyCycle:                    map[string]float64{},
				dcnTransferLatencies:                []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies:                 []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				mlRuntimeUptimeKvlist:               []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:        []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:            []KvlistAttributesMetricInfo{},
			},
		},
		{
			desc:                                           "Empty runtime metrics",
			mockListSupportedMetricsReponse:                []string{},
			mockMemoryTotalMetrics:                         nil,
			mockMemoryUsedMetrics:                          nil,
			mockRuntimeDutyCycleMetrics:                    nil,
			mockDcnTransferLatenciesMetrics:                nil,
			mockMxlaComputeLatenciesMetrics:                nil,
			mockDcnInboundTransferLatenciesMetrics:         nil,
			mockGrpcClientCallLatenciesMetrics:             nil,
			mockGrpcServerCallLatenciesMetrics:             nil,
			mockGrpcTCPMinRttMetrics:                       nil,
			mockGrpcTCPDeliveryRateMetrics:                 nil,
			mockGrpcTCPPacketsSentMetrics:                  nil,
			mockGrpcTCPPacketsRetransmittedMetrics:         nil,
			mockGrpcTCPPacketsSpuriousRetransmittedMetrics: nil,
			mockDeviceToHostTransferLatenciesMetrics:       nil,
			mockHostToDeviceTransferLatenciesMetrics:       nil,
			mockMlRuntimeUptimeMetrics:                     nil,
			mockMegascaleErrorDetectedMetrics:              nil,
			mockSliceErrorDetectionMetrics:                 nil,
			wantErr:                                        false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{},
				memoryUsed:                          map[string]int64{},
				runtimeDutyCycle:                    map[string]float64{},
				dcnTransferLatencies:                []DistributionInfo{},
				mxlaComputeLatencies:                []DistributionInfo{},
				dcnInboundTransferLatencies:         []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies:                 []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				mlRuntimeUptimeKvlist:               []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:        []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:            []KvlistAttributesMetricInfo{},
				dcnInboundTransferSizes:             []DistributionInfo{},
				dcnTransferSizes:                    []DistributionInfo{},
				mxlaComputeOperandSize:              []DistributionInfo{},
				collectiveInputSizes:                []DistributionInfo{},
				deviceToHostTransferSizes:           []DistributionInfo{},
				hostToDeviceTransferSizes:           []DistributionInfo{},
			},
		},
		{
			desc: "Partial metrics supported",
			mockListSupportedMetricsReponse: []string{
				"tpu.runtime.hbm.memory.total.bytes",
				"tpu.runtime.tensorcore.dutycycle.percent",
			},
			mockMemoryTotalMetrics:      buildRuntimeResponse(1, 2),
			mockRuntimeDutyCycleMetrics: buildRuntimeResponse(1, 0),
			wantErr:                     false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{"1": 2},
				memoryUsed:                          map[string]int64{},
				runtimeDutyCycle:                    map[string]float64{"1": 0},
				dcnTransferLatencies:                []DistributionInfo{},
				mxlaComputeLatencies:                []DistributionInfo{},
				dcnInboundTransferLatencies:         []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies:                 []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				mlRuntimeUptimeKvlist:               []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:        []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:            []KvlistAttributesMetricInfo{},
				dcnInboundTransferSizes:             []DistributionInfo{},
				dcnTransferSizes:                    []DistributionInfo{},
				mxlaComputeOperandSize:              []DistributionInfo{},
				collectiveInputSizes:                []DistributionInfo{},
				deviceToHostTransferSizes:           []DistributionInfo{},
				hostToDeviceTransferSizes:           []DistributionInfo{},
			},
		},
		{
			desc: "Another partial metrics supported",
			mockListSupportedMetricsReponse: []string{
				"tpu.runtime.hbm.memory.usage.bytes",
				"megascale.dcn_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.mxla_compute_latencies.microsecond.cumulative.distribution",
				"megascale.dcn_inbound_transfer_latencies.microsecond.cumulative.distribution",
			},
			mockMemoryUsedMetrics: buildRuntimeResponse(1, 5),
			mockDcnTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1KB", "type": "rdma"}),
				},
			},
			mockMxlaComputeLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1KB"}),
				},
			},
			mockDcnInboundTransferLatenciesMetrics: []*pb.Metric{
				{
					StartTimestamp: start_timestamp_pb,
					Measure:        buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute:      buildMXLAAttribute(map[string]string{"buffer_size": "1KB", "type": "rdma"}),
				},
			},
			wantErr: false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:      map[string]int64{},
				memoryUsed:       map[string]int64{"1": 5},
				runtimeDutyCycle: map[string]float64{},
				dcnTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB", "type": "rdma"},
					},
				},
				mxlaComputeLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB"},
					},
				},
				dcnInboundTransferLatencies: []DistributionInfo{
					{
						start_time: start_timestamp,
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB", "type": "rdma"},
					},
				},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies:                 []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				mlRuntimeUptimeKvlist:               []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:        []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:            []KvlistAttributesMetricInfo{},
				dcnInboundTransferSizes:             []DistributionInfo{},
				dcnTransferSizes:                    []DistributionInfo{},
				mxlaComputeOperandSize:              []DistributionInfo{},
				collectiveInputSizes:                []DistributionInfo{},
				deviceToHostTransferSizes:           []DistributionInfo{},
				hostToDeviceTransferSizes:           []DistributionInfo{},
			},
		},
		{
			desc: "Start Timestamps not present",
			mockListSupportedMetricsReponse: []string{
				"megascale.dcn_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.mxla_compute_latencies.microsecond.cumulative.distribution",
				"megascale.dcn_inbound_transfer_latencies.microsecond.cumulative.distribution",
				"megascale.grpc_tcp_packets_sent.cumulative.count",
			},
			mockDcnTransferLatenciesMetrics: []*pb.Metric{
				{
					Measure:   buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute: buildMXLAAttribute(map[string]string{"buffer_size": "1KB", "type": "rdma"}),
				},
			},
			mockMxlaComputeLatenciesMetrics: []*pb.Metric{
				{
					Measure:   buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute: buildMXLAAttribute(map[string]string{"buffer_size": "1KB"}),
				},
			},
			mockDcnInboundTransferLatenciesMetrics: []*pb.Metric{
				{
					Measure:   buildDistMeasure([]int64{10, 20, 5}, 1),
					Attribute: buildMXLAAttribute(map[string]string{"buffer_size": "1KB", "type": "rdma"}),
				},
			},
			mockGrpcTCPPacketsSentMetrics: buildCumulativeRuntimeResponseWithoutTimestamps(1, 2),
			wantErr:                       false,
			wantRuntimeInfo: runtimeMetricsInfo{
				memoryTotal:      map[string]int64{},
				memoryUsed:       map[string]int64{},
				runtimeDutyCycle: map[string]float64{},
				dcnTransferLatencies: []DistributionInfo{
					{
						start_time: nowTime.Add(-1 * time.Microsecond),
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB", "type": "rdma"},
					},
				},
				mxlaComputeLatencies: []DistributionInfo{
					{
						start_time: nowTime.Add(-1 * time.Microsecond),
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB"},
					},
				},
				dcnInboundTransferLatencies: []DistributionInfo{
					{
						start_time: nowTime.Add(-1 * time.Microsecond),
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{10, 20, 5},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"buffer_size": "1KB", "type": "rdma"},
					},
				},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{"1": {mockMetricServer.lastTimeGCMExport, nowTime, 2}},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{},
				collectiveLatencies:                 []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				mlRuntimeUptimeKvlist:               []KvlistAttributesMetricInfo{},
				megascaleErrorDetectedKvlist:        []KvlistAttributesMetricInfo{},
				sliceErrorDetectedKvlist:            []KvlistAttributesMetricInfo{},
				dcnInboundTransferSizes:             []DistributionInfo{},
				dcnTransferSizes:                    []DistributionInfo{},
				mxlaComputeOperandSize:              []DistributionInfo{},
				collectiveInputSizes:                []DistributionInfo{},
				deviceToHostTransferSizes:           []DistributionInfo{},
				hostToDeviceTransferSizes:           []DistributionInfo{},
			},
		},
		{
			desc:    "ListSupportedMetrics returns error",
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Logf("Running testcase: %s", tc.desc)
		fakeReponseMemoryTotal := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "tpu.runtime.hbm.memory.total.bytes", Metrics: tc.mockMemoryTotalMetrics}}}
		fakeReponseMemoryUsage := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "tpu.runtime.hbm.memory.usage.bytes", Metrics: tc.mockMemoryUsedMetrics}}}
		fakeReponseDutyCycle := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "tpu.runtime.tensorcore.dutycycle.percent", Metrics: tc.mockRuntimeDutyCycleMetrics}}}
		fakeReponseDcnTransferLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.dcn_transfer_latencies.microsecond.cumulative.distribution", Metrics: tc.mockDcnTransferLatenciesMetrics}}}
		fakeReponseMxlaComputeLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.mxla_compute_latencies.microsecond.cumulative.distribution", Metrics: tc.mockMxlaComputeLatenciesMetrics}}}
		fakeReponseDcnInboundTransferLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.dcn_inbound_transfer_latencies.microsecond.cumulative.distribution", Metrics: tc.mockDcnInboundTransferLatenciesMetrics}}}
		fakeReponseGrpcClientCallLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_client_call_latencies.microsecond.cumulative.distribution", Metrics: tc.mockGrpcClientCallLatenciesMetrics}}}
		fakeReponseGrpcServerCallLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_server_call_latencies.microsecond.cumulative.distribution", Metrics: tc.mockGrpcServerCallLatenciesMetrics}}}
		fakeReponseGrpcTCPMinRtt := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_min_rtt.microsecond.cumulative.distribution", Metrics: tc.mockGrpcTCPMinRttMetrics}}}
		fakeReponseGrpcTCPDeliveryRate := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_delivery_rate.Mbps.cumulative.distribution", Metrics: tc.mockGrpcTCPDeliveryRateMetrics}}}
		fakeReponseGrpcTCPPacketsSent := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_packets_sent.cumulative.count", Metrics: tc.mockGrpcTCPPacketsSentMetrics}}}
		fakeReponseGrpcTCPPacketsRetransmitted := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_packets_retransmitted.cumulative.count", Metrics: tc.mockGrpcTCPPacketsRetransmittedMetrics}}}
		fakeReponseGrpcTCPPacketsSpuriousRetransmitted := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_packets_spurious_retransmitted.cumulative.count", Metrics: tc.mockGrpcTCPPacketsSpuriousRetransmittedMetrics}}}
		fakeReponseGrpcTCPRecurringRetransmits := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_recurring_retransmits.cumulative.count", Metrics: tc.mockGrpcTCPRecurringRetransmitsMetrics}}}
		fakeReponseGrpcTCPBytesSent := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_bytes_sent.cumulative.count", Metrics: tc.mockGrpcTCPBytesSentMetrics}}}
		fakeReponseBamm2BitsSent := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.bamm2_bits_sent.cumulative.count", Metrics: tc.mockBamm2BitsSentMetrics}}}
		fakeReponseBamm2BitsReceived := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.bamm2_bits_received.cumulative.count", Metrics: tc.mockBamm2BitsReceivedMetrics}}}
		fakeReponseBamm2BitsRead := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.bamm2_bits_read.cumulative.count", Metrics: tc.mockBamm2BitsReadMetrics}}}
		fakeReponseBamm2BitsWritten := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.bamm2_bits_written.cumulative.count", Metrics: tc.mockBamm2BitsWrittenMetrics}}}
		fakeReponseBamm2BitsWrittenWithImm := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.bamm2_bits_written_with_imm.cumulative.count", Metrics: tc.mockBamm2BitsWrittenWithImmMetrics}}}
		fakeReponseGrpcTCPBytesRetransmitted := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_bytes_retransmitted.cumulative.count", Metrics: tc.mockGrpcTCPBytesRetransmittedMetrics}}}
		fakeReponseGrpcTCPSyscallWrites := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_syscall_writes.cumulative.count", Metrics: tc.mockGrpcTCPSyscallWritesMetrics}}}
		fakeReponseGrpcTCPSyscallReads := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_syscall_reads.cumulative.count", Metrics: tc.mockGrpcTCPSyscallReadsMetrics}}}
		fakeReponseGrpcTCPWriteSize := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_write_size.cumulative.distribution", Metrics: tc.mockGrpcTCPWriteSizeMetrics}}}
		fakeReponseGrpcTCPReadSize := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_read_size.cumulative.distribution", Metrics: tc.mockGrpcTCPReadSizeMetrics}}}
		fakeReponseGrpcTCPSenderLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_sender_latency.microsecond.cumulative.distribution", Metrics: tc.mockGrpcTCPSenderLatencyMetrics}}}
		fakeReponseGrpcTCPTransferLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.grpc_tcp_transfer_latency.microsecond.cumulative.distribution", Metrics: tc.mockGrpcTCPTransferLatencyMetrics}}}
		fakeReponseCollectiveLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.collective_end_to_end_latencies.microsecond.cumulative.distribution", Metrics: tc.mockCollectiveLatenciesMetrics}}}
		fakeReponseHostToDeviceTransferLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.host_to_device_transfer_latencies.microsecond.cumulative.distribution", Metrics: tc.mockHostToDeviceTransferLatenciesMetrics}}}
		fakeReponseDeviceToHostTransferLatency := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.device_to_host_transfer_latencies.microsecond.cumulative.distribution", Metrics: tc.mockDeviceToHostTransferLatenciesMetrics}}}
		fakeReponseDcnInboundTransferSizes := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.dcn_inbound_transfer_size.bytes.cumulative.distribution", Metrics: tc.mockDcnInboundTransferSizesMetrics}}}
		fakeReponseDcnTransferSizes := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.dcn_transfer_size.bytes.cumulative.distribution", Metrics: tc.mockDcnTransferSizesMetrics}}}
		fakeReponseMxlaComputeOperandSize := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.mxla_compute_operand_size.bytes.cumulative.distribution", Metrics: tc.mockMxlaComputeOperandSizeMetrics}}}
		fakeReponseCollectiveInputSizes := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.collective_input_size.bytes.cumulative.distribution", Metrics: tc.mockCollectiveInputSizesMetrics}}}
		fakeReponseDeviceToHostTransferSizes := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.device_to_host_transfer_size.bytes.cumulative.distribution", Metrics: tc.mockDeviceToHostTransferSizesMetrics}}}
		fakeReponseHostToDeviceTransferSizes := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.host_to_device_transfer_size.bytes.cumulative.distribution", Metrics: tc.mockHostToDeviceTransferSizesMetrics}}}
		fakeMlRuntimeUptime := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "tpu.runtime.uptime.seconds.gauge", Metrics: tc.mockMlRuntimeUptimeMetrics}}}
		fakeReponseMegascaleErrorDetected := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "megascale.error.detected.gauge", Metrics: tc.mockMegascaleErrorDetectedMetrics}}}
		fakeReponseSliceErrorDetect := pb.MetricResponse{Response: &pb.MetricResponse_Metric{Metric: &pb.TPUMetric{Name: "slice.error.detected.gauge", Metrics: tc.mockSliceErrorDetectionMetrics}}}
		fakeSupportedMetrics := pb.ListSupportedMetricsResponse{}
		for _, metricName := range tc.mockListSupportedMetricsReponse {
			fakeSupportedMetrics.SupportedMetric = append(fakeSupportedMetrics.SupportedMetric, &pb.SupportedMetric{MetricName: metricName})
		}

		ctrl := gomock.NewController(t)
		defer func() {
			grpcDialFunctionAlias = grpc.Dial
			pbNewRuntimeMetricServiceClientFunctionAlias = generateRuntimeClient
			ctrl.Finish()
		}()
		mockedRuntimeClient := mocks.NewMockRuntimeClient(ctrl)

		grpcDialFunctionAlias = func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
			return grpc.Dial("localhost", grpc.WithInsecure())
		}

		pbNewRuntimeMetricServiceClientFunctionAlias = func(cc grpc.ClientConnInterface) RuntimeClient {
			return mockedRuntimeClient
		}

		if tc.desc == "ListSupportedMetrics returns error" {
			mockedRuntimeClient.EXPECT().ListSupportedMetrics(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("fake error"))
		} else {
			mockedRuntimeClient.EXPECT().ListSupportedMetrics(gomock.Any(), gomock.Any(), gomock.Any()).Return(&fakeSupportedMetrics, nil)
			metricToResponse := map[string]*pb.MetricResponse{
				memoryTotalMetricsName:                         &fakeReponseMemoryTotal,
				memoryUsedMetricsName:                          &fakeReponseMemoryUsage,
				dutyCycleMetricsName:                           &fakeReponseDutyCycle,
				dcnTransferLatenciesName:                       &fakeReponseDcnTransferLatency,
				mxlaComputeLatenciesName:                       &fakeReponseMxlaComputeLatency,
				dcnInboundTransferLatenciesName:                &fakeReponseDcnInboundTransferLatency,
				grpcClientCallLatenciesMetricName:              &fakeReponseGrpcClientCallLatency,
				grpcServerCallLatenciesMetricName:              &fakeReponseGrpcServerCallLatency,
				grpcTCPMinRttMetricName:                        &fakeReponseGrpcTCPMinRtt,
				grpcTCPDeliveryRateMetricName:                  &fakeReponseGrpcTCPDeliveryRate,
				grpcTCPPacketsSentMetricName:                   &fakeReponseGrpcTCPPacketsSent,
				grpcTCPPacketsRetransmittedMetricName:          &fakeReponseGrpcTCPPacketsRetransmitted,
				grpcTCPPacketsSpuriousRetransmittedMetricName:  &fakeReponseGrpcTCPPacketsSpuriousRetransmitted,
				grpcTCPRecurringRetransmitsCollectorMetricName: &fakeReponseGrpcTCPRecurringRetransmits,
				grpcTCPBytesSentCollectorMetricName:            &fakeReponseGrpcTCPBytesSent,
				megascaleBamm2BitsSentMetricName:               &fakeReponseBamm2BitsSent,
				megascaleBamm2BitsReceivedMetricName:           &fakeReponseBamm2BitsReceived,
				megascaleBamm2BitsReadMetricName:               &fakeReponseBamm2BitsRead,
				megascaleBamm2BitsWrittenMetricName:            &fakeReponseBamm2BitsWritten,
				megascaleBamm2BitsWrittenWithImmMetricName:     &fakeReponseBamm2BitsWrittenWithImm,
				grpcTCPBytesRetransmittedCollectorMetricName:   &fakeReponseGrpcTCPBytesRetransmitted,
				grpcTCPSyscallWritesCollectorMetricName:        &fakeReponseGrpcTCPSyscallWrites,
				grpcTCPSyscallReadsCollectorMetricName:         &fakeReponseGrpcTCPSyscallReads,
				grpcTCPWriteSizeCollectorMetricName:            &fakeReponseGrpcTCPWriteSize,
				grpcTCPReadSizeCollectorMetricName:             &fakeReponseGrpcTCPReadSize,
				grpcTCPSenderLatencyCollectorMetricName:        &fakeReponseGrpcTCPSenderLatency,
				grpcTCPTransferLatencyCollectorMetricName:      &fakeReponseGrpcTCPTransferLatency,
				collectiveLatenciesName:                        &fakeReponseCollectiveLatency,
				hostToDeviceTransferLatenciesName:              &fakeReponseHostToDeviceTransferLatency,
				deviceToHostTransferLatenciesName:              &fakeReponseDeviceToHostTransferLatency,
				dcnInboundTransferSizesCollectorMetricName:     &fakeReponseDcnInboundTransferSizes,
				dcnTransferSizesCollectorMetricName:            &fakeReponseDcnTransferSizes,
				mxlaComputeOperandSizeCollectorMetricName:      &fakeReponseMxlaComputeOperandSize,
				collectiveInputSizesCollectorMetricName:        &fakeReponseCollectiveInputSizes,
				deviceToHostTransferSizesCollectorMetricName:   &fakeReponseDeviceToHostTransferSizes,
				hostToDeviceTransferSizesCollectorMetricName:   &fakeReponseHostToDeviceTransferSizes,

				mlRuntimeUptimeMetricsName:        &fakeMlRuntimeUptime,
				megascaleErrorDetectedMetricsName: &fakeReponseMegascaleErrorDetected,
				sliceErrorDetectedMetricsName:     &fakeReponseSliceErrorDetect,
			}
			for _, metricName := range orderedMetricNames {
				if slices.Contains(tc.mockListSupportedMetricsReponse, metricName) {
					mockedRuntimeClient.EXPECT().GetRuntimeMetric(gomock.Any(), gomock.Any(), gomock.Any()).Return(metricToResponse[metricName], nil)
				}
			}
		}

		runtimeMetricsInfo, err := mockMetricServer.runtimeMetrics("localhost", mockRealTimeProvider)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: Wanted error but didn't get any", tc.desc)
			}
		} else {
			if err != nil {
				t.Errorf("%q: Unexpected error: %v", tc.desc, err)
			} else {
				assert.Equal(t, tc.wantRuntimeInfo.memoryTotal, runtimeMetricsInfo.memoryTotal)
				assert.Equal(t, tc.wantRuntimeInfo.memoryUsed, runtimeMetricsInfo.memoryUsed)
				assert.Equal(t, tc.wantRuntimeInfo.runtimeDutyCycle, runtimeMetricsInfo.runtimeDutyCycle)
				if len(tc.wantRuntimeInfo.dcnTransferLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.dcnTransferLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.dcnTransferLatencies, runtimeMetricsInfo.dcnTransferLatencies)
				}
				if len(tc.wantRuntimeInfo.mxlaComputeLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.mxlaComputeLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.mxlaComputeLatencies, runtimeMetricsInfo.mxlaComputeLatencies)
				}
				if len(tc.wantRuntimeInfo.dcnInboundTransferLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.dcnInboundTransferLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.dcnInboundTransferLatencies, runtimeMetricsInfo.dcnInboundTransferLatencies)
				}
				if len(tc.wantRuntimeInfo.grpcClientCallLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcClientCallLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcClientCallLatencies, runtimeMetricsInfo.grpcClientCallLatencies)
				}
				if len(tc.wantRuntimeInfo.grpcServerCallLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcServerCallLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcServerCallLatencies, runtimeMetricsInfo.grpcServerCallLatencies)
				}
				if len(tc.wantRuntimeInfo.grpcTCPMinRtt) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPMinRtt)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPMinRtt, runtimeMetricsInfo.grpcTCPMinRtt)
				}
				if len(tc.wantRuntimeInfo.grpcTCPDeliveryRate) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPDeliveryRate)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPDeliveryRate, runtimeMetricsInfo.grpcTCPDeliveryRate)
				}
				if len(tc.wantRuntimeInfo.grpcTCPPacketsSent) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPPacketsSent)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPPacketsSent, runtimeMetricsInfo.grpcTCPPacketsSent)
				}
				if len(tc.wantRuntimeInfo.grpcTCPPacketsRetransmitted) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPPacketsRetransmitted)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPPacketsRetransmitted, runtimeMetricsInfo.grpcTCPPacketsRetransmitted)
				}
				if len(tc.wantRuntimeInfo.grpcTCPPacketsSpuriousRetransmitted) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPPacketsSpuriousRetransmitted)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPPacketsSpuriousRetransmitted, runtimeMetricsInfo.grpcTCPPacketsSpuriousRetransmitted)
				}
				if len(tc.wantRuntimeInfo.grpcTCPRecurringRetransmits) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPRecurringRetransmits)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPRecurringRetransmits, runtimeMetricsInfo.grpcTCPRecurringRetransmits)
				}
				if len(tc.wantRuntimeInfo.grpcTCPBytesSent) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPBytesSent)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPBytesSent, runtimeMetricsInfo.grpcTCPBytesSent)
				}
				if len(tc.wantRuntimeInfo.megascaleBamm2BitsSent) == 0 {
					assert.Empty(t, runtimeMetricsInfo.megascaleBamm2BitsSent)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.megascaleBamm2BitsSent, runtimeMetricsInfo.megascaleBamm2BitsSent)
				}
				if len(tc.wantRuntimeInfo.megascaleBamm2BitsReceived) == 0 {
					assert.Empty(t, runtimeMetricsInfo.megascaleBamm2BitsReceived)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.megascaleBamm2BitsReceived, runtimeMetricsInfo.megascaleBamm2BitsReceived)
				}
				if len(tc.wantRuntimeInfo.megascaleBamm2BitsRead) == 0 {
					assert.Empty(t, runtimeMetricsInfo.megascaleBamm2BitsRead)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.megascaleBamm2BitsRead, runtimeMetricsInfo.megascaleBamm2BitsRead)
				}
				if len(tc.wantRuntimeInfo.megascaleBamm2BitsWritten) == 0 {
					assert.Empty(t, runtimeMetricsInfo.megascaleBamm2BitsWritten)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.megascaleBamm2BitsWritten, runtimeMetricsInfo.megascaleBamm2BitsWritten)
				}
				if len(tc.wantRuntimeInfo.megascaleBamm2BitsWrittenWithImm) == 0 {
					assert.Empty(t, runtimeMetricsInfo.megascaleBamm2BitsWrittenWithImm)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.megascaleBamm2BitsWrittenWithImm, runtimeMetricsInfo.megascaleBamm2BitsWrittenWithImm)
				}
				if len(tc.wantRuntimeInfo.grpcTCPBytesRetransmitted) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPBytesRetransmitted)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPBytesRetransmitted, runtimeMetricsInfo.grpcTCPBytesRetransmitted)
				}
				if len(tc.wantRuntimeInfo.grpcTCPSyscallWrites) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPSyscallWrites)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPSyscallWrites, runtimeMetricsInfo.grpcTCPSyscallWrites)
				}
				if len(tc.wantRuntimeInfo.grpcTCPSyscallReads) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPSyscallReads)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPSyscallReads, runtimeMetricsInfo.grpcTCPSyscallReads)
				}
				if len(tc.wantRuntimeInfo.grpcTCPWriteSize) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPWriteSize)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPWriteSize, runtimeMetricsInfo.grpcTCPWriteSize)
				}
				if len(tc.wantRuntimeInfo.grpcTCPReadSize) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPReadSize)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPReadSize, runtimeMetricsInfo.grpcTCPReadSize)
				}
				if len(tc.wantRuntimeInfo.grpcTCPSenderLatency) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPSenderLatency)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPSenderLatency, runtimeMetricsInfo.grpcTCPSenderLatency)
				}
				if len(tc.wantRuntimeInfo.grpcTCPTransferLatency) == 0 {
					assert.Empty(t, runtimeMetricsInfo.grpcTCPTransferLatency)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.grpcTCPTransferLatency, runtimeMetricsInfo.grpcTCPTransferLatency)
				}
				if len(tc.wantRuntimeInfo.collectiveLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.collectiveLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.collectiveLatencies, runtimeMetricsInfo.collectiveLatencies)
				}
				if len(tc.wantRuntimeInfo.hostToDeviceTransferLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.hostToDeviceTransferLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.hostToDeviceTransferLatencies, runtimeMetricsInfo.hostToDeviceTransferLatencies)
				}
				if len(tc.wantRuntimeInfo.deviceToHostTransferLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.deviceToHostTransferLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.deviceToHostTransferLatencies, runtimeMetricsInfo.deviceToHostTransferLatencies)
				}
				if len(tc.wantRuntimeInfo.dcnInboundTransferLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.dcnInboundTransferLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.dcnInboundTransferLatencies, runtimeMetricsInfo.dcnInboundTransferLatencies)
				}
				if len(tc.wantRuntimeInfo.dcnInboundTransferSizes) == 0 {
					assert.Empty(t, runtimeMetricsInfo.dcnInboundTransferSizes)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.dcnInboundTransferSizes, runtimeMetricsInfo.dcnInboundTransferSizes)
				}
				if len(tc.wantRuntimeInfo.dcnTransferSizes) == 0 {
					assert.Empty(t, runtimeMetricsInfo.dcnTransferSizes)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.dcnTransferSizes, runtimeMetricsInfo.dcnTransferSizes)
				}

				if len(tc.wantRuntimeInfo.mxlaComputeOperandSize) == 0 {
					assert.Empty(t, runtimeMetricsInfo.mxlaComputeOperandSize)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.mxlaComputeOperandSize, runtimeMetricsInfo.mxlaComputeOperandSize)
				}
				if len(tc.wantRuntimeInfo.mxlaComputeLatencies) == 0 {
					assert.Empty(t, runtimeMetricsInfo.mxlaComputeLatencies)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.mxlaComputeLatencies, runtimeMetricsInfo.mxlaComputeLatencies)
				}
				if len(tc.wantRuntimeInfo.collectiveInputSizes) == 0 {
					assert.Empty(t, runtimeMetricsInfo.collectiveInputSizes)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.collectiveInputSizes, runtimeMetricsInfo.collectiveInputSizes)
				}

				if len(tc.wantRuntimeInfo.deviceToHostTransferSizes) == 0 {
					assert.Empty(t, runtimeMetricsInfo.deviceToHostTransferSizes)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.deviceToHostTransferSizes, runtimeMetricsInfo.deviceToHostTransferSizes)
				}

				if len(tc.wantRuntimeInfo.hostToDeviceTransferSizes) == 0 {
					assert.Empty(t, runtimeMetricsInfo.hostToDeviceTransferSizes)
				} else {
					assert.Equal(t, tc.wantRuntimeInfo.hostToDeviceTransferSizes, runtimeMetricsInfo.hostToDeviceTransferSizes)
				}
				assert.Equal(t, tc.wantRuntimeInfo.mlRuntimeUptimeKvlist, runtimeMetricsInfo.mlRuntimeUptimeKvlist)
				assert.Equal(t, tc.wantRuntimeInfo.megascaleErrorDetectedKvlist, runtimeMetricsInfo.megascaleErrorDetectedKvlist)
				assert.Equal(t, tc.wantRuntimeInfo.sliceErrorDetectedKvlist, runtimeMetricsInfo.sliceErrorDetectedKvlist)
			}
		}
	}
}

func TestValidateDistributionInfo(t *testing.T) {
	maxBucketUBs := make([]float64, 201)
	maxBucketCounts := make([]int64, 201)
	for i := range maxBucketUBs {
		maxBucketUBs[i] = 1
		maxBucketCounts[i] = 1
	}
	testCases := []struct {
		desc             string
		distributionInfo DistributionInfo
		err              string
	}{
		{
			desc: "Happy Path",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          []int64{1, 2, 3},
				},
				bucketUpperBounds: []float64{1, 2, math.Inf(1)},
				attributes:        map[string]string{"buffer_size": "8MB+", "type": "grpc"},
			},
			err: "",
		},
		{
			desc: "Different Bucket Counts",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          []int64{1, 2, 3, 4},
				},
				bucketUpperBounds: []float64{1, 2, math.Inf(1)},
				attributes:        map[string]string{"buffer_size": "8MB+", "type": "grpc"},
			},
			err: "bucketCounts is different (4) than expected 3",
		},
		{
			desc: "Exceeding bucketCounts",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          maxBucketCounts,
				},
				bucketUpperBounds: maxBucketUBs,
				attributes:        map[string]string{"buffer_size": "8MB+", "type": "grpc"},
			},
			err: "# of buckets 201 exceed the max 200",
		},
		{
			desc: "Missing Attribute",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          []int64{1, 2, 3},
				},
				bucketUpperBounds: []float64{1, 2, math.Inf(1)},
				attributes:        map[string]string{"buffer_size": "8MB+", "type": "grpc"},
			},
			err: "",
		},
		{
			desc: "Empty Attribute",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          []int64{1, 2, 3},
				},
				bucketUpperBounds: []float64{1, 2, math.Inf(1)},
				attributes:        map[string]string{"buffer_size": "8MB+", "type": ""},
			},
			err: "",
		},
		{
			desc: "Extra Attributes",
			distributionInfo: DistributionInfo{
				data: DistributionData{
					Count:                 10,
					Min:                   1,
					Max:                   10,
					Mean:                  5,
					SumOfSquaredDeviation: 1,
					BucketCounts:          []int64{1, 2, 3},
				},
				bucketUpperBounds: []float64{1, 2, math.Inf(1)},
				attributes:        map[string]string{"buffer_size": "8MB+", "type": "grpc", "extra_attr": "value"},
			},
			err: "",
		},
	}
	for _, tc := range testCases {
		t.Logf("Running testcase: %s", tc.desc)
		err := validateDistributionInfo(&tc.distributionInfo)
		if err != nil {
			assert.Equal(t, tc.err, err.Error())
		} else if tc.err != "" {
			t.Errorf("Expected error (%s) but got none", tc.err)
		}
	}

}

func TestStartNodeConditionCaches(t *testing.T) {
	nodeName := "test-node"
	fakeTime := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	second_faketime := fakeTime.Add(10 * time.Minute)
	dummyCondition := v1.NodeCondition{
		Type:               v1.NodeReady,
		Status:             v1.ConditionTrue,
		LastHeartbeatTime:  metav1.NewTime(fakeTime),
		LastTransitionTime: metav1.NewTime(fakeTime),
	}

	testCases := []struct {
		desc                                 string
		nodeConditions                       []v1.NodeCondition
		wantHangLastHeartbeatTimeCache       time.Time
		wantSliceErrorLastHeartbeatTimeCache time.Time
	}{
		{
			desc:                                 "All empty",
			nodeConditions:                       []v1.NodeCondition{},
			wantHangLastHeartbeatTimeCache:       time.Time{},
			wantSliceErrorLastHeartbeatTimeCache: time.Time{},
		},
		{
			desc: "No condition",
			nodeConditions: []v1.NodeCondition{
				dummyCondition,
			},
			wantHangLastHeartbeatTimeCache:       time.Time{},
			wantSliceErrorLastHeartbeatTimeCache: time.Time{},
		},
		{
			desc: "Only Hang condition",
			nodeConditions: []v1.NodeCondition{
				{
					Type:              "MegascaleHangDetected",
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(fakeTime),
				},
				dummyCondition,
			},
			wantHangLastHeartbeatTimeCache:       fakeTime,
			wantSliceErrorLastHeartbeatTimeCache: time.Time{},
		},
		{
			desc: "Only Slice error condition",
			nodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(second_faketime),
				},
			},
			wantHangLastHeartbeatTimeCache:       time.Time{},
			wantSliceErrorLastHeartbeatTimeCache: second_faketime,
		},
		{
			desc: "Both Hang and Slice error conditions (True)",
			nodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              hangConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(fakeTime),
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(second_faketime),
				},
			},
			wantHangLastHeartbeatTimeCache:       fakeTime,
			wantSliceErrorLastHeartbeatTimeCache: second_faketime,
		},
		{
			desc: "Both Hang and Slice error conditions (slice error False)",
			nodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              hangConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(fakeTime),
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionFalse,
					LastHeartbeatTime: metav1.NewTime(second_faketime),
				},
			},
			wantHangLastHeartbeatTimeCache:       fakeTime,
			wantSliceErrorLastHeartbeatTimeCache: time.Time{},
		},
		{
			desc: "Both Hang and Slice error conditions (Hang False)",
			nodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              hangConditionType,
					Status:            v1.ConditionFalse,
					LastHeartbeatTime: metav1.NewTime(fakeTime),
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(second_faketime),
				},
			},
			wantHangLastHeartbeatTimeCache:       time.Time{},
			wantSliceErrorLastHeartbeatTimeCache: second_faketime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKubeClient := buildMockKubeClientWithConditions(nodeName, tc.nodeConditions)
			localMockMetricServer := buildMetricServer(nodeName, mockKubeClient)

			localMockMetricServer.startNodeConditionCaches(ctx)
			gotHangLastHeartbeatTimeCache := localMockMetricServer.hangLastHeartbeatTimeCache
			if diff := cmp.Diff(tc.wantHangLastHeartbeatTimeCache, gotHangLastHeartbeatTimeCache); diff != "" {
				t.Errorf("%q: incorrect result: %s", tc.desc, diff)
			}
			gotSliceErrorLastHeartbeatTimeCache := localMockMetricServer.sliceErrorLastHeartbeatTimeCache
			if diff := cmp.Diff(tc.wantSliceErrorLastHeartbeatTimeCache, gotSliceErrorLastHeartbeatTimeCache); diff != "" {
				t.Errorf("%q: incorrect result: %s", tc.desc, diff)
			}
		})
	}
}

func TestNodeConditionLifecycle(t *testing.T) {
	nodeName := "test-node"
	nowTime := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	recentTime := nowTime.Add(-15 * time.Minute)
	expiredTime := nowTime.Add(-60 * time.Minute)
	transitionTime := nowTime.Add(-120 * time.Minute)
	dummyCondition := v1.NodeCondition{
		Type:               v1.NodeReady,
		Status:             v1.ConditionTrue,
		LastHeartbeatTime:  metav1.NewTime(recentTime),
		LastTransitionTime: metav1.NewTime(recentTime),
	}

	testCases := []struct {
		desc                     string
		initialHangCache         time.Time
		initialSliceErrorCache   time.Time
		initialNodeConditions    []v1.NodeCondition
		mockRuntimeMetrics       runtimeMetricsInfo
		wantFinalHangCache       time.Time
		wantFinalSliceErrorCache time.Time
		wantFinalNodeConditions  []v1.NodeCondition
	}{
		{
			desc:                     "First-time hang detection, no other conditions",
			initialHangCache:         time.Time{},
			initialSliceErrorCache:   time.Time{},
			initialNodeConditions:    []v1.NodeCondition{dummyCondition},
			mockRuntimeMetrics:       runtimeMetricsInfo{megascaleErrorDetectedKvlist: buildHangMetric(true)},
			wantFinalHangCache:       nowTime,
			wantFinalSliceErrorCache: time.Time{},
			wantFinalNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               hangConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(nowTime),
					LastTransitionTime: metav1.NewTime(nowTime),
					Message:            "",
				},
			},
		},
		{
			desc:                     "First-time slice error detection with message",
			initialHangCache:         time.Time{},
			initialSliceErrorCache:   time.Time{},
			initialNodeConditions:    []v1.NodeCondition{dummyCondition},
			mockRuntimeMetrics:       runtimeMetricsInfo{sliceErrorDetectedKvlist: buildSliceErrorMetric(true, "test slice error")},
			wantFinalHangCache:       time.Time{},
			wantFinalSliceErrorCache: nowTime,
			wantFinalNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               sliceErrorConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(nowTime),
					LastTransitionTime: metav1.NewTime(nowTime),
					Message:            "test slice error",
				},
			},
		},
		{
			desc:                   "Slice error updated, hang not detected",
			initialHangCache:       time.Time{},
			initialSliceErrorCache: time.Time{},
			initialNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               sliceErrorConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(nowTime),
					LastTransitionTime: metav1.NewTime(nowTime),
					Message:            "test slice error",
				},
			},
			mockRuntimeMetrics:       runtimeMetricsInfo{sliceErrorDetectedKvlist: buildSliceErrorMetric(true, "test updated slice error")},
			wantFinalHangCache:       time.Time{},
			wantFinalSliceErrorCache: nowTime,
			wantFinalNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               sliceErrorConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(nowTime),
					LastTransitionTime: metav1.NewTime(nowTime),
					Message:            "test updated slice error",
				},
			},
		},
		{
			desc:                   "Slice error is expired, hang is not detected",
			initialHangCache:       time.Time{},
			initialSliceErrorCache: expiredTime,
			initialNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(expiredTime),
					Message:           "test slice error",
				},
			},
			mockRuntimeMetrics:       runtimeMetricsInfo{megascaleErrorDetectedKvlist: buildHangMetric(false)},
			wantFinalHangCache:       time.Time{},
			wantFinalSliceErrorCache: time.Time{},
			wantFinalNodeConditions:  []v1.NodeCondition{dummyCondition},
		},
		{
			desc:                   "Hang is updated, slice error is expired and removed",
			initialHangCache:       recentTime,
			initialSliceErrorCache: expiredTime,
			initialNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               hangConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(recentTime),
					LastTransitionTime: metav1.NewTime(transitionTime),
					Message:            "",
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(expiredTime),
					Message:           "test slice error",
				},
			},
			mockRuntimeMetrics:       runtimeMetricsInfo{megascaleErrorDetectedKvlist: buildHangMetric(true)},
			wantFinalHangCache:       nowTime,
			wantFinalSliceErrorCache: time.Time{},
			wantFinalNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:               hangConditionType,
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(nowTime),
					LastTransitionTime: metav1.NewTime(transitionTime),
					Message:            "",
				},
			},
		},
		{
			desc:                   "No new detections, no expiries",
			initialHangCache:       recentTime,
			initialSliceErrorCache: recentTime,
			initialNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              hangConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(recentTime),
					Message:           "",
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(recentTime),
					Message:           "test slice error",
				},
			},
			mockRuntimeMetrics:       runtimeMetricsInfo{megascaleErrorDetectedKvlist: buildHangMetric(false), sliceErrorDetectedKvlist: buildSliceErrorMetric(false, "")},
			wantFinalHangCache:       recentTime,
			wantFinalSliceErrorCache: recentTime,
			wantFinalNodeConditions: []v1.NodeCondition{
				dummyCondition,
				{
					Type:              hangConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(recentTime),
					Message:           "",
				},
				{
					Type:              sliceErrorConditionType,
					Status:            v1.ConditionTrue,
					LastHeartbeatTime: metav1.NewTime(recentTime),
					Message:           "test slice error",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKubeClient := buildMockKubeClientWithConditions(nodeName, tc.initialNodeConditions)
			mockRealTimeProvider := mocks.NewMockRealTimeProvider(ctrl)
			mockRealTimeProvider.EXPECT().Now().AnyTimes().Return(nowTime)
			server := buildMetricServer(nodeName, mockKubeClient)
			server.hangLastHeartbeatTimeCache = tc.initialHangCache
			server.sliceErrorLastHeartbeatTimeCache = tc.initialSliceErrorCache

			// Update Conditions
			updateChecks := []NodeConditionCheckInfo{
				{
					conditionType:          hangConditionType,
					logMessage:             "hang update failed",
					lastHeartbeatTimeCache: &server.hangLastHeartbeatTimeCache,
					detectionFunc:          func() (bool, string) { return hangDetected(tc.mockRuntimeMetrics.megascaleErrorDetectedKvlist) },
				},
				{
					conditionType:          sliceErrorConditionType,
					logMessage:             "slice error update failed",
					lastHeartbeatTimeCache: &server.sliceErrorLastHeartbeatTimeCache,
					detectionFunc:          func() (bool, string) { return sliceErrorDetected(tc.mockRuntimeMetrics.sliceErrorDetectedKvlist) },
				},
			}
			for _, check := range updateChecks {
				detected, message := check.detectionFunc()
				newTime, err := server.updateNodeCondition(ctx, check.conditionType, detected, message, *check.lastHeartbeatTimeCache, mockRealTimeProvider)
				assert.NoError(t, err)
				*check.lastHeartbeatTimeCache = newTime
			}

			// Remove Expired Conditions
			removalChecks := []NodeConditionCheckInfo{
				{
					conditionType:          hangConditionType,
					logMessage:             "hang removal failed",
					lastHeartbeatTimeCache: &server.hangLastHeartbeatTimeCache,
					expirePeriod:           hangExpirePeriod,
				},
				{
					conditionType:          sliceErrorConditionType,
					logMessage:             "slice error removal failed",
					lastHeartbeatTimeCache: &server.sliceErrorLastHeartbeatTimeCache,
					expirePeriod:           sliceErrorConditionExpirePeriod,
				},
			}
			for _, check := range removalChecks {
				newTime, err := server.removeExpiredNodeCondition(ctx, check.conditionType, *check.lastHeartbeatTimeCache, check.expirePeriod, mockRealTimeProvider)
				assert.NoError(t, err)
				*check.lastHeartbeatTimeCache = newTime
			}

			// Assert final state of caches
			assert.Equal(t, tc.wantFinalHangCache, server.hangLastHeartbeatTimeCache, "Final hang cache state is incorrect")
			assert.Equal(t, tc.wantFinalSliceErrorCache, server.sliceErrorLastHeartbeatTimeCache, "Final slice error cache state is incorrect")

			node, _ := mockKubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})

			timeTransformer := cmp.Transformer("UnixTime", func(t metav1.Time) int64 { return t.Time.Unix() })
			opts := []cmp.Option{
				timeTransformer,
				// This sorter ensures the slice order doesn't matter
				cmp.Transformer("Sort", func(in []v1.NodeCondition) []v1.NodeCondition {
					out := append([]v1.NodeCondition(nil), in...)
					sort.Slice(out, func(i, j int) bool {
						return out[i].Type < out[j].Type
					})
					return out
				}),
			}
			// Assert final state of node conditions
			if diff := cmp.Diff(tc.wantFinalNodeConditions, node.Status.Conditions, opts...); diff != "" {
				t.Errorf("Final node conditions differ (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHangDetected(t *testing.T) {
	testCases := []struct {
		desc        string
		metricInfo  []KvlistAttributesMetricInfo
		want        bool
		wantMessage string
	}{
		{
			desc:        "Empty",
			metricInfo:  []KvlistAttributesMetricInfo{},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Hang error type present + value non zero",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"error_type": "HANG_DETECTED"},
					value:      1,
				},
			},
			want:        true,
			wantMessage: "",
		},
		{
			desc: "Hang error type present + value zero",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"error_type": "HANG_DETECTED"},
					value:      0,
				},
			},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Hang error type not present",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      1,
				},
			},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Multiple entries with detection",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"error_type": "HANG_DETECTED"},
					value:      1,
				},
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      0,
				},
			},
			want:        true,
			wantMessage: "",
		},
		{
			desc: "Multiple entries without detection",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"error_type": "HANG_DETECTED"},
					value:      0,
				},
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      1,
				},
			},
			want:        false,
			wantMessage: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, gotMessage := hangDetected(tc.metricInfo)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("%q: incorrect result: %s", tc.desc, diff)
			}
			if diff := cmp.Diff(tc.wantMessage, gotMessage); diff != "" {
				t.Errorf("%q: incorrect result: %s", tc.desc, diff)
			}
		})
	}
}

func TestSliceErrorDetected(t *testing.T) {
	const testErrorMessage = "slice error"
	testCases := []struct {
		desc        string
		metricInfo  []KvlistAttributesMetricInfo
		want        bool
		wantMessage string
	}{
		{
			desc:        "Empty",
			metricInfo:  []KvlistAttributesMetricInfo{},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Slice error type present + value non zero",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
					},
					value: 1,
				},
			},
			want:        true,
			wantMessage: testErrorMessage,
		},
		{
			desc: "Slice error type present + value zero",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
					},
					value: 0,
				},
			},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Slice error required key missing",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						// missing type and topology
					},
					value: 1,
				},
			},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Slice error required key present + extra key present + value non zero",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
						"extra_key":     "extra_value",
					},
					value: 1,
				},
			},
			want:        true,
			wantMessage: testErrorMessage,
		},
		{
			desc: "Other error type present",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      1,
				},
			},
			want:        false,
			wantMessage: "",
		},
		{
			desc: "Multiple entries with detection",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      0,
				},
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
					},
					value: 1,
				},
			},
			want:        true,
			wantMessage: testErrorMessage,
		},
		{
			desc: "Multiple entries without detection",
			metricInfo: []KvlistAttributesMetricInfo{
				{
					attributes: map[string]string{
						"error_message": testErrorMessage,
						"session_id":    "123",
						"type":          "test",
						"topology":      "2x2",
					},
					value: 0,
				},
				{
					attributes: map[string]string{"other_key": "other_value"},
					value:      1,
				},
			},
			want:        false,
			wantMessage: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, gotMessage := sliceErrorDetected(tc.metricInfo)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("%q: incorrect result (-want +got):\n%s", tc.desc, diff)
			}
			if diff := cmp.Diff(tc.wantMessage, gotMessage); diff != "" {
				t.Errorf("%q: incorrect result (-want +got):\n%s", tc.desc, diff)
			}
		})
	}
}

func TestUpdateTPUChipIDsAnnotation(t *testing.T) {
	testCases := []struct {
		desc                   string
		tpuChipIDsCache        string
		newRawTPUChipIDs       map[string]string
		currentNodeAnnotations map[string]string
		wantNodeAnnotations    map[string]string
	}{
		{
			desc:                   "All empty",
			tpuChipIDsCache:        "",
			newRawTPUChipIDs:       map[string]string{},
			currentNodeAnnotations: map[string]string{},
			wantNodeAnnotations:    map[string]string{},
		},
		{
			desc:             "Empty structure - No change to empty annotation (e.g., node was just provioned and libtpu couldn't find the tpus)",
			tpuChipIDsCache:  "",
			newRawTPUChipIDs: map[string]string{},
			currentNodeAnnotations: map[string]string{
				"abc": "123",
			},
			wantNodeAnnotations: map[string]string{
				"abc": "123",
			},
		},
		{
			desc:            "Empty IDs - No change to empty annotation (e.g., node was just provioned and libtpu couldn't find the IDs)",
			tpuChipIDsCache: "",
			newRawTPUChipIDs: map[string]string{
				"0": "",
				"1": "",
				"2": "",
				"3": "",
			},
			currentNodeAnnotations: map[string]string{
				"abc": "123",
			},
			wantNodeAnnotations: map[string]string{
				"abc": "123",
			},
		},
		{
			desc:            "Base case with empty cache - Creating node annotation (e.g., first real read of TPU Chip IDs)",
			tpuChipIDsCache: "",
			newRawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "tpu-chip-id-1",
				"2": "tpu-chip-id-2",
				"3": "tpu-chip-id-3",
			},
			currentNodeAnnotations: map[string]string{
				"abc": "123",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
		},
		{
			desc:            "Base case with updated cache - No change to existing annotation (e.g., second real read of TPU Chip IDs)",
			tpuChipIDsCache: "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			newRawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "tpu-chip-id-1",
				"2": "tpu-chip-id-2",
				"3": "tpu-chip-id-3",
			},
			currentNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
		},
		{
			desc:            "Same IDs with empty cache - No change to existing annotation (e.g., TPU Device Plugin was restarted)",
			tpuChipIDsCache: "",
			newRawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "tpu-chip-id-1",
				"2": "tpu-chip-id-2",
				"3": "tpu-chip-id-3",
			},
			currentNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
		},
		{
			desc:            "New IDs - Updating node annotation (e.g., VM migration)",
			tpuChipIDsCache: "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			newRawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-a",
				"1": "tpu-chip-id-b",
				"2": "tpu-chip-id-c",
				"3": "tpu-chip-id-d",
			},
			currentNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-a,tpu-chip-id-b,tpu-chip-id-c,tpu-chip-id-d",
			},
		},
		{
			desc:             "Check if annotation is not incorrectly deleted with empty structure",
			tpuChipIDsCache:  "",
			newRawTPUChipIDs: map[string]string{},
			currentNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
		},
		{
			desc:            "Check if annotation is not incorrectly deleted with empty ids",
			tpuChipIDsCache: "",
			newRawTPUChipIDs: map[string]string{
				"0": "",
				"1": "",
				"2": "",
				"3": "",
			},
			currentNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
			wantNodeAnnotations: map[string]string{
				"abc":                      "123",
				"node.gke.io/tpu-chip-ids": "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockHostMetricsClient := mocks.NewMockHostMetricsClient(ctrl)
			mockHostMetricsClient.EXPECT().ChipIdentifierPerDevice().Return(tc.newRawTPUChipIDs)
			mockKubeClient := buildMockKubeClientWithAnnotations(mockMetricServer.nodeName, tc.currentNodeAnnotations)
			mockMetricServer.tpuChipIDsCache = tc.tpuChipIDsCache
			localMockMetricServer := buildMetricServer("test-node", mockKubeClient)
			err := localMockMetricServer.updateTPUChipIDsAnnotation(ctx, mockHostMetricsClient)
			if err != nil {
				t.Errorf("failed to update node annotation for TPU Chip GUIDs: %v", err)
			}
			node, _ := mockKubeClient.CoreV1().Nodes().Get(ctx, mockMetricServer.nodeName, metav1.GetOptions{})
			gotNodeAnnotations := node.Annotations
			if diff := cmp.Diff(tc.wantNodeAnnotations, gotNodeAnnotations); diff != "" {
				t.Errorf("%q: incorrect result: %s", tc.desc, diff)
			}
		})
	}
}

func TestGetTPUChipIDs(t *testing.T) {
	testCases := []struct {
		desc           string
		rawTPUChipIDs  map[string]string
		wantTPUChipIDs string
	}{
		{
			desc:           "No TPU chip IDs",
			rawTPUChipIDs:  map[string]string{},
			wantTPUChipIDs: "",
		},
		{
			desc: "Single TPU chip ID",
			rawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
			},
			wantTPUChipIDs: "tpu-chip-id-0",
		},
		{
			desc: "Multiple TPU chip IDs",
			rawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "tpu-chip-id-1",
				"2": "tpu-chip-id-2",
				"3": "tpu-chip-id-3",
			},
			wantTPUChipIDs: "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
		},
		{
			desc: "Multiple TPU chip IDs (unsorted)",
			rawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "tpu-chip-id-2",
				"2": "tpu-chip-id-3",
				"3": "tpu-chip-id-1",
			},
			wantTPUChipIDs: "tpu-chip-id-0,tpu-chip-id-1,tpu-chip-id-2,tpu-chip-id-3",
		},
		{
			desc: "Multiple TPU chip IDs (single missing)",
			rawTPUChipIDs: map[string]string{
				"0": "tpu-chip-id-0",
				"1": "",
				"2": "tpu-chip-id-2",
				"3": "tpu-chip-id-3",
			},
			wantTPUChipIDs: "tpu-chip-id-0,tpu-chip-id-2,tpu-chip-id-3",
		},
		{
			desc: "Multiple TPU chip IDs (all missing)",
			rawTPUChipIDs: map[string]string{
				"0": "",
				"1": "",
				"2": "",
				"3": "",
			},
			wantTPUChipIDs: "",
		},
	}
	for _, tc := range testCases {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockHostMetricsClient := mocks.NewMockHostMetricsClient(ctrl)
		mockHostMetricsClient.EXPECT().ChipIdentifierPerDevice().Return(tc.rawTPUChipIDs)

		gotTPUChipIDs := mockMetricServer.getTPUChipIDs(mockHostMetricsClient)
		if diff := cmp.Diff(tc.wantTPUChipIDs, gotTPUChipIDs); diff != "" {
			t.Errorf("%q: incorrect result: %s", tc.desc, diff)
		}
	}
}

func buildDistMeasure(bucketCounts []int64, numBuckets int32) *pb.Metric_Distribution {
	return &pb.Metric_Distribution{
		Distribution: &pb.Distribution{
			Count:                 10,
			Min:                   1,
			Max:                   10,
			Mean:                  5,
			SumOfSquaredDeviation: 1,
			BucketCounts:          bucketCounts,
			BucketOptions: &pb.Distribution_BucketOptions{
				Options: &pb.Distribution_BucketOptions_ExponentialBuckets{
					ExponentialBuckets: &pb.Distribution_BucketOptions_Exponential{
						NumFiniteBuckets: numBuckets,
						GrowthFactor:     2,
						Scale:            1,
					},
				},
			},
		},
	}
}

func buildMXLAAttribute(kv_map map[string]string) *pb.Attribute {
	return buildMXLAAttributeWithInts(kv_map, map[string]int64{})
}

func buildMXLAAttributeWithInts(kv_map map[string]string, int_kv_map map[string]int64) *pb.Attribute {
	attrs := []*pb.Attribute{}
	for k, v := range kv_map {
		attrs = append(attrs,
			&pb.Attribute{
				Key: k,
				Value: &pb.AttrValue{
					Attr: &pb.AttrValue_StringAttr{StringAttr: v},
				},
			})
	}
	for k, v := range int_kv_map {
		attrs = append(attrs,
			&pb.Attribute{
				Key: k,
				Value: &pb.AttrValue{
					Attr: &pb.AttrValue_IntAttr{IntAttr: v},
				},
			})
	}

	return &pb.Attribute{
		Key: "megascale",
		Value: &pb.AttrValue{
			Attr: &pb.AttrValue_KvlistAttr{
				KvlistAttr: &pb.KeyValueList{
					Attributes: attrs,
				},
			},
		},
	}
}

func buildMetricServer(nodeName string, kubeClient kubernetes.Interface) *MetricServer {
	return NewMetricServer(
		time.Duration(10*time.Second),
		time.Duration(30*time.Second),
		time.Duration(30*time.Second),
		nodeName,
		"1234567890123456789",
		"tpu-v4-device",
		"runtime-metrics-port",
		"v4",
		"2x2x2",
		util.ContainerInfoExtractor{},
		"/metrics",
		2112,
		true,
		true,
		true,
		util.EnvInfo{
			ClusterName:      "cluster-name",
			ClusterProjectID: "1010101",
			ClusterLocation:  "us-central1-a",
			PodNamespace:     "kube-system",
			PodName:          "tpu-device-plugin",
			ContainerName:    "tpu-device-plugin",
		},
		nil,
		kubeClient,
	)
}

func buildRuntimeResponse(device_id int, value int) []*pb.Metric {
	attrValue := &pb.AttrValue{
		Attr: &pb.AttrValue_IntAttr{
			IntAttr: int64(device_id),
		},
	}
	attribute := &pb.Attribute{Key: "device-id", Value: attrValue}

	measure := &pb.Metric_Gauge{
		Gauge: &pb.Gauge{
			Value: &pb.Gauge_AsInt{AsInt: int64(value)},
		},
	}
	metric := &pb.Metric{Attribute: attribute, Measure: measure}
	metricsfinal := []*pb.Metric{metric}
	return metricsfinal
}

func buildCumulativeRuntimeResponse(device_id int, value int, start_timestamp time.Time, timestamp time.Time) []*pb.Metric {
	attrValue := &pb.AttrValue{
		Attr: &pb.AttrValue_IntAttr{
			IntAttr: int64(device_id),
		},
	}
	attribute := &pb.Attribute{Key: "device-id", Value: attrValue}

	measure := &pb.Metric_Counter{
		Counter: &pb.Counter{
			Value: &pb.Counter_AsInt{AsInt: uint64(value)},
		},
	}
	metric := &pb.Metric{Attribute: attribute, Measure: measure, StartTimestamp: timestamppb.New(start_timestamp), Timestamp: timestamppb.New(timestamp)}
	metricsfinal := []*pb.Metric{metric}
	return metricsfinal
}

func buildCumulativeRuntimeResponseWithoutTimestamps(device_id int, value int) []*pb.Metric {
	attrValue := &pb.AttrValue{
		Attr: &pb.AttrValue_IntAttr{
			IntAttr: int64(device_id),
		},
	}
	attribute := &pb.Attribute{Key: "device-id", Value: attrValue}

	measure := &pb.Metric_Counter{
		Counter: &pb.Counter{
			Value: &pb.Counter_AsInt{AsInt: uint64(value)},
		},
	}
	metric := &pb.Metric{Attribute: attribute, Measure: measure}
	metricsfinal := []*pb.Metric{metric}
	return metricsfinal
}

func buildMlRuntimeUptimeResponse(kv_map map[string]string, value int) *pb.Metric {
	attrs := []*pb.Attribute{}
	for k, v := range kv_map {
		attrs = append(attrs,
			&pb.Attribute{
				Key: k,
				Value: &pb.AttrValue{
					Attr: &pb.AttrValue_StringAttr{StringAttr: v},
				},
			})
	}

	attrValue := &pb.AttrValue{
		Attr: &pb.AttrValue_KvlistAttr{
			KvlistAttr: &pb.KeyValueList{
				Attributes: attrs,
			},
		},
	}

	attribute := &pb.Attribute{Key: "uptime_attributes", Value: attrValue}

	measure := &pb.Metric_Gauge{
		Gauge: &pb.Gauge{
			Value: &pb.Gauge_AsInt{AsInt: int64(value)},
		},
	}

	metric := &pb.Metric{Attribute: attribute, Measure: measure}
	return metric
}

func buildMockKubeClientWithAnnotations(nodeName string, annotations map[string]string) *fake.Clientset {
	return fake.NewSimpleClientset(
		&v1.NodeList{
			Items: []v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        nodeName,
						Annotations: annotations,
					},
				},
			},
		},
	)
}

func buildMockKubeClientWithConditions(nodeName string, conditions []v1.NodeCondition) *fake.Clientset {
	return fake.NewSimpleClientset(
		&v1.NodeList{
			Items: []v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Status: v1.NodeStatus{
						Conditions: conditions,
					},
				},
			},
		},
	)
}

func buildHangMetric(detected bool) []KvlistAttributesMetricInfo {
	if !detected {
		return []KvlistAttributesMetricInfo{}
	}
	return []KvlistAttributesMetricInfo{
		{
			attributes: map[string]string{
				"error_type": "HANG_DETECTED",
				"host_name":  "gke-tpu-abc-123",
				"launch_id":  "123",
				"start_time": "2025-01-01 01:02:03 UTC",
			},
			value: 1,
		},
	}
}

func buildSliceErrorMetric(detected bool, message string) []KvlistAttributesMetricInfo {
	if !detected {
		return []KvlistAttributesMetricInfo{}
	}
	return []KvlistAttributesMetricInfo{
		{
			attributes: map[string]string{
				"error_message": message,
				"session_id":    "123",
				"type":          "test",
				"topology":      "2x2",
			},
			value: 1,
		},
	}
}

func TestGetAttrValueAsString(t *testing.T) {
	testCases := []struct {
		desc string
		val  *pb.AttrValue
		want string
	}{
		{
			desc: "Nil value",
			val:  nil,
			want: "",
		},
		{
			desc: "Empty attribute",
			val:  &pb.AttrValue{},
			want: "unknown",
		},
		{
			desc: "String attribute",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_StringAttr{StringAttr: "test-string"},
			},
			want: "test-string",
		},
		{
			desc: "Positive Int attribute",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_IntAttr{IntAttr: 123},
			},
			want: "123",
		},
		{
			desc: "Negative Int attribute",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_IntAttr{IntAttr: -456},
			},
			want: "-456",
		},
		{
			desc: "Bool attribute true",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_BoolAttr{BoolAttr: true},
			},
			want: "true",
		},
		{
			desc: "Bool attribute false",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_BoolAttr{BoolAttr: false},
			},
			want: "false",
		},
		{
			desc: "Double attribute",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_DoubleAttr{DoubleAttr: 123.456},
			},
			want: "123.456",
		},
		{
			desc: "Kvlist attribute (unsupported)",
			val: &pb.AttrValue{
				Attr: &pb.AttrValue_KvlistAttr{KvlistAttr: &pb.KeyValueList{}},
			},
			want: "unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := getAttrValueAsString(tc.val)
			assert.Equal(t, tc.want, got)
		})
	}
}
