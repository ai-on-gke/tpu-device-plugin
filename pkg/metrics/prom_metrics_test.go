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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestUpdateRuntimeMetrics(t *testing.T) {
	start_time := time.Now()
	testCases := []struct {
		desc string
		ci   containerInfo
		mi   runtimeMetricsInfo
	}{
		{
			desc: "Empty containerInfo",
			ci:   containerInfo{},
			mi: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{"1": 2},
				memoryUsed:                          map[string]int64{"1": 1},
				runtimeDutyCycle:                    map[string]float64{"1": 10},
				dcnTransferLatencies:                []DistributionInfo{},
				mxlaComputeLatencies:                []DistributionInfo{},
				dcnInboundTransferLatencies:         []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				collectiveLatencies:                 []DistributionInfo{},
				hostToDeviceTransferLatencies:       []DistributionInfo{},
				deviceToHostTransferLatencies:       []DistributionInfo{},
				dcnInboundTransferSizes:             []DistributionInfo{},
				dcnTransferSizes:                    []DistributionInfo{},
				mxlaComputeOperandSize:              []DistributionInfo{},
				collectiveInputSizes:                []DistributionInfo{},
				deviceToHostTransferSizes:           []DistributionInfo{},
				hostToDeviceTransferSizes:           []DistributionInfo{},
				grpcTCPWriteSize:                    []DistributionInfo{},
				grpcTCPReadSize:                     []DistributionInfo{},
				grpcTCPSenderLatency:                []DistributionInfo{},
				grpcTCPTransferLatency:              []DistributionInfo{},
				grpcTCPRecurringRetransmits:         map[string]CumulativeCounterInfo{},
				grpcTCPBytesSent:                    map[string]CumulativeCounterInfo{},
				grpcTCPBytesRetransmitted:           map[string]CumulativeCounterInfo{},
				grpcTCPSyscallWrites:                map[string]CumulativeCounterInfo{},
				grpcTCPSyscallReads:                 map[string]CumulativeCounterInfo{},
				megascaleBamm2BitsSent:              map[string]CumulativeCounterInfo{},
				megascaleBamm2BitsReceived:          map[string]CumulativeCounterInfo{},
				megascaleBamm2BitsRead:              map[string]CumulativeCounterInfo{},
				megascaleBamm2BitsWritten:           map[string]CumulativeCounterInfo{},
				megascaleBamm2BitsWrittenWithImm:    map[string]CumulativeCounterInfo{},
			},
		},
		{
			desc: "happy Path",
			ci: containerInfo{
				Namespace: "default",
				Pod:       "my-pod",
				Container: "my-container",
			},
			mi: runtimeMetricsInfo{
				memoryTotal:      map[string]int64{"1": 2},
				memoryUsed:       map[string]int64{"1": 1, "2": 2},
				runtimeDutyCycle: map[string]float64{"1": 10, "3": 5},
				dcnTransferLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 15,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 15,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 15,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 15,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 3,
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
						data: DistributionData{
							Count:                 3,
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
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				collectiveLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 21,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, math.Inf(1)},
						attributes:        map[string]string{"input_size": "512B+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"input_size": "4MB+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 6,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"input_size": "8MB+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 3,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"input_size": "16MB+", "collective_type": "ALL_GATHER"},
					},
				},
				hostToDeviceTransferLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 28,
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
				deviceToHostTransferLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 36,
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
				},
				grpcTCPWriteSize: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 3,
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
				grpcTCPReadSize: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 3,
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
				grpcTCPSenderLatency: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 3,
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
				grpcTCPTransferLatency: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 3,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"transfer_size": "512B+"},
					},
				},
				grpcTCPRecurringRetransmits:      map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPBytesSent:                 map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPBytesRetransmitted:        map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPSyscallWrites:             map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPSyscallReads:              map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsSent:           map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsReceived:       map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsRead:           map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsWritten:        map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsWrittenWithImm: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
			},
		},
		{
			desc: "happy Path + Missing Info",
			ci: containerInfo{
				Namespace: "default",
				Pod:       "my-pod",
				Container: "my-container",
			},
			mi: runtimeMetricsInfo{
				memoryTotal:                         map[string]int64{"1": 2},
				memoryUsed:                          map[string]int64{"1": 1, "2": 2},
				runtimeDutyCycle:                    map[string]float64{},
				dcnTransferLatencies:                []DistributionInfo{},
				mxlaComputeLatencies:                []DistributionInfo{},
				dcnInboundTransferLatencies:         []DistributionInfo{},
				grpcClientCallLatencies:             []DistributionInfo{},
				grpcServerCallLatencies:             []DistributionInfo{},
				grpcTCPMinRtt:                       []DistributionInfo{},
				grpcTCPDeliveryRate:                 []DistributionInfo{},
				grpcTCPPacketsSent:                  map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsRetransmitted:         map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPPacketsSpuriousRetransmitted: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				collectiveLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 21,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5, 6},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, 16, math.Inf(1)},
						attributes:        map[string]string{"input_size": "512B+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 10,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4},
						},
						bucketUpperBounds: []float64{1, 2, 4, math.Inf(1)},
						attributes:        map[string]string{"input_size": "4MB+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 6,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3},
						},
						bucketUpperBounds: []float64{1, 2, math.Inf(1)},
						attributes:        map[string]string{"input_size": "8MB+", "collective_type": "ALL_REDUCE"},
					},
					{
						data: DistributionData{
							Count:                 3,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2},
						},
						bucketUpperBounds: []float64{1, math.Inf(1)},
						attributes:        map[string]string{"input_size": "16MB+", "collective_type": "ALL_GATHER"},
					},
				},
				hostToDeviceTransferLatencies: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 28,
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
				deviceToHostTransferLatencies:    []DistributionInfo{},
				megascaleBamm2BitsSent:           map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsReceived:       map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsRead:           map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsWritten:        map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				megascaleBamm2BitsWrittenWithImm: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				dcnInboundTransferSizes: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"type": "grpc"},
					},
				},
				dcnTransferSizes: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"type": "grpc"},
					},
				},
				mxlaComputeOperandSize: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
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
				collectiveInputSizes: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
							Min:                   1,
							Max:                   10,
							Mean:                  5,
							SumOfSquaredDeviation: 1,
							BucketCounts:          []int64{1, 2, 3, 4, 5},
						},
						bucketUpperBounds: []float64{1, 2, 4, 8, math.Inf(1)},
						attributes:        map[string]string{"collective_type": "ALL_REDUCE", "execution_type": "th"},
					},
				},
				deviceToHostTransferSizes: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
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
				hostToDeviceTransferSizes: []DistributionInfo{
					{
						data: DistributionData{
							Count:                 15,
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
				grpcTCPWriteSize:            []DistributionInfo{},
				grpcTCPReadSize:             []DistributionInfo{},
				grpcTCPSenderLatency:        []DistributionInfo{},
				grpcTCPTransferLatency:      []DistributionInfo{},
				grpcTCPRecurringRetransmits: map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPBytesSent:            map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPBytesRetransmitted:   map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPSyscallWrites:        map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
				grpcTCPSyscallReads:         map[string]CumulativeCounterInfo{"1": {start_time, start_time.Add(time.Second), 2}},
			},
		},
	}
	for _, tc := range testCases {
		pms := NewPromMetricsServer(2112, "/metrics", "fake-instance", "tpu", "v5", "2x2")
		pms.UpdateRuntimeMetrics(tc.ci, tc.mi)
		var want string
		var wantNode string
		if tc.ci != (containerInfo{}) && len(tc.mi.runtimeDutyCycle) != 0 {
			want = `
# HELP duty_cycle Percent of time when the TPU was actively processing
# TYPE duty_cycle gauge
duty_cycle{accelerator_id="fake-instance-1",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
duty_cycle{accelerator_id="fake-instance-3",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 5
			`
			wantNode = `
# HELP duty_cycle_node Percent of time when the TPU was actively processing
# TYPE duty_cycle_node gauge
duty_cycle_node{accelerator_id="fake-instance-1",make="tpu",model="v5",tpu_topology="2x2"} 10
duty_cycle_node{accelerator_id="fake-instance-3",make="tpu",model="v5",tpu_topology="2x2"} 5
			`
		}
		if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(want), "duty_cycle"); err != nil {
			t.Fatalf("Test: %s. Failed comparing DutyCycleProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(DutyCycleNodeProm, strings.NewReader(wantNode), "duty_cycle_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing DutyCycleNodeProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.memoryTotal) != 0 {
			want = `
# HELP memory_total Total memory available on the TPU in bytes
# TYPE memory_total gauge
memory_total{accelerator_id="fake-instance-1",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
			wantNode = `
# HELP memory_total_node Total memory available on the TPU in bytes
# TYPE memory_total_node gauge
memory_total_node{accelerator_id="fake-instance-1",make="tpu",model="v5",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(MemoryTotalProm, strings.NewReader(want), "memory_total"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryTotalProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryTotalNodeProm, strings.NewReader(wantNode), "memory_total_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryTotalNodeProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.memoryUsed) != 0 {
			want = `
# HELP memory_used Allocated TPU memory in bytes
# TYPE memory_used gauge
memory_used{accelerator_id="fake-instance-1",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 1
memory_used{accelerator_id="fake-instance-2",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
			wantNode = `
# HELP memory_used_node Allocated TPU memory in bytes
# TYPE memory_used_node gauge
memory_used_node{accelerator_id="fake-instance-1",make="tpu",model="v5",tpu_topology="2x2"} 1
memory_used_node{accelerator_id="fake-instance-2",make="tpu",model="v5",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(MemoryUsedProm, strings.NewReader(want), "memory_used"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryUsedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryUsedNodeProm, strings.NewReader(wantNode), "memory_used_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryUsedNodeProm metric: %v", tc.desc, err)
		}

		want = `
# HELP dcn_transfer_latencies_microsecond Distribution of network-transfer latencies for multislice traffic
# TYPE dcn_transfer_latencies_microsecond histogram
dcn_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="8.0"} 10
dcn_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 15
dcn_transfer_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 75
dcn_transfer_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
dcn_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 10
dcn_transfer_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 50
dcn_transfer_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 10
dcn_transfer_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_transfer_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 3
dcn_transfer_latencies_microsecond_sum{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
dcn_transfer_latencies_microsecond_count{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.dcnTransferLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP compute_latencies_microsecond Host compute latency in microseconds. Measures the time it takes to compute a reduction operation in microseconds.
# TYPE compute_latencies_microsecond histogram
compute_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
compute_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
compute_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
compute_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
compute_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
compute_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
compute_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
compute_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
compute_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
compute_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
compute_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 10
compute_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 50
compute_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
compute_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
compute_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
compute_latencies_microsecond_sum{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
compute_latencies_microsecond_count{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
# HELP dcn_inbound_transfer_latencies_microsecond Distribution of network-transfer latencies for inbound multislice traffic
# TYPE dcn_inbound_transfer_latencies_microsecond histogram
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="8.0"} 10
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 15
dcn_inbound_transfer_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 75
dcn_inbound_transfer_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 10
dcn_inbound_transfer_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 50
dcn_inbound_transfer_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 10
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_inbound_transfer_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 3
dcn_inbound_transfer_latencies_microsecond_sum{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
dcn_inbound_transfer_latencies_microsecond_count{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.dcnInboundTransferLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "compute_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing compute_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_inbound_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_inbound_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_client_call_latencies_microsecond Distribution of network-transfer latencies for the gRPC library, measuring the time it takes to complete an RPC from the application's perspective
# TYPE grpc_client_call_latencies_microsecond histogram
grpc_client_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_client_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
grpc_client_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
grpc_client_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
grpc_client_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
grpc_client_call_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
grpc_client_call_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_client_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_client_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
grpc_client_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
grpc_client_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 10
grpc_client_call_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 50
grpc_client_call_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
grpc_client_call_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_client_call_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_client_call_latencies_microsecond_sum{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_client_call_latencies_microsecond_count{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcClientCallLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_client_call_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_client_call_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_server_call_latencies_microsecond Distribution of network-transfer latencies for gRPC server to complete an RPC on transport’s perspective
# TYPE grpc_server_call_latencies_microsecond histogram
grpc_server_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_server_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
grpc_server_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
grpc_server_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
grpc_server_call_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
grpc_server_call_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
grpc_server_call_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_server_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_server_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
grpc_server_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
grpc_server_call_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 10
grpc_server_call_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 50
grpc_server_call_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
grpc_server_call_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_server_call_latencies_microsecond_bucket{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_server_call_latencies_microsecond_sum{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_server_call_latencies_microsecond_count{buffer_size="2MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcServerCallLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_server_call_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_server_call_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_min_round_trip_times_microsecond Distribution of minimum network-transfer latencies per TCP connection
# TYPE grpc_tcp_min_round_trip_times_microsecond histogram
grpc_tcp_min_round_trip_times_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1"} 1
grpc_tcp_min_round_trip_times_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_tcp_min_round_trip_times_microsecond_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_tcp_min_round_trip_times_microsecond_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPMinRtt) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_min_round_trip_times_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_min_round_trip_times_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_delivery_rates_Mbps Distribution of the TCP connections’ data transfer rates
# TYPE grpc_tcp_delivery_rates_Mbps histogram
grpc_tcp_delivery_rates_Mbps_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1"} 1
grpc_tcp_delivery_rates_Mbps_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_tcp_delivery_rates_Mbps_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_tcp_delivery_rates_Mbps_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
				`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPDeliveryRate) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_delivery_rates_Mbps"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_delivery_rates_Mbps metric: %v", tc.desc, err)
		}
		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPPacketsSent) != 0 {
			want = `
# HELP grpc_tcp_packets_sent_count Total count of packets TCP sends
# TYPE grpc_tcp_packets_sent_count counter
grpc_tcp_packets_sent_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsSentProm, strings.NewReader(want), "grpc_tcp_packets_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsSentProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPPacketsRetransmitted) != 0 {
			want = `
# HELP grpc_tcp_packets_retransmitted_count Total count of packets TCP retransmits
# TYPE grpc_tcp_packets_retransmitted_count counter
grpc_tcp_packets_retransmitted_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsRetransmittedProm, strings.NewReader(want), "grpc_tcp_packets_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsRetransmittedProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPPacketsSpuriousRetransmitted) != 0 {
			want = `
# HELP grpc_tcp_packets_spurious_retransmitted_count Total count of packets TCP spurious retransmits
# TYPE grpc_tcp_packets_spurious_retransmitted_count counter
grpc_tcp_packets_spurious_retransmitted_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsSpuriousRetransmittedProm, strings.NewReader(want), "grpc_tcp_packets_spurious_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsSpuriousRetransmittedProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPRecurringRetransmits) != 0 {
			want = `
# HELP grpc_tcp_recurring_retransmits_count Total count of packets TCP recurring retransmits
# TYPE grpc_tcp_recurring_retransmits_count counter
grpc_tcp_recurring_retransmits_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPRecurringRetransmitsProm, strings.NewReader(want), "grpc_tcp_recurring_retransmits_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPRecurringRetransmitsProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPBytesSent) != 0 {
			want = `
# HELP grpc_tcp_bytes_sent_count Total count of bytes TCP sends
# TYPE grpc_tcp_bytes_sent_count counter
grpc_tcp_bytes_sent_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPBytesSentProm, strings.NewReader(want), "grpc_tcp_bytes_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPBytesSentProm metric: %v", tc.desc, err)
		}

		want = ""
		if tc.ci != (containerInfo{}) && len(tc.mi.megascaleBamm2BitsSent) != 0 {
			want = `
# HELP bamm2_bits_sent_count Total count of bits sent by BAMM2
# TYPE bamm2_bits_sent_count counter
bamm2_bits_sent_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(Bamm2BitsSentProm, strings.NewReader(want), "bamm2_bits_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsSentProm metric: %v", tc.desc, err)
		}

		want = ""
		if tc.ci != (containerInfo{}) && len(tc.mi.megascaleBamm2BitsReceived) != 0 {
			want = `
# HELP bamm2_bits_received_count Total count of bits received by BAMM2
# TYPE bamm2_bits_received_count counter
bamm2_bits_received_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(Bamm2BitsReceivedProm, strings.NewReader(want), "bamm2_bits_received_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsReceivedProm metric: %v", tc.desc, err)
		}

		want = ""
		if tc.ci != (containerInfo{}) && len(tc.mi.megascaleBamm2BitsRead) != 0 {
			want = `
# HELP bamm2_bits_read_count Total count of bits read by BAMM2
# TYPE bamm2_bits_read_count counter
bamm2_bits_read_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(Bamm2BitsReadProm, strings.NewReader(want), "bamm2_bits_read_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsReadProm metric: %v", tc.desc, err)
		}

		want = ""
		if tc.ci != (containerInfo{}) && len(tc.mi.megascaleBamm2BitsWritten) != 0 {
			want = `
# HELP bamm2_bits_written_count Total count of bits written by BAMM2
# TYPE bamm2_bits_written_count counter
bamm2_bits_written_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(Bamm2BitsWrittenProm, strings.NewReader(want), "bamm2_bits_written_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsWrittenProm metric: %v", tc.desc, err)
		}

		want = ""
		if tc.ci != (containerInfo{}) && len(tc.mi.megascaleBamm2BitsWrittenWithImm) != 0 {
			want = `
# HELP bamm2_bits_written_with_imm_count Total count of bits written with imm by BAMM2
# TYPE bamm2_bits_written_with_imm_count counter
bamm2_bits_written_with_imm_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(Bamm2BitsWrittenWithImmProm, strings.NewReader(want), "bamm2_bits_written_with_imm_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsWrittenWithImmProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPBytesRetransmitted) != 0 {
			want = `
# HELP grpc_tcp_bytes_retransmitted_count Total count of bytes TCP retransmits
# TYPE grpc_tcp_bytes_retransmitted_count counter
grpc_tcp_bytes_retransmitted_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPBytesRetransmittedProm, strings.NewReader(want), "grpc_tcp_bytes_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPBytesRetransmittedProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPSyscallWrites) != 0 {
			want = `
# HELP grpc_tcp_syscall_writes_count Total count of TCP syscall writes
# TYPE grpc_tcp_syscall_writes_count counter
grpc_tcp_syscall_writes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPSyscallWritesProm, strings.NewReader(want), "grpc_tcp_syscall_writes_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPSyscallWritesProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) && len(tc.mi.grpcTCPSyscallReads) != 0 {
			want = `
# HELP grpc_tcp_syscall_reads_count Total count of TCP syscall reads
# TYPE grpc_tcp_syscall_reads_count counter
grpc_tcp_syscall_reads_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(GrpcTCPSyscallReadsProm, strings.NewReader(want), "grpc_tcp_syscall_reads_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPSyscallReadsProm metric: %v", tc.desc, err)
		}

		want = `
# HELP collective_end_to_end_latencies_microsecond Distribution of end to end collective latency for multislice traffic
# TYPE collective_end_to_end_latencies_microsecond histogram
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="16.0"} 15
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 21
collective_end_to_end_latencies_microsecond_sum{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 105
collective_end_to_end_latencies_microsecond_count{collective_type="ALL_REDUCE",container="my-container",input_size="512B+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 21
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 10
collective_end_to_end_latencies_microsecond_sum{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 50
collective_end_to_end_latencies_microsecond_count{collective_type="ALL_REDUCE",container="my-container",input_size="4MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="8MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="8MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_REDUCE",container="my-container",input_size="8MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 6
collective_end_to_end_latencies_microsecond_sum{collective_type="ALL_REDUCE",container="my-container",input_size="8MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 30
collective_end_to_end_latencies_microsecond_count{collective_type="ALL_REDUCE",container="my-container",input_size="8MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 6
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_GATHER",container="my-container",input_size="16MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
collective_end_to_end_latencies_microsecond_bucket{collective_type="ALL_GATHER",container="my-container",input_size="16MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
collective_end_to_end_latencies_microsecond_sum{collective_type="ALL_GATHER",container="my-container",input_size="16MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
collective_end_to_end_latencies_microsecond_count{collective_type="ALL_GATHER",container="my-container",input_size="16MB+",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.collectiveLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "collective_end_to_end_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing collective_end_to_end_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP host_to_device_transfer_latencies_microsecond Distribution of host to device transfer latency for each chunk of data for multislice traffic
# TYPE host_to_device_transfer_latencies_microsecond histogram
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="16.0"} 15
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="32.0"} 21
host_to_device_transfer_latencies_microsecond_bucket{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 28
host_to_device_transfer_latencies_microsecond_sum{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 140
host_to_device_transfer_latencies_microsecond_count{buffer_size="1MB+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 28
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.hostToDeviceTransferLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "host_to_device_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing host_to_device_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP device_to_host_transfer_latencies_microsecond Distribution of device to host transfer latency for each chunk of data for multislice traffic
# TYPE device_to_host_transfer_latencies_microsecond histogram
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="16.0"} 15
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="32.0"} 21
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="64.0"} 28
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 36
device_to_host_transfer_latencies_microsecond_sum{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 180
device_to_host_transfer_latencies_microsecond_count{buffer_size="512B+",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 36
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
device_to_host_transfer_latencies_microsecond_bucket{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 10
device_to_host_transfer_latencies_microsecond_sum{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 50
device_to_host_transfer_latencies_microsecond_count{buffer_size="",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 10
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.deviceToHostTransferLatencies) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "device_to_host_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing device_to_host_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_write_sizes_bytes Distribution of TCP write sizes
# TYPE grpc_tcp_write_sizes_bytes histogram
grpc_tcp_write_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_tcp_write_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_tcp_write_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_tcp_write_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPWriteSize) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_write_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_write_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_read_sizes_bytes Distribution of TCP read sizes
# TYPE grpc_tcp_read_sizes_bytes histogram
grpc_tcp_read_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_tcp_read_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_tcp_read_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_tcp_read_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPReadSize) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_read_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_read_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_sender_latencies_microsecond Distribution of TCP sender latencies
# TYPE grpc_tcp_sender_latencies_microsecond histogram
grpc_tcp_sender_latencies_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
grpc_tcp_sender_latencies_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 3
grpc_tcp_sender_latencies_microsecond_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
grpc_tcp_sender_latencies_microsecond_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPSenderLatency) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_sender_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_sender_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP grpc_tcp_transfer_latencies_microsecond Distribution of TCP transfer latencies
# TYPE grpc_tcp_transfer_latencies_microsecond histogram
grpc_tcp_transfer_latencies_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",transfer_size="512B+",le="1.0"} 1
grpc_tcp_transfer_latencies_microsecond_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",transfer_size="512B+",le="+Inf"} 3
grpc_tcp_transfer_latencies_microsecond_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",transfer_size="512B+"} 15
grpc_tcp_transfer_latencies_microsecond_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",transfer_size="512B+"} 3
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.grpcTCPTransferLatency) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}

		want = `
# HELP dcn_inbound_transfer_sizes_bytes Distribution of network inbound transfer sizes
# TYPE dcn_inbound_transfer_sizes_bytes histogram
dcn_inbound_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_inbound_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_inbound_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_inbound_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="8.0"} 10
dcn_inbound_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 15
dcn_inbound_transfer_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 75
dcn_inbound_transfer_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.dcnInboundTransferSizes) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_inbound_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_inbound_transfer_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP dcn_transfer_sizes_bytes Distribution of network transfer sizes
# TYPE dcn_transfer_sizes_bytes histogram
dcn_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="1.0"} 1
dcn_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="2.0"} 3
dcn_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="4.0"} 6
dcn_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="8.0"} 10
dcn_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc",le="+Inf"} 15
dcn_transfer_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 75
dcn_transfer_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",type="grpc"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.dcnTransferSizes) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_transfer_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP mxla_compute_operand_sizes_bytes Distribution of compute operand size
# TYPE mxla_compute_operand_sizes_bytes histogram
mxla_compute_operand_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
mxla_compute_operand_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
mxla_compute_operand_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
mxla_compute_operand_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
mxla_compute_operand_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
mxla_compute_operand_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
mxla_compute_operand_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.mxlaComputeOperandSize) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "mxla_compute_operand_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing mxla_compute_operand_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP collective_input_sizes_bytes Distribution of collective input sizes
# TYPE collective_input_sizes_bytes histogram
collective_input_sizes_bytes_bucket{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
collective_input_sizes_bytes_bucket{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
collective_input_sizes_bytes_bucket{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
collective_input_sizes_bytes_bucket{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
collective_input_sizes_bytes_bucket{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
collective_input_sizes_bytes_sum{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
collective_input_sizes_bytes_count{collective_type="ALL_REDUCE",container="my-container",execution_type="th",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.collectiveInputSizes) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "collective_input_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing collective_input_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP device_to_host_transfer_sizes_bytes Distribution of device to host transfer sizes
# TYPE device_to_host_transfer_sizes_bytes histogram
device_to_host_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
device_to_host_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
device_to_host_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
device_to_host_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
device_to_host_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
device_to_host_transfer_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
device_to_host_transfer_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.deviceToHostTransferSizes) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "device_to_host_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing device_to_host_transfer_sizes_bytes metric: %v", tc.desc, err)
		}

		want = `
# HELP host_to_device_transfer_sizes_bytes Distribution of host to device transfer sizes
# TYPE host_to_device_transfer_sizes_bytes histogram
host_to_device_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="1.0"} 1
host_to_device_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="2.0"} 3
host_to_device_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="4.0"} 6
host_to_device_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="8.0"} 10
host_to_device_transfer_sizes_bytes_bucket{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2",le="+Inf"} 15
host_to_device_transfer_sizes_bytes_sum{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 75
host_to_device_transfer_sizes_bytes_count{container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 15
		`
		if tc.ci == (containerInfo{}) || len(tc.mi.hostToDeviceTransferSizes) == 0 {
			want = ``
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "host_to_device_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing host_to_device_transfer_sizes_bytes metric: %v", tc.desc, err)
		}

		// Test CleanupStaleMetrics
		// Call CleanupStaleMetrics with an empty active containers list and a threshold of 0
		// to trigger a cleanup of all metrics.
		pms.CleanupStaleMetrics([]containerInfo{}, 0)

		want = ""
		wantNode = ""
		if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(want), "duty_cycle"); err != nil {
			t.Fatalf("Test: %s. Failed comparing DutyCycleProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(DutyCycleNodeProm, strings.NewReader(wantNode), "duty_cycle_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing DutyCycleNodeProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryTotalProm, strings.NewReader(want), "memory_total"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryTotalProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryTotalNodeProm, strings.NewReader(wantNode), "memory_total_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryTotalNodeProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryUsedProm, strings.NewReader(want), "memory_used"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryUsedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryUsedNodeProm, strings.NewReader(wantNode), "memory_used_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryUsedNodeProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "compute_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing compute_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_inbound_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_inbound_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_client_call_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_client_call_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_server_call_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_server_call_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_min_round_trip_times_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_min_round_trip_times_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_delivery_rates_Mbps"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_delivery_rates_Mbps metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsSentProm, strings.NewReader(want), "grpc_tcp_packets_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsSentProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsRetransmittedProm, strings.NewReader(want), "grpc_tcp_packets_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsRetransmittedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPPacketsSpuriousRetransmittedProm, strings.NewReader(want), "grpc_tcp_packets_spurious_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPPacketsSpuriousRetransmittedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPRecurringRetransmitsProm, strings.NewReader(want), "grpc_tcp_recurring_retransmits_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPRecurringRetransmitsProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPBytesSentProm, strings.NewReader(want), "grpc_tcp_bytes_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPBytesSentProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(Bamm2BitsSentProm, strings.NewReader(want), "bamm2_bits_sent_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsSentProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(Bamm2BitsReceivedProm, strings.NewReader(want), "bamm2_bits_received_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsReceivedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(Bamm2BitsReadProm, strings.NewReader(want), "bamm2_bits_read_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsReadProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(Bamm2BitsWrittenProm, strings.NewReader(want), "bamm2_bits_written_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsWrittenProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(Bamm2BitsWrittenWithImmProm, strings.NewReader(want), "bamm2_bits_written_with_imm_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing Bamm2BitsWrittenWithImmProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPBytesRetransmittedProm, strings.NewReader(want), "grpc_tcp_bytes_retransmitted_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPBytesRetransmittedProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPSyscallWritesProm, strings.NewReader(want), "grpc_tcp_syscall_writes_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPSyscallWritesProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(GrpcTCPSyscallReadsProm, strings.NewReader(want), "grpc_tcp_syscall_reads_count"); err != nil {
			t.Fatalf("Test: %s. Failed comparing GrpcTCPSyscallReadsProm metric: %v", tc.desc, err)
		}

		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_write_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_write_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_read_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_read_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_sender_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_sender_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "grpc_tcp_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing grpc_tcp_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "collective_end_to_end_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing collective_end_to_end_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "host_to_device_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing host_to_device_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "device_to_host_transfer_latencies_microsecond"); err != nil {
			t.Fatalf("Test: %s. Failed comparing device_to_host_transfer_latencies_microsecond metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_inbound_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_inbound_transfer_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "dcn_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing dcn_transfer_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "mxla_compute_operand_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing mxla_compute_operand_sizes_bytes metric: %v", tc.desc, err)
		}

		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "collective_input_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing collective_input_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "device_to_host_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing device_to_host_transfer_sizes_bytes metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(pms.hc, strings.NewReader(want), "host_to_device_transfer_sizes_bytes"); err != nil {
			t.Fatalf("Test: %s. Failed comparing host_to_device_transfer_sizes_bytes metric: %v", tc.desc, err)
		}

	}
}

func TestUpdateHostMetrics(t *testing.T) {
	testCases := []struct {
		desc     string
		ci       containerInfo
		mi       hostMetricsInfo
		skipNode bool
	}{
		{
			desc: "Empty containerInfo + Skip UpdatingNodeMetric",
			ci:   containerInfo{},
			mi: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{"1": 2},
				memoryBandwidthUtilization: map[string]float64{"1": 2, "2": 3},
			},
			skipNode: true,
		},
		{
			desc: "Empty containerInfo + Update HostMetrics for only Node",
			ci:   containerInfo{},
			mi: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{"1": 2},
				memoryBandwidthUtilization: map[string]float64{"1": 2, "2": 3},
			},
			skipNode: false,
		},
		{
			desc: "Update HostMetrics + Skip UpdatingNodeMetric",
			ci: containerInfo{
				Namespace: "default",
				Pod:       "my-pod",
				Container: "my-container",
			},
			mi: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{"1": 2},
				memoryBandwidthUtilization: map[string]float64{"1": 2, "2": 3},
			},
			skipNode: true,
		},
		{
			desc: "Update HostMetrics annd NodeMetric",
			ci: containerInfo{
				Namespace: "default",
				Pod:       "my-pod",
				Container: "my-container",
			},
			mi: hostMetricsInfo{
				tensorcoreUtilization:      map[string]float64{"1": 2},
				memoryBandwidthUtilization: map[string]float64{"1": 2, "2": 3},
			},
			skipNode: false,
		},
	}
	for _, tc := range testCases {
		pms := NewPromMetricsServer(2112, "/metrics", "fake-instance", "tpu", "v5", "2x2")
		pms.UpdateHostMetrics(tc.ci, tc.mi, tc.skipNode)
		var want string
		var wantNode string
		if tc.ci != (containerInfo{}) {
			want = `
# HELP tensorcore_utilization Tensorcore percent utilization of the TPU device
# TYPE tensorcore_utilization gauge
tensorcore_utilization{accelerator_id="fake-instance-1",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
			`
		}
		if !tc.skipNode {
			wantNode = `
# HELP tensorcore_utilization_node Tensorcore percent utilization of the TPU device per node
# TYPE tensorcore_utilization_node gauge
tensorcore_utilization_node{accelerator_id="fake-instance-1",make="tpu",model="v5",tpu_topology="2x2"} 2
			`
		}
		if err := testutil.CollectAndCompare(TensorCoreUtilizationProm, strings.NewReader(want), "tensorcore_utilization"); err != nil {
			t.Fatalf("Test: %s. Failed comparing TensorCoreUtilizationProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(TensorCoreUtilizationNodeProm, strings.NewReader(wantNode), "tensorcore_utilization_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing TensorCoreUtilizationNodeProm metric: %v", tc.desc, err)
		}

		if tc.ci != (containerInfo{}) {
			want = `
# HELP memory_bandwidth_utilization Memory bandwidth utilization of the TPU device
# TYPE memory_bandwidth_utilization gauge
memory_bandwidth_utilization{accelerator_id="fake-instance-1",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 2
memory_bandwidth_utilization{accelerator_id="fake-instance-2",container="my-container",make="tpu",model="v5",namespace="default",pod="my-pod",tpu_topology="2x2"} 3
			`
		}
		if !tc.skipNode {
			wantNode = `
# HELP memory_bandwidth_utilization_node Memory bandwidth utilization of the TPU device per node
# TYPE memory_bandwidth_utilization_node gauge
memory_bandwidth_utilization_node{accelerator_id="fake-instance-1",make="tpu",model="v5",tpu_topology="2x2"} 2
memory_bandwidth_utilization_node{accelerator_id="fake-instance-2",make="tpu",model="v5",tpu_topology="2x2"} 3
			`
		}
		if err := testutil.CollectAndCompare(MemoryBandwidthUtilizationProm, strings.NewReader(want), "memory_bandwidth_utilization"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryBandwidthUtilizationProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryBandwidthUtilizationNodeProm, strings.NewReader(wantNode), "memory_bandwidth_utilization_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryBandwidthUtilizationNodeProm metric: %v", tc.desc, err)
		}
		// Test ResetHostMetrics
		pms.lastHostMetricsResetTime = time.Now().Add(-2 * metricsResetInterval)
		pms.UpdateHostMetrics(containerInfo{}, tc.mi, true)

		want = ""
		wantNode = ""
		if err := testutil.CollectAndCompare(TensorCoreUtilizationProm, strings.NewReader(want), "tensorcore_utilization"); err != nil {
			t.Fatalf("Test: %s. Failed comparing TensorCoreUtilizationProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(TensorCoreUtilizationNodeProm, strings.NewReader(wantNode), "tensorcore_utilization_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing TensorCoreUtilizationNodeProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryBandwidthUtilizationProm, strings.NewReader(want), "memory_bandwidth_utilization"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryBandwidthUtilizationProm metric: %v", tc.desc, err)
		}
		if err := testutil.CollectAndCompare(MemoryBandwidthUtilizationNodeProm, strings.NewReader(wantNode), "memory_bandwidth_utilization_node"); err != nil {
			t.Fatalf("Test: %s. Failed comparing MemoryBandwidthUtilizationNodeProm metric: %v", tc.desc, err)
		}
	}
}

func TestCleanupStaleMetricsLifecycle(t *testing.T) {
	pms := NewPromMetricsServer(2112, "/metrics", "fake-instance", "tpu", "v5", "2x2")

	DutyCycleProm.Reset()
	DutyCycleNodeProm.Reset()
	MemoryUsedProm.Reset()

	ci1 := containerInfo{Namespace: "default", Pod: "pod1", Container: "cont1"}
	ci2 := containerInfo{Namespace: "default", Pod: "pod2", Container: "cont2"}

	mi1 := runtimeMetricsInfo{
		runtimeDutyCycle: map[string]float64{"0": 10},
		memoryUsed:       map[string]int64{"0": 100},
	}
	mi2 := runtimeMetricsInfo{
		runtimeDutyCycle: map[string]float64{"1": 20},
		memoryUsed:       map[string]int64{"1": 200},
	}

	pms.UpdateRuntimeMetrics(ci1, mi1)
	pms.UpdateRuntimeMetrics(ci2, mi2)

	wantBoth := `
# HELP duty_cycle Percent of time when the TPU was actively processing
# TYPE duty_cycle gauge
duty_cycle{accelerator_id="fake-instance-0",container="cont1",make="tpu",model="v5",namespace="default",pod="pod1",tpu_topology="2x2"} 10
duty_cycle{accelerator_id="fake-instance-1",container="cont2",make="tpu",model="v5",namespace="default",pod="pod2",tpu_topology="2x2"} 20
`
	if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(wantBoth), "duty_cycle"); err != nil {
		t.Fatalf("Failed comparing DutyCycleProm after initial update: %v", err)
	}

	// First miss for ci2
	pms.CleanupStaleMetrics([]containerInfo{ci1}, 2)

	// Should still be there since threshold is 2
	if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(wantBoth), "duty_cycle"); err != nil {
		t.Fatalf("Failed comparing DutyCycleProm after first miss: %v", err)
	}

	// Second miss for ci2
	pms.CleanupStaleMetrics([]containerInfo{ci1}, 2)

	wantOnlyOne := `
# HELP duty_cycle Percent of time when the TPU was actively processing
# TYPE duty_cycle gauge
duty_cycle{accelerator_id="fake-instance-0",container="cont1",make="tpu",model="v5",namespace="default",pod="pod1",tpu_topology="2x2"} 10
`
	if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(wantOnlyOne), "duty_cycle"); err != nil {
		t.Fatalf("Failed comparing DutyCycleProm after second miss (deletion): %v", err)
	}

	if _, exists := pms.lastRuntimeMetrics[ci2]; exists {
		t.Fatalf("Expected ci2 to be removed from lastRuntimeMetrics cache")
	}

	// ci2 returns
	pms.UpdateRuntimeMetrics(ci2, mi2)
	if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(wantBoth), "duty_cycle"); err != nil {
		t.Fatalf("Failed comparing DutyCycleProm after ci2 returns: %v", err)
	}

	// All containers exit, threshold 1 so it deletes immediately
	pms.CleanupStaleMetrics([]containerInfo{}, 1)
	wantEmpty := ""
	if err := testutil.CollectAndCompare(DutyCycleProm, strings.NewReader(wantEmpty), "duty_cycle"); err != nil {
		t.Fatalf("Failed comparing DutyCycleProm after all containers exit: %v", err)
	}
}

func TestUpdateAcceleratorRequest(t *testing.T) {
	AcceleratorRequestProm.Reset()
	pms := NewPromMetricsServer(2112, "/metrics", "fake-instance", "tpu", "v5", "2x2")
	ci := containerInfo{Namespace: "default", Pod: "my-pod", Container: "my-container"}
	pms.UpdateAcceleratorRequest(ci, 4)

	want := `
# HELP accelerator_request Requested TPU chip count
# TYPE accelerator_request gauge
accelerator_request{container="my-container",namespace="default",pod="my-pod",resource_name="google.com/tpu"} 4
	`
	if err := testutil.CollectAndCompare(AcceleratorRequestProm, strings.NewReader(want), "accelerator_request"); err != nil {
		t.Fatalf("Failed comparing AcceleratorRequestProm metric: %v", err)
	}
}

func TestUpdateNodeTokenBrokerStatus(t *testing.T) {
	NodeTokenBrokerStatusProm.Reset()
	pms := NewPromMetricsServer(2112, "/metrics", "fake-instance", "tpu", "v5", "2x2")
	pms.UpdateNodeTokenBrokerStatus("SUCCESS", "TOKEN_BROKER")
	pms.UpdateNodeTokenBrokerStatus("SUCCESS", "TOKEN_BROKER")
	pms.UpdateNodeTokenBrokerStatus("FAILURE", "FALLBACK")

	want := `
# HELP node_token_broker_status Count of token source requests, broken down by status and mode.
# TYPE node_token_broker_status counter
node_token_broker_status{mode="FALLBACK",status="FAILURE"} 1
node_token_broker_status{mode="TOKEN_BROKER",status="SUCCESS"} 2
	`
	if err := testutil.CollectAndCompare(NodeTokenBrokerStatusProm, strings.NewReader(want), "node_token_broker_status"); err != nil {
		t.Fatalf("Failed comparing NodeTokenBrokerStatusProm metric: %v", err)
	}
}
