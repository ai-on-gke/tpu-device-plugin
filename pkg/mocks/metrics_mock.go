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

package mocks

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	grpc "google.golang.org/grpc"
	utilization_metrics_go_proto "tpu-device-plugin/pkg/monitoring/proto/utilization_metrics_go_proto"
	tpu_metric_service_go_proto "tpu-device-plugin/pkg/monitoring/runtime/proto/tpu_metric_service_go_proto"
)

// MockRealTimeProvider is a mock of RealTimeProvider interface.
type MockRealTimeProvider struct {
	ctrl     *gomock.Controller
	recorder *MockRealTimeProviderMockRecorder
}

// MockRealTimeProviderMockRecorder is the mock recorder for MockRealTimeProvider.
type MockRealTimeProviderMockRecorder struct {
	mock *MockRealTimeProvider
}

// NewMockRealTimeProvider creates a new mock instance.
func NewMockRealTimeProvider(ctrl *gomock.Controller) *MockRealTimeProvider {
	mock := &MockRealTimeProvider{ctrl: ctrl}
	mock.recorder = &MockRealTimeProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRealTimeProvider) EXPECT() *MockRealTimeProviderMockRecorder {
	return m.recorder
}

// Now mocks base method.
func (m *MockRealTimeProvider) Now() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Now")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// Now indicates an expected call of Now.
func (mr *MockRealTimeProviderMockRecorder) Now() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Now", reflect.TypeOf((*MockRealTimeProvider)(nil).Now))
}

// MockRuntimeClient is a mock of RuntimeClient interface.
type MockRuntimeClient struct {
	ctrl     *gomock.Controller
	recorder *MockRuntimeClientMockRecorder
}

// MockRuntimeClientMockRecorder is the mock recorder for MockRuntimeClient.
type MockRuntimeClientMockRecorder struct {
	mock *MockRuntimeClient
}

// NewMockRuntimeClient creates a new mock instance.
func NewMockRuntimeClient(ctrl *gomock.Controller) *MockRuntimeClient {
	mock := &MockRuntimeClient{ctrl: ctrl}
	mock.recorder = &MockRuntimeClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRuntimeClient) EXPECT() *MockRuntimeClientMockRecorder {
	return m.recorder
}

// GetRuntimeMetric mocks base method.
func (m *MockRuntimeClient) GetRuntimeMetric(ctx context.Context, in *tpu_metric_service_go_proto.MetricRequest, opts ...grpc.CallOption) (*tpu_metric_service_go_proto.MetricResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetRuntimeMetric", varargs...)
	ret0, _ := ret[0].(*tpu_metric_service_go_proto.MetricResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRuntimeMetric indicates an expected call of GetRuntimeMetric.
func (mr *MockRuntimeClientMockRecorder) GetRuntimeMetric(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRuntimeMetric", reflect.TypeOf((*MockRuntimeClient)(nil).GetRuntimeMetric), varargs...)
}

// ListSupportedMetrics mocks base method.
func (m *MockRuntimeClient) ListSupportedMetrics(ctx context.Context, in *tpu_metric_service_go_proto.ListSupportedMetricsRequest, opts ...grpc.CallOption) (*tpu_metric_service_go_proto.ListSupportedMetricsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListSupportedMetrics", varargs...)
	ret0, _ := ret[0].(*tpu_metric_service_go_proto.ListSupportedMetricsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListSupportedMetrics indicates an expected call of ListSupportedMetrics.
func (mr *MockRuntimeClientMockRecorder) ListSupportedMetrics(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListSupportedMetrics", reflect.TypeOf((*MockRuntimeClient)(nil).ListSupportedMetrics), varargs...)
}

// MockHostMetricsClient is a mock of HostMetricsClient interface.
type MockHostMetricsClient struct {
	ctrl     *gomock.Controller
	recorder *MockHostMetricsClientMockRecorder
}

// MockHostMetricsClientMockRecorder is the mock recorder for MockHostMetricsClient.
type MockHostMetricsClientMockRecorder struct {
	mock *MockHostMetricsClient
}

// NewMockHostMetricsClient creates a new mock instance.
func NewMockHostMetricsClient(ctrl *gomock.Controller) *MockHostMetricsClient {
	mock := &MockHostMetricsClient{ctrl: ctrl}
	mock.recorder = &MockHostMetricsClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHostMetricsClient) EXPECT() *MockHostMetricsClientMockRecorder {
	return m.recorder
}

// ChipIdentifierPerDevice mocks base method.
func (m *MockHostMetricsClient) ChipIdentifierPerDevice() map[string]string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChipIdentifierPerDevice")
	ret0, _ := ret[0].(map[string]string)
	return ret0
}

// ChipIdentifierPerDevice indicates an expected call of ChipIdentifierPerDevice.
func (mr *MockHostMetricsClientMockRecorder) ChipIdentifierPerDevice() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChipIdentifierPerDevice", reflect.TypeOf((*MockHostMetricsClient)(nil).ChipIdentifierPerDevice))
}

// UtilizationPercentagePerDevice mocks base method.
func (m *MockHostMetricsClient) UtilizationPercentagePerDevice(since time.Duration, metricType utilization_metrics_go_proto.UtilizationMetricType) (map[string]float64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UtilizationPercentagePerDevice", since, metricType)
	ret0, _ := ret[0].(map[string]float64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UtilizationPercentagePerDevice indicates an expected call of UtilizationPercentagePerDevice.
func (mr *MockHostMetricsClientMockRecorder) UtilizationPercentagePerDevice(since, metricType interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UtilizationPercentagePerDevice", reflect.TypeOf((*MockHostMetricsClient)(nil).UtilizationPercentagePerDevice), since, metricType)
}
