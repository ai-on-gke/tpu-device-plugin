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

package tpu_metric_service_go_proto

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Distribution holds a histogram-style measurement.
type Distribution struct {
	Count                 int64
	Mean                  float64
	Min                   float64
	Max                   float64
	SumOfSquaredDeviation float64
	BucketOptions         *Distribution_BucketOptions
	BucketCounts          []int64
}

func (x *Distribution) GetCount() int64 {
	if x != nil {
		return x.Count
	}
	return 0
}

func (x *Distribution) GetMean() float64 {
	if x != nil {
		return x.Mean
	}
	return 0
}

func (x *Distribution) GetMin() float64 {
	if x != nil {
		return x.Min
	}
	return 0
}

func (x *Distribution) GetMax() float64 {
	if x != nil {
		return x.Max
	}
	return 0
}

func (x *Distribution) GetSumOfSquaredDeviation() float64 {
	if x != nil {
		return x.SumOfSquaredDeviation
	}
	return 0
}

func (x *Distribution) GetBucketOptions() *Distribution_BucketOptions {
	if x != nil {
		return x.BucketOptions
	}
	return nil
}

func (x *Distribution) GetBucketCounts() []int64 {
	if x != nil {
		return x.BucketCounts
	}
	return nil
}

// Distribution_BucketOptions describes the bucket boundaries of a Distribution.
type Distribution_BucketOptions struct {
	// Options is one of Distribution_BucketOptions_ExponentialBuckets.
	Options isDistribution_BucketOptions_Options
}

type isDistribution_BucketOptions_Options interface {
	isDistribution_BucketOptions_Options()
}

func (x *Distribution_BucketOptions) GetOptions() isDistribution_BucketOptions_Options {
	if x != nil {
		return x.Options
	}
	return nil
}

func (x *Distribution_BucketOptions) GetExponentialBuckets() *Distribution_BucketOptions_Exponential {
	if x, ok := x.GetOptions().(*Distribution_BucketOptions_ExponentialBuckets); ok {
		return x.ExponentialBuckets
	}
	return nil
}

type Distribution_BucketOptions_ExponentialBuckets struct {
	ExponentialBuckets *Distribution_BucketOptions_Exponential
}

func (*Distribution_BucketOptions_ExponentialBuckets) isDistribution_BucketOptions_Options() {}

// Distribution_BucketOptions_Exponential describes exponentially sized buckets.
type Distribution_BucketOptions_Exponential struct {
	NumFiniteBuckets int32
	GrowthFactor     float64
	Scale            float64
}

// Gauge is an instantaneous measurement.
type Gauge struct {
	// Value is one of Gauge_AsInt or Gauge_AsDouble.
	Value isGauge_Value
}

type isGauge_Value interface {
	isGauge_Value()
}

type Gauge_AsDouble struct {
	AsDouble float64
}

type Gauge_AsInt struct {
	AsInt int64
}

func (*Gauge_AsDouble) isGauge_Value() {}
func (*Gauge_AsInt) isGauge_Value()    {}

func (x *Gauge) GetValue() isGauge_Value {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Gauge) GetAsDouble() float64 {
	if x, ok := x.GetValue().(*Gauge_AsDouble); ok {
		return x.AsDouble
	}
	return 0
}

func (x *Gauge) GetAsInt() int64 {
	if x, ok := x.GetValue().(*Gauge_AsInt); ok {
		return x.AsInt
	}
	return 0
}

// Counter is a monotonically increasing measurement.
type Counter struct {
	// Value is one of Counter_AsInt or Counter_AsDouble.
	Value isCounter_Value
}

type isCounter_Value interface {
	isCounter_Value()
}

type Counter_AsDouble struct {
	AsDouble float64
}

type Counter_AsInt struct {
	AsInt uint64
}

func (*Counter_AsDouble) isCounter_Value() {}
func (*Counter_AsInt) isCounter_Value()    {}

func (x *Counter) GetValue() isCounter_Value {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Counter) GetAsDouble() float64 {
	if x, ok := x.GetValue().(*Counter_AsDouble); ok {
		return x.AsDouble
	}
	return 0
}

func (x *Counter) GetAsInt() uint64 {
	if x, ok := x.GetValue().(*Counter_AsInt); ok {
		return x.AsInt
	}
	return 0
}

// AttrValue is a typed attribute value.
type AttrValue struct {
	// Attr is one of AttrValue_StringAttr, AttrValue_BoolAttr,
	// AttrValue_IntAttr, AttrValue_DoubleAttr, or AttrValue_KvlistAttr.
	Attr isAttrValue_Attr
}

type isAttrValue_Attr interface {
	isAttrValue_Attr()
}

type AttrValue_StringAttr struct {
	StringAttr string
}

type AttrValue_BoolAttr struct {
	BoolAttr bool
}

type AttrValue_IntAttr struct {
	IntAttr int64
}

type AttrValue_DoubleAttr struct {
	DoubleAttr float64
}

type AttrValue_KvlistAttr struct {
	KvlistAttr *KeyValueList
}

func (*AttrValue_StringAttr) isAttrValue_Attr() {}
func (*AttrValue_BoolAttr) isAttrValue_Attr()   {}
func (*AttrValue_IntAttr) isAttrValue_Attr()    {}
func (*AttrValue_DoubleAttr) isAttrValue_Attr() {}
func (*AttrValue_KvlistAttr) isAttrValue_Attr() {}

func (x *AttrValue) GetAttr() isAttrValue_Attr {
	if x != nil {
		return x.Attr
	}
	return nil
}

func (x *AttrValue) GetStringAttr() string {
	if x, ok := x.GetAttr().(*AttrValue_StringAttr); ok {
		return x.StringAttr
	}
	return ""
}

func (x *AttrValue) GetBoolAttr() bool {
	if x, ok := x.GetAttr().(*AttrValue_BoolAttr); ok {
		return x.BoolAttr
	}
	return false
}

func (x *AttrValue) GetIntAttr() int64 {
	if x, ok := x.GetAttr().(*AttrValue_IntAttr); ok {
		return x.IntAttr
	}
	return 0
}

func (x *AttrValue) GetDoubleAttr() float64 {
	if x, ok := x.GetAttr().(*AttrValue_DoubleAttr); ok {
		return x.DoubleAttr
	}
	return 0
}

func (x *AttrValue) GetKvlistAttr() *KeyValueList {
	if x, ok := x.GetAttr().(*AttrValue_KvlistAttr); ok {
		return x.KvlistAttr
	}
	return nil
}

// KeyValueList is a list of key/value attributes.
type KeyValueList struct {
	Attributes []*Attribute
}

func (x *KeyValueList) GetAttributes() []*Attribute {
	if x != nil {
		return x.Attributes
	}
	return nil
}

// Attribute is a single named attribute value.
type Attribute struct {
	Key   string
	Value *AttrValue
}

func (x *Attribute) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Attribute) GetValue() *AttrValue {
	if x != nil {
		return x.Value
	}
	return nil
}

// Metric is a single measurement with an attribute and optional timestamps.
type Metric struct {
	Attribute      *Attribute
	StartTimestamp *timestamppb.Timestamp
	Timestamp      *timestamppb.Timestamp
	// Measure is one of Metric_Gauge, Metric_Counter, or Metric_Distribution.
	Measure isMetric_Measure
}

type isMetric_Measure interface {
	isMetric_Measure()
}

type Metric_Gauge struct {
	Gauge *Gauge
}

type Metric_Counter struct {
	Counter *Counter
}

type Metric_Distribution struct {
	Distribution *Distribution
}

func (*Metric_Gauge) isMetric_Measure()        {}
func (*Metric_Counter) isMetric_Measure()      {}
func (*Metric_Distribution) isMetric_Measure() {}

func (x *Metric) GetAttribute() *Attribute {
	if x != nil {
		return x.Attribute
	}
	return nil
}

func (x *Metric) GetStartTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTimestamp
	}
	return nil
}

func (x *Metric) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *Metric) GetMeasure() isMetric_Measure {
	if x != nil {
		return x.Measure
	}
	return nil
}

func (x *Metric) GetGauge() *Gauge {
	if x, ok := x.GetMeasure().(*Metric_Gauge); ok {
		return x.Gauge
	}
	return nil
}

func (x *Metric) GetCounter() *Counter {
	if x, ok := x.GetMeasure().(*Metric_Counter); ok {
		return x.Counter
	}
	return nil
}

func (x *Metric) GetDistribution() *Distribution {
	if x, ok := x.GetMeasure().(*Metric_Distribution); ok {
		return x.Distribution
	}
	return nil
}

// TPUMetric is a named collection of per-device metrics.
type TPUMetric struct {
	Name    string
	Metrics []*Metric
}

func (x *TPUMetric) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TPUMetric) GetMetrics() []*Metric {
	if x != nil {
		return x.Metrics
	}
	return nil
}

// MetricRequest requests a single named metric from the runtime.
type MetricRequest struct {
	MetricName string
}

func (x *MetricRequest) GetMetricName() string {
	if x != nil {
		return x.MetricName
	}
	return ""
}

// MetricResponse carries the metric returned for a MetricRequest.
type MetricResponse struct {
	// Response is one of MetricResponse_Metric.
	Response isMetricResponse_Response
}

type isMetricResponse_Response interface {
	isMetricResponse_Response()
}

type MetricResponse_Metric struct {
	Metric *TPUMetric
}

func (*MetricResponse_Metric) isMetricResponse_Response() {}

func (x *MetricResponse) GetResponse() isMetricResponse_Response {
	if x != nil {
		return x.Response
	}
	return nil
}

func (x *MetricResponse) GetMetric() *TPUMetric {
	if x, ok := x.GetResponse().(*MetricResponse_Metric); ok {
		return x.Metric
	}
	return nil
}

// ListSupportedMetricsRequest requests the set of metrics the runtime supports.
type ListSupportedMetricsRequest struct{}

// SupportedMetric describes a single metric the runtime can report.
type SupportedMetric struct {
	MetricName string
}

func (x *SupportedMetric) GetMetricName() string {
	if x != nil {
		return x.MetricName
	}
	return ""
}

// ListSupportedMetricsResponse lists the metrics the runtime supports.
type ListSupportedMetricsResponse struct {
	SupportedMetric []*SupportedMetric
}

func (x *ListSupportedMetricsResponse) GetSupportedMetric() []*SupportedMetric {
	if x != nil {
		return x.SupportedMetric
	}
	return nil
}

// RuntimeMetricServiceClient is the client contract for the TPU runtime metric
// service exposed by the TPU runtime.
type RuntimeMetricServiceClient interface {
	GetRuntimeMetric(ctx context.Context, in *MetricRequest, opts ...grpc.CallOption) (*MetricResponse, error)
	ListSupportedMetrics(ctx context.Context, in *ListSupportedMetricsRequest, opts ...grpc.CallOption) (*ListSupportedMetricsResponse, error)
}

// errUnimplemented is returned by the stub client for every RPC.
var errUnimplemented = status.Error(codes.Unimplemented, "tpu runtime metric service client is a non-functional stub")

type runtimeMetricServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewRuntimeMetricServiceClient returns a non-functional RuntimeMetricService
// client whose RPCs always return codes.Unimplemented.
func NewRuntimeMetricServiceClient(cc grpc.ClientConnInterface) RuntimeMetricServiceClient {
	return &runtimeMetricServiceClient{cc: cc}
}

func (c *runtimeMetricServiceClient) GetRuntimeMetric(ctx context.Context, in *MetricRequest, opts ...grpc.CallOption) (*MetricResponse, error) {
	return nil, errUnimplemented
}

func (c *runtimeMetricServiceClient) ListSupportedMetrics(ctx context.Context, in *ListSupportedMetricsRequest, opts ...grpc.CallOption) (*ListSupportedMetricsResponse, error) {
	return nil, errUnimplemented
}
