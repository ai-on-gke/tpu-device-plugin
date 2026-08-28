// Copyright 2017 Google Inc. All Rights Reserved.
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
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetLabels(t *testing.T) {
	for _, test := range []struct {
		desc     string
		labels   map[string]string
		wantResp map[string]string
		wantErr  bool
	}{
		{
			desc:     "Get labels should succeed",
			labels:   map[string]string{"cloud.google.com/test-label": "test-value"},
			wantResp: map[string]string{"cloud.google.com/test-label": "test-value"},
			wantErr:  false,
		},
	} {
		node := makeNode(test.labels, nil)
		clientset := fake.NewSimpleClientset(&v1.NodeList{Items: []v1.Node{node}})
		labelHandler := NewNodeMetadataHandler("test-node", clientset)

		labels, err := labelHandler.GetLabels()
		if err != nil && !test.wantErr {
			t.Errorf("GetLabels failed with %v, expected success", err)
		}
		if diff := cmp.Diff(test.wantResp, labels); diff != "" {
			t.Errorf("GetLabels() returned diff (-want +got):\n%s", diff)
		}

		k8sNode, _ := clientset.CoreV1().Nodes().Get(context.Background(), "test-node", metav1.GetOptions{})
		if diff := cmp.Diff(test.wantResp, k8sNode.GetObjectMeta().GetLabels()); diff != "" {
			t.Errorf("GetLabels() returned diff (-want +got):\n%s", diff)
		}
	}
}

func TestApplyLabel(t *testing.T) {
	for _, test := range []struct {
		desc          string
		labels        map[string]string
		applyLabelKey string
		applyLabelVal string
		wantLabels    map[string]string
		wantErr       bool
	}{
		{
			desc:          "Apply label should succeed",
			labels:        map[string]string{},
			applyLabelKey: "cloud.google.com/test-label",
			applyLabelVal: "test-value",
			wantLabels: map[string]string{
				"cloud.google.com/test-label": "test-value",
			},
			wantErr: false,
		},
		{
			desc:          "Apply label when label already present with same value should skip update",
			labels:        map[string]string{"cloud.google.com/test-label": "test-value"},
			applyLabelKey: "cloud.google.com/test-label",
			applyLabelVal: "test-value",
			wantLabels: map[string]string{
				"cloud.google.com/test-label": "test-value",
			},
			wantErr: false,
		},
		{
			desc:          "Apply label when label already present with a different value should update",
			labels:        map[string]string{"cloud.google.com/test-label": "test-value"},
			applyLabelKey: "cloud.google.com/test-label",
			applyLabelVal: "new-value",
			wantLabels: map[string]string{
				"cloud.google.com/test-label": "new-value",
			},
			wantErr: false,
		},
	} {
		node := makeNode(test.labels, nil)
		clientset := fake.NewSimpleClientset(&v1.NodeList{Items: []v1.Node{node}})
		labelHandler := NewNodeMetadataHandler("test-node", clientset)

		err := labelHandler.ApplyLabel(context.Background(), test.applyLabelKey, test.applyLabelVal)
		if err != nil && !test.wantErr {
			t.Errorf("Apply label failed with %v, expected success", err)
		}

		updatedNode, _ := clientset.CoreV1().Nodes().Get(context.Background(), "test-node", metav1.GetOptions{})
		if diff := cmp.Diff(test.wantLabels, updatedNode.GetObjectMeta().GetLabels()); diff != "" {
			t.Errorf("ApplyLabel() returned diff (-want +got):\n%s", diff)
		}
	}
}

func TestRemoveLabel(t *testing.T) {
	for _, test := range []struct {
		desc        string
		labels      map[string]string
		removeLabel string
		wantLabels  map[string]string
		wantErr     bool
	}{
		{
			desc:        "Remove label when label present should succeed",
			labels:      map[string]string{"cloud.google.com/test-label": "test-value"},
			removeLabel: "cloud.google.com/test-label",
			wantLabels:  map[string]string{},
			wantErr:     false,
		},
		{
			desc:        "Remove label when label not present should succeed",
			labels:      map[string]string{},
			removeLabel: "cloud.google.com/test-label",
			wantLabels:  map[string]string{},
			wantErr:     false,
		},
	} {
		node := makeNode(test.labels, nil)
		clientset := fake.NewSimpleClientset(&v1.NodeList{Items: []v1.Node{node}})
		labelHandler := NewNodeMetadataHandler("test-node", clientset)

		err := labelHandler.RemoveLabel(context.Background(), test.removeLabel)
		if err != nil && !test.wantErr {
			t.Errorf("RemoveLabel() returned err: %v", err)
		}

		updatedNode, _ := clientset.CoreV1().Nodes().Get(context.Background(), "test-node", metav1.GetOptions{})
		if diff := cmp.Diff(test.wantLabels, updatedNode.GetObjectMeta().GetLabels()); diff != "" {
			t.Errorf("RemoveLabel() returned diff (-want +got):\n%s", diff)
		}
	}
}

func makeNode(labels map[string]string, annotations map[string]string) v1.Node {
	metadata := metav1.ObjectMeta{
		Name: "test-node",
	}
	if labels != nil {
		metadata.Labels = labels
	}
	if annotations != nil {
		metadata.Annotations = annotations
	}
	return v1.Node{
		ObjectMeta: metadata,
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		Status: v1.NodeStatus{
			NodeInfo: v1.NodeSystemInfo{
				BootID: "1234",
			},
		},
	}
}
