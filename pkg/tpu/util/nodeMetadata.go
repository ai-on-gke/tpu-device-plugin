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

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	client "k8s.io/client-go/kubernetes"
)

const (
	// FieldManager denotes the actor making the change in Kubernetes.
	FieldManager = "tpu-device-plugin"
)

type NodeMetadataHandler interface {
	// GetLabels returns the labels on the node.
	GetLabels() (map[string]string, error)
	// ApplyLabel applies the specified node label.
	ApplyLabel(context.Context, string, string) error
	// RemoveLabel removes the specified node label identified by the key.
	RemoveLabel(context.Context, string) error
}

type nodeMetadataHandler struct {
	node   string
	client client.Interface
}

func NewNodeMetadataHandler(node string, client client.Interface) NodeMetadataHandler {
	return &nodeMetadataHandler{
		node:   node,
		client: client,
	}
}

type PatchOperation func(*v1.Node, string, any) (map[string]any, bool)

func (n *nodeMetadataHandler) PatchLabelMetadata(node *v1.Node, key string, value any) (map[string]any, bool) {
	if val, present := node.GetObjectMeta().GetLabels()[key]; !present && value == nil || val == value {
		glog.Infof("Node %q has had label %s:%s already. Skip the label change.", n.node, key, val)
		return nil, false
	}
	patch := map[string]any{ // Node
		"metadata": map[string]any{ // ObjectMeta
			"labels": map[string]any{
				key: value,
			},
		},
	}
	return patch, true
}

func (n *nodeMetadataHandler) ChangeMetadata(ctx context.Context, op PatchOperation, key string, value any) error {
	var (
		node *v1.Node
		err  error
	)

	node, err = n.client.CoreV1().Nodes().Get(ctx, n.node, metav1.GetOptions{})
	if err != nil {
		return err
	}
	patch, complete := op(node, key, value)
	if !complete {
		return nil
	}
	data, err := json.Marshal(patch)
	if err != nil {
		glog.Infof("Failed to build patch for metadata: %v", err)
	}

	// Add metadata to node using Patch with JSON Merge Patch
	_, err = n.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, data, metav1.PatchOptions{FieldManager: FieldManager})
	if err != nil {
		glog.Infof("Failed to apply metadata, could not patch node object: %v", err)
		return err
	}

	return nil
}

func (n *nodeMetadataHandler) GetLabels() (map[string]string, error) {
	options := metav1.GetOptions{ResourceVersion: "0"}
	node, err := n.client.CoreV1().Nodes().Get(context.Background(), n.node, options)
	if err != nil {
		return nil, err
	}
	return node.GetObjectMeta().GetLabels(), nil
}

func (n *nodeMetadataHandler) ApplyLabel(ctx context.Context, key, value string) error {
	return n.ChangeMetadata(ctx, n.PatchLabelMetadata, key, value)
}

func (n *nodeMetadataHandler) RemoveLabel(ctx context.Context, key string) error {
	return n.ChangeMetadata(ctx, n.PatchLabelMetadata, key, nil)
}
