#!/usr/bin/env bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

NAMESPACE="kube-system"
DAEMONSET="tpu-device-plugin"

# Get the list of pods
# Ideally we also need to check .metadata.ownerReferences[0].kind=='DaemonSet', but kubectl doesn't support multiple filters
PODS=$(kubectl get pods -n $NAMESPACE -o jsonpath="{.items[?(.metadata.ownerReferences[0].name == '$DAEMONSET')].metadata.name}" )

# Check if there are no pods found
if [ -z "$PODS" ]; then
  echo "No pods found. There are no system tpu device plugins to deactivate."
  exit 0
fi

# Loop over the pods
for POD in $PODS; do
  # Get the node where the pod is running
  NODE=$(kubectl get pod $POD -n $NAMESPACE -o jsonpath="{.spec.nodeName}")

  # Print the pod and node information
  echo "Changing image of pod $POD on node $NODE to pause"

  # Change the image of the pod to busybox
  kubectl set image -n $NAMESPACE pod/$POD tpu-device-plugin=gcr.io/google-containers/pause:3.2
done

