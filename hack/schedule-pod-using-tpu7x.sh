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

#
# Schedules a dummy pod that consumes 4 TPU7x chips to test allocation and env injection.

echo "Scheduling dummy pod tpu7x-consumer-0..."

kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: tpu7x-consumer-0
  namespace: default
spec:
  nodeSelector:
    cloud.google.com/gke-tpu-accelerator: tpu7x
    cloud.google.com/gke-tpu-topology: 2x2x1
    cloud.google.com/gke-accelerator-count: "4"
  containers:
    - name: pause
      image: busybox:latest
      command:
        - /bin/sh
        - -c
        - |
          echo "=== Injected TPU Environment Variables ==="
          env | grep -E "TPU_|VBAR_|WORKLOAD_|ALT|WRAP|BOUNDS"
          echo "=== Mounted TPU Devices in /dev/vfio ==="
          ls -la /dev/vfio/ || true
          echo "=== Sleeping indefinitely ==="
          sleep infinity
      resources:
        requests:
          "google.com/tpu": 4
        limits:
          "google.com/tpu": 4
EOF
