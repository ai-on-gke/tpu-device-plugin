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



CLUSTER_NAME="${CLUSTER_NAME:-"cluster-device-plugin"}"
NODE_POOL_NAME="${NODE_POOL_NAME:-"almost-real-tpu-accelerator-1"}"

PROJECT_NAME=$(gcloud config get-value project)
ZONE=$(gcloud config get-value compute/zone)

echo "Project: $PROJECT_NAME"
echo "Zone: $ZONE"
echo "Cluster: $CLUSTER_NAME"
echo "Node Pool: $NODE_POOL_NAME"

read -p "Proceed with these settings? (Y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  exit 1
fi

LABELS="cloud.google.com/gke-tpu-accelerator=tpu-v4-podslice,\
cloud.google.com/gke-tpu-topology=2x2x2,\
cloud.google.com/gke-accelerator-count=4,\
gke-no-default-tpu-device-plugin=true"

gcloud container node-pools create $NODE_POOL_NAME \
  --cluster $CLUSTER_NAME --num-nodes 1 \
  --node-labels $LABELS

# Wait for the node pool to be ready
while true; do
  NODE_POOL_STATUS=$(gcloud container node-pools describe $NODE_POOL_NAME \
    --cluster $CLUSTER_NAME --format="value(status)")
  if [[ $NODE_POOL_STATUS == "RUNNING" ]]; then
    break
  fi
  echo "Waiting for the node pool to be ready..."
  sleep 10
done

echo "Creating fake devices on nodes in this nodepool"

# Define the DaemonSet
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fake-devices-$NODE_POOL_NAME
  labels:
    app: fake-devices
spec:
  selector:
    matchLabels:
      app: fake-devices
  template:
    metadata:
      labels:
        app: fake-devices
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: cloud.google.com/gke-nodepool
                operator: In
                values:
                - $NODE_POOL_NAME
      initContainers:
      - name: fake-devices
        image: gke.gcr.io/debian-base:bookworm-v1.0.0-gke.1
        # devices cannot be created directly from the container so we
        # need to create a systemd service to create the devices
        command:
        - /bin/sh
        - -c
        - |
          cat <<EOF > /host/etc/systemd/system/fake-devices.service
          [Unit]
          Description=Fake Devices Creation Service

          [Service]
          ExecStart=/bin/bash -c 'echo "Creating devices"; mknod /dev/accel0 b 100 0; mknod /dev/accel1 b 100 1; mknod /dev/accel2 b 100 2; mknod /dev/accel3 b 100 3;'

          [Install]
          WantedBy=multi-user.target
          EOF

          nsenter -a -t1 -- systemctl daemon-reload
          nsenter -a -t1 -- systemctl enable fake-devices.service
          nsenter -a -t1 -- systemctl start fake-devices.service
        securityContext:
          privileged: true
        volumeMounts:
        - name: host-systemd
          mountPath: /host/etc/systemd/system
      containers:
      - image: gcr.io/google-containers/pause:3.2
        name: pause
      hostPID: true
      volumes:
      - name: host-systemd
        hostPath:
          path: /etc/systemd/system
          type: Directory
EOF
