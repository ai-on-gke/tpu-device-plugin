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
# Creates a fake TensorNode (tpu7x) node pool on a GKE development cluster.
# This script simulates a 4-chip Trillium / TensorNode environment by creating
# /dev/vfio passthrough devices and /sys/bus/pci/devices/0000:xx:yy.z entries
# matching Google PCI vendor ID (0x1ae0) and Trillium device ID (0x0076).

CLUSTER_NAME="${CLUSTER_NAME:-"cluster-device-plugin"}"
NODE_POOL_NAME="fake-tpu7x-nodepool-1"
ZONE="us-central1-a"
PROJECT_NAME=$(gcloud config get-value project)

echo "Project: $PROJECT_NAME"
echo "Zone: $ZONE"
echo "Cluster: $CLUSTER_NAME"
echo "Node Pool: $NODE_POOL_NAME"
echo "Emulating: tpu7x (4 chips, 2x2x1 topology, VFIO passthrough)"

read -p "Proceed with creating fake tpu7x nodepool? (Y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  exit 1
fi

LABELS="cloud.google.com/gke-tpu-accelerator=tpu7x,\
cloud.google.com/gke-tpu-topology=2x2x1,\
cloud.google.com/gke-accelerator-count=4,\
gke-no-default-tpu-device-plugin=true"

echo "Creating GKE node pool $NODE_POOL_NAME..."
gcloud container node-pools create $NODE_POOL_NAME \
  --cluster $CLUSTER_NAME \
  --zone $ZONE \
  --num-nodes 1 \
  --machine-type e2-standard-4 \
  --node-labels $LABELS

# Wait for the node pool to be ready
while true; do
  NODE_POOL_STATUS=$(gcloud container node-pools describe $NODE_POOL_NAME \
    --cluster $CLUSTER_NAME --zone $ZONE --format="value(status)" 2>/dev/null)
  if [[ $NODE_POOL_STATUS == "RUNNING" ]]; then
    break
  fi
  echo "Waiting for node pool $NODE_POOL_NAME to become RUNNING..."
  sleep 10
done

echo "Node pool ready. Deploying fake VFIO and PCI device emulation DaemonSet..."

# Define the DaemonSet that emulates VFIO and PCI slots on the VM filesystem
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fake-tpu7x-devices-$NODE_POOL_NAME
  namespace: kube-system
  labels:
    app: fake-tpu7x-devices
spec:
  selector:
    matchLabels:
      app: fake-tpu7x-devices
  template:
    metadata:
      labels:
        app: fake-tpu7x-devices
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
      - name: fake-tpu7x-devices-init
        image: gke.gcr.io/debian-base:bookworm-v1.0.0-gke.1
        command:
        - /bin/sh
        - -c
        - |
          cat <<'SCRIPT_EOF' > /host/etc/systemd/system/fake-tpu7x-devices.service
          [Unit]
          Description=Fake TPU7x VFIO and PCI Devices Creation Service

          [Service]
          ExecStart=/bin/bash -c '\
            echo "Initializing fake tpu7x devices..."; \
            mkdir -p /dev/vfio /var/run/tpu-fake-sys/bus/pci/devices; \
            mknod -m 666 /dev/vfio/vfio c 10 200 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/100 c 242 0 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/101 c 242 1 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/102 c 242 2 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/103 c 242 3 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/104 c 242 4 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/105 c 242 5 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/106 c 242 6 2>/dev/null || true; \
            mknod -m 666 /dev/vfio/107 c 242 7 2>/dev/null || true; \
            mkdir -p /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.0 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.1 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.0 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.1 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.0 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.1 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.0 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.1; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.0/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.0/device; echo 0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.0/numa_node; ln -sf /dev/vfio/100 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.0/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.1/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.1/device; echo 0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.1/numa_node; ln -sf /dev/vfio/101 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:00.1/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.0/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.0/device; echo 0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.0/numa_node; ln -sf /dev/vfio/102 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.0/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.1/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.1/device; echo 0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.1/numa_node; ln -sf /dev/vfio/103 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:01.1/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.0/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.0/device; echo 1 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.0/numa_node; ln -sf /dev/vfio/104 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.0/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.1/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.1/device; echo 1 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.1/numa_node; ln -sf /dev/vfio/105 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:02.1/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.0/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.0/device; echo 1 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.0/numa_node; ln -sf /dev/vfio/106 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.0/iommu_group 2>/dev/null || true; \
            echo 0x1ae0 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.1/vendor; echo 0x0076 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.1/device; echo 1 > /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.1/numa_node; ln -sf /dev/vfio/107 /var/run/tpu-fake-sys/bus/pci/devices/0000:c0:03.1/iommu_group 2>/dev/null || true; \
            echo "Successfully emulated 8 Trillium/tpu7x virtual functions under PCI bus.";'

          [Install]
          WantedBy=multi-user.target
          SCRIPT_EOF

          nsenter -a -t1 -- systemctl daemon-reload
          nsenter -a -t1 -- systemctl enable fake-tpu7x-devices.service
          nsenter -a -t1 -- systemctl start fake-tpu7x-devices.service
        securityContext:
          privileged: true
        volumeMounts:
        - name: host-systemd
          mountPath: /host/etc/systemd/system
        - name: host-dev
          mountPath: /host/dev
        - name: host-sys
          mountPath: /host/sys
        - name: host-var-run
          mountPath: /var/run
      containers:
      - image: gcr.io/google-containers/pause:3.2
        name: pause
      hostPID: true
      volumes:
      - name: host-systemd
        hostPath:
          path: /etc/systemd/system
          type: Directory
      - name: host-dev
        hostPath:
          path: /dev
          type: Directory
      - name: host-sys
        hostPath:
          path: /sys
          type: Directory
      - name: host-var-run
        hostPath:
          path: /var/run
          type: DirectoryOrCreate
EOF

echo "Fake tpu7x node pool setup complete!"
echo "Deploy your custom device plugin DaemonSet to start testing."
