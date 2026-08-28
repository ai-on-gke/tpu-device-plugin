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


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FAKE_FILE=${SCRIPT_DIR}/../local-daemonset.yaml

IMAGE=$(cat ${SCRIPT_DIR}/../VERSION)

# Process daemonset.yaml and write directly to local-daemonset.yaml without in-place editing (which can fail with EXDEV on some filesystems):
# 1. Remove 'namespace: kube-system' and 'priorityClassName: system-node-critical' so it can deploy to default namespace.
# 2. Replace the tpu-device-plugin image tag with the locally built dev image from gcr.io/$USER-gke-dev.
# 3. Rewrite hostPath '/sys' to '/var/run/tpu-fake-sys': On Linux VMs, kernel sysfs (/sys) is read-only, which prevents our
#    fake nodepool script from emulating PCI devices directly under /sys/bus/pci/devices. Redirecting the volume path in
#    local-daemonset.yaml allows local emulation against /var/run/tpu-fake-sys while keeping production daemonset.yaml untouched.
sed -e '/namespace: kube-system/d' \
    -e '/priorityClassName: system-node-critical/d' \
    -e "s|- image:.*tpu-device-plugin.*|- image: gcr.io/$USER-gke-dev/tpu-device-plugin:$IMAGE|g" \
    -e "s|path: /sys|path: /var/run/tpu-fake-sys|g" \
    ${SCRIPT_DIR}/../daemonset.yaml > ${FAKE_FILE}

