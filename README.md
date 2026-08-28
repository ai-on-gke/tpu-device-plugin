# TPU Device Plugin

This repository consists of code for 1-VM TPU Device Plugin on GKE.

> **Disclaimer:** This project is intended for demonstration purposes only. It is not intended for use in a production environment.

See [CONTRIBUTING](CONTRIBUTING.md) for information on how to build, test, get code review approvals, and deploy the plugin.

The minimum GKE node version supported is 1.26.

## Compatibility matrix

### Do we preload the image?

Not currently.

### Compatibility

1. Supported node versions 1.26+.
   The plugin uses device plugin API, `/pods` endpoint.

2. Compatibility with libtpu.
   The plugin sets environment variables for the Pod that will be used by libtpu installed with the application.

  - TODO: libtpu versions

3. TPU devices. The plugin parses TPU node labels and collects metrics specific to device versions.

## Functionality

### Device Plugin registration

Device Plugin will be registering itself with [kubelet](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/#device-plugin-registration) and register TPU devices with it so kubelet will be able to accept Pods using TPU devices.

Some caveats:

- Two device plugins running on a single Node will cause a conflict as they will register the same ResourceName to advertise.
  So if by mistake TPU device plugin was run with two separate DaemonSet, one of them will fail to start as a socket will already be in use.
- TPU device plugin will re-register itself after kubelet restart.
- If TPU device plugin crashed and restarted - it will re-register itself.

### TPU devices discovery

TPU device type and count are discovered based on Node Labels (like `cloud.google.com/gke-tpu-accelerator`).
The control plane creating Nodes is responsible for labels consistency with the hardware.

TPU device plugin has a built-in hardcoded list of possible values for the node labels.
New TPU device types are not automatically recognized by the old device plugin.

### Metrics collection

Two types of metrics are being collected by the device plugin:

- Host metrics
- Runtime (container) metrics

See https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#runtime_metrics for public documentation.

Host metrics are collected generically from the host.

Runtime metrics are relying on each Pod using TPU to expose the metrics on a port `8431` in the form of a gRPC server. In order to obtain runtime metrics, Pod must be running and have an IP address.

tpu-device-plugin calls this gRPC server to collect runtime metrics and pushes
to GCM and also exposes it in prometheus format on port 2112.

### TPU devices health monitoring

It is rare that TPU become unhealthy. TPU hardware problems generally manifesting in the VMs crashing alongside the TPU device.
The rudimentary health monitoring is checking for TPU device presence in `/dev` using `file.exists` API.

## Security

Eligibility for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security) is determined by the [Google Open Source Software Vulnerability Reward Program Rules](https://bughunters.google.com/about/rules/open-source/google-open-source-software-vulnerability-reward-program-rules).

## Contributing

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on setting up your development environment and running tests.

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## License

Copyright 2025 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

