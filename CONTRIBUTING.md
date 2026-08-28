# How to Contribute to TPU Device Plugin

Please note: This project is currently not accepting external contributions or pull requests. Instead please open an issue and project maintainers will be in touch.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License Agreement (CLA). You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project. Head over to <https://cla.developers.google.com/> to see your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one (even if it was for a different project), you probably don't need to do it again.

## Community Guidelines

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## Development Workflow

### Build Docker Image

```sh
make push REGISTRY=gcr.io/<your-project>/tpu-device-plugin
```

The image with the version from the `VERSION` file will be built and pushed to the specified registry.

### Testing and Presubmits

1. **Verify license headers**:
   ```sh
   make check-license
   ```
   To automatically add missing license headers:
   ```sh
   make addlicense
   ```

2. **Run tests**:
   ```sh
   make test
   ```

3. **Run all presubmits**:
   ```sh
   make presubmit
   ```

## Manual Testing

Once you have a TPU node pool, you can test the local version of the TPU device plugin.

### Test the DaemonSet Device Plugin

1. Deactivate `kube-system`'s default `tpu-device-plugin` by adding the node label `gke-no-default-tpu-device-plugin=true` to your nodepool during creation (supported on GKE `1.32+`).
   - For clusters before `1.32`, deactivate `kube-system`'s TPU manager by running `./hack/deactivate-system-daemonset.sh`.
2. Prepare the local version of the daemonset by running `./hack/prepare-local-daemonset.sh`.
   - This adjusts the namespace and image in the daemonset definition.
3. Apply the local daemonset:
   ```sh
   kubectl apply -f local-daemonset.yaml
   ```
4. Verify that your nodes register `google.com/tpu` resources.

### Test on Fake TPU Nodes

If you want to test basic device plugin functionality without physical ASIC accelerators, you can emulate fake TPU nodes on a standard GKE development cluster.

#### 1. Choose Your Hardware Emulation Target

We provide dedicated scripts to emulate both modern TensorNode topologies and legacy character devices:

- **(Recommended) Trillium / TensorNode (`tpu7x`, `2x2x1` topology)**:
  - Run `./hack/fake-nodepool-tpu7x.sh` to create a nodepool emulating 4 PCIe slots (`0000:c0:00`..`03`) and 8 VFIO endpoints (`/dev/vfio/100`..`107`).
  - *Note on sysfs emulation*: Because Linux kernel sysfs (`/sys`) is read-only on live VMs, the Trillium emulation script creates simulated PCI device directories under `/var/run/tpu-fake-sys`. When you run `./hack/prepare-local-daemonset.sh`, it automatically rewrites hostPath `/sys` to `/var/run/tpu-fake-sys` in `local-daemonset.yaml`, enabling full PCI discovery during tests while keeping production `daemonset.yaml` untouched.
- **Legacy TPU v4 (`tpu-v4-podslice`)**:
  - Run `./hack/fake-nodepool.sh` to create a nodepool emulating older character devices (`/dev/accel0`..`/dev/accel3`).

#### 2. Deploy and Verify Local Device Plugin

1. Build and push your custom container image:
   ```sh
   make push REGISTRY=gcr.io/<your-project>/tpu-device-plugin
   ```
2. Generate and apply dev manifests:
   ```sh
   ./hack/prepare-local-daemonset.sh
   kubectl apply -f local-daemonset.yaml
   ```
3. Verify that your node registers capacity:
   ```sh
   kubectl describe node -l cloud.google.com/gke-nodepool=<nodepool-name> | grep -A 10 "Allocatable:"
   ```

#### 3. Schedule Dummy Test Workloads

Since simulated devices cannot execute tensor kernels, use dummy workloads that request hardware using the `pause` or `busybox` image to test socket mounting and environment variable injection:

- **(Recommended) For Trillium `tpu7x`**: `./hack/schedule-pod-using-tpu7x.sh`
- **For Legacy v4**: `./hack/schedule-pod-using-tpu.sh`

