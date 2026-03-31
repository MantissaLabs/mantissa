# GPU Setup for Mantissa (NVIDIA)

This document covers the host setup required to let Mantissa reserve NVIDIA GPUs and wire them
into Docker containers. Mantissa only schedules GPUs if they are detected on the host and the
container runtime is configured correctly.

## Prerequisites

- Linux host with NVIDIA GPU(s).
- Docker installed and running (Mantissa uses Docker via Bollard).
- NVIDIA kernel driver installed and working.
- NVIDIA Container Toolkit installed and configured for Docker.

## 1) Install NVIDIA driver

Install the NVIDIA driver using your distro packaging or the NVIDIA installer. Verify the driver
is working before moving on:

```bash
nvidia-smi
```

If `nvidia-smi` fails, the driver is not installed or not loaded.

## 2) Install NVIDIA Container Toolkit

Install the NVIDIA Container Toolkit using the official instructions for your distro. Example for
Debian/Ubuntu (adapt if your environment differs):

```bash
# Install the NVIDIA Container Toolkit
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

# Configure Docker to use the NVIDIA runtime
sudo nvidia-ctk runtime configure --runtime=docker

# Restart Docker
sudo systemctl restart docker
```

## 3) Validate GPU access from Docker

Make sure Docker can see the GPU devices:

```bash
docker run --rm --gpus all nvidia/cuda:12.4.0-base-ubuntu22.04 nvidia-smi
```

You should see the GPU inventory. If this fails, the runtime is not configured correctly.

## 4) How Mantissa maps GPUs

Mantissa detects GPUs using NVML and tracks each GPU separately from CPU/memory slots. GPU
devices are identified by their NVML UUIDs (stable across reboots). When a task requests
`gpu_count > 0`, Mantissa reserves that many GPU device IDs and passes them to Docker via
`NVIDIA_VISIBLE_DEVICES` and the Docker `DeviceRequests` API.

## 5) Optional per-device overrides

Mantissa reads per-node overrides from the config file (`gpu.device_overrides`) to disable GPUs
or override the device IDs used for scheduling and Docker binding. Environment variables still
override the config for backwards compatibility.

Format (semicolon-delimited entries):

```bash
gpu: (
    device_overrides: "uuid:GPU-abc=id:GPU-abc; pci:0000:81:00.0=disable; index:0=id:0",
)
```

Selectors:
- `uuid:<value>` (preferred, stable)
- `pci:<bus>` / `pcibus:<bus>` / `pcibusid:<bus>`
- `index:<n>`

Actions:
- `disable` or `disabled` (exclude the device)
- `id:<device_id>` (override the device ID passed to Docker)

Notes:
- The override `id` must be accepted by the NVIDIA container runtime (typically a GPU UUID or index).
- Use UUID-based selectors whenever possible to keep bindings stable across reboots.

## 6) Request GPUs in Mantissa

### CLI

```bash
mantissa tasks start my-task \
  --image ghcr.io/org/app:latest \
  --cpu-millis 1000 \
  --memory-bytes 1073741824 \
  --gpu-count 1
```

### Service manifest

```ron
(
    name: "gpu-service",
    task_templates: [
        (
            name: "inference",
            image: "ghcr.io/org/inference:latest",
            replicas: 1,
            resources: (
                cpu_millis: 1000,
                memory_mb: 4096,
                gpu_count: 1,
            ),
        ),
    ],
)
```

## 7) Common failure modes

- `nvidia-smi` fails: driver missing or not loaded.
- `docker run --gpus all ...` fails: NVIDIA Container Toolkit not installed or runtime not
  configured.
- Mantissa schedules GPU tasks but containers cannot see GPUs: Docker runtime not configured
  or the node was not restarted after installation.

## Notes

- Mantissa currently allocates whole GPUs (one device per reservation). MIG or time-slicing is
  not yet supported.
- GPU scheduling is only enabled on nodes where NVML detects GPUs and UUIDs are available.
