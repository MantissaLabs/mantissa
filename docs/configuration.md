# Mantissa Configuration (RON)

Mantissa can load a RON configuration file to replace most `MANTISSA_*` environment variables.
The CLI already accepts `--config`, and when it is not provided Mantissa searches for the first
existing file in this order:

1) `/etc/mantissa/config.ron`
2) `~/.config/mantissa/config.ron`
3) `~/.mantissa/config.ron`
4) `./mantissa.ron`

If no file is found, Mantissa falls back to built-in defaults. Environment variables still
override the config for backwards compatibility.

## CLI helpers

- `mantissa config show` prints the resolved configuration.
- `mantissa config validate` validates the resolved configuration and exits.
- `mantissa config path` prints the config file path in use (or `<default>`).

## Example

```ron
(
    network: (
        wireguard: (
            enabled: true,
            port: 51820,
            manage_firewall: true,
        ),
        bpf: (
            attach: true,
            artifact_dir: "/opt/mantissa/bpf",
        ),
        nodeport: (
            enabled: true,
            iface: "eth0",
            ip: "192.168.1.10",
        ),
        discovery: (
            health_port: 30080,
        ),
    ),
    docker: (
        host: "unix:///var/run/docker.sock",
    ),
    gpu: (
        device_overrides: "uuid:GPU-abc=id:GPU-abc; pci:0000:81:00.0=disable; index:0=id:0",
    ),
)
```

## Config keys (and legacy env vars)

- `network.wireguard.enabled` (legacy: `MANTISSA_WIREGUARD_DISABLE`)
- `network.wireguard.port` (legacy: `MANTISSA_WIREGUARD_PORT`)
- `network.wireguard.manage_firewall` (legacy: `MANTISSA_WIREGUARD_NO_FIREWALL`)
- `network.bpf.attach` (legacy: `MANTISSA_BPF_NO_ATTACH`, `MANTISSA_SKIP_BPF`)
- `network.bpf.artifact_dir` (legacy: `MANTISSA_BPF_DIR`)
- `network.nodeport.enabled` (legacy: disabled when BPF attach is disabled)
- `network.nodeport.iface` (legacy: `MANTISSA_NODEPORT_IFACE`)
- `network.nodeport.ip` (legacy: `MANTISSA_NODEPORT_IP`)
- `network.discovery.health_port` (legacy: `MANTISSA_LB_HEALTH_PORT`)
- `docker.host` (legacy: `MANTISSA_DOCKER_HOST`, still falls back to `DOCKER_HOST`)
- `gpu.device_overrides` (legacy: `MANTISSA_GPU_DEVICE_OVERRIDES`)
