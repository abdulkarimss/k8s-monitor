# ⬡ NEBULA — Kubernetes Intelligence Platform

A real-time terminal UI for Kubernetes and OpenShift clusters, built with [Textual](https://github.com/Textualize/textual).

```
  ███╗   ██╗███████╗██████╗ ██╗   ██╗██╗      █████╗
  ████╗  ██║██╔════╝██╔══██╗██║   ██║██║     ██╔══██╗
  ██╔██╗ ██║█████╗  ██████╔╝██║   ██║██║     ███████║
  ██║╚██╗██║██╔══╝  ██╔══██╗██║   ██║██║     ██╔══██║
  ██║ ╚████║███████╗██████╔╝╚██████╔╝███████╗██║  ██║
  ╚═╝  ╚═══╝╚══════╝╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝
```

## Features

- **14 tabs** — Pods, Nodes, Workloads, Events, Services, Routes, Helm, Operators, CSRs, ClusterOps, PVCs, Ceph, Node Metrics, Top Pods
- **OpenShift + Kubernetes** — auto-detects `oc` or `kubectl`, shows OCP version badge, includes Routes / CSVs / ClusterOperators
- **Live counts** — each tab label shows the current row count
- **Alerts panel** — surfaces CrashLoopBackOff, OOMKilled, high-restart pods, NotReady nodes, recent Warning events
- **Node & pod metrics** — CPU/memory usage bars; falls back to `oc adm top` on OpenShift if metrics-server is absent
- **Full-text filter** — press `/` to search across all tabs simultaneously
- **Column sort** — click any column header to sort ascending / descending
- **Logs viewer** — press `l` on any pod row; multi-container picker included
- **Describe panel** — press `d` or select a row to see `kubectl describe` output inline
- **Context switcher** — press `c` to switch kubeconfig contexts without leaving the TUI
- **Auto-refresh** — data every 60 s, metrics every 120 s

## Requirements

- Python 3.10+
- `kubectl` or `oc` on your `$PATH` and a valid kubeconfig
- `helm` (optional — enables the Helm tab)

## Installation

```bash
git clone https://github.com/abdulkarimss/k8s-monitor.git
cd k8s-monitor
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Usage

```bash
./run.sh
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `1` | Dashboard |
| `2` | Pods |
| `3` | Nodes |
| `4` | Workloads |
| `5` | Events |
| `6` | Routes |
| `7` | ClusterOps |
| `8` | Services |
| `9` | Helm |
| `0` | Operators |
| `-` | CSRs |
| `p` | PVCs |
| `b` | Ceph |
| `m` | Node Metrics |
| `t` | Top Pods |
| `r` | Refresh now |
| `/` | Toggle filter |
| `d` | Describe selected resource |
| `l` | View pod logs |
| `n` | Focus namespace selector |
| `c` | Switch context |
| `q` | Quit |

## Tabs

| Tab | Resources |
|-----|-----------|
| Dashboard | Cluster overview, alerts, node resource bars, top pods |
| Pods | All pods with status, restarts, age, node |
| Nodes | Node status, roles, version |
| Workloads | Deployments + DeploymentConfigs (OCP) |
| Events | Warning and Normal events, sorted by time |
| Services | ClusterIP / NodePort / LoadBalancer services |
| Routes | OpenShift Routes with TLS status |
| Helm | Helm releases across all namespaces |
| Operators | OLM ClusterServiceVersions |
| CSRs | Certificate signing requests and approval status |
| ClusterOps | OpenShift ClusterOperator health |
| PVCs | PersistentVolumeClaims with capacity and status |
| Ceph | Rook-Ceph cluster health and OSD state |
| Node Metrics | Per-node CPU and memory usage (requires metrics-server) |
| Top Pods | Top pods by CPU usage |

## License

MIT
