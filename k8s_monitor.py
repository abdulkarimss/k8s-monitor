#!/usr/bin/env python3
"""
NEBULA — Kubernetes Intelligence Platform
Real-time cluster visibility: pods · nodes · deployments · events · metrics · alerts
"""

import concurrent.futures
import json
import shutil
import subprocess
import threading
from datetime import datetime
from typing import Optional

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Log,
    Select,
    Static,
    Tab,
    TabbedContent,
    TabPane,
)

KUBE = "oc" if shutil.which("oc") else "kubectl"
HELM = shutil.which("helm")

APP_NAME    = "NEBULA"
APP_TAGLINE = "Kubernetes Intelligence Platform"
APP_MARK    = f"⬡ {APP_NAME}  •  {APP_TAGLINE}"

COLS: dict[str, list[str]] = {
    # Core
    "pods":       ["NAME", "NAMESPACE", "STATUS", "RESTARTS", "AGE", "NODE"],
    "nodes":      ["NAME", "STATUS", "ROLES", "AGE", "VERSION"],
    "deploys":    ["KIND", "NAME", "NAMESPACE", "READY", "AVAILABLE", "AGE"],
    "events":     ["TYPE", "REASON", "NAMESPACE", "OBJECT", "MESSAGE", "AGE"],
    # Networking
    "svcs":       ["NAME", "NAMESPACE", "TYPE", "CLUSTER-IP", "EXTERNAL-IP", "PORT(S)", "AGE"],
    "routes":     ["NAME", "NAMESPACE", "HOST / PORT", "PATH", "SERVICE", "PORT", "TLS", "AGE"],
    # Helm
    "helm":       ["NAME", "NAMESPACE", "STATUS", "CHART", "APP VER", "REVISION", "UPDATED"],
    # OLM / Operators
    "csvs":       ["NAME", "NAMESPACE", "DISPLAY NAME", "VERSION", "PHASE", "AGE"],
    # Cluster
    "csrs":       ["NAME", "AGE", "SIGNER", "REQUESTOR", "CONDITION"],
    "clusterops": ["NAME", "VERSION", "AVAILABLE", "PROGRESSING", "DEGRADED", "SINCE"],
    # Storage
    "pvcs":       ["NAME", "NAMESPACE", "STATUS", "VOLUME", "CAPACITY", "ACCESS MODES", "STORAGECLASS", "AGE"],
    "ceph":       ["NAME", "NAMESPACE", "PHASE", "HEALTH", "MONITORS", "OSD ACTIVE", "OSD UP", "AGE"],
    # Metrics
    "node_metrics": ["NODE", "ROLES", "STATUS", "CPU", "CPU %", "MEM", "MEM %"],
    "pod_metrics":  ["NAME", "NAMESPACE", "CPU", "MEM"],
}

TAB_LABELS: dict[str, str] = {
    "pods":         "Pods [2]",
    "nodes":        "Nodes [3]",
    "deploys":      "Workloads [4]",
    "events":       "Events [5]",
    "routes":       "Routes [6]",
    "clusterops":   "ClusterOps [7]",
    "svcs":         "Services [8]",
    "helm":         "Helm [9]",
    "csvs":         "Operators [0]",
    "csrs":         "CSRs [-]",
    "pvcs":         "PVCs [p]",
    "ceph":         "Ceph [b]",
    "node_metrics": "Node Metrics [m]",
    "pod_metrics":  "Top Pods [t]",
}

ALERT_PHASES = {
    "ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff", "OOMKilled",
    "Error", "Failed", "CreateContainerConfigError", "InvalidImageName",
    "RunContainerError", "ContainerCannotRun",
}

# ── helpers ───────────────────────────────────────────────────────────────────

def run(args: list[str], timeout: int = 15, ctx: str = "",
        req_timeout: str = "8s") -> tuple[str, str, int]:
    cmd = [KUBE, f"--request-timeout={req_timeout}"]
    if ctx:
        cmd += ["--context", ctx]
    cmd += args
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "cluster unreachable (timeout)", 1
    except Exception as e:
        return "", str(e), 1


def age(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return secs_to_age(int((datetime.now(dt.tzinfo) - dt).total_seconds()))
    except Exception:
        return "-"


def age_secs(ts: str) -> int:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int((datetime.now(dt.tzinfo) - dt).total_seconds())
    except Exception:
        return 0


def secs_to_age(s: int) -> str:
    for v, u in [(86400, "d"), (3600, "h"), (60, "m")]:
        if s >= v:
            return f"{s // v}{u}"
    return f"{s}s"


def node_short(name: str) -> str:
    return name.split(".")[0]


def parse_cpu_m(s: str) -> int:
    s = s.strip()
    if s.endswith("m"):
        try:
            return int(s[:-1])
        except ValueError:
            return 0
    try:
        return int(float(s) * 1000)
    except ValueError:
        return 0


def parse_mem_ki(s: str) -> int:
    s = s.strip()
    for suf, mult in [("Ti", 1024 ** 3), ("Gi", 1024 ** 2), ("Mi", 1024), ("Ki", 1),
                      ("T", 1024 ** 3), ("G", 1024 ** 2), ("M", 1024), ("K", 1)]:
        if s.endswith(suf):
            try:
                return int(s[: -len(suf)]) * mult
            except ValueError:
                return 0
    try:
        return int(s) // 1024
    except ValueError:
        return 0


def fmt_mem(ki: int) -> str:
    if ki >= 1024 * 1024:
        return f"{ki / 1024 / 1024:.1f}Gi"
    if ki >= 1024:
        return f"{ki / 1024:.0f}Mi"
    return f"{ki}Ki"


def fmt_cpu(m: int) -> str:
    return f"{m / 1000:.2f}" if m >= 1000 else f"{m}m"


def pct_bar(pct: float, width: int = 16) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(pct / 100 * width)
    bar = "█" * filled + "░" * (width - filled)
    color = "green" if pct < 70 else "yellow" if pct < 90 else "bold red"
    return f"[{color}]{bar}[/] [{color}]{pct:5.1f}%[/]"


STATUS_STYLE: dict[str, str] = {
    "Running": "bold green", "Ready": "bold green",
    "Completed": "cyan", "Succeeded": "cyan",
    "Pending": "yellow", "ContainerCreating": "yellow", "Terminating": "yellow",
    "Failed": "bold red", "Error": "bold red",
    "CrashLoopBackOff": "bold red", "OOMKilled": "red",
    "ImagePullBackOff": "red", "ErrImagePull": "red",
    "Unknown": "dim", "NotReady": "bold red",
    "True": "green", "False": "red",
    "Warning": "yellow", "Normal": "dim",
}


def styled(text: str, key: str = None) -> Text:
    style = STATUS_STYLE.get(key or text, "")
    return Text(text, style=style)


# ── data fetchers ─────────────────────────────────────────────────────────────

def fetch_contexts() -> tuple[list[str], str]:
    out, _, rc = run(["config", "get-contexts", "-o", "name"])
    contexts = [c.strip() for c in out.strip().splitlines() if c.strip()] if rc == 0 else []
    cur, _, crc = run(["config", "current-context"])
    return contexts, cur.strip() if crc == 0 else ""


def detect_platform(ctx: str = "") -> dict:
    """Probe the cluster, return platform + server version + logged-in user."""
    info: dict = {"is_openshift": False, "version": "", "platform": "Kubernetes",
                  "ocp_major": 0, "server_ver": "", "user": ""}

    # ── logged-in user ────────────────────────────────────────────────────────
    if KUBE == "oc":
        u, _, _ = run(["whoami"], ctx=ctx, timeout=6)
        info["user"] = u.strip()
    else:
        u, _, _ = run(
            ["config", "view", "--minify",
             "-o", "jsonpath={.contexts[0].context.user}"],
            ctx=ctx, timeout=6,
        )
        info["user"] = u.strip()

    # ── OpenShift 4.x — ClusterVersion ───────────────────────────────────────
    out, _, rc = run(
        ["get", "clusterversion", "version",
         "-o", "jsonpath={.status.history[0].version}"],
        ctx=ctx, timeout=8,
    )
    if rc == 0 and out.strip():
        ver = out.strip()
        info.update({"is_openshift": True, "version": ver,
                     "platform": f"OpenShift {ver}",
                     "server_ver": ver,
                     "ocp_major": int(ver.split(".")[0])})
        return info

    # ── OpenShift 3.x ─────────────────────────────────────────────────────────
    _, _, rc2 = run(["get", "projects", "--no-headers"],
                    ctx=ctx, timeout=6)
    if rc2 == 0:
        info.update({"is_openshift": True, "version": "3.x",
                     "platform": "OpenShift 3.x", "ocp_major": 3})
        return info

    # ── Plain Kubernetes — server version ─────────────────────────────────────
    sv, _, rcv = run(
        ["version", "-o", "json"],
        ctx=ctx, timeout=8,
    )
    if rcv == 0 and sv.strip():
        try:
            d = json.loads(sv)
            info["server_ver"] = (
                d.get("serverVersion", {}).get("gitVersion", "")
                or d.get("clientVersion", {}).get("gitVersion", "")
            )
        except Exception:
            pass

    return info


def fetch_namespaces(ctx: str = "", is_openshift: bool = False) -> tuple[list[str], str]:
    # On OpenShift, projects == namespaces but `oc get projects` honours RBAC better
    if is_openshift:
        out, err, rc = run(
            ["get", "projects", "-o", "jsonpath={.items[*].metadata.name}"], ctx=ctx
        )
    else:
        out, err, rc = run(
            ["get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}"], ctx=ctx
        )
    if rc != 0 or not out.strip():
        return ["default"], err.strip().splitlines()[0] if err.strip() else "cannot reach cluster"
    return out.strip().split(), ""


def fetch_pods(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "pods"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_nodes(ctx: str = "") -> list[dict]:
    out, _, rc = run(["get", "nodes", "-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_deployments(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "deployments"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_events(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(
        ["get", "events"] + flag + ["--sort-by=.lastTimestamp", "-o", "json"], ctx=ctx
    )
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_routes(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "routes"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_deploymentconfigs(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "dc"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_cluster_operators(ctx: str = "") -> list[dict]:
    out, _, rc = run(["get", "clusteroperators", "-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_services(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "services"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_helm_releases(ctx: str = "") -> list[dict]:
    if not HELM:
        return []
    cmd = [HELM, "list", "--all-namespaces", "-o", "json"]
    if ctx:
        cmd += ["--kube-context", ctx]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode != 0 or not r.stdout.strip():
            return []
        data = json.loads(r.stdout)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def fetch_csvs(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "csv"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_csrs(ctx: str = "") -> list[dict]:
    out, _, rc = run(["get", "csr", "-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_pvcs(ns: str, ctx: str = "") -> list[dict]:
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, _, rc = run(["get", "pvc"] + flag + ["-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def fetch_ceph_clusters(ctx: str = "") -> list[dict]:
    """Fetch Rook-CephCluster custom resources (all namespaces)."""
    out, _, rc = run(["get", "cephcluster", "--all-namespaces", "-o", "json"], ctx=ctx)
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def _parse_top_nodes(out: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) >= 5:
            # NAME  CPU(cores)  CPU%  MEMORY(bytes)  MEMORY%
            result[parts[0]] = {"cpu_m": parse_cpu_m(parts[1]), "mem_ki": parse_mem_ki(parts[3])}
        elif len(parts) >= 3:
            # NAME  CPU(cores)  MEMORY(bytes)
            result[parts[0]] = {"cpu_m": parse_cpu_m(parts[1]), "mem_ki": parse_mem_ki(parts[2])}
    return result


def fetch_node_metrics(ctx: str = "", is_openshift: bool = False) -> tuple[dict[str, dict], str]:
    """Returns (metrics_dict, error_string). Tries oc adm top nodes on OCP as fallback."""
    out, err, rc = run(["top", "nodes", "--no-headers"],
                       timeout=35, ctx=ctx, req_timeout="25s")
    if rc == 0 and out.strip():
        return _parse_top_nodes(out), ""

    # OpenShift fallback — uses built-in Prometheus stack
    if is_openshift and KUBE == "oc":
        out2, err2, rc2 = run(["adm", "top", "nodes", "--no-headers"],
                               timeout=35, ctx=ctx, req_timeout="25s")
        if rc2 == 0 and out2.strip():
            return _parse_top_nodes(out2), ""
        err = err2 or err

    return {}, (err.strip().splitlines()[0] if err.strip() else "metrics-server unavailable")


def fetch_pod_metrics(ns: str, ctx: str = "",
                      is_openshift: bool = False) -> tuple[list[dict], str]:
    """Returns (metrics_list, error_string)."""
    flag = ["--all-namespaces"] if ns == "_all" else ["-n", ns]
    out, err, rc = run(["top", "pods"] + flag + ["--no-headers"],
                       timeout=35, ctx=ctx, req_timeout="25s")

    # OpenShift fallback
    if (rc != 0 or not out.strip()) and is_openshift and KUBE == "oc":
        out2, err2, rc2 = run(["adm", "top", "pods"] + flag + ["--no-headers"],
                               timeout=35, ctx=ctx, req_timeout="25s")
        if rc2 == 0 and out2.strip():
            out, rc = out2, 0
        else:
            err = err2 or err

    result: list[dict] = []
    if rc == 0 and out.strip():
        for line in out.strip().splitlines():
            parts = line.split()
            if ns == "_all":
                if len(parts) >= 6:
                    # NAMESPACE NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
                    result.append({"namespace": parts[0], "name": parts[1],
                                   "cpu_m": parse_cpu_m(parts[2]), "mem_ki": parse_mem_ki(parts[4])})
                elif len(parts) >= 4:
                    result.append({"namespace": parts[0], "name": parts[1],
                                   "cpu_m": parse_cpu_m(parts[2]), "mem_ki": parse_mem_ki(parts[3])})
            else:
                if len(parts) >= 5:
                    # NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
                    result.append({"namespace": ns, "name": parts[0],
                                   "cpu_m": parse_cpu_m(parts[1]), "mem_ki": parse_mem_ki(parts[3])})
                elif len(parts) >= 3:
                    result.append({"namespace": ns, "name": parts[0],
                                   "cpu_m": parse_cpu_m(parts[1]), "mem_ki": parse_mem_ki(parts[2])})

    err_msg = ""
    if not result:
        err_msg = err.strip().splitlines()[0] if err.strip() else ""
    return result, err_msg


def fetch_pod_containers(name: str, ns: str, ctx: str = "") -> list[str]:
    out, _, rc = run(
        ["get", "pod", name, "-n", ns,
         "-o", "jsonpath={.spec.containers[*].name}"],
        ctx=ctx,
    )
    if rc != 0 or not out.strip():
        return []
    return out.strip().split()


def fetch_logs(name: str, ns: str, container: str = "", tail: int = 500, ctx: str = "") -> str:
    args = ["logs", name, "--tail", str(tail)]
    if ns and ns != "_all":
        args += ["-n", ns]
    if container:
        args += ["-c", container]
    out, err, _ = run(args, timeout=30, ctx=ctx)
    return out or err


def fetch_describe(kind: str, name: str, ns: Optional[str], ctx: str = "") -> str:
    args = ["describe", kind, name]
    if ns and ns != "_all":
        args += ["-n", ns]
    out, err, _ = run(args, timeout=20, ctx=ctx)
    return out or err


# ── pod status ────────────────────────────────────────────────────────────────

def pod_status(pod: dict) -> tuple[str, int]:
    status   = pod.get("status", {})
    phase    = status.get("phase", "Unknown")
    cstats   = status.get("containerStatuses", [])
    icstats  = status.get("initContainerStatuses", [])
    restarts = sum(c.get("restartCount", 0) for c in cstats + icstats)

    init_specs = pod.get("spec", {}).get("initContainers", [])
    if init_specs and phase in ("Pending", "Running"):
        init_done = sum(1 for c in icstats if c.get("ready", False))
        total_init = len(init_specs)
        if init_done < total_init:
            for c in icstats:
                state = c.get("state", {})
                if "waiting" in state:
                    reason = state["waiting"].get("reason", "")
                    if reason in ALERT_PHASES:
                        return reason, restarts
            return f"Init:{init_done}/{total_init}", restarts

    for c in cstats:
        state = c.get("state", {})
        if "waiting" in state:
            return state["waiting"].get("reason", "Waiting"), restarts
        if "terminated" in state:
            reason = state["terminated"].get("reason", "Terminated")
            if reason not in ("Completed", "Succeeded"):
                return reason, restarts

    return phase, restarts


# ── alert detection ───────────────────────────────────────────────────────────

def detect_alerts(
    pods: list[dict],
    nodes: list[dict],
    events: list[dict],
) -> list[str]:
    alerts: list[str] = []
    seen_pods: set[str] = set()

    for raw in pods:
        keys = raw["keys"]
        name, ns, phase, restarts, secs = keys[0], keys[1], keys[2], keys[3], keys[4]
        key = f"{ns}/{name}"
        if phase in ALERT_PHASES and key not in seen_pods:
            seen_pods.add(key)
            alerts.append(f"[bold red]● {phase:<22}[/] [dim]{ns}/[/][white]{name}[/]")
        elif phase == "Pending" and secs > 300 and key not in seen_pods:
            seen_pods.add(key)
            alerts.append(f"[yellow]● Pending {secs_to_age(secs):<15}[/] [dim]{ns}/[/][white]{name}[/]")
        if isinstance(restarts, int) and restarts >= 5 and phase not in ALERT_PHASES:
            alerts.append(
                f"[orange3]● High restarts ({restarts:>3}×)[/]  [dim]{ns}/[/][white]{name}[/]"
            )

    for raw in nodes:
        name, status = raw["keys"][0], raw["keys"][1]
        if status == "NotReady":
            alerts.append(f"[bold red]● Node NotReady          [/] [white]{node_short(name)}[/]")

    seen_ev: set[str] = set()
    for raw in events:
        keys = raw["keys"]
        ev_type, reason, ns, obj, msg, secs = (
            keys[0], keys[1], keys[2], keys[3], str(keys[4])[:55], keys[5]
        )
        if ev_type == "Warning" and secs < 1800:
            dedup = f"{reason}/{obj}"
            if dedup not in seen_ev:
                seen_ev.add(dedup)
                alerts.append(
                    f"[yellow]⚠ {reason:<22}[/] [dim]{ns}/[/][white]{obj}[/]  [dim]{msg}[/]"
                )

    return alerts


# ── widgets ───────────────────────────────────────────────────────────────────

class DashboardPane(VerticalScroll):
    DEFAULT_CSS = """
    DashboardPane {
        height: 1fr;
        padding: 1 3;
        background: $surface;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static("  [dim]Connecting to cluster…[/]", id="dash-body")

    def update_content(self, markup: str) -> None:
        self.query_one("#dash-body", Static).update(markup)
        self.scroll_home(animate=False)


class DetailPanel(VerticalScroll):
    DEFAULT_CSS = """
    DetailPanel {
        height: 14;
        border-top: double $accent;
        border-title-color: $accent;
        border-title-align: left;
        padding: 0 1;
        background: $surface;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static(
            "[dim]  ↑↓ select a row · [bold]d[/] describe · [bold]l[/] logs (pods)[/]",
            id="detail-body",
        )

    def set_loading(self, title: str) -> None:
        self.border_title = f" ⟳ {title} "
        self.query_one("#detail-body", Static).update("[dim]Loading…[/]")
        self.scroll_home(animate=False)

    def update(self, content: str, title: str = "") -> None:
        if title:
            self.border_title = f" {title} "
        self.query_one("#detail-body", Static).update(content or "[dim](no output)[/]")
        self.scroll_home(animate=False)


# ── modal screens ─────────────────────────────────────────────────────────────

SPLASH = """\
[bold cyan]
  ███╗   ██╗███████╗██████╗ ██╗   ██╗██╗      █████╗
  ████╗  ██║██╔════╝██╔══██╗██║   ██║██║     ██╔══██╗
  ██╔██╗ ██║█████╗  ██████╔╝██║   ██║██║     ███████║
  ██║╚██╗██║██╔══╝  ██╔══██╗██║   ██║██║     ██╔══██║
  ██║ ╚████║███████╗██████╔╝╚██████╔╝███████╗██║  ██║
  ╚═╝  ╚═══╝╚══════╝╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝
[/][bold white]
         Kubernetes Intelligence Platform
[/][dim]
         ⬡  pods  ·  nodes  ·  deployments
         ⬡  metrics  ·  alerts  ·  logs
[/]
"""


class SplashScreen(ModalScreen):
    """Startup splash — auto-dismissed once first data loads, or after 3 s."""

    # Only Escape/Enter skip it; other keys pass through to the app.
    BINDINGS = [
        Binding("escape", "skip", "Skip", show=False),
        Binding("enter",  "skip", "Skip", show=False),
    ]

    DEFAULT_CSS = """
    SplashScreen { align: center middle; background: $background 90%; }
    SplashScreen > Vertical {
        width: 62;
        height: auto;
        border: double $accent;
        background: $surface;
        padding: 1 2;
        align: center middle;
    }
    #splash-body { text-align: center; }
    #splash-hint { color: $text-muted; text-align: center; height: 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(SPLASH, id="splash-body")
            yield Static(
                "  Connecting to cluster…  Enter / Esc to skip",
                id="splash-hint",
            )

    def on_mount(self) -> None:
        # Safety valve: always dismiss after 3 s even if cluster never responds
        self.set_timer(3.0, self._auto_dismiss)

    def _auto_dismiss(self) -> None:
        if not self._is_dismissed():
            self.dismiss()

    def action_skip(self) -> None:
        self.dismiss()

    def _is_dismissed(self) -> bool:
        try:
            return self.app.screen is not self
        except Exception:
            return True


class ContextModal(ModalScreen):
    BINDINGS = [Binding("escape,q", "dismiss_cancel", "Cancel")]

    DEFAULT_CSS = """
    ContextModal { align: center middle; }
    ContextModal > Vertical {
        width: 76;
        max-height: 26;
        border: thick $accent;
        background: $surface;
    }
    #ctx-modal-title {
        background: $accent;
        color: $background;
        text-style: bold;
        padding: 0 2;
        height: 1;
    }
    #ctx-modal-hint { color: $text-muted; padding: 0 2; height: 1; }
    ListView { height: 1fr; border: none; padding: 0; }
    ListView > ListItem { padding: 0 2; }
    ListView > ListItem.--highlight Label { color: $accent; text-style: bold; }
    """

    def __init__(self, contexts: list[str], current: str) -> None:
        super().__init__()
        self._contexts = contexts
        self._current  = current

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(" ⎇  Switch Context", id="ctx-modal-title")
            yield Static(" ↑↓ navigate   Enter select   Esc cancel", id="ctx-modal-hint")
            with ListView(id="ctx-list"):
                for ctx in self._contexts:
                    active = ctx == self._current
                    yield ListItem(
                        Label(("● " if active else "  ") + ctx),
                        classes="is-current" if active else "",
                    )

    def on_mount(self) -> None:
        lv = self.query_one(ListView)
        if self._current in self._contexts:
            lv.index = self._contexts.index(self._current)
        lv.focus()

    @on(ListView.Selected)
    def _picked(self, event: ListView.Selected) -> None:
        # Read index from the event directly — more reliable than re-querying
        idx = event.list_view.index
        if idx is None:
            idx = self.query_one(ListView).index
        if idx is not None and 0 <= idx < len(self._contexts):
            self.dismiss(self._contexts[idx])

    def action_dismiss_cancel(self) -> None:
        self.dismiss(None)


class ContainerPickerModal(ModalScreen):
    """Choose a container when a pod has multiple containers."""

    BINDINGS = [Binding("escape,q", "dismiss_cancel", "Cancel")]

    DEFAULT_CSS = """
    ContainerPickerModal { align: center middle; }
    ContainerPickerModal > Vertical {
        width: 60;
        max-height: 18;
        border: thick $accent;
        background: $surface;
    }
    #cp-title {
        background: $accent;
        color: $background;
        text-style: bold;
        padding: 0 2;
        height: 1;
    }
    #cp-hint { color: $text-muted; padding: 0 2; height: 1; }
    ListView { height: 1fr; border: none; padding: 0; }
    ListView > ListItem { padding: 0 2; }
    """

    def __init__(self, containers: list[str]) -> None:
        super().__init__()
        self._containers = containers

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(" Select container", id="cp-title")
            yield Static(" ↑↓ navigate   Enter select   Esc cancel", id="cp-hint")
            with ListView(id="cp-list"):
                for name in self._containers:
                    yield ListItem(Label("  " + name))

    def on_mount(self) -> None:
        self.query_one(ListView).focus()

    @on(ListView.Selected)
    def _picked(self, event: ListView.Selected) -> None:
        idx = event.list_view.index
        if idx is None:
            idx = self.query_one(ListView).index
        if idx is not None and 0 <= idx < len(self._containers):
            self.dismiss(self._containers[idx])

    def action_dismiss_cancel(self) -> None:
        self.dismiss(None)


class LogsScreen(ModalScreen):
    BINDINGS = [Binding("q,escape", "dismiss", "Close")]

    DEFAULT_CSS = """
    LogsScreen { align: center middle; }
    LogsScreen > Vertical {
        width: 96%;
        height: 92%;
        border: thick $accent;
        background: $surface;
    }
    LogsScreen .log-title {
        background: $accent;
        color: $background;
        text-style: bold;
        padding: 0 2;
        height: 1;
    }
    LogsScreen Log { height: 1fr; }
    """

    def __init__(self, pod: str, ns: str, container: str = "", ctx: str = "") -> None:
        super().__init__()
        self._pod       = pod
        self._ns        = ns
        self._container = container
        self._ctx       = ctx

    def compose(self) -> ComposeResult:
        cname = f"  container: {self._container}" if self._container else ""
        with Vertical():
            yield Static(
                f" ▶  {self._pod}  [{self._ns}]{cname}     q / Esc → close",
                classes="log-title",
            )
            yield Log(id="log-out", highlight=True)

    def on_mount(self) -> None:
        self._load()

    @work(thread=True)
    def _load(self) -> None:
        text = fetch_logs(self._pod, self._ns, container=self._container, ctx=self._ctx)
        lines = text.splitlines() if text.strip() else ["[dim](no log output)[/]"]

        def write_all() -> None:
            log = self.query_one("#log-out", Log)
            for line in lines:
                log.write_line(line)

        # call_from_thread lives on App, not Screen
        self.app.call_from_thread(write_all)


# ── main app ──────────────────────────────────────────────────────────────────

class K8sMonitor(App):
    TITLE = APP_MARK

    CSS = """
    Screen { layout: vertical; }

    Header { background: $panel; color: $accent; }

    #toolbar {
        height: 3;
        layout: horizontal;
        background: $panel;
        border-bottom: solid $accent-darken-2;
        padding: 0 1;
        align: left middle;
    }
    .tb-label {
        width: auto; padding: 0 1;
        content-align: left middle;
        color: $text-muted;
    }
    .tb-sep {
        width: 1; padding: 0 1;
        content-align: center middle;
        color: $accent-darken-2;
    }
    #ctx-display {
        width: auto;
        max-width: 36;
        padding: 0 1;
        content-align: left middle;
        color: $accent;
        text-style: bold;
    }
    #ns-select { width: 26; }
    #clock {
        width: 1fr;
        content-align: right middle;
        padding: 0 2;
        color: $text-muted;
    }

    #filter-bar {
        height: 3;
        layout: horizontal;
        background: $boost;
        border-bottom: solid $warning;
        padding: 0 1;
        align: left middle;
        display: none;
    }
    #filter-label { width: auto; padding: 0 1; color: $warning; text-style: bold; }
    #filter-input { width: 1fr; }

    TabbedContent { height: 1fr; }
    TabPane       { padding: 0; }
    DataTable     { height: 1fr; }

    DetailPanel { background: $panel; }

    Footer { height: 1; }
    """

    BINDINGS = [
        Binding("q",   "quit",              "Quit"),
        Binding("r",   "refresh",           "Refresh"),
        Binding("l",   "open_logs",         "Logs"),
        Binding("d",   "describe",          "Describe"),
        Binding("n",   "focus_ns",          "Namespace"),
        Binding("c",   "focus_ctx",         "Context"),
        Binding("/",   "toggle_filter",     "Search"),
        Binding("escape", "clear_filter",   "Clear",   show=False),
        Binding("1",   "switch_tab('dashboard')",  "Dashboard",  show=False),
        Binding("2",   "switch_tab('pods')",       "Pods",       show=False),
        Binding("3",   "switch_tab('nodes')",      "Nodes",      show=False),
        Binding("4",   "switch_tab('deploys')",    "Deploys",    show=False),
        Binding("5",   "switch_tab('events')",     "Events",     show=False),
        Binding("6",   "switch_tab('routes')",     "Routes",     show=False),
        Binding("7",   "switch_tab('clusterops')", "ClusterOps", show=False),
        Binding("8",   "switch_tab('svcs')",       "Services",   show=False),
        Binding("9",   "switch_tab('helm')",       "Helm",       show=False),
        Binding("0",   "switch_tab('csvs')",       "Operators",  show=False),
        Binding("-",   "switch_tab('csrs')",       "CSRs",       show=False),
        Binding("p",   "switch_tab('pvcs')",         "PVCs",         show=False),
        Binding("b",   "switch_tab('ceph')",         "Ceph",         show=False),
        Binding("m",   "switch_tab('node_metrics')", "Node Metrics", show=False),
        Binding("t",   "switch_tab('pod_metrics')",  "Top Pods",     show=False),
    ]

    namespace: reactive[str] = reactive("default")

    def __init__(self):
        super().__init__()
        self._selected:        dict       = {}
        self._kube_ctx:        str        = ""
        self._contexts:        list[str]  = []
        self._raw: dict[str, list[dict]] = {
            "pods": [], "nodes": [], "deploys": [], "events": [],
            "routes": [], "clusterops": [],
            "svcs": [], "helm": [], "csvs": [], "csrs": [],
            "pvcs": [], "ceph": [],
            "node_metrics": [], "pod_metrics": [],
        }
        self._node_metrics_meta: list[dict] = []
        self._pod_metrics_meta:  list[dict] = []
        self._pod_meta:        list[dict] = []
        self._node_meta:       list[dict] = []
        self._deploy_meta:     list[dict] = []
        self._route_meta:      list[dict] = []
        self._clusterops_meta: list[dict] = []
        self._svc_meta:        list[dict] = []
        self._helm_meta:       list[dict] = []
        self._csv_meta:        list[dict] = []
        self._csr_meta:        list[dict] = []
        self._pvc_meta:        list[dict] = []
        self._ceph_meta:       list[dict] = []
        self._sort: dict[str, tuple[int, bool]] = {}
        self._filter_text: str = ""
        self._metrics: dict = {"nodes": {}, "pods": []}
        self._node_capacity: dict[str, tuple[int, int]] = {}
        self._metrics_ts:  str = ""
        self._metrics_err: str = ""
        self._fetch_lock  = threading.Lock()
        # Platform detection
        self._is_openshift:  bool = False
        self._platform_ver:  str  = ""
        self._platform_name: str  = "Kubernetes"
        self._server_ver:    str  = ""
        self._cluster_user:  str  = ""

    # ── layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header()

        with Horizontal(id="toolbar"):
            yield Label("CTX", classes="tb-label")
            yield Label("loading…", id="ctx-display")
            yield Label("│", classes="tb-sep")
            yield Label("", id="platform-badge")   # e.g. "OCP 4.14"
            yield Label("│", classes="tb-sep", id="badge-sep")
            yield Label("NS", classes="tb-label")
            yield Select([("default", "default")], id="ns-select", value="default")
            yield Label("", id="clock")

        with Horizontal(id="filter-bar"):
            yield Label(" 🔍 Search:", id="filter-label")
            yield Input(
                placeholder="filter all tabs — name, namespace, status…   Enter → table   Esc → clear",
                id="filter-input",
            )

        with TabbedContent(id="tabs"):
            with TabPane("⬡ Dashboard [1]", id="dashboard"):
                yield DashboardPane(id="dash-pane")
            with TabPane("⬡ Pods [2]", id="pods"):
                yield DataTable(id="pods-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Nodes [3]", id="nodes"):
                yield DataTable(id="nodes-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Workloads [4]", id="deploys"):
                yield DataTable(id="deploys-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Events [5]", id="events"):
                yield DataTable(id="events-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Routes [6]", id="routes"):
                yield DataTable(id="routes-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ ClusterOps [7]", id="clusterops"):
                yield DataTable(id="clusterops-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Services [8]", id="svcs"):
                yield DataTable(id="svcs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Helm [9]", id="helm"):
                yield DataTable(id="helm-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Operators [0]", id="csvs"):
                yield DataTable(id="csvs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ CSRs [-]", id="csrs"):
                yield DataTable(id="csrs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ PVCs [p]", id="pvcs"):
                yield DataTable(id="pvcs-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Ceph [b]", id="ceph"):
                yield DataTable(id="ceph-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Node Metrics [m]", id="node_metrics"):
                yield DataTable(id="node_metrics-table", cursor_type="row", zebra_stripes=True)
            with TabPane("⬡ Top Pods [t]", id="pod_metrics"):
                yield DataTable(id="pod_metrics-table", cursor_type="row", zebra_stripes=True)

        yield DetailPanel(id="detail")
        yield Footer()

    def on_mount(self) -> None:
        self._init_tables()
        self.push_screen(SplashScreen(), callback=lambda _: None)
        self._boot()
        self.set_interval(60,  self._refresh_data)
        self.set_interval(120, self._refresh_metrics)
        self.set_timer(20, self._refresh_metrics)  # initial metrics fetch after boot

    def _init_tables(self) -> None:
        for tab, cols in COLS.items():
            self.query_one(f"#{tab}-table", DataTable).add_columns(*cols)
        # Placeholder in OCP-only tabs until platform is detected
        for tab_id in ("routes", "clusterops", "csvs"):
            t = self.query_one(f"#{tab_id}-table", DataTable)
            t.add_row("[dim]OCP only — will populate if OpenShift is detected[/]",
                      *[""] * (len(COLS[tab_id]) - 1))
        # Helm placeholder if not installed
        if not HELM:
            t = self.query_one("#helm-table", DataTable)
            t.add_row("[dim]helm not found — install helm CLI to enable this tab[/]",
                      *[""] * (len(COLS["helm"]) - 1))
        # Ceph placeholder until first load detects clusters
        t = self.query_one("#ceph-table", DataTable)
        t.add_row("[dim]Checking for Rook-Ceph clusters…[/]", *[""] * (len(COLS["ceph"]) - 1))
        # Metrics placeholders until first metrics fetch (~20s after startup)
        for mid in ("node_metrics", "pod_metrics"):
            t = self.query_one(f"#{mid}-table", DataTable)
            t.add_row("[dim]Fetching metrics…  (appears ~20s after startup)[/]",
                      *[""] * (len(COLS[mid]) - 1))

    # ── boot ──────────────────────────────────────────────────────────────────

    @work(thread=True)
    def _boot(self) -> None:
        contexts, current = fetch_contexts()

        def apply_ctx():
            self._contexts  = contexts
            self._kube_ctx  = current
            ctx_short = current.split("/")[-1][:34] if current else "(no context)"
            self.query_one("#ctx-display", Label).update(ctx_short)
            self.sub_title = current or "(no context)"

        self.call_from_thread(apply_ctx)
        self._load_cluster()

    def _load_cluster(self) -> None:
        # Detect platform first (fast probe: ~1 API call)
        self.call_from_thread(self._set_clock, "⟳ Detecting platform…")
        pinfo = detect_platform(ctx=self._kube_ctx)
        self._is_openshift  = pinfo["is_openshift"]
        self._platform_ver  = pinfo["version"]
        self._platform_name = pinfo["platform"]
        self._server_ver    = pinfo.get("server_ver", "")
        self._cluster_user  = pinfo.get("user", "")

        def apply_platform():
            badge = self.query_one("#platform-badge", Label)
            sep   = self.query_one("#badge-sep", Label)
            if self._is_openshift:
                badge.update(f"[bold red]OCP {self._platform_ver}[/]")
            elif self._server_ver:
                badge.update(f"[dim]{self._server_ver}[/]")
            else:
                badge.update("")
                sep.update("")   # hide separator if no badge

        self.call_from_thread(apply_platform)

        # Use oc get projects on OpenShift, kubectl get namespaces on plain k8s
        namespaces, err = fetch_namespaces(ctx=self._kube_ctx,
                                           is_openshift=self._is_openshift)
        # Prefer "default" or first item; on OCP "default" project might not exist
        preferred = ("default" if "default" in namespaces
                     else (namespaces[0] if namespaces else "default"))
        opts = [("All namespaces", "_all")] + [(ns, ns) for ns in namespaces]

        def apply_ns():
            sel = self.query_one("#ns-select", Select)
            sel.set_options(opts)
            sel.value = preferred
            self.namespace = preferred

        self.call_from_thread(apply_ns)

        if err:
            self.call_from_thread(self._set_clock, f"[bold red]✗ {err[:80]}")

    # ── parallel fetch ────────────────────────────────────────────────────────

    def _fetch_all_parallel(self, ns: str) -> None:
        """Fire API calls in parallel. Lock serialises concurrent refreshes."""
        with self._fetch_lock:  # Blocks — never silently drops a namespace change
            max_w = 12 if self._is_openshift else 8
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
                futures: dict[concurrent.futures.Future, str] = {
                    pool.submit(self._do_pods, ns):      "pods",
                    pool.submit(self._do_nodes):          "nodes",
                    pool.submit(self._do_workloads, ns): "deploys",
                    pool.submit(self._do_events, ns):    "events",
                    pool.submit(self._do_svcs, ns):      "svcs",
                    pool.submit(self._do_csrs):          "csrs",
                    pool.submit(self._do_pvcs, ns):      "pvcs",
                    pool.submit(self._do_ceph):          "ceph",
                }
                if HELM:
                    futures[pool.submit(self._do_helm)] = "helm"
                if self._is_openshift:
                    futures[pool.submit(self._do_routes, ns)]  = "routes"
                    futures[pool.submit(self._do_clusterops)]  = "clusterops"
                    futures[pool.submit(self._do_csvs, ns)]    = "csvs"
                results: dict[str, bool] = {}
                for f, name in futures.items():
                    try:
                        results[name] = f.result()
                    except Exception:
                        results[name] = False

            ts = datetime.now().strftime("%H:%M:%S")
            ctx_label = f"  [{self._kube_ctx}]" if self._kube_ctx else ""
            if not any(results.values()):
                self.call_from_thread(
                    self._set_clock,
                    f"[bold red]✗ Cluster unreachable{ctx_label}  ({ts})",
                )
                self.call_from_thread(
                    self.notify,
                    f"Cannot reach cluster — check VPN / token for {self._kube_ctx}",
                    severity="error", timeout=8,
                )
            else:
                self.call_from_thread(
                    self._set_clock,
                    f"[green]● Updated {ts}[/]  •  NS: {ns}{ctx_label}",
                )
                self.call_from_thread(self._update_dashboard)
                self.call_from_thread(self._dismiss_splash)

    def _dismiss_splash(self) -> None:
        if self.screen.__class__.__name__ == "SplashScreen":
            self.screen.dismiss()

    @work(thread=True, exclusive=True)
    def _refresh_data(self) -> None:
        ns = self.namespace
        self.call_from_thread(self._set_clock, "⟳ Loading…")
        self._fetch_all_parallel(ns)

    @work(thread=True)
    def _refresh_metrics(self) -> None:
        ocp = self._is_openshift
        node_m, node_err = fetch_node_metrics(ctx=self._kube_ctx, is_openshift=ocp)
        pod_m,  _        = fetch_pod_metrics(self.namespace, ctx=self._kube_ctx, is_openshift=ocp)
        ts = datetime.now().strftime("%H:%M:%S")

        def apply():
            self._metrics["nodes"] = node_m
            self._metrics["pods"]  = pod_m
            self._metrics_ts  = ts
            self._metrics_err = node_err
            self._update_dashboard()
            self._update_metrics_tables()

        self.call_from_thread(apply)

    def _update_metrics_tables(self) -> None:
        """Populate Node Metrics and Top Pods tabs from current _metrics data."""
        nm = self._metrics["nodes"]
        nr = self._raw["nodes"]

        # ── Node Metrics tab ─────────────────────────────────────────────────
        node_raw: list[dict] = []
        for r in nr:
            fname      = r["keys"][0]
            role_str   = r["keys"][2]
            node_status = r["keys"][1]
            m          = nm.get(fname, {})
            cpu_m      = m.get("cpu_m", 0)
            mem_ki     = m.get("mem_ki", 0)
            cap_c, cap_m = self._node_capacity.get(fname, (0, 0))
            cpu_pct    = cpu_m / cap_c * 100 if cap_c and cpu_m else 0.0
            mem_pct    = mem_ki / cap_m * 100 if cap_m and mem_ki else 0.0

            def _color(pct: float) -> str:
                return "bold red" if pct > 90 else "yellow" if pct > 70 else "green"

            if m:
                cpu_val = fmt_cpu(cpu_m)
                mem_val = fmt_mem(mem_ki)
                cpu_pct_s = f"{cpu_pct:.1f}%"
                mem_pct_s = f"{mem_pct:.1f}%"
                cpu_cell = Text(cpu_val, style=_color(cpu_pct))
                cpu_pct_cell = Text(cpu_pct_s, style=_color(cpu_pct))
                mem_cell = Text(mem_val, style=_color(mem_pct))
                mem_pct_cell = Text(mem_pct_s, style=_color(mem_pct))
            else:
                cpu_cell = cpu_pct_cell = mem_cell = mem_pct_cell = Text("-", style="dim")
                cpu_pct = mem_pct = 0.0

            node_raw.append({
                "keys": (fname, role_str, node_status, cpu_m, cpu_pct, mem_ki, mem_pct),
                "row":  (node_short(fname)[:22], role_str, styled(node_status),
                         cpu_cell, cpu_pct_cell, mem_cell, mem_pct_cell),
                "meta": {"name": fname, "kind": "node"},
            })
        # Sort by CPU% descending
        node_raw.sort(key=lambda d: d["keys"][4], reverse=True)
        self._raw["node_metrics"] = node_raw
        self._rebuild("node_metrics")

        # ── Top Pods tab ──────────────────────────────────────────────────────
        pm = self._metrics["pods"]
        pod_raw: list[dict] = []
        for p in sorted(pm, key=lambda x: x["cpu_m"], reverse=True):
            cpu_m  = p["cpu_m"]
            mem_ki = p["mem_ki"]
            c = "bold red" if cpu_m > 2000 else "yellow" if cpu_m > 500 else "green"
            pod_raw.append({
                "keys": (p["name"], p["namespace"], cpu_m, mem_ki),
                "row":  (p["name"], p["namespace"],
                         Text(fmt_cpu(cpu_m), style=c),
                         Text(fmt_mem(mem_ki), style="cyan")),
                "meta": {"name": p["name"], "namespace": p["namespace"], "kind": "pod"},
            })
        self._raw["pod_metrics"] = pod_raw
        self._rebuild("pod_metrics")

    def _set_clock(self, msg: str) -> None:
        self.query_one("#clock", Label).update(msg)

    # ── data processors ───────────────────────────────────────────────────────

    def _do_pods(self, ns: str) -> bool:
        items = fetch_pods(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for pod in items:
            m    = pod["metadata"]
            spec = pod.get("spec", {})
            name      = m["name"]
            namespace = m.get("namespace", ns)
            ts        = m.get("creationTimestamp", "")
            node      = spec.get("nodeName", "-")
            phase, restarts = pod_status(pod)
            secs = age_secs(ts)
            raw.append({
                "keys": (name, namespace, phase, restarts, secs, node),
                "row": (
                    name, namespace,
                    styled(phase),
                    Text(str(restarts), style="bold red" if restarts > 5 else ""),
                    age(ts),
                    node_short(node) if node != "-" else "-",
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "pod"},
            })
        self._raw["pods"] = raw
        self.call_from_thread(self._rebuild, "pods")
        return bool(items)

    def _do_nodes(self) -> bool:
        items = fetch_nodes(ctx=self._kube_ctx)
        raw: list[dict] = []
        for node in items:
            m    = node["metadata"]
            st   = node.get("status", {})
            name = m["name"]
            ts   = m.get("creationTimestamp", "")
            ver  = st.get("nodeInfo", {}).get("kubeletVersion", "-")
            secs = age_secs(ts)
            labels   = m.get("labels", {})
            roles    = [k.split("/")[-1] for k in labels if "node-role.kubernetes.io/" in k]
            role_str = ",".join(roles) if roles else "worker"
            conditions  = st.get("conditions", [])
            ready       = next((c for c in conditions if c["type"] == "Ready"), {})
            node_status = "Ready" if ready.get("status") == "True" else "NotReady"
            cap = st.get("capacity", {})
            self._node_capacity[name] = (
                parse_cpu_m(cap.get("cpu", "0")),
                parse_mem_ki(cap.get("memory", "0")),
            )
            raw.append({
                "keys": (name, node_status, role_str, secs, ver),
                "row":  (node_short(name), styled(node_status), role_str, age(ts), ver),
                "meta": {"name": name, "kind": "node"},
            })
        self._raw["nodes"] = raw
        self.call_from_thread(self._rebuild, "nodes")
        return bool(items)

    def _do_workloads(self, ns: str) -> bool:
        """Deployments + DeploymentConfigs (OCP) merged into one table."""
        deploy_items = fetch_deployments(ns, ctx=self._kube_ctx)
        dc_items     = fetch_deploymentconfigs(ns, ctx=self._kube_ctx) if self._is_openshift else []
        raw: list[dict] = []

        def _parse(item: dict, kind: str) -> None:
            m         = item["metadata"]
            spec      = item.get("spec", {})
            st        = item.get("status", {})
            name      = m["name"]
            namespace = m.get("namespace", ns)
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            desired   = spec.get("replicas", 0)
            ready_r   = st.get("readyReplicas", 0)
            available = st.get("availableReplicas", 0)
            ok        = ready_r == desired and desired > 0
            kind_badge = Text("DC", style="cyan") if kind == "dc" else Text("D", style="blue")
            raw.append({
                "keys": (kind, name, namespace, ready_r, available, secs, desired),
                "row": (
                    kind_badge, name, namespace,
                    Text(f"{ready_r}/{desired}", style="green" if ok else "yellow"),
                    str(available), age(ts),
                ),
                "meta": {"name": name, "namespace": namespace,
                         "kind": "deploymentconfig" if kind == "dc" else "deployment"},
            })

        for item in deploy_items:
            _parse(item, "deploy")
        for item in dc_items:
            _parse(item, "dc")

        self._raw["deploys"] = raw
        self.call_from_thread(self._rebuild, "deploys")
        return bool(deploy_items or dc_items)

    def _do_routes(self, ns: str) -> bool:
        items = fetch_routes(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for r in items:
            m    = r["metadata"]
            spec = r.get("spec", {})
            name      = m["name"]
            namespace = m.get("namespace", "-")
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            host   = spec.get("host", "-")
            path   = spec.get("path") or "/"
            tls    = spec.get("tls")
            tls_s  = tls.get("termination", "-") if tls else "none"
            to     = spec.get("to", {})
            svc    = to.get("name", "-")
            port   = spec.get("port", {}).get("targetPort", "-") if spec.get("port") else "-"
            tls_color = "green" if tls else "dim"
            raw.append({
                "keys": (name, namespace, host, path, svc, str(port), tls_s, secs),
                "row": (
                    name, namespace, host, path, svc, str(port),
                    Text(tls_s, style=tls_color),
                    age(ts),
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "route"},
            })
        self._raw["routes"] = raw
        self.call_from_thread(self._rebuild, "routes")
        return bool(items)

    def _do_clusterops(self) -> bool:
        items = fetch_cluster_operators(ctx=self._kube_ctx)
        raw: list[dict] = []
        for co in items:
            m          = co["metadata"]
            st         = co.get("status", {})
            name       = m["name"]
            conditions = st.get("conditions", [])
            versions   = st.get("versions", [])
            ver        = next((v["version"] for v in versions if v.get("name") == "operator"), "-")

            def cond(type_: str) -> tuple[str, str]:
                c = next((x for x in conditions if x["type"] == type_), {})
                return c.get("status", "Unknown"), c.get("lastTransitionTime", "")

            avail_s, avail_ts = cond("Available")
            prog_s,  _        = cond("Progressing")
            degrad_s, _       = cond("Degraded")
            secs = age_secs(avail_ts) if avail_ts else 0

            avail_style  = "green"    if avail_s  == "True" else "red"
            prog_style   = "yellow"   if prog_s   == "True" else "dim"
            degrad_style = "bold red" if degrad_s == "True" else "dim"

            raw.append({
                "keys": (name, ver, avail_s, prog_s, degrad_s, secs),
                "row": (
                    name, ver,
                    Text(avail_s,  style=avail_style),
                    Text(prog_s,   style=prog_style),
                    Text(degrad_s, style=degrad_style),
                    secs_to_age(secs) if secs else "-",
                ),
                "meta": {"name": name, "kind": "clusteroperator"},
            })
        self._raw["clusterops"] = raw
        self.call_from_thread(self._rebuild, "clusterops")
        return bool(items)

    def _do_events(self, ns: str) -> bool:
        items = fetch_events(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for ev in reversed(items[-300:]):
            m         = ev["metadata"]
            namespace = m.get("namespace", "-")
            ev_type   = ev.get("type", "Normal")
            reason    = ev.get("reason", "-")
            msg       = (ev.get("message") or "-")[:90]
            obj       = ev.get("involvedObject", {})
            obj_str   = f"{obj.get('kind','')}/{obj.get('name','')}"
            ts = (
                ev.get("lastTimestamp")
                or ev.get("eventTime")
                or m.get("creationTimestamp", "")
            )
            secs = age_secs(ts) if ts else 0
            raw.append({
                "keys": (ev_type, reason, namespace, obj_str, msg, secs),
                "row":  (styled(ev_type), reason, namespace, obj_str, msg, age(ts) if ts else "-"),
                "meta": None,
            })
        self._raw["events"] = raw
        self.call_from_thread(self._rebuild, "events")
        return bool(items)

    def _do_svcs(self, ns: str) -> bool:
        items = fetch_services(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for svc in items:
            m    = svc["metadata"]
            spec = svc.get("spec", {})
            st   = svc.get("status", {})
            name      = m["name"]
            namespace = m.get("namespace", ns)
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            svc_type  = spec.get("type", "-")
            cluster_ip = spec.get("clusterIP", "-")
            # External IP from status.loadBalancer or spec.externalIPs
            lb_ing = st.get("loadBalancer", {}).get("ingress", [])
            if lb_ing:
                ext_ip = lb_ing[0].get("ip") or lb_ing[0].get("hostname") or "-"
            else:
                ext_ips = spec.get("externalIPs", [])
                ext_ip = ext_ips[0] if ext_ips else "<none>"
            ports = spec.get("ports", [])
            port_str = ",".join(
                f"{p.get('port','-')}/{p.get('protocol','TCP')}" for p in ports[:4]
            ) if ports else "-"
            type_color = {"LoadBalancer": "cyan", "NodePort": "yellow",
                          "ExternalName": "magenta"}.get(svc_type, "")
            raw.append({
                "keys": (name, namespace, svc_type, cluster_ip, ext_ip, port_str, secs),
                "row": (
                    name, namespace,
                    Text(svc_type, style=type_color) if type_color else svc_type,
                    cluster_ip, ext_ip, port_str, age(ts),
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "service"},
            })
        self._raw["svcs"] = raw
        self.call_from_thread(self._rebuild, "svcs")
        return bool(items)

    def _do_helm(self) -> bool:
        releases = fetch_helm_releases(ctx=self._kube_ctx)
        raw: list[dict] = []
        for r in releases:
            name      = r.get("name", "-")
            namespace = r.get("namespace", "-")
            status    = r.get("status", "-")
            chart     = r.get("chart", "-")
            app_ver   = r.get("app_version", "-")
            revision  = str(r.get("revision", "-"))
            updated   = r.get("updated", "-")[:19] if r.get("updated") else "-"
            status_color = {"deployed": "green", "failed": "bold red",
                            "pending-install": "yellow", "pending-upgrade": "yellow",
                            "superseded": "dim"}.get(status, "")
            raw.append({
                "keys": (name, namespace, status, chart, app_ver, revision, updated),
                "row": (
                    name, namespace,
                    Text(status, style=status_color) if status_color else status,
                    chart, app_ver, revision, updated,
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "helmrelease"},
            })
        self._raw["helm"] = raw
        self.call_from_thread(self._rebuild, "helm")
        return bool(releases)

    def _do_csvs(self, ns: str) -> bool:
        items = fetch_csvs(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for csv in items:
            m    = csv["metadata"]
            spec = csv.get("spec", {})
            st   = csv.get("status", {})
            name      = m["name"]
            namespace = m.get("namespace", ns)
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            display   = spec.get("displayName", name)
            version   = spec.get("version", "-")
            phase     = st.get("phase", "-")
            phase_color = {"Succeeded": "green", "Failed": "bold red",
                           "Installing": "yellow", "Replacing": "yellow"}.get(phase, "")
            raw.append({
                "keys": (name, namespace, display, version, phase, secs),
                "row": (
                    name, namespace, display, version,
                    Text(phase, style=phase_color) if phase_color else phase,
                    age(ts),
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "clusterserviceversion"},
            })
        self._raw["csvs"] = raw
        self.call_from_thread(self._rebuild, "csvs")
        return bool(items)

    def _do_csrs(self) -> bool:
        items = fetch_csrs(ctx=self._kube_ctx)
        raw: list[dict] = []
        for csr in items:
            m    = csr["metadata"]
            spec = csr.get("spec", {})
            st   = csr.get("status", {})
            name      = m["name"]
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            signer    = spec.get("signerName", "-")
            requestor = spec.get("username", "-")
            conditions = st.get("conditions", [])
            if conditions:
                cond = conditions[-1].get("type", "Pending")
            else:
                cond = "Pending"
            cond_color = {"Approved": "green", "Denied": "bold red",
                          "Failed": "bold red"}.get(cond, "yellow")
            raw.append({
                "keys": (name, secs, signer, requestor, cond),
                "row": (
                    name, age(ts),
                    signer.split("/")[-1] if "/" in signer else signer,
                    requestor,
                    Text(cond, style=cond_color),
                ),
                "meta": {"name": name, "kind": "certificatesigningrequest"},
            })
        self._raw["csrs"] = raw
        self.call_from_thread(self._rebuild, "csrs")
        return bool(items)

    def _do_pvcs(self, ns: str) -> bool:
        items = fetch_pvcs(ns, ctx=self._kube_ctx)
        raw: list[dict] = []
        for pvc in items:
            m    = pvc["metadata"]
            spec = pvc.get("spec", {})
            st   = pvc.get("status", {})
            name      = m["name"]
            namespace = m.get("namespace", ns)
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            phase     = st.get("phase", "-")
            volume    = spec.get("volumeName", "-")
            capacity  = st.get("capacity", {}).get("storage", spec.get("resources", {}).get("requests", {}).get("storage", "-"))
            access    = ",".join(spec.get("accessModes", []))
            sc        = spec.get("storageClassName", "-")
            phase_color = {"Bound": "green", "Lost": "bold red",
                           "Pending": "yellow"}.get(phase, "")
            raw.append({
                "keys": (name, namespace, phase, volume, capacity, access, sc, secs),
                "row": (
                    name, namespace,
                    Text(phase, style=phase_color) if phase_color else phase,
                    volume, capacity, access, sc, age(ts),
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "persistentvolumeclaim"},
            })
        self._raw["pvcs"] = raw
        self.call_from_thread(self._rebuild, "pvcs")
        return bool(items)

    def _do_ceph(self) -> bool:
        items = fetch_ceph_clusters(ctx=self._kube_ctx)
        raw: list[dict] = []
        for cluster in items:
            m    = cluster["metadata"]
            st   = cluster.get("status", {})
            name      = m["name"]
            namespace = m.get("namespace", "-")
            ts        = m.get("creationTimestamp", "")
            secs      = age_secs(ts)
            phase     = st.get("phase", "-")
            ceph_st   = st.get("ceph", {})
            health    = ceph_st.get("health", "-")
            mons      = st.get("quorum", [])
            mon_count = str(len(mons)) if mons else "-"
            storage   = st.get("storage", {})
            osd_active = str(storage.get("deviceClasses", [{}])[0].get("active", "-")) if storage.get("deviceClasses") else "-"
            # Use osdMap from ceph status if available
            osd_map   = ceph_st.get("osdMap", {})
            osd_in    = str(osd_map.get("osdCountByState", {}).get("up", "-")) if osd_map else "-"
            phase_color  = {"Ready": "green", "Progressing": "yellow",
                            "Error": "bold red", "Failure": "bold red"}.get(phase, "")
            health_color = {"HEALTH_OK": "green", "HEALTH_WARN": "yellow",
                            "HEALTH_ERR": "bold red"}.get(health, "")
            raw.append({
                "keys": (name, namespace, phase, health, mon_count, osd_active, osd_in, secs),
                "row": (
                    name, namespace,
                    Text(phase,  style=phase_color)  if phase_color  else phase,
                    Text(health, style=health_color) if health_color else health,
                    mon_count, osd_active, osd_in, age(ts),
                ),
                "meta": {"name": name, "namespace": namespace, "kind": "cephcluster"},
            })
        self._raw["ceph"] = raw
        self.call_from_thread(self._rebuild, "ceph")
        return bool(items)

    # ── rebuild: filter + sort ────────────────────────────────────────────────

    def _rebuild(self, tab: str) -> None:
        try:
            self._rebuild_inner(tab)
        except Exception:
            pass  # Never let a UI rebuild crash the app

    def _rebuild_inner(self, tab: str) -> None:
        data  = self._raw[tab]
        ftext = self._filter_text.lower()
        filtered = (
            [d for d in data if ftext in " ".join(str(k) for k in d["keys"]).lower()]
            if ftext else list(data)
        )

        if tab in self._sort:
            col_idx, asc = self._sort[tab]
            try:
                filtered.sort(key=lambda d: d["keys"][col_idx], reverse=not asc)
            except TypeError:
                filtered.sort(key=lambda d: str(d["keys"][col_idx]), reverse=not asc)

        if tab == "pods":
            self._pod_meta = [d["meta"] for d in filtered]
        elif tab == "nodes":
            self._node_meta = [d["meta"] for d in filtered]
        elif tab == "deploys":
            self._deploy_meta = [d["meta"] for d in filtered]
        elif tab == "routes":
            self._route_meta = [d["meta"] for d in filtered]
        elif tab == "clusterops":
            self._clusterops_meta = [d["meta"] for d in filtered]
        elif tab == "svcs":
            self._svc_meta = [d["meta"] for d in filtered]
        elif tab == "helm":
            self._helm_meta = [d["meta"] for d in filtered]
        elif tab == "csvs":
            self._csv_meta = [d["meta"] for d in filtered]
        elif tab == "csrs":
            self._csr_meta = [d["meta"] for d in filtered]
        elif tab == "pvcs":
            self._pvc_meta = [d["meta"] for d in filtered]
        elif tab == "ceph":
            self._ceph_meta = [d["meta"] for d in filtered]
        elif tab == "node_metrics":
            self._node_metrics_meta = [d["meta"] for d in filtered if d["meta"]]
        elif tab == "pod_metrics":
            self._pod_metrics_meta  = [d["meta"] for d in filtered if d["meta"]]

        t = self.query_one(f"#{tab}-table", DataTable)
        col_names = COLS[tab]
        for i, (col_key, col_name) in enumerate(zip(list(t.columns), col_names)):
            if tab in self._sort and self._sort[tab][0] == i:
                indicator = " ▲" if self._sort[tab][1] else " ▼"
                t.columns[col_key].label = Text(col_name + indicator)
            else:
                t.columns[col_key].label = Text(col_name)

        t.clear()
        for d in filtered:
            t.add_row(*d["row"])

        # Update tab label with live count
        if tab in TAB_LABELS:
            try:
                count = len(filtered)
                base  = TAB_LABELS[tab]
                label = f"⬡ {base} ({count})" if count else f"⬡ {base}"
                self.query_one(f"Tab#{tab}", Tab).label = label
            except Exception:
                pass

    # ── dashboard ─────────────────────────────────────────────────────────────

    def _update_dashboard(self) -> None:
        self.query_one(DashboardPane).update_content(self._build_dashboard())

    def _build_dashboard(self) -> str:
        W   = 72
        div = f"[bold cyan]{'━' * W}[/]"
        sep = f"[dim]{'─' * W}[/]"
        out: list[str] = []

        # ── Overview ─────────────────────────────────────────────────────────
        platform_color = "bold red" if self._is_openshift else "bold cyan"
        platform_label = self._platform_name or "Kubernetes"
        out += [
            div,
            f"[{platform_color}]  {platform_label.upper()}  —  CLUSTER OVERVIEW[/]",
            sep,
        ]
        ctx_s = self._kube_ctx or "(no context)"
        ts    = datetime.now().strftime("%H:%M:%S")
        out.append(f"  [dim]Context [/]  [bold cyan]{ctx_s}[/]   [dim]as of[/] {ts}")

        # Logged-in user
        if self._cluster_user:
            out.append(f"  [dim]User    [/]  [bold yellow]{self._cluster_user}[/]")

        # Server / cluster version
        if self._server_ver:
            out.append(f"  [dim]Version [/]  [white]{self._server_ver}[/]")

        out.append("")

        # Nodes
        nr = self._raw["nodes"]
        n_ready = sum(1 for r in nr if r["keys"][1] == "Ready")
        n_bad   = len(nr) - n_ready
        n_str   = f"[green]{n_ready} Ready[/]"
        if n_bad:
            n_str += f"  [bold red]{n_bad} NotReady[/]"
        out.append(f"  [dim]Nodes      [/]  {n_str}  [dim]({len(nr)} total)[/]")

        # Pods
        pr = self._raw["pods"]
        counts: dict[str, int] = {}
        for r in pr:
            ph = r["keys"][2]
            counts[ph] = counts.get(ph, 0) + 1
        running = counts.get("Running", 0)
        pending = sum(v for k, v in counts.items() if k == "Pending" or k.startswith("Init:"))
        failed  = sum(v for k, v in counts.items() if k in ALERT_PHASES or k == "Failed")
        other   = len(pr) - running - pending - failed
        p_str   = f"[green]{running} Running[/]"
        if pending: p_str += f"  [yellow]{pending} Pending[/]"
        if failed:  p_str += f"  [bold red]{failed} Failed[/]"
        if other > 0: p_str += f"  [dim]{other} Other[/]"
        out.append(f"  [dim]Pods       [/]  {p_str}  [dim]({len(pr)} total)[/]")

        # Workloads (Deployments + DCs)
        dr    = self._raw["deploys"]
        d_ok  = sum(1 for r in dr if r["keys"][3] == r["keys"][6] and r["keys"][6] > 0)
        d_deg = len(dr) - d_ok
        d_str = f"[green]{d_ok} Healthy[/]"
        if d_deg: d_str += f"  [yellow]{d_deg} Degraded[/]"
        wl_label = "Workloads  " if self._is_openshift else "Deploys    "
        out.append(f"  [dim]{wl_label}[/]  {d_str}  [dim]({len(dr)} total)[/]")

        # Services (always)
        sr = self._raw["svcs"]
        if sr:
            lb_count = sum(1 for r in sr if r["keys"][2] == "LoadBalancer")
            svc_s = f"[white]{len(sr)} total[/]"
            if lb_count:
                svc_s += f"  [cyan]{lb_count} LB[/]"
            out.append(f"  [dim]Services   [/]  {svc_s}")

        # Helm (if available)
        hr = self._raw["helm"]
        if HELM and hr is not None:
            h_ok  = sum(1 for r in hr if r["keys"][2] == "deployed")
            h_bad = sum(1 for r in hr if r["keys"][2] == "failed")
            h_s   = f"[green]{h_ok} deployed[/]"
            if h_bad:  h_s += f"  [bold red]{h_bad} failed[/]"
            if hr:
                out.append(f"  [dim]Helm       [/]  {h_s}  [dim]({len(hr)} total)[/]")

        # OCP-specific: Routes + ClusterOperators + Operators/CSVs
        if self._is_openshift:
            rr = self._raw["routes"]
            out.append(f"  [dim]Routes     [/]  [white]{len(rr)} total[/]")

            co = self._raw["clusterops"]
            if co:
                co_avail  = sum(1 for r in co if r["keys"][2] == "True")
                co_degrad = sum(1 for r in co if r["keys"][4] == "True")
                co_prog   = sum(1 for r in co if r["keys"][3] == "True")
                co_s = f"[green]{co_avail} Available[/]"
                if co_degrad: co_s += f"  [bold red]{co_degrad} Degraded[/]"
                if co_prog:   co_s += f"  [yellow]{co_prog} Progressing[/]"
                out.append(f"  [dim]ClusterOps [/]  {co_s}  [dim]({len(co)} total)[/]")

            cv = self._raw["csvs"]
            if cv:
                csv_ok  = sum(1 for r in cv if r["keys"][4] == "Succeeded")
                csv_bad = sum(1 for r in cv if r["keys"][4] == "Failed")
                csv_s   = f"[green]{csv_ok} Succeeded[/]"
                if csv_bad: csv_s += f"  [bold red]{csv_bad} Failed[/]"
                out.append(f"  [dim]Operators  [/]  {csv_s}  [dim]({len(cv)} total)[/]")

        # CSRs
        cr = self._raw["csrs"]
        if cr:
            csr_pending  = sum(1 for r in cr if r["keys"][4] == "Pending")
            csr_approved = sum(1 for r in cr if r["keys"][4] == "Approved")
            csr_denied   = sum(1 for r in cr if r["keys"][4] == "Denied")
            csr_s = f"[white]{len(cr)} total[/]"
            if csr_pending:  csr_s += f"  [yellow]{csr_pending} Pending[/]"
            if csr_denied:   csr_s += f"  [bold red]{csr_denied} Denied[/]"
            if csr_approved: csr_s += f"  [green]{csr_approved} Approved[/]"
            out.append(f"  [dim]CSRs       [/]  {csr_s}")

        # PVCs (always)
        pv = self._raw["pvcs"]
        if pv:
            pvc_bound   = sum(1 for r in pv if r["keys"][2] == "Bound")
            pvc_pending = sum(1 for r in pv if r["keys"][2] == "Pending")
            pvc_lost    = sum(1 for r in pv if r["keys"][2] == "Lost")
            pvc_s = f"[green]{pvc_bound} Bound[/]"
            if pvc_pending: pvc_s += f"  [yellow]{pvc_pending} Pending[/]"
            if pvc_lost:    pvc_s += f"  [bold red]{pvc_lost} Lost[/]"
            out.append(f"  [dim]PVCs       [/]  {pvc_s}  [dim]({len(pv)} total)[/]")

        # Ceph (only if detected)
        ceph_r = self._raw["ceph"]
        if ceph_r:
            for r in ceph_r:
                health = r["keys"][3]
                h_color = {"HEALTH_OK": "green", "HEALTH_WARN": "yellow",
                           "HEALTH_ERR": "bold red"}.get(health, "white")
                out.append(
                    f"  [dim]Ceph [{r['keys'][0]}][/]  "
                    f"[{h_color}]{health}[/]  [dim]phase:[/] {r['keys'][2]}"
                )

        out.append("")

        # ── Alerts ───────────────────────────────────────────────────────────
        alerts = detect_alerts(self._raw["pods"], self._raw["nodes"], self._raw["events"])
        out.append(div)
        if alerts:
            out.append(f"[bold red]  ALERTS  [{len(alerts)}][/]")
            out.append(sep)
            for a in alerts[:25]:
                out.append(f"  {a}")
            if len(alerts) > 25:
                out.append(f"  [dim]… {len(alerts) - 25} more alerts — press 2 for Pods or 5 for Events[/]")
        else:
            out.append("[bold green]  ALERTS  — ✓ All Clear[/]")
            out.append(sep)
            out.append("  [green]No critical issues detected.[/]")
        out.append("")

        # ── Node Resources ────────────────────────────────────────────────────
        nm = self._metrics["nodes"]
        out.append(div)
        out.append("[bold white]  NODE RESOURCES[/]")
        out.append(sep)
        if not nm:
            if self._metrics_err:
                out.append(f"  [yellow]⚠  {self._metrics_err[:90]}[/]")
            elif self._metrics_ts:
                out.append(f"  [dim]No metrics returned (last tried: {self._metrics_ts})[/]")
            else:
                out.append(f"  [dim]Fetching metrics…  (first load ~20s after startup)[/]")
            cmd_hint = "oc adm top nodes" if self._is_openshift else "kubectl top nodes"
            out.append(f"  [dim]Tip: run [italic]{cmd_hint}[/italic] to verify metrics-server access[/]")
        else:
            out.append(f"  [bold dim]{'NODE':<22}  {'CPU':<28}  {'MEM':<28}[/]")
            sorted_n = sorted(
                nr,
                key=lambda r: nm.get(r["keys"][0], {}).get("cpu_m", 0),
                reverse=True,
            )
            for r in sorted_n:
                fname = r["keys"][0]
                short = node_short(fname)[:20]
                m = nm.get(fname, {})
                cpu_m  = m.get("cpu_m", 0)
                mem_ki = m.get("mem_ki", 0)
                cap_c, cap_m = self._node_capacity.get(fname, (0, 0))

                if cap_c:
                    cpu_bar = pct_bar(cpu_m / cap_c * 100, 14)
                    cpu_val = fmt_cpu(cpu_m)
                else:
                    cpu_bar = f"[dim]{'░' * 14}[/] [dim]  N/A[/]"
                    cpu_val = "-"

                if cap_m:
                    mem_bar = pct_bar(mem_ki / cap_m * 100, 14)
                    mem_val = fmt_mem(mem_ki)
                else:
                    mem_bar = f"[dim]{'░' * 14}[/] [dim]  N/A[/]"
                    mem_val = "-"

                out.append(
                    f"  [white]{short:<22}[/]  [dim]{cpu_val:>5}[/] {cpu_bar}  "
                    f"[dim]{mem_val:>7}[/] {mem_bar}"
                )
        out.append("")

        # ── Top Pods by CPU ───────────────────────────────────────────────────
        pm = self._metrics["pods"]
        out.append(div)
        out.append("[bold white]  TOP PODS BY CPU[/]")
        out.append(sep)
        if not pm:
            msg = "  [dim]Fetching pod metrics…[/]" if not self._metrics_ts else "  [dim]No pod metrics available[/]"
            out.append(msg)
        else:
            top = sorted(pm, key=lambda p: p["cpu_m"], reverse=True)[:12]
            out.append(f"  [bold dim]{'NAME':<36}  {'NAMESPACE':<20}  {'CPU':>7}  {'MEM':>8}[/]")
            for p in top:
                n_s = p["name"][:36]
                ns_s = p["namespace"][:20]
                cpu_s = fmt_cpu(p["cpu_m"])
                mem_s = fmt_mem(p["mem_ki"])
                c = "bold red" if p["cpu_m"] > 2000 else "yellow" if p["cpu_m"] > 500 else "green"
                out.append(
                    f"  [white]{n_s:<36}[/]  [dim]{ns_s:<20}[/]  [{c}]{cpu_s:>7}[/]  [cyan]{mem_s:>8}[/]"
                )
        out.append("")
        # ── watermark ─────────────────────────────────────────────────────────
        out.append(f"[dim]{'─' * 72}[/]")
        out.append(
            f"[dim]  ⬡ {APP_NAME}  —  {APP_TAGLINE}"
            f"   │   engine: {KUBE}   │   auto-refresh: 60s data  120s metrics[/]"
        )
        out.append("")
        return "\n".join(out)

    # ── sort on column-header click ───────────────────────────────────────────

    @on(DataTable.HeaderSelected)
    def _header_selected(self, event: DataTable.HeaderSelected) -> None:
        tab = event.data_table.id.removesuffix("-table")
        col = event.column_index
        cur = self._sort.get(tab)
        self._sort[tab] = (col, not cur[1]) if cur and cur[0] == col else (col, True)
        self._rebuild(tab)

    # ── filter ────────────────────────────────────────────────────────────────

    @on(Input.Changed, "#filter-input")
    def _filter_changed(self, event: Input.Changed) -> None:
        self._filter_text = event.value
        for tab in ("pods", "nodes", "deploys", "events", "routes", "clusterops",
                    "svcs", "helm", "csvs", "csrs", "pvcs", "ceph",
                    "node_metrics", "pod_metrics"):
            if self._raw[tab]:
                self._rebuild(tab)

    @on(Input.Submitted, "#filter-input")
    def _filter_submitted(self, _: Input.Submitted) -> None:
        active = self.query_one("#tabs", TabbedContent).active
        if active != "dashboard":
            self.query_one(f"#{active}-table", DataTable).focus()

    # ── row selection → describe ──────────────────────────────────────────────

    @on(DataTable.RowSelected, "#pods-table")
    def _pod_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._pod_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#nodes-table")
    def _node_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._node_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], None)
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#deploys-table")
    def _deploy_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._deploy_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#routes-table")
    def _route_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._route_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#clusterops-table")
    def _clusterops_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._clusterops_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], None)
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#svcs-table")
    def _svc_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._svc_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#helm-table")
    def _helm_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._helm_meta[event.cursor_row]
            self._selected = d
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#csvs-table")
    def _csv_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._csv_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#csrs-table")
    def _csr_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._csr_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], None)
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#pvcs-table")
    def _pvc_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._pvc_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#ceph-table")
    def _ceph_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._ceph_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#node_metrics-table")
    def _node_metrics_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._node_metrics_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], None)
        except IndexError:
            pass

    @on(DataTable.RowSelected, "#pod_metrics-table")
    def _pod_metrics_row(self, event: DataTable.RowSelected) -> None:
        try:
            d = self._pod_metrics_meta[event.cursor_row]
            self._selected = d
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        except IndexError:
            pass

    def _trigger_describe(self, kind: str, name: str, ns: Optional[str]) -> None:
        self.query_one("#detail", DetailPanel).set_loading(f"{kind}/{name}")
        self._load_describe(kind, name, ns)

    @work(thread=True)
    def _load_describe(self, kind: str, name: str, ns: Optional[str]) -> None:
        out = fetch_describe(kind, name, ns, ctx=self._kube_ctx)
        self.call_from_thread(
            lambda: self.query_one("#detail", DetailPanel).update(out, f"describe {kind}/{name}")
        )

    # ── context switch ────────────────────────────────────────────────────────

    @on(Select.Changed, "#ns-select")
    def _ns_changed(self, event: Select.Changed) -> None:
        if event.value is Select.BLANK:
            return
        self.namespace = str(event.value)
        self._refresh_data()

    @work(thread=True)
    def _switch_context(self) -> None:
        self._load_cluster()

    # ── actions ───────────────────────────────────────────────────────────────

    def action_refresh(self) -> None:
        self._refresh_data()

    def action_open_logs(self) -> None:
        active = self.query_one("#tabs", TabbedContent).active
        meta: Optional[dict] = None

        # Prefer cursor row on pods table — no need to press Enter first
        if active == "pods" and self._pod_meta:
            table = self.query_one("#pods-table", DataTable)
            row   = min(table.cursor_row, len(self._pod_meta) - 1)
            meta  = self._pod_meta[row]
        elif self._selected.get("kind") == "pod":
            meta = self._selected

        if not meta:
            self.notify("Navigate to Pods [2] and highlight a pod row, then press L", severity="warning")
            return

        pod_name = meta["name"]
        pod_ns   = meta["namespace"]
        self._open_logs_for(pod_name, pod_ns)

    @work(thread=True)
    def _open_logs_for(self, pod_name: str, pod_ns: str) -> None:
        containers = fetch_pod_containers(pod_name, pod_ns, ctx=self._kube_ctx)

        def push(container: str = "") -> None:
            self.push_screen(
                LogsScreen(pod_name, pod_ns, container=container, ctx=self._kube_ctx)
            )

        if len(containers) <= 1:
            self.call_from_thread(push, containers[0] if containers else "")
        else:
            def pick_container() -> None:
                def on_pick(c: Optional[str]) -> None:
                    if c:
                        push(c)
                self.push_screen(ContainerPickerModal(containers), on_pick)

            self.call_from_thread(pick_container)

    def action_describe(self) -> None:
        active = self.query_one("#tabs", TabbedContent).active
        if active == "pods" and self._pod_meta:
            table = self.query_one("#pods-table", DataTable)
            row   = min(table.cursor_row, len(self._pod_meta) - 1)
            d     = self._pod_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "nodes" and self._node_meta:
            table = self.query_one("#nodes-table", DataTable)
            row   = min(table.cursor_row, len(self._node_meta) - 1)
            d     = self._node_meta[row]
            self._trigger_describe(d["kind"], d["name"], None)
        elif active == "deploys" and self._deploy_meta:
            table = self.query_one("#deploys-table", DataTable)
            row   = min(table.cursor_row, len(self._deploy_meta) - 1)
            d     = self._deploy_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "routes" and self._route_meta:
            table = self.query_one("#routes-table", DataTable)
            row   = min(table.cursor_row, len(self._route_meta) - 1)
            d     = self._route_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "clusterops" and self._clusterops_meta:
            table = self.query_one("#clusterops-table", DataTable)
            row   = min(table.cursor_row, len(self._clusterops_meta) - 1)
            d     = self._clusterops_meta[row]
            self._trigger_describe(d["kind"], d["name"], None)
        elif active == "helm":
            self.notify("Helm: describe not applicable — use 'helm status <name>' in terminal", timeout=5)
        elif active == "svcs" and self._svc_meta:
            table = self.query_one("#svcs-table", DataTable)
            row   = min(table.cursor_row, len(self._svc_meta) - 1)
            d     = self._svc_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "csvs" and self._csv_meta:
            table = self.query_one("#csvs-table", DataTable)
            row   = min(table.cursor_row, len(self._csv_meta) - 1)
            d     = self._csv_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "csrs" and self._csr_meta:
            table = self.query_one("#csrs-table", DataTable)
            row   = min(table.cursor_row, len(self._csr_meta) - 1)
            d     = self._csr_meta[row]
            self._trigger_describe(d["kind"], d["name"], None)
        elif active == "pvcs" and self._pvc_meta:
            table = self.query_one("#pvcs-table", DataTable)
            row   = min(table.cursor_row, len(self._pvc_meta) - 1)
            d     = self._pvc_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "ceph" and self._ceph_meta:
            table = self.query_one("#ceph-table", DataTable)
            row   = min(table.cursor_row, len(self._ceph_meta) - 1)
            d     = self._ceph_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif active == "node_metrics" and self._node_metrics_meta:
            table = self.query_one("#node_metrics-table", DataTable)
            row   = min(table.cursor_row, len(self._node_metrics_meta) - 1)
            d     = self._node_metrics_meta[row]
            self._trigger_describe(d["kind"], d["name"], None)
        elif active == "pod_metrics" and self._pod_metrics_meta:
            table = self.query_one("#pod_metrics-table", DataTable)
            row   = min(table.cursor_row, len(self._pod_metrics_meta) - 1)
            d     = self._pod_metrics_meta[row]
            self._trigger_describe(d["kind"], d["name"], d["namespace"])
        elif self._selected:
            self._trigger_describe(
                self._selected.get("kind", "pod"),
                self._selected["name"],
                self._selected.get("namespace"),
            )

    def action_focus_ns(self) -> None:
        self.query_one("#ns-select").focus()

    def action_focus_ctx(self) -> None:
        if not self._contexts:
            self.notify("Re-loading contexts…", timeout=3)
            self._reload_contexts()
            return
        self._show_context_modal()

    @work(thread=True)
    def _reload_contexts(self) -> None:
        contexts, current = fetch_contexts()
        if not contexts:
            self.call_from_thread(
                self.notify, "No kubeconfig contexts found — check oc/kubectl config",
                severity="error", timeout=6,
            )
            return

        def apply():
            self._contexts = contexts
            if not self._kube_ctx:
                self._kube_ctx = current
            self._show_context_modal()

        self.call_from_thread(apply)

    def _show_context_modal(self) -> None:
        def _on_pick(ctx: Optional[str]) -> None:
            if not ctx or ctx == self._kube_ctx:
                return
            prev_ctx = self._kube_ctx
            self._kube_ctx = ctx
            ctx_short = ctx.split("/")[-1][:34]
            self.query_one("#ctx-display", Label).update(ctx_short)
            self.sub_title = ctx
            self._set_clock(f"⟳ Switching to {ctx_short}…")
            self.notify(f"⎇  Switching to {ctx_short}", timeout=4)
            self._switch_context()

        self.push_screen(ContextModal(self._contexts, self._kube_ctx), _on_pick)

    def action_switch_tab(self, tab: str) -> None:
        self.query_one("#tabs", TabbedContent).active = tab

    def action_toggle_filter(self) -> None:
        bar = self.query_one("#filter-bar")
        if bar.display:
            self._clear_filter()
        else:
            bar.display = True
            self.query_one("#filter-input", Input).focus()

    def action_clear_filter(self) -> None:
        bar = self.query_one("#filter-bar")
        if bar.display:
            self._clear_filter()

    def _clear_filter(self) -> None:
        self.query_one("#filter-bar").display = False
        self.query_one("#filter-input", Input).value = ""
        self._filter_text = ""
        for tab in ("pods", "nodes", "deploys", "events", "routes", "clusterops",
                    "svcs", "helm", "csvs", "csrs", "pvcs", "ceph",
                    "node_metrics", "pod_metrics"):
            if self._raw[tab]:
                self._rebuild(tab)


if __name__ == "__main__":
    K8sMonitor().run()
