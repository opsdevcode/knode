"""
Kubectl invocation helpers for node operations.

All cluster access goes through subprocess kubectl calls. Run after
setting cluster context (e.g. set-clus staging).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
from typing import Optional


def run_kubectl(args: list[str], capture: bool = True, timeout: int | None = 120) -> subprocess.CompletedProcess:
    """
    Run kubectl with the given args.

    Args:
        args: List of arguments (e.g. ["get", "nodes", "-o", "json"]).
        capture: If True, capture stdout/stderr; otherwise inherit from process.
        timeout: Max seconds to wait (default 120). None = no timeout.

    Returns:
        CompletedProcess with returncode, stdout, stderr.
    """
    cmd = ["kubectl"] + args
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        timeout=timeout,
    )


def _cluster_name_to_profile(cluster_name: str) -> str | None:
    """
    Derive AWS profile from cluster name for EKS API calls.
    Cluster format: 3p-{tenant}-{region}-{stage}-eks-cluster
    Profile format: 3p-{tenant}-gbl-{stage}-admin
    """
    if not cluster_name or "-eks-cluster" not in cluster_name:
        return None
    rest = cluster_name.replace("-eks-cluster", "")
    parts = rest.split("-")
    if len(parts) < 4:
        return None
    tenant = parts[1]
    stage = parts[-1]
    return f"3p-{tenant}-gbl-{stage}-admin"


def get_cluster_name() -> str:
    """Get the current cluster name from kubeconfig (current context)."""
    result = run_kubectl(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].name}"])
    if result.returncode != 0 or not result.stdout or not result.stdout.strip():
        raise RuntimeError(
            "Could not determine cluster name. Ensure cluster context is set (e.g. set-clus staging)."
        )
    raw = result.stdout.strip()
    if "/" in raw and "arn:aws:eks:" in raw:
        return raw.split("/")[-1]
    return raw


def get_cluster_region() -> str | None:
    """Get the AWS region for the current cluster from kubeconfig (ARN or server URL)."""
    result = run_kubectl(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].name}"])
    if result.returncode != 0 or not result.stdout:
        return None
    raw = result.stdout.strip()
    if "arn:aws:eks:" in raw:
        parts = raw.split(":")
        if len(parts) >= 4:
            return parts[3]
    result = run_kubectl(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"])
    if result.returncode == 0 and result.stdout and ".eks." in result.stdout:
        m = re.search(r"\.eks\.([a-z0-9-]+)\.amazonaws\.com", result.stdout)
        if m:
            return m.group(1)
    return None


def get_cluster_profile() -> str | None:
    """Get AWS profile for the current cluster (for aws eks API calls)."""
    try:
        name = get_cluster_name()
        return _cluster_name_to_profile(name)
    except RuntimeError:
        return None


def count_pods_on_nodes(node_names: list[str]) -> int:
    """Count pods (excluding DaemonSet) on the given nodes."""
    if not node_names:
        return 0
    node_set = set(node_names)
    result = run_kubectl(["get", "pods", "-A", "-o", "json"], timeout=60)
    if result.returncode != 0 or not result.stdout:
        return 0
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return 0
    count = 0
    for item in data.get("items", []):
        node = item.get("spec", {}).get("nodeName", "")
        if node in node_set:
            owner_refs = item.get("metadata", {}).get("ownerReferences", [])
            if any(ref.get("kind") == "DaemonSet" for ref in owner_refs):
                continue
            count += 1
    return count


def get_nodes_json() -> Optional[dict]:
    """
    Get all nodes as JSON from the current cluster context.

    Returns:
        Parsed JSON dict with "items" list of node objects, or None on failure.
    """
    result = run_kubectl(["get", "nodes", "-o", "json"])
    if result.returncode != 0 or not result.stdout:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def get_pods_json() -> Optional[dict]:
    """
    Get all pods across all namespaces as JSON.

    Returns:
        Parsed JSON dict with "items" list of pod objects, or None on failure.
    """
    result = run_kubectl(["get", "pods", "-A", "-o", "json"], timeout=60)
    if result.returncode != 0 or not result.stdout:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def cordon_nodes(node_names: list[str]) -> tuple[int, str]:
    """
    Cordon the given nodes (mark unschedulable).

    Returns:
        (returncode, stderr or stdout)
    """
    if not node_names:
        return 0, ""
    result = run_kubectl(["cordon"] + node_names)
    output = result.stderr if result.stderr else result.stdout
    return result.returncode, output or ""


def uncordon_nodes(node_names: list[str]) -> tuple[int, str]:
    """
    Uncordon the given nodes (mark schedulable).

    Returns:
        (returncode, stderr or stdout)
    """
    if not node_names:
        return 0, ""
    result = run_kubectl(["uncordon"] + node_names)
    output = result.stderr if result.stderr else result.stdout
    return result.returncode, output or ""


def _format_progress(evicted: int, total: int, width: int = 30) -> str:
    """Format a progress bar string."""
    if total <= 0:
        return "Draining... (evicting pods)"
    pct = min(1.0, evicted / total) if total else 0
    filled = int(width * pct)
    bar = "=" * filled + ">" * (1 if filled < width and evicted < total else 0)
    bar = bar.ljust(width)
    return f"Draining... [{bar}] {evicted}/{total} pods evicted"


def drain_nodes(
    node_names: list[str],
    *,
    ignore_daemonsets: bool = True,
    delete_emptydir_data: bool = True,
    ignore_errors: bool = False,
    show_progress: bool = True,
) -> tuple[int, str]:
    """
    Drain the given nodes (evict pods, mark unschedulable).

    Args:
        node_names: Nodes to drain.
        ignore_daemonsets: Add --ignore-daemonsets.
        delete_emptydir_data: Add --delete-emptydir-data.
        ignore_errors: Add --ignore-errors (continue on eviction failures).
        show_progress: Show progress bar during drain.

    Returns:
        (returncode, stderr or stdout)
    """
    if not node_names:
        return 0, ""
    args = ["drain"]
    if ignore_daemonsets:
        args.append("--ignore-daemonsets")
    if delete_emptydir_data:
        args.append("--delete-emptydir-data")
    if ignore_errors:
        args.append("--ignore-errors")
    args.extend(node_names)

    if not show_progress:
        result = run_kubectl(args, timeout=600)
        output = result.stderr if result.stderr else result.stdout
        return result.returncode, output or ""

    total = count_pods_on_nodes(node_names)
    evicted = 0
    output_lines: list[str] = []

    cmd = ["kubectl"] + args
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def read_output() -> None:
        nonlocal evicted
        assert proc.stdout is not None
        for line in proc.stdout:
            output_lines.append(line)
            if "pod/" in line and " evicted" in line.lower():
                evicted += 1

    reader = threading.Thread(target=read_output)
    reader.daemon = True
    reader.start()

    try:
        while proc.poll() is None:
            reader.join(timeout=0.3)
            msg = _format_progress(evicted, total)
            sys.stderr.write(f"\r{msg}    ")
            sys.stderr.flush()
        reader.join(timeout=2)
    except (KeyboardInterrupt, Exception):
        proc.terminate()
        proc.wait(timeout=10)
        raise
    finally:
        sys.stderr.write("\r" + " " * 60 + "\r")
        sys.stderr.flush()

    output = "".join(output_lines)
    return proc.returncode or 0, output
