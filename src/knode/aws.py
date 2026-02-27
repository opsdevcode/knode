"""
AWS EKS API helpers for managed node group operations.

Uses subprocess to call aws eks. Run after setting cluster context (e.g. set-clus staging).
Requires AWS credentials (AWS_PROFILE or similar) to be configured.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Optional


def run_aws(
    args: list[str],
    region: str | None = None,
    profile: str | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run aws CLI with the given args. Uses profile for cluster account access."""
    cmd = ["aws", "eks"] + args
    if region:
        cmd.extend(["--region", region])
    if profile:
        cmd.extend(["--profile", profile])
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        timeout=60,
    )


@dataclass
class NodeGroupInfo:
    """Scaling and metadata for an EKS managed node group."""

    name: str
    status: str
    capacity_type: str
    min_size: int
    max_size: int
    desired_size: int


def list_nodegroups(
    cluster_name: str,
    region: str | None = None,
    profile: str | None = None,
) -> list[str]:
    """List node group names in the cluster."""
    result = run_aws(
        ["list-nodegroups", "--cluster-name", cluster_name, "--query", "nodegroups", "--output", "json"],
        region=region,
        profile=profile,
    )
    if result.returncode != 0:
        if result.stderr:
            raise RuntimeError(f"aws eks list-nodegroups failed: {result.stderr.strip()}")
        return []
    if not result.stdout:
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def describe_nodegroup(
    cluster_name: str,
    nodegroup_name: str,
    region: str | None = None,
    profile: str | None = None,
) -> Optional[dict]:
    """Get full node group description."""
    result = run_aws(
        ["describe-nodegroup", "--cluster-name", cluster_name, "--nodegroup-name", nodegroup_name],
        region=region,
        profile=profile,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def get_nodegroup_info(
    cluster_name: str,
    nodegroup_name: str,
    region: str | None = None,
    profile: str | None = None,
) -> Optional[NodeGroupInfo]:
    """Get scaling config for a node group."""
    data = describe_nodegroup(cluster_name, nodegroup_name, region, profile)
    if not data:
        return None
    ng = data.get("nodegroup", {})
    scaling = ng.get("scalingConfig", {})
    raw_cap = ng.get("capacityType", "")
    capacity_type = raw_cap.replace("_", "-").lower() if raw_cap else ""
    return NodeGroupInfo(
        name=ng.get("nodegroupName", nodegroup_name),
        status=ng.get("status", "UNKNOWN"),
        capacity_type=capacity_type,
        min_size=scaling.get("minSize", 0),
        max_size=scaling.get("maxSize", 0),
        desired_size=scaling.get("desiredSize", 0),
    )


def update_nodegroup_scaling(
    cluster_name: str,
    nodegroup_name: str,
    *,
    region: str | None = None,
    profile: str | None = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    desired_size: Optional[int] = None,
) -> tuple[int, str]:
    """
    Update node group scaling config via aws eks update-nodegroup-config.

    Only passes values that are explicitly set. AWS API accepts partial updates.
    """
    current = get_nodegroup_info(cluster_name, nodegroup_name, region, profile)
    if not current:
        return 1, f"Could not describe node group {nodegroup_name}"

    min_s = min_size if min_size is not None else current.min_size
    max_s = max_size if max_size is not None else current.max_size
    desired_s = desired_size if desired_size is not None else current.desired_size

    scaling = f"minSize={min_s},maxSize={max_s},desiredSize={desired_s}"
    result = run_aws(
        [
            "update-nodegroup-config",
            "--cluster-name",
            cluster_name,
            "--nodegroup-name",
            nodegroup_name,
            "--scaling-config",
            scaling,
        ],
        region=region,
        profile=profile,
    )
    output = result.stderr if result.stderr else result.stdout
    return result.returncode, output or ""
