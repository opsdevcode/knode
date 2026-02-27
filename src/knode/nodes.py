"""
Node and pod lookup and representation for knode.

Fetches nodes and pods from kubectl and provides a simple interface
for listing and filtering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .kubectl import get_nodes_json, get_pods_json


def _capacity_type(labels: dict) -> str:
    """
    Derive capacity type: spot, on-demand, or fargate.
    Matches shownodes logic.
    """
    ctype = labels.get("karpenter.sh/capacity-type")
    if not ctype:
        ctype = labels.get("eks.amazonaws.com/capacityType")
        if ctype:
            ctype = ctype.replace("_", "-").lower()
        else:
            ctype = labels.get("eks.amazonaws.com/compute-type") or ""
    return ctype


def _is_in_nodegroup(labels: dict) -> bool:
    """True if node is in an EKS managed node group."""
    return bool(labels.get("eks.amazonaws.com/nodegroup"))


@dataclass
class NodeInfo:
    """Minimal node info for display and selection."""

    name: str
    status: str
    instance_type: str
    zone: str
    captype: str
    unschedulable: bool

    @classmethod
    def from_dict(cls, obj: dict) -> "NodeInfo":
        """Build NodeInfo from kubectl get nodes -o json item."""
        metadata = obj.get("metadata", {})
        spec = obj.get("spec", {})
        status = obj.get("status", {})

        # Status: conditions that are True
        conditions = status.get("conditions", [])
        status_parts = [c["type"] for c in conditions if c.get("status") == "True"]
        if spec.get("unschedulable"):
            status_parts.append("NoSchedule")
        status_str = ",".join(status_parts)

        labels = metadata.get("labels", {})
        instance_type = (
            labels.get("node.kubernetes.io/instance-type")
            or labels.get("beta.kubernetes.io/instance-type")
            or ""
        )
        zone = labels.get("topology.kubernetes.io/zone", "")

        # Capacity type: spot, on-demand, fargate; NG/ prefix for managed node groups
        ctype = _capacity_type(labels)
        if _is_in_nodegroup(labels) and ctype:
            captype = f"NG/{ctype}"
        else:
            captype = ctype or "-"

        return cls(
            name=metadata.get("name", ""),
            status=status_str,
            instance_type=instance_type,
            zone=zone,
            captype=captype,
            unschedulable=bool(spec.get("unschedulable")),
        )


def get_all_nodes() -> list[NodeInfo]:
    """
    Fetch all nodes from the current cluster context.

    Returns:
        List of NodeInfo. Raises if kubectl fails or returns invalid data.
    """
    data = get_nodes_json()
    if not data:
        raise RuntimeError(
            "Failed to get nodes. Ensure cluster context is set (e.g. set-clus staging)."
        )
    items = data.get("items", [])
    return [NodeInfo.from_dict(item) for item in items]


def get_node_names() -> list[str]:
    """Return list of node names in the cluster."""
    return [n.name for n in get_all_nodes()]


@dataclass
class PodInfo:
    """Minimal pod info for display."""

    namespace: str
    name: str
    status: str
    node: str
    restarts: int
    is_daemonset: bool

    @classmethod
    def from_dict(cls, obj: dict) -> "PodInfo":
        """Build PodInfo from kubectl get pods -o json item."""
        metadata = obj.get("metadata", {})
        spec = obj.get("spec", {})
        pod_status = obj.get("status", {})

        phase = pod_status.get("phase", "Unknown")
        container_statuses = pod_status.get("containerStatuses", [])
        restarts = sum(cs.get("restartCount", 0) for cs in container_statuses)

        # Detect waiting containers to show a more specific status (e.g. CrashLoopBackOff)
        for cs in container_statuses:
            waiting = cs.get("state", {}).get("waiting", {})
            reason = waiting.get("reason", "")
            if reason:
                phase = reason
                break

        owner_refs = metadata.get("ownerReferences", [])
        is_ds = any(ref.get("kind") == "DaemonSet" for ref in owner_refs)

        return cls(
            namespace=metadata.get("namespace", ""),
            name=metadata.get("name", ""),
            status=phase,
            node=spec.get("nodeName", ""),
            restarts=restarts,
            is_daemonset=is_ds,
        )


def get_pods_for_nodes(
    node_names: list[str],
    include_daemonsets: bool = False,
) -> list[PodInfo]:
    """
    Fetch pods running on the given nodes.

    Args:
        node_names: Node names to filter by. Empty list returns no pods.
        include_daemonsets: If True, include DaemonSet-managed pods.

    Returns:
        List of PodInfo sorted by (node, namespace, name).
    """
    if not node_names:
        return []
    data = get_pods_json()
    if not data:
        raise RuntimeError(
            "Failed to get pods. Ensure cluster context is set (e.g. set-clus staging)."
        )
    node_set = set(node_names)
    pods: list[PodInfo] = []
    for item in data.get("items", []):
        pod = PodInfo.from_dict(item)
        if pod.node not in node_set:
            continue
        if pod.is_daemonset and not include_daemonsets:
            continue
        pods.append(pod)
    pods.sort(key=lambda p: (p.node, p.namespace, p.name))
    return pods
