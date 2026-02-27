"""
CLI entry point for knode.

Cordon, uncordon, drain, and scale EKS nodes. Run after setting cluster context (e.g. set-clus staging).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

import click
from click.shell_completion import CompletionItem

from .aws import NodeGroupInfo, get_nodegroup_info, list_nodegroups, update_nodegroup_scaling
from .kubectl import cordon_nodes, drain_nodes, get_cluster_name, get_cluster_profile, get_cluster_region, uncordon_nodes
from .nodes import NodeInfo, PodInfo, get_all_nodes, get_pods_for_nodes

CAPTYPE_CHOICES = ["spot", "on-demand", "NG/spot", "NG/on-demand", "fargate"]
SCALE_CAPTYPE_CHOICES = ["spot", "on-demand"]


def _complete_nodegroups(ctx: click.Context, param: click.Parameter, incomplete: str) -> List[CompletionItem]:
    """Shell complete node group names from the current cluster."""
    try:
        cluster = get_cluster_name()
        region = get_cluster_region()
        profile = get_cluster_profile()
        names = list_nodegroups(cluster, region, profile)
        return [CompletionItem(n) for n in names if n.startswith(incomplete)]
    except Exception:
        return []


def _complete_node_names(ctx: click.Context, param: click.Parameter, incomplete: str) -> List[CompletionItem]:
    """Shell complete node names from the current cluster."""
    try:
        nodes = get_all_nodes()
        return [CompletionItem(n.name) for n in nodes if n.name.startswith(incomplete)]
    except Exception:
        return []


def _complete_captype(ctx: click.Context, param: click.Parameter, incomplete: str) -> List[CompletionItem]:
    """Shell complete capacity type values."""
    return [CompletionItem(c) for c in CAPTYPE_CHOICES if c.startswith(incomplete)]


def _complete_scale_captype(ctx: click.Context, param: click.Parameter, incomplete: str) -> List[CompletionItem]:
    """Shell complete capacity type values for scale (MNG types only)."""
    return [CompletionItem(c) for c in SCALE_CAPTYPE_CHOICES if c.startswith(incomplete)]


def _supported_shells() -> List[str]:
    return ["bash", "zsh", "fish"]


def _generate_completion_script(cli: click.Command, prog_name: str, shell: str) -> str:
    """Generate completion script using Click's ShellComplete (includes subcommands)."""
    if shell not in _supported_shells():
        raise click.ClickException(f"Unsupported shell '{shell}'. Use one of: {', '.join(_supported_shells())}")

    try:
        from click.shell_completion import get_completion_class
    except ImportError:
        raise click.ClickException("Shell completion requires Click 8.0+") from None

    comp_cls = get_completion_class(shell)
    if comp_cls is None:
        raise click.ClickException(f"No completion class for shell '{shell}'")

    complete_var = f"_{prog_name.upper().replace('-', '_')}_COMPLETE"
    comp = comp_cls(cli, {}, prog_name, complete_var)
    return comp.source()


def _completion_install_path(shell: str) -> Path:
    base = Path.home() / ".config" / "knode"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"completion.{shell}"


EPILOG = """
Examples:

  knode list                    # List all nodes in the cluster
  knode list -o name             # List node names only (one per line)
  knode pods ip-10-128-1-20...   # Show pods on a node
  knode pods -c spot             # Show pods on all spot nodes
  knode cordon --captype=spot    # Cordon all spot nodes
  knode uncordon ip-10-128-...   # Uncordon a node (mark schedulable)
  knode drain ip-10-128-1-20...  # Drain a node (evict pods)
  knode nodegroups              # List managed node groups and scaling config
  knode scale main --desired=0          # Scale one node group to 0
  knode scale --all --desired=0         # Scale all MNGs to 0
  knode scale -c spot --desired=0       # Scale all spot MNGs to 0
  knode scale -c on-demand --min=1      # Set min=1 on all on-demand MNGs

Shell completion:

  knode --show-completion bash   # Print completion script
  knode --install-completion bash  # Install to ~/.config/knode/

Run after setting cluster context (e.g. set-clus staging).
From workstation: bash3p "set-clus dev; knode list"
"""


def _resolve_nodes(
    node_args: tuple[str, ...],
    captype: Optional[str] = None,
) -> list[str]:
    """
    Resolve node names from explicit args and/or --captype filter.
    Returns union of nodes matching either. Requires at least one of nodes or captype.
    """
    result: set[str] = set()

    if node_args:
        result.update(node_args)

    if captype:
        try:
            all_nodes = get_all_nodes()
            for n in all_nodes:
                if n.captype == captype:
                    result.add(n.name)
        except RuntimeError as e:
            click.echo(str(e), err=True)
            sys.exit(1)

    return list(result)


def _print_nodes(nodes: list[NodeInfo], output: str) -> None:
    """Print nodes in table or name-only format."""
    if output == "name":
        for n in nodes:
            click.echo(n.name)
        return

    name_w = max(len(n.name) for n in nodes) if nodes else 10
    type_w = max(len(n.instance_type) for n in nodes) if nodes else 12
    captype_w = max(len(n.captype) for n in nodes) if nodes else 10
    zone_w = max(len(n.zone) for n in nodes) if nodes else 10
    status_w = max(len(n.status) for n in nodes) if nodes else 12

    header = f"{'NAME':<{name_w}}  {'TYPE':<{type_w}}  {'CAPTYPE':<{captype_w}}  {'ZONE':<{zone_w}}  {'STATUS':<{status_w}}"
    click.echo(header)
    click.echo("-" * len(header))

    for n in nodes:
        cordon_mark = " [cordoned]" if n.unschedulable else ""
        click.echo(f"{n.name:<{name_w}}  {n.instance_type:<{type_w}}  {n.captype:<{captype_w}}  {n.zone:<{zone_w}}  {n.status}{cordon_mark}")


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog=EPILOG,
    invoke_without_command=True,
)
@click.option(
    "--show-completion",
    type=click.Choice(_supported_shells()),
    help="Print shell completion script and exit",
)
@click.option(
    "--install-completion",
    type=click.Choice(_supported_shells()),
    help="Install shell completion script under ~/.config/knode/",
)
@click.pass_context
def main(
    ctx: click.Context,
    show_completion: Optional[str],
    install_completion: Optional[str],
) -> None:
    """
    List, cordon, uncordon, drain, and scale EKS nodes.

    List nodes, cordon (mark unschedulable), uncordon (mark schedulable),
    drain (evict pods), or change managed node group sizing. Requires cluster
    context (e.g. set-clus staging).
    """
    if show_completion:
        script = _generate_completion_script(ctx.command, ctx.info_name or "knode", show_completion)
        click.echo(script)
        raise SystemExit(0)
    if install_completion:
        script = _generate_completion_script(ctx.command, ctx.info_name or "knode", install_completion)
        out_path = _completion_install_path(install_completion)
        out_path.write_text(script, encoding="utf-8")
        click.echo(f"Wrote completion script to: {out_path}\n")
        if install_completion == "bash":
            click.echo(f'Add to ~/.bashrc:\n  source "{out_path}"')
        elif install_completion == "zsh":
            click.echo(f'Add to ~/.zshrc:\n  source "{out_path}"')
        else:
            click.echo(f'Add to ~/.config/fish/config.fish:\n  source "{out_path}"')
        raise SystemExit(0)
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        raise SystemExit(0)


@main.command("list")
@click.option(
    "-o",
    "--output",
    "output",
    type=click.Choice(["table", "name"]),
    default="table",
    help="Output format: table (default) or name (one per line)",
)
def list_cmd(output: str) -> None:
    """List all nodes in the cluster."""
    try:
        nodes = get_all_nodes()
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    if not nodes:
        click.echo("No nodes found.")
        return
    _print_nodes(nodes, output)


def _print_pods(pods: list[PodInfo], nodes_by_name: dict[str, NodeInfo]) -> None:
    """Print pods grouped by node with node header showing type/captype."""
    from itertools import groupby

    for node_name, group in groupby(pods, key=lambda p: p.node):
        pod_list = list(group)
        node = nodes_by_name.get(node_name)
        if node:
            header = f"{node.name}  ({node.instance_type}, {node.captype})"
        else:
            header = node_name
        click.echo(header)

        ns_w = max(len(p.namespace) for p in pod_list)
        name_w = max(len(p.name) for p in pod_list)
        status_w = max(len(p.status) for p in pod_list)

        for p in pod_list:
            restart_str = f"  restarts={p.restarts}" if p.restarts > 0 else ""
            click.echo(f"  {p.namespace:<{ns_w}}  {p.name:<{name_w}}  {p.status:<{status_w}}{restart_str}")
        click.echo()


@main.command("pods")
@click.argument("nodes", nargs=-1, shell_complete=_complete_node_names)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_captype,
    help="Show pods on nodes matching capacity type (e.g. spot, on-demand, NG/spot, NG/on-demand, fargate)",
)
@click.option(
    "--include-daemonsets",
    is_flag=True,
    help="Include DaemonSet pods (excluded by default)",
)
def pods_cmd(nodes: tuple[str, ...], captype: Optional[str], include_daemonsets: bool) -> None:
    """Show pods running on nodes. Pass node names and/or -c/--captype to filter."""
    node_list = _resolve_nodes(nodes, captype)
    if not node_list:
        click.echo(
            "No nodes specified. Provide node names and/or --captype (e.g. -c spot).",
            err=True,
        )
        sys.exit(1)

    try:
        all_nodes = get_all_nodes()
        nodes_by_name = {n.name: n for n in all_nodes}
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    # Validate that requested node names actually exist
    unknown = [n for n in node_list if n not in nodes_by_name]
    if unknown:
        click.echo(f"Unknown node(s): {', '.join(unknown)}", err=True)
        sys.exit(1)

    try:
        pods = get_pods_for_nodes(node_list, include_daemonsets=include_daemonsets)
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    if not pods:
        click.echo("No pods found on the specified node(s).")
        return

    _print_pods(pods, nodes_by_name)
    click.echo(f"{len(pods)} pod(s) on {len(set(p.node for p in pods))} node(s)")


@main.command()
@click.argument("nodes", nargs=-1, shell_complete=_complete_node_names)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_captype,
    help="Target nodes by capacity type (e.g. spot, on-demand, NG/spot, NG/on-demand, fargate)",
)
def cordon(nodes: tuple[str, ...], captype: Optional[str]) -> None:
    """Cordon nodes (mark unschedulable). Pass node names and/or --captype."""
    node_list = _resolve_nodes(nodes, captype)
    if not node_list:
        click.echo(
            "No nodes specified. Provide node names and/or --captype (e.g. --captype=spot).",
            err=True,
        )
        sys.exit(1)
    click.echo(f"=> kubectl cordon {' '.join(node_list)}")
    rc, out = cordon_nodes(node_list)
    if out:
        click.echo(out, err=(rc != 0))
    sys.exit(rc)


@main.command()
@click.argument("nodes", nargs=-1, shell_complete=_complete_node_names)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_captype,
    help="Target nodes by capacity type (e.g. spot, on-demand, NG/spot, NG/on-demand, fargate)",
)
def uncordon(nodes: tuple[str, ...], captype: Optional[str]) -> None:
    """Uncordon nodes (mark schedulable). Pass node names and/or --captype."""
    node_list = _resolve_nodes(nodes, captype)
    if not node_list:
        click.echo(
            "No nodes specified. Provide node names and/or --captype (e.g. --captype=spot).",
            err=True,
        )
        sys.exit(1)
    click.echo(f"=> kubectl uncordon {' '.join(node_list)}")
    rc, out = uncordon_nodes(node_list)
    if out:
        click.echo(out, err=(rc != 0))
    sys.exit(rc)


@main.command()
@click.argument("nodes", nargs=-1, shell_complete=_complete_node_names)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_captype,
    help="Target nodes by capacity type (e.g. spot, on-demand, NG/spot, NG/on-demand, fargate)",
)
@click.option(
    "--ignore-errors",
    is_flag=True,
    help="Continue even if there are errors during eviction",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bar during drain",
)
def drain(nodes: tuple[str, ...], captype: Optional[str], ignore_errors: bool, no_progress: bool) -> None:
    """Drain nodes (evict pods). Pass node names and/or --captype. Uses --ignore-daemonsets and --delete-emptydir-data."""
    node_list = _resolve_nodes(nodes, captype)
    show_progress = not no_progress
    if not node_list:
        click.echo(
            "No nodes specified. Provide node names and/or --captype (e.g. --captype=spot).",
            err=True,
        )
        sys.exit(1)
    click.echo(f"=> kubectl drain --delete-emptydir-data --ignore-daemonsets {' '.join(node_list)}")
    rc, out = drain_nodes(node_list, ignore_errors=ignore_errors, show_progress=show_progress)
    if out:
        click.echo(out, err=(rc != 0))
    sys.exit(rc)


@main.command("cordon-drain")
@click.argument("nodes", nargs=-1, shell_complete=_complete_node_names)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_captype,
    help="Target nodes by capacity type (e.g. spot, on-demand, NG/spot, NG/on-demand, fargate)",
)
@click.option(
    "--ignore-errors",
    is_flag=True,
    help="Continue even if there are errors during eviction",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable progress bar during drain",
)
def cordon_drain(nodes: tuple[str, ...], captype: Optional[str], ignore_errors: bool, no_progress: bool) -> None:
    """Cordon nodes, then drain them. Pass node names and/or --captype."""
    node_list = _resolve_nodes(nodes, captype)
    show_progress = not no_progress
    if not node_list:
        click.echo(
            "No nodes specified. Provide node names and/or --captype (e.g. --captype=spot).",
            err=True,
        )
        sys.exit(1)
    click.echo(f"=> kubectl cordon {' '.join(node_list)}")
    rc, out = cordon_nodes(node_list)
    if out:
        click.echo(out, err=(rc != 0))
    if rc != 0:
        sys.exit(rc)

    click.echo(f"=> kubectl drain --delete-emptydir-data --ignore-daemonsets {' '.join(node_list)}")
    rc, out = drain_nodes(node_list, ignore_errors=ignore_errors, show_progress=show_progress)
    if out:
        click.echo(out, err=(rc != 0))
    sys.exit(rc)


@main.command("nodegroups")
def nodegroups_cmd() -> None:
    """List managed node groups and their scaling config (min/max/desired)."""
    try:
        cluster = get_cluster_name()
        region = get_cluster_region()
        profile = get_cluster_profile()
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    try:
        names = list_nodegroups(cluster, region, profile)
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    if not names:
        click.echo("No managed node groups in this cluster.")
        return

    infos: list[tuple[str, Optional[NodeGroupInfo]]] = []
    for ng_name in sorted(names):
        infos.append((ng_name, get_nodegroup_info(cluster, ng_name, region, profile)))

    name_w = max(len(n) for n, _ in infos) if infos else 10
    cap_w = max((len(i.capacity_type) for _, i in infos if i), default=10)
    cap_w = max(cap_w, len("CAPTYPE"))
    header = f"{'NODEGROUP':<{name_w}}  {'STATUS':<12}  {'CAPTYPE':<{cap_w}}  {'MIN':>6}  {'MAX':>6}  {'DESIRED':>8}"
    click.echo(header)
    click.echo("-" * len(header))

    for ng_name, info in infos:
        if info:
            click.echo(
                f"{info.name:<{name_w}}  {info.status:<12}  {info.capacity_type:<{cap_w}}  {info.min_size:>6}  {info.max_size:>6}  {info.desired_size:>8}"
            )
        else:
            click.echo(f"{ng_name:<{name_w}}  (unable to describe)")


@main.command("scale")
@click.argument("nodegroup", type=str, required=False, shell_complete=_complete_nodegroups)
@click.option(
    "--all",
    "scale_all",
    is_flag=True,
    help="Apply scaling to all managed node groups in the cluster",
)
@click.option(
    "-c",
    "--captype",
    type=str,
    default=None,
    shell_complete=_complete_scale_captype,
    help="Filter node groups by capacity type (spot or on-demand)",
)
@click.option("--min", "min_size", type=int, default=None, help="Minimum number of nodes")
@click.option("--max", "max_size", type=int, default=None, help="Maximum number of nodes")
@click.option("--desired", "desired_size", type=int, default=None, help="Desired number of nodes")
def scale_cmd(
    nodegroup: Optional[str],
    scale_all: bool,
    captype: Optional[str],
    min_size: Optional[int],
    max_size: Optional[int],
    desired_size: Optional[int],
) -> None:
    """Update managed node group scaling (min, max, desired). Use --all or --captype to target multiple MNGs."""
    if min_size is None and max_size is None and desired_size is None:
        click.echo("Specify at least one of --min, --max, or --desired.", err=True)
        sys.exit(1)

    if nodegroup and (scale_all or captype):
        click.echo("Cannot specify nodegroup name together with --all or --captype.", err=True)
        sys.exit(1)
    if scale_all and captype:
        click.echo("Cannot specify both --all and --captype.", err=True)
        sys.exit(1)
    if not nodegroup and not scale_all and not captype:
        click.echo("Specify a node group name, --all, or --captype.", err=True)
        sys.exit(1)

    try:
        cluster = get_cluster_name()
        region = get_cluster_region()
        profile = get_cluster_profile()
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    try:
        if scale_all or captype:
            all_names = list_nodegroups(cluster, region, profile)
            if not all_names:
                click.echo("No managed node groups in this cluster.", err=True)
                sys.exit(1)
            if captype:
                names = []
                for ng_name in all_names:
                    info = get_nodegroup_info(cluster, ng_name, region, profile)
                    if info and info.capacity_type == captype:
                        names.append(ng_name)
                if not names:
                    click.echo(f"No managed node groups with capacity type '{captype}'.", err=True)
                    sys.exit(1)
            else:
                names = all_names
        else:
            names = [nodegroup]
            info = get_nodegroup_info(cluster, nodegroup, region, profile)
            if not info:
                click.echo(f"Node group '{nodegroup}' not found or not accessible.", err=True)
                sys.exit(1)
    except RuntimeError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    failed = 0
    for ng in sorted(names):
        click.echo(f"=> aws eks update-nodegroup-config --cluster-name {cluster} --nodegroup-name {ng}")
        rc, out = update_nodegroup_scaling(
            cluster, ng, region=region, profile=profile, min_size=min_size, max_size=max_size, desired_size=desired_size
        )
        if out:
            click.echo(out, err=(rc != 0))
        if rc != 0:
            failed += 1

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
