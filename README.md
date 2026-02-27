# knode

CLI to list, cordon, uncordon, drain, and scale EKS nodes. Lists all nodes in a cluster, supports cordoning (mark unschedulable), uncordoning (mark schedulable), draining (evict pods with progress bar), and managed node group scaling.

## Usage

Run after setting cluster context (e.g. `set-clus staging`). From your workstation, use bash3p:

```bash
bash3p "set-clus dev; knode list"
bash3p "set-clus dev; knode nodegroups"
bash3p "set-clus dev; knode scale main-use1-az1 --desired=0"
bash3p "set-clus dev; knode cordon --captype=spot"
bash3p "set-clus dev; knode drain ip-10-128-1-20.ec2.internal"
```

## Commands

- **list** – List all nodes (table or `-o name`). Shows captype (spot, on-demand, NG/spot, NG/on-demand, fargate).
- **cordon** – Cordon nodes (mark unschedulable). Use `--captype` to target by capacity type.
- **uncordon** – Uncordon nodes (mark schedulable). Use `--captype` to target by capacity type.
- **drain** – Drain nodes (evict pods). Shows progress bar. Use `--captype`, `--ignore-errors`, `--no-progress`.
- **cordon-drain** – Cordon then drain.
- **nodegroups** – List managed node groups and scaling (min/max/desired).
- **scale** – Update node group scaling. Use `--all` to resize all MNGs.

Use `-c`/`--captype` with cordon, drain, or cordon-drain to target nodes by capacity type. Use `knode --install-completion bash` for shell completion.

## Installation

Installed in the Geodesic (3p) container via the Dockerfile. To test changes before rebuild, install in a running container:

```bash
uv pip install --system /usr/local/share/knode/
```

Or run from the repo path (inside the container):

```bash
python -m knode.cli list
```
