# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import base64
import json
import shutil
from pathlib import Path
from typing import Optional

import docker
from docker.errors import DockerException, NotFound
from django.conf import settings
from django.utils import timezone

from .models import ManagedNode


class NodeLauncherError(Exception):
    pass


def launcher_enabled():
    return getattr(settings, "NODE_LAUNCHER_ENABLED", False)


def docker_client():
    if not launcher_enabled():
        raise NodeLauncherError("Node launcher is disabled.")

    base_url = (settings.NODE_LAUNCHER_DOCKER_HOST or "").strip() or None
    try:
        if base_url:
            client = docker.DockerClient(base_url=base_url)
        else:
            client = docker.from_env()
        client.ping()
        return client
    except DockerException as exc:
        raise NodeLauncherError(f"Docker engine is unavailable: {exc}") from exc


def launcher_root() -> Path:
    root = Path(settings.NODE_LAUNCHER_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    return root


def node_root(node: ManagedNode) -> Path:
    path = launcher_root() / node.name
    path.mkdir(parents=True, exist_ok=True)
    return path


def config_path(node: ManagedNode) -> Path:
    return node_root(node) / "config.toml"


def genesis_path(node: ManagedNode) -> Path:
    return node_root(node) / "genesis.toml"


def data_path(node: ManagedNode) -> Path:
    path = node_root(node) / "data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def render_config(node: ManagedNode) -> str:
    data_dir = "/data/kumquat/data"
    node_id_line = f'node_id = "{node.reward_address}"\n' if node.reward_address else ""
    return f"""[node]
node_name = "{node.name}"
{node_id_line}data_dir = "{data_dir}"
log_level = "info"
enable_metrics = true
metrics_port = {node.metrics_port}
enable_api = true
api_port = {node.api_port}
api_host = "0.0.0.0"

[network]
listen_addr = "0.0.0.0"
listen_port = {node.p2p_port}
bootstrap_nodes = []
max_peers = 16
min_peers = 0
discovery_interval = 30
connection_timeout = 10
handshake_timeout = 5
enable_upnp = false
enable_natpmp = false
enable_dht = false
dht_bootstrap_nodes = []

[consensus]
chain_id = {node.chain_id}
enable_mining = {"true" if node.enable_mining else "false"}
mining_threads = {node.mining_threads}
target_block_time = 5
initial_difficulty = 100
difficulty_adjustment_interval = 2016
max_transactions_per_block = 10000
max_block_size = 1048576
max_gas_per_block = 10000000
gas_price_minimum = 1
enable_poh = true
poh_tick_interval = 10
poh_ticks_per_block = 1000

[storage]
db_path = "{data_dir}/db"
cache_size = 512
max_open_files = 1000
write_buffer_size = 64
max_write_buffer_number = 3
enable_wal = true
enable_statistics = false
enable_compression = true
compression_type = "lz4"
enable_bloom_filters = true
bloom_filter_bits_per_key = 10
enable_auto_compaction = true
compaction_style = "level"
enable_pruning = false
pruning_keep_recent = 10000
pruning_interval = 100
"""


def render_genesis(node: ManagedNode) -> str:
    return f"""chain_id = {node.chain_id}
timestamp = {int(timezone.now().timestamp())}
initial_difficulty = 100

[initial_accounts.0000000000000000000000000000000000000000000000000000000000000001]
denominations = ["100", "50", "20", "10", "5", "2", "1", "0.5", "0.25", "0.1", "0.05", "0.01"]
account_type = "User"

[initial_accounts.0000000000000000000000000000000000000000000000000000000000000002]
denominations = ["100", "50", "20", "10", "5", "2", "1", "0.5", "0.25", "0.1", "0.05", "0.01"]
account_type = "User"
"""


def ensure_runtime_files(node: ManagedNode):
    config_path(node).write_text(render_config(node))
    genesis_path(node).write_text(render_genesis(node))
    data_path(node)


def container_name(node: ManagedNode) -> str:
    return f"kumquat-managed-{node.name}"


def dashboard_proxy_path(node: ManagedNode) -> str:
    return f"/nodes/{node.id}/proxy/dashboard"


def _split_image_reference(image: str):
    registry, remainder = image.split("/", 1)
    repository = remainder
    tag = None
    if ":" in remainder.rsplit("/", 1)[-1]:
        repository, tag = remainder.rsplit(":", 1)
    return registry, repository, tag


def _docker_auth_candidates(registry: str):
    normalized = registry.rstrip("/")
    return (
        normalized,
        f"https://{normalized}",
        f"https://{normalized}/v1/",
    )


def _decode_auth_entry(entry: dict):
    username = entry.get("username")
    password = entry.get("password")
    auth = entry.get("auth")
    if (not username or not password) and auth:
        try:
            decoded = base64.b64decode(auth).decode("utf-8")
            username, password = decoded.split(":", 1)
        except (ValueError, UnicodeDecodeError, base64.binascii.Error):
            return None
    if username and password:
        return {"username": username, "password": password}
    return None


def _registry_auth_config(image: str):
    auth_path = (settings.NODE_LAUNCHER_REGISTRY_AUTH_FILE or "").strip()
    if not auth_path:
        return None
    auth_file = Path(auth_path)
    if not auth_file.exists():
        raise NodeLauncherError(f"Registry auth file is missing: {auth_file}")

    try:
        config = json.loads(auth_file.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise NodeLauncherError(f"Registry auth file could not be read: {exc}") from exc

    registry, _, _ = _split_image_reference(image)
    auths = config.get("auths") or {}
    for candidate in _docker_auth_candidates(registry):
        auth_entry = auths.get(candidate)
        if not isinstance(auth_entry, dict):
            continue
        auth_config = _decode_auth_entry(auth_entry)
        if auth_config:
            auth_config["serveraddress"] = registry
            return auth_config
    return None


def _is_same_repository(left: str, right: str) -> bool:
    try:
        left_registry, left_repository, _ = _split_image_reference(left)
        right_registry, right_repository, _ = _split_image_reference(right)
    except ValueError:
        return False
    return left_registry == right_registry and left_repository == right_repository


def _resolve_node_image(node: ManagedNode) -> str:
    configured_image = (getattr(settings, "NODE_LAUNCHER_IMAGE", "") or "").strip()
    current_image = (node.image or "").strip()
    if not current_image:
        return configured_image
    if configured_image and current_image != configured_image and _is_same_repository(current_image, configured_image):
        node.image = configured_image
        node.save(update_fields=["image", "updated_at"])
        return configured_image
    return current_image


def ensure_image_available(client, image: str):
    pull_policy = getattr(settings, "NODE_LAUNCHER_IMAGE_PULL_POLICY", "ifnotpresent")
    if pull_policy == "never":
        return

    if pull_policy != "always":
        try:
            client.images.get(image)
            return
        except NotFound:
            pass
        except DockerException as exc:
            raise NodeLauncherError(f"Failed to inspect node image {image}: {exc}") from exc

    registry, repository, tag = _split_image_reference(image)
    try:
        client.images.pull(
            f"{registry}/{repository}",
            tag=tag,
            auth_config=_registry_auth_config(image),
        )
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to pull node image {image}: {exc}") from exc


def fetch_container(node: ManagedNode):
    client = docker_client()
    if node.container_id:
        try:
            return client.containers.get(node.container_id)
        except NotFound:
            pass
    if node.container_name:
        try:
            return client.containers.get(node.container_name)
        except NotFound:
            return None
    return None


def fetch_container_by_id(container_id: str):
    client = docker_client()
    try:
        return client.containers.get(container_id)
    except NotFound:
        return None
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to inspect container {container_id}: {exc}") from exc


def list_runtime_containers():
    client = docker_client()
    try:
        return client.containers.list(all=True)
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to list runtime containers: {exc}") from exc


def launch_node(node: ManagedNode) -> ManagedNode:
    ensure_runtime_files(node)
    client = docker_client()
    existing = fetch_container(node)
    if existing is not None and existing.status in {"created", "running", "restarting"}:
        raise NodeLauncherError("Managed node is already running.")
    image = _resolve_node_image(node)

    root = node_root(node)
    volumes = {
        str(root): {
            "bind": "/data/kumquat",
            "mode": "rw",
        }
    }
    command = [
        "--config",
        "/data/kumquat/config.toml",
        "--genesis",
        "/data/kumquat/genesis.toml",
        "--network",
        node.network_name,
    ]

    try:
        if existing is not None:
            try:
                existing.remove(force=True)
            except DockerException:
                pass

        ensure_image_available(client, image)
        container = client.containers.run(
            image,
            command=command,
            detach=True,
            name=container_name(node),
            hostname=node.name,
            labels={
                "kumquat.managed-node": "true",
                "kumquat.managed-node-id": str(node.id),
            },
            network_mode="host",
            volumes=volumes,
            restart_policy={"Name": "unless-stopped"},
        )
    except DockerException as exc:
        node.status = ManagedNode.STATUS_FAILED
        node.last_error = str(exc)
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "last_error", "last_status_at", "updated_at"])
        raise NodeLauncherError(f"Failed to launch node container: {exc}") from exc

    node.container_name = container.name
    node.container_id = container.id
    node.status = ManagedNode.STATUS_RUNNING
    node.last_error = ""
    node.stopped_at = None
    node.last_status_at = timezone.now()
    node.save(
        update_fields=[
            "container_name",
            "container_id",
            "status",
            "last_error",
            "stopped_at",
            "last_status_at",
            "updated_at",
        ]
    )
    return refresh_node(node)


def stop_node(node: ManagedNode) -> ManagedNode:
    container = fetch_container(node)
    if container is None:
        node.status = ManagedNode.STATUS_STOPPED
        node.stopped_at = timezone.now()
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
        return node

    try:
        container.stop(timeout=10)
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to stop node container: {exc}") from exc

    node.status = ManagedNode.STATUS_STOPPED
    node.stopped_at = timezone.now()
    node.last_status_at = timezone.now()
    node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
    return refresh_node(node)


def restart_node(node: ManagedNode) -> ManagedNode:
    container = fetch_container(node)
    if container is None:
        return launch_node(node)

    try:
        container.restart(timeout=10)
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to restart node container: {exc}") from exc

    node.stopped_at = None
    node.last_error = ""
    node.last_status_at = timezone.now()
    node.save(update_fields=["stopped_at", "last_error", "last_status_at", "updated_at"])
    return refresh_node(node)


def delete_container(node: ManagedNode) -> ManagedNode:
    container = fetch_container(node)
    if container is not None:
        try:
            container.remove(force=True)
        except DockerException as exc:
            raise NodeLauncherError(f"Failed to delete node container: {exc}") from exc

    node.container_name = ""
    node.container_id = ""
    node.status = ManagedNode.STATUS_STOPPED
    node.stopped_at = timezone.now()
    node.last_status_at = timezone.now()
    node.save(
        update_fields=[
            "container_name",
            "container_id",
            "status",
            "stopped_at",
            "last_status_at",
            "updated_at",
        ]
    )
    return node


def delete_deployment(node: ManagedNode):
    delete_container(node)
    root = node_root(node)
    if root.exists():
        shutil.rmtree(root, ignore_errors=False)
    node.delete()


def restart_runtime_container(container_id: str):
    container = fetch_container_by_id(container_id)
    if container is None:
        raise NodeLauncherError("Container not found.")
    try:
        container.restart(timeout=10)
        container.reload()
        return container
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to restart container: {exc}") from exc


def delete_runtime_container(container_id: str):
    container = fetch_container_by_id(container_id)
    if container is None:
        raise NodeLauncherError("Container not found.")
    try:
        container.remove(force=True)
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to delete container: {exc}") from exc


def tail_logs(node: ManagedNode, lines: int = 120) -> str:
    container = fetch_container(node)
    if container is None:
        return node.last_logs or ""
    try:
        output = container.logs(tail=lines).decode("utf-8", errors="replace")
    except DockerException as exc:
        raise NodeLauncherError(f"Failed to read node logs: {exc}") from exc
    node.last_logs = output[-12000:]
    node.last_status_at = timezone.now()
    node.save(update_fields=["last_logs", "last_status_at", "updated_at"])
    return node.last_logs


def refresh_node(node: ManagedNode) -> ManagedNode:
    container = fetch_container(node)
    if container is None:
        if node.status == ManagedNode.STATUS_RUNNING:
            node.status = ManagedNode.STATUS_EXITED
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "last_status_at", "updated_at"])
        return node

    try:
        container.reload()
        state = container.attrs.get("State") or {}
        status = state.get("Status") or container.status or ManagedNode.STATUS_PENDING
        exit_code = state.get("ExitCode")
        error_message = state.get("Error") or ""
        node.status = map_container_status(status)
        if error_message:
            node.last_error = error_message
        elif exit_code not in (None, 0) and node.status != ManagedNode.STATUS_RUNNING:
            node.last_error = f"Container exited with code {exit_code}"
        if node.status in {ManagedNode.STATUS_STOPPED, ManagedNode.STATUS_EXITED} and node.stopped_at is None:
            node.stopped_at = timezone.now()
        node.last_status_at = timezone.now()
        node.last_logs = container.logs(tail=120).decode("utf-8", errors="replace")[-12000:]
        node.container_name = container.name
        node.container_id = container.id
        node.save()
        return node
    except DockerException as exc:
        node.status = ManagedNode.STATUS_FAILED
        node.last_error = str(exc)
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "last_error", "last_status_at", "updated_at"])
        raise NodeLauncherError(f"Failed to inspect node container: {exc}") from exc


def map_container_status(status: str) -> str:
    normalized = (status or "").lower()
    if normalized == "running":
        return ManagedNode.STATUS_RUNNING
    if normalized in {"exited", "dead"}:
        return ManagedNode.STATUS_EXITED
    if normalized in {"created", "restarting"}:
        return ManagedNode.STATUS_PENDING
    if normalized in {"paused"}:
        return ManagedNode.STATUS_STOPPED
    return ManagedNode.STATUS_FAILED
