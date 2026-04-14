# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from pathlib import Path
from typing import Optional

from django.conf import settings
from django.utils import timezone
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException

from .models import ManagedNode


class NodeLauncherError(Exception):
    pass


_KUBE_CONFIG_LOADED = False


def launcher_enabled():
    return getattr(settings, "NODE_LAUNCHER_ENABLED", False)


def _load_kubernetes_config():
    global _KUBE_CONFIG_LOADED
    if _KUBE_CONFIG_LOADED:
        return

    kubeconfig_path = (getattr(settings, "NODE_LAUNCHER_KUBECONFIG", "") or "").strip()
    try:
        if kubeconfig_path:
            k8s_config.load_kube_config(config_file=kubeconfig_path)
        else:
            try:
                k8s_config.load_incluster_config()
            except ConfigException:
                k8s_config.load_kube_config()
    except Exception as exc:
        raise NodeLauncherError(f"Kubernetes client is unavailable: {exc}") from exc

    _KUBE_CONFIG_LOADED = True


def _core_api():
    if not launcher_enabled():
        raise NodeLauncherError("Node launcher is disabled.")
    _load_kubernetes_config()
    return k8s_client.CoreV1Api()


def _apps_api():
    if not launcher_enabled():
        raise NodeLauncherError("Node launcher is disabled.")
    _load_kubernetes_config()
    return k8s_client.AppsV1Api()


def _launcher_namespace() -> str:
    return (getattr(settings, "NODE_LAUNCHER_KUBERNETES_NAMESPACE", "") or "kumquat").strip()


def _suffix_name(base: str, suffix: str) -> str:
    max_base_length = 63 - len(suffix) - 1
    trimmed = base[:max_base_length].rstrip("-")
    return f"{trimmed}-{suffix}" if trimmed else suffix[:63]


def workload_name(node: ManagedNode) -> str:
    return node.name


def configmap_name(node: ManagedNode) -> str:
    return _suffix_name(node.name, "config")


def peer_service_name(node: ManagedNode) -> str:
    return _suffix_name(node.name, "peer")


def peer_service_host(node: ManagedNode) -> str:
    return f"{peer_service_name(node)}.{_launcher_namespace()}.svc.cluster.local"


def _service_host(service_name: str) -> str:
    return f"{service_name}.{_launcher_namespace()}.svc.cluster.local"


def rpc_service_name(node: ManagedNode) -> str:
    return _suffix_name(node.name, "rpc")


def pod_name(node: ManagedNode) -> str:
    return f"{workload_name(node)}-0"


def pvc_name(node: ManagedNode) -> str:
    return f"data-{workload_name(node)}-0"


def rpc_service_host(node: ManagedNode) -> str:
    return f"{rpc_service_name(node)}.{_launcher_namespace()}.svc.cluster.local"


def upstream_rpc_url(node: ManagedNode, path: str) -> str:
    return f"http://{rpc_service_host(node)}:{node.api_port}{path}"


def dashboard_proxy_path(node: ManagedNode) -> str:
    return f"/nodes/{node.id}/proxy/dashboard"


def dashboard_subdomain_host(node: ManagedNode) -> str:
    return f"{node.name}.{settings.NODE_PROXY_BASE_DOMAIN}"


def dashboard_subdomain_url(node: ManagedNode) -> str:
    site_origin = (settings.SITE_URL or "https://kumquat.info").rstrip("/")
    if "://" in site_origin:
        scheme = site_origin.split("://", 1)[0]
    else:
        scheme = "https"
    return f"{scheme}://{dashboard_subdomain_host(node)}/dashboard"


def _is_same_repository(left: str, right: str) -> bool:
    try:
        left_registry, left_remainder = left.split("/", 1)
        right_registry, right_remainder = right.split("/", 1)
    except ValueError:
        return False

    left_repository = left_remainder.rsplit(":", 1)[0]
    right_repository = right_remainder.rsplit(":", 1)[0]
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


def _image_pull_policy() -> str:
    pull_policy = (getattr(settings, "NODE_LAUNCHER_IMAGE_PULL_POLICY", "") or "IfNotPresent").strip()
    normalized = pull_policy.lower()
    if normalized == "always":
        return "Always"
    if normalized == "never":
        return "Never"
    return "IfNotPresent"


def _image_pull_secrets():
    secrets = getattr(settings, "NODE_LAUNCHER_IMAGE_PULL_SECRETS", []) or []
    return [k8s_client.V1LocalObjectReference(name=name) for name in secrets if name]


def _node_selector():
    selector = getattr(settings, "NODE_LAUNCHER_NODE_SELECTOR", {}) or {}
    return selector if selector else None


def _rust_log() -> str:
    return (getattr(settings, "NODE_LAUNCHER_RUST_LOG", "") or "info").strip()


def _shared_genesis_file() -> Path:
    genesis_path = (getattr(settings, "NODE_LAUNCHER_GENESIS_FILE", "") or "").strip()
    if not genesis_path:
        raise NodeLauncherError("NODE_LAUNCHER_GENESIS_FILE is not configured.")
    genesis_file = Path(genesis_path)
    if not genesis_file.exists():
        raise NodeLauncherError(f"Shared genesis file does not exist: {genesis_file}")
    if not genesis_file.is_file():
        raise NodeLauncherError(f"Shared genesis path is not a file: {genesis_file}")
    return genesis_file


def _shared_genesis_hash() -> str:
    genesis_hash = (getattr(settings, "NODE_LAUNCHER_GENESIS_HASH", "") or "").strip().lower()
    if not genesis_hash:
        raise NodeLauncherError("NODE_LAUNCHER_GENESIS_HASH is not configured.")
    return genesis_hash


def _shared_genesis_contents() -> str:
    try:
        return _shared_genesis_file().read_text(encoding="utf-8")
    except OSError as exc:
        raise NodeLauncherError(f"Failed to read shared genesis file: {exc}") from exc


def _bootstrap_nodes(node: ManagedNode) -> list[str]:
    if node.name == "genesis" or not node.pk:
        return []

    configured_seed_host = (
        getattr(settings, "NODE_LAUNCHER_GENESIS_SEED_HOST", "") or ""
    ).strip().lower()
    configured_seed_port = int(getattr(settings, "NODE_LAUNCHER_GENESIS_SEED_PORT", 30333))
    if configured_seed_host:
        return [f"{configured_seed_host}:{configured_seed_port}"]

    seed = (
        ManagedNode.objects.filter(name="genesis")
        .exclude(pk=node.pk)
        .order_by("created_at")
        .first()
    )
    if seed:
        return [f"{peer_service_host(seed)}:{seed.p2p_port}"]

    service_name = (
        getattr(settings, "NODE_LAUNCHER_GENESIS_SEED_SERVICE_NAME", "") or "kumquat-blockchain-headless"
    ).strip()
    return [f"{_service_host(service_name)}:{configured_seed_port}"]


def render_config(node: ManagedNode) -> str:
    data_dir = "/data/kumquat/data"
    node_id_line = f'node_id = "{node.reward_address}"\n' if node.reward_address else ""
    shared_chain_id = int(getattr(settings, "NODE_LAUNCHER_CHAIN_ID", 1337))
    if node.chain_id != shared_chain_id:
        raise NodeLauncherError(
            f"Managed node {node.name} requested chain_id={node.chain_id}, "
            f"but the launcher is pinned to shared chain_id={shared_chain_id}."
        )
    genesis_hash = _shared_genesis_hash()
    bootstrap_nodes = _bootstrap_nodes(node)
    bootstrap_nodes_block = ",\n".join(f'  "{entry}"' for entry in bootstrap_nodes)
    return f"""[node]
node_name = "{node.name}"
{node_id_line}data_dir = "{data_dir}"
log_level = "debug"
enable_metrics = true
metrics_port = {node.metrics_port}
enable_api = true
api_port = {node.api_port}
api_host = "0.0.0.0"

[network]
listen_addr = "0.0.0.0"
listen_port = {node.p2p_port}
bootstrap_nodes = [
{bootstrap_nodes_block}
]
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
genesis_hash = "{genesis_hash}"
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
    return _shared_genesis_contents()


def _managed_labels(node: ManagedNode):
    return {
        "app.kubernetes.io/name": "kumquat-managed-node",
        "app.kubernetes.io/instance": node.name,
        "app.kubernetes.io/managed-by": "kumquat-website",
        "kumquat.managed-node": "true",
        "kumquat.managed-node-id": str(node.id),
        "kumquat.managed-node-name": node.name,
    }


def _managed_label_selector(node: ManagedNode) -> str:
    return f"kumquat.managed-node-id={node.id}"


def _read_pod(node: ManagedNode):
    try:
        return _core_api().read_namespaced_pod(name=pod_name(node), namespace=_launcher_namespace())
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise NodeLauncherError(f"Failed to inspect pod for {node.name}: {exc}") from exc


def _read_statefulset(node: ManagedNode):
    try:
        return _apps_api().read_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_launcher_namespace(),
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise NodeLauncherError(f"Failed to inspect workload for {node.name}: {exc}") from exc


def _build_configmap(node: ManagedNode):
    return k8s_client.V1ConfigMap(
        metadata=k8s_client.V1ObjectMeta(
            name=configmap_name(node),
            namespace=_launcher_namespace(),
            labels=_managed_labels(node),
        ),
        data={
            "config.toml": render_config(node),
            "genesis.toml": render_genesis(node),
        },
    )


def _build_service(node: ManagedNode, name: str, port_name: str, port: int, headless: bool = False):
    return k8s_client.V1Service(
        metadata=k8s_client.V1ObjectMeta(
            name=name,
            namespace=_launcher_namespace(),
            labels=_managed_labels(node),
        ),
        spec=k8s_client.V1ServiceSpec(
            cluster_ip="None" if headless else None,
            publish_not_ready_addresses=headless,
            selector=_managed_labels(node),
            ports=[
                k8s_client.V1ServicePort(
                    name=port_name,
                    port=port,
                    target_port=port,
                )
            ],
        ),
    )


def _build_statefulset(node: ManagedNode, image: str):
    labels = _managed_labels(node)
    command = """
mkdir -p /data/kumquat
cp /config/config.toml /data/kumquat/config.toml
cp /config/genesis.toml /data/kumquat/genesis.toml
exec /usr/local/bin/kumquat \
  --config /data/kumquat/config.toml \
  --genesis /data/kumquat/genesis.toml \
  --network "{network}"
""".strip().format(network=node.network_name)

    container = k8s_client.V1Container(
        name="blockchain",
        image=image,
        image_pull_policy=_image_pull_policy(),
        command=["/bin/sh", "-ec"],
        args=[command],
        env=[
            k8s_client.V1EnvVar(name="RUST_LOG", value=_rust_log()),
        ],
        ports=[
            k8s_client.V1ContainerPort(name="p2p", container_port=node.p2p_port),
            k8s_client.V1ContainerPort(name="rpc", container_port=node.api_port),
            k8s_client.V1ContainerPort(name="metrics", container_port=node.metrics_port),
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(name="config", mount_path="/config", read_only=True),
            k8s_client.V1VolumeMount(name="data", mount_path="/data/kumquat"),
        ],
    )

    pod_spec = k8s_client.V1PodSpec(
        containers=[container],
        image_pull_secrets=_image_pull_secrets() or None,
        node_selector=_node_selector(),
        volumes=[
            k8s_client.V1Volume(
                name="config",
                config_map=k8s_client.V1ConfigMapVolumeSource(name=configmap_name(node)),
            )
        ],
    )

    pod_template = k8s_client.V1PodTemplateSpec(
        metadata=k8s_client.V1ObjectMeta(labels=labels),
        spec=pod_spec,
    )

    pvc_template = k8s_client.V1PersistentVolumeClaim(
        metadata=k8s_client.V1ObjectMeta(labels=labels, name="data"),
        spec=k8s_client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=getattr(settings, "NODE_LAUNCHER_STORAGE_CLASS_NAME", "") or None,
            resources=k8s_client.V1ResourceRequirements(
                requests={"storage": getattr(settings, "NODE_LAUNCHER_STORAGE_SIZE", "20Gi")}
            ),
        ),
    )

    return k8s_client.V1StatefulSet(
        metadata=k8s_client.V1ObjectMeta(
            name=workload_name(node),
            namespace=_launcher_namespace(),
            labels=labels,
        ),
        spec=k8s_client.V1StatefulSetSpec(
            replicas=1,
            selector=k8s_client.V1LabelSelector(match_labels=labels),
            service_name=peer_service_name(node),
            persistent_volume_claim_retention_policy=(
                k8s_client.V1StatefulSetPersistentVolumeClaimRetentionPolicy(
                    when_deleted="Delete",
                    when_scaled="Retain",
                )
            ),
            template=pod_template,
            volume_claim_templates=[pvc_template],
        ),
    )


def _create_or_replace_configmap(node: ManagedNode):
    core = _core_api()
    configmap = _build_configmap(node)
    try:
        core.create_namespaced_config_map(namespace=_launcher_namespace(), body=configmap)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(f"Failed to create config for {node.name}: {exc}") from exc
        core.replace_namespaced_config_map(
            name=configmap_name(node),
            namespace=_launcher_namespace(),
            body=configmap,
        )


def _create_service_if_missing(node: ManagedNode, service_body):
    core = _core_api()
    try:
        core.create_namespaced_service(namespace=_launcher_namespace(), body=service_body)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(
                f"Failed to create service {service_body.metadata.name} for {node.name}: {exc}"
            ) from exc
        core.patch_namespaced_service(
            name=service_body.metadata.name,
            namespace=_launcher_namespace(),
            body={"spec": {"ports": [{"name": port.name, "port": port.port, "targetPort": port.target_port} for port in service_body.spec.ports]}},
        )


def _delete_named_service(name: str):
    try:
        _core_api().delete_namespaced_service(name=name, namespace=_launcher_namespace())
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete service {name}: {exc}") from exc


def _delete_named_configmap(name: str):
    try:
        _core_api().delete_namespaced_config_map(name=name, namespace=_launcher_namespace())
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete configmap {name}: {exc}") from exc


def _delete_pod(name: str):
    try:
        _core_api().delete_namespaced_pod(name=name, namespace=_launcher_namespace())
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete pod {name}: {exc}") from exc


def _ensure_workload(node: ManagedNode) -> str:
    image = _resolve_node_image(node)
    _create_or_replace_configmap(node)
    _create_service_if_missing(
        node,
        _build_service(node, peer_service_name(node), "p2p", node.p2p_port, headless=True),
    )
    _create_service_if_missing(
        node,
        _build_service(node, rpc_service_name(node), "rpc", node.api_port),
    )

    apps = _apps_api()
    body = _build_statefulset(node, image)
    try:
        apps.create_namespaced_stateful_set(namespace=_launcher_namespace(), body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(f"Failed to create workload for {node.name}: {exc}") from exc

        apps.patch_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_launcher_namespace(),
            body={
                "spec": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "blockchain",
                                    "image": image,
                                    "imagePullPolicy": _image_pull_policy(),
                                    "args": [body.spec.template.spec.containers[0].args[0]],
                                }
                            ]
                        }
                    }
                }
            },
        )
    return image


def launch_node(node: ManagedNode) -> ManagedNode:
    image = _ensure_workload(node)
    node.image = image
    node.status = ManagedNode.STATUS_PENDING
    node.last_error = ""
    node.stopped_at = None
    node.last_status_at = timezone.now()
    node.save(update_fields=["image", "status", "last_error", "stopped_at", "last_status_at", "updated_at"])
    return refresh_node(node)


def stop_node(node: ManagedNode) -> ManagedNode:
    statefulset = _read_statefulset(node)
    if statefulset is None:
        node.status = ManagedNode.STATUS_STOPPED
        node.stopped_at = timezone.now()
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
        return node

    _apps_api().patch_namespaced_stateful_set(
        name=workload_name(node),
        namespace=_launcher_namespace(),
        body={"spec": {"replicas": 0}},
    )
    node.status = ManagedNode.STATUS_STOPPED
    node.stopped_at = timezone.now()
    node.last_status_at = timezone.now()
    node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
    return refresh_node(node)


def restart_node(node: ManagedNode) -> ManagedNode:
    statefulset = _read_statefulset(node)
    if statefulset is None:
        return launch_node(node)

    replicas = getattr(statefulset.spec, "replicas", 0) or 0
    if replicas == 0:
        _apps_api().patch_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_launcher_namespace(),
            body={"spec": {"replicas": 1}},
        )
    else:
        _delete_pod(pod_name(node))

    node.stopped_at = None
    node.last_error = ""
    node.last_status_at = timezone.now()
    node.save(update_fields=["stopped_at", "last_error", "last_status_at", "updated_at"])
    return refresh_node(node)


def delete_container(node: ManagedNode) -> ManagedNode:
    stop_node(node)
    node.container_name = ""
    node.container_id = ""
    node.save(update_fields=["container_name", "container_id", "updated_at"])
    return node


def delete_deployment(node: ManagedNode):
    try:
        _apps_api().delete_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_launcher_namespace(),
            propagation_policy="Foreground",
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete workload for {node.name}: {exc}") from exc

    _delete_named_service(rpc_service_name(node))
    _delete_named_service(peer_service_name(node))
    _delete_named_configmap(configmap_name(node))

    try:
        _core_api().delete_namespaced_persistent_volume_claim(
            name=pvc_name(node),
            namespace=_launcher_namespace(),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete PVC for {node.name}: {exc}") from exc

    node.delete()


def restart_runtime_container(container_id: str):
    _delete_pod(container_id)
    return True


def delete_runtime_container(container_id: str):
    _delete_pod(container_id)
    return True


def tail_logs(node: ManagedNode, lines: int = 120) -> str:
    pod = _read_pod(node)
    if pod is None:
        return node.last_logs or ""

    try:
        output = _core_api().read_namespaced_pod_log(
            name=pod.metadata.name,
            namespace=_launcher_namespace(),
            tail_lines=lines,
        )
    except ApiException as exc:
        raise NodeLauncherError(f"Failed to read node logs: {exc}") from exc

    node.last_logs = (output or "")[-12000:]
    node.last_status_at = timezone.now()
    node.save(update_fields=["last_logs", "last_status_at", "updated_at"])
    return node.last_logs


def _extract_pod_status_details(pod) -> tuple[str, str]:
    container_statuses = pod.status.container_statuses or []
    if container_statuses:
        status = container_statuses[0]
        waiting = status.state.waiting if status.state else None
        terminated = status.state.terminated if status.state else None
        if waiting is not None:
            reason = waiting.reason or pod.status.phase or "Pending"
            message = waiting.message or ""
            return map_container_status(reason), f"{reason}: {message}".strip(": ")
        if terminated is not None:
            reason = terminated.reason or "Exited"
            message = terminated.message or f"Exit code {terminated.exit_code}"
            return map_container_status(reason), f"{reason}: {message}".strip(": ")
        if status.ready:
            return ManagedNode.STATUS_RUNNING, ""

    return map_container_status(pod.status.phase or "Pending"), ""


def refresh_node(node: ManagedNode) -> ManagedNode:
    statefulset = _read_statefulset(node)
    if statefulset is None:
        if node.status == ManagedNode.STATUS_RUNNING:
            node.status = ManagedNode.STATUS_EXITED
        node.container_name = ""
        node.container_id = ""
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "container_name", "container_id", "last_status_at", "updated_at"])
        return node

    replicas = getattr(statefulset.spec, "replicas", 0) or 0
    if replicas == 0:
        node.status = ManagedNode.STATUS_STOPPED
        node.container_name = ""
        node.container_id = ""
        node.stopped_at = node.stopped_at or timezone.now()
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "container_name", "container_id", "stopped_at", "last_status_at", "updated_at"])
        return node

    pod = _read_pod(node)
    if pod is None:
        node.status = ManagedNode.STATUS_PENDING
        node.container_name = ""
        node.container_id = ""
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "container_name", "container_id", "last_status_at", "updated_at"])
        return node

    status, error_message = _extract_pod_status_details(pod)
    node.status = status
    node.container_name = pod.metadata.name
    node.container_id = pod.metadata.uid or ""
    node.last_status_at = timezone.now()
    if error_message:
        node.last_error = error_message
    if status in {ManagedNode.STATUS_STOPPED, ManagedNode.STATUS_EXITED} and node.stopped_at is None:
        node.stopped_at = timezone.now()
    elif status == ManagedNode.STATUS_RUNNING:
        node.stopped_at = None

    try:
        node.last_logs = (
            _core_api().read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=_launcher_namespace(),
                tail_lines=120,
            )
            or ""
        )[-12000:]
    except ApiException:
        pass

    node.save()
    return node


def list_runtime_containers():
    try:
        pods = _core_api().list_namespaced_pod(
            namespace=_launcher_namespace(),
            label_selector="kumquat.managed-node=true",
        ).items
    except ApiException as exc:
        raise NodeLauncherError(f"Failed to list managed node workloads: {exc}") from exc

    workloads = []
    for pod in pods:
        status, error_message = _extract_pod_status_details(pod)
        image = ""
        if pod.spec and pod.spec.containers:
            image = pod.spec.containers[0].image
        workloads.append(
            {
                "id": pod.metadata.name,
                "short_id": (pod.metadata.uid or pod.metadata.name)[:12],
                "name": pod.metadata.name,
                "image": image,
                "status": status,
                "docker_status": pod.status.phase or "",
                "created": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else "",
                "labels": pod.metadata.labels or {},
                "managed_node_id": int((pod.metadata.labels or {}).get("kumquat.managed-node-id", "0") or 0) or None,
                "managed_node_name": (pod.metadata.labels or {}).get("kumquat.managed-node-name", ""),
                "dashboard_url": "",
                "dashboard_proxy_url": "",
                "last_error": error_message,
            }
        )
    return workloads


def map_container_status(status: str) -> str:
    normalized = (status or "").lower()
    if normalized in {"running"}:
        return ManagedNode.STATUS_RUNNING
    if normalized in {"succeeded", "completed", "exited"}:
        return ManagedNode.STATUS_EXITED
    if normalized in {"pending", "containercreating", "podinitializing", "created", "restarting"}:
        return ManagedNode.STATUS_PENDING
    if normalized in {"stopped", "paused"}:
        return ManagedNode.STATUS_STOPPED
    return ManagedNode.STATUS_FAILED
