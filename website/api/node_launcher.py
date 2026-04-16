# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import base64
import hashlib
from pathlib import Path
from tempfile import gettempdir
import time
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


def launcher_enabled():
    return getattr(settings, "NODE_LAUNCHER_ENABLED", False)


def _auth_value(auth_context, key: str, default: str = "") -> str:
    if auth_context and auth_context.get(key) is not None:
        return (auth_context.get(key) or "").strip()
    return (getattr(settings, key, default) or default).strip()


def _auth_namespace(auth_context) -> str:
    if auth_context and auth_context.get("namespace") is not None:
        return (auth_context.get("namespace") or "").strip() or _launcher_namespace()
    return _launcher_namespace()


def _materialize_ca_cert(ca_cert_b64: str) -> str:
    digest = hashlib.sha256(ca_cert_b64.encode("utf-8")).hexdigest()
    path = Path(gettempdir()) / f"kumquat-kube-ca-{digest}.crt"
    if not path.exists():
        path.write_bytes(base64.b64decode(ca_cert_b64))
    return str(path)


def _api_client(auth_context=None):
    kubeconfig_path = _auth_value(auth_context, "NODE_LAUNCHER_KUBECONFIG")
    api_server = _auth_value(auth_context, "NODE_LAUNCHER_KUBE_API_SERVER")
    bearer_token = _auth_value(auth_context, "NODE_LAUNCHER_KUBE_BEARER_TOKEN")
    ca_cert_b64 = _auth_value(auth_context, "NODE_LAUNCHER_KUBE_CA_CERT_B64")

    try:
        if kubeconfig_path:
            return k8s_config.new_client_from_config(config_file=kubeconfig_path)
        if api_server and bearer_token:
            configuration = k8s_client.Configuration()
            configuration.host = api_server
            configuration.api_key = {"authorization": bearer_token}
            configuration.api_key_prefix = {"authorization": "Bearer"}
            if ca_cert_b64:
                configuration.ssl_ca_cert = _materialize_ca_cert(ca_cert_b64)
                configuration.verify_ssl = True
            else:
                configuration.verify_ssl = False
            return k8s_client.ApiClient(configuration)
        try:
            k8s_config.load_incluster_config()
            return k8s_client.ApiClient()
        except ConfigException:
            return k8s_config.new_client_from_config()
    except Exception as exc:
        raise NodeLauncherError(f"Kubernetes client is unavailable: {exc}") from exc


def _core_api(auth_context=None):
    if not launcher_enabled():
        raise NodeLauncherError("Node launcher is disabled.")
    return k8s_client.CoreV1Api(_api_client(auth_context))


def _apps_api(auth_context=None):
    if not launcher_enabled():
        raise NodeLauncherError("Node launcher is disabled.")
    return k8s_client.AppsV1Api(_api_client(auth_context))


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


def peer_service_host(node: ManagedNode, auth_context=None) -> str:
    return f"{peer_service_name(node)}.{_auth_namespace(auth_context)}.svc.cluster.local"


def _service_host(service_name: str, namespace: str) -> str:
    return f"{service_name}.{namespace}.svc.cluster.local"


def rpc_service_name(node: ManagedNode) -> str:
    return _suffix_name(node.name, "rpc")


def pod_name(node: ManagedNode) -> str:
    return f"{workload_name(node)}-0"


def pvc_name(node: ManagedNode) -> str:
    return f"data-{workload_name(node)}-0"


def rpc_service_host(node: ManagedNode, auth_context=None) -> str:
    return f"{rpc_service_name(node)}.{_auth_namespace(auth_context)}.svc.cluster.local"


def upstream_rpc_url(node: ManagedNode, path: str, auth_context=None) -> str:
    return f"http://{rpc_service_host(node, auth_context)}:{node.api_port}{path}"


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


def _bootstrap_nodes(node: ManagedNode, auth_context=None) -> list[str]:
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
        return [f"{peer_service_host(seed, auth_context)}:{seed.p2p_port}"]

    service_name = (
        getattr(settings, "NODE_LAUNCHER_GENESIS_SEED_SERVICE_NAME", "") or "kumquat-blockchain-headless"
    ).strip()
    return [f"{_service_host(service_name, _auth_namespace(auth_context))}:{configured_seed_port}"]


def render_config(node: ManagedNode, auth_context=None) -> str:
    data_dir = "/data/kumquat/data"
    node_id_line = f'node_id = "{node.reward_address}"\n' if node.reward_address else ""
    shared_chain_id = int(getattr(settings, "NODE_LAUNCHER_CHAIN_ID", 1337))
    if node.chain_id != shared_chain_id:
        raise NodeLauncherError(
            f"Managed node {node.name} requested chain_id={node.chain_id}, "
            f"but the launcher is pinned to shared chain_id={shared_chain_id}."
        )
    genesis_hash = _shared_genesis_hash()
    bootstrap_nodes = _bootstrap_nodes(node, auth_context)
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
hybrid_activation_height = {int(getattr(settings, "NODE_LAUNCHER_HYBRID_ACTIVATION_HEIGHT", 0))}
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


def _read_pod(node: ManagedNode, auth_context=None):
    try:
        return _core_api(auth_context).read_namespaced_pod(
            name=pod_name(node),
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise NodeLauncherError(f"Failed to inspect pod for {node.name}: {exc}") from exc


def _read_statefulset(node: ManagedNode, auth_context=None):
    try:
        return _apps_api(auth_context).read_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise NodeLauncherError(f"Failed to inspect workload for {node.name}: {exc}") from exc


def _build_configmap(node: ManagedNode, auth_context=None):
    return k8s_client.V1ConfigMap(
        metadata=k8s_client.V1ObjectMeta(
            name=configmap_name(node),
            namespace=_launcher_namespace(),
            labels=_managed_labels(node),
        ),
        data={
            "config.toml": render_config(node, auth_context),
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


def _create_or_replace_configmap(node: ManagedNode, auth_context=None):
    namespace = _auth_namespace(auth_context)
    core = _core_api(auth_context)
    configmap = _build_configmap(node, auth_context)
    configmap.metadata.namespace = namespace
    try:
        core.create_namespaced_config_map(namespace=namespace, body=configmap)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(f"Failed to create config for {node.name}: {exc}") from exc
        core.replace_namespaced_config_map(
            name=configmap_name(node),
            namespace=namespace,
            body=configmap,
        )


def _create_service_if_missing(node: ManagedNode, service_body, auth_context=None):
    namespace = _auth_namespace(auth_context)
    core = _core_api(auth_context)
    service_body.metadata.namespace = namespace
    try:
        core.create_namespaced_service(namespace=namespace, body=service_body)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(
                f"Failed to create service {service_body.metadata.name} for {node.name}: {exc}"
            ) from exc
        core.patch_namespaced_service(
            name=service_body.metadata.name,
            namespace=namespace,
            body={"spec": {"ports": [{"name": port.name, "port": port.port, "targetPort": port.target_port} for port in service_body.spec.ports]}},
        )


def _delete_named_service(name: str, auth_context=None):
    try:
        _core_api(auth_context).delete_namespaced_service(
            name=name,
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete service {name}: {exc}") from exc


def _delete_named_configmap(name: str, auth_context=None):
    try:
        _core_api(auth_context).delete_namespaced_config_map(
            name=name,
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete configmap {name}: {exc}") from exc


def _delete_pod(name: str, auth_context=None):
    try:
        _core_api(auth_context).delete_namespaced_pod(
            name=name,
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete pod {name}: {exc}") from exc


def _wait_for_statefulset_absence(name: str, auth_context=None, timeout_seconds: int = 60):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            _apps_api(auth_context).read_namespaced_stateful_set(
                name=name,
                namespace=_auth_namespace(auth_context),
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise NodeLauncherError(f"Failed to confirm workload deletion for {name}: {exc}") from exc
        time.sleep(1)
    raise NodeLauncherError(f"Timed out waiting for workload {name} to be deleted.")


def _list_managed_pvcs(node: ManagedNode, auth_context=None):
    try:
        pvc_list = _core_api(auth_context).list_namespaced_persistent_volume_claim(
            namespace=_auth_namespace(auth_context),
            label_selector=_managed_label_selector(node),
        )
    except ApiException as exc:
        raise NodeLauncherError(f"Failed to list PVCs for {node.name}: {exc}") from exc

    names = {item.metadata.name for item in pvc_list.items if item.metadata and item.metadata.name}
    names.add(pvc_name(node))
    return sorted(names)


def _delete_named_pvc(name: str, auth_context=None):
    try:
        _core_api(auth_context).delete_namespaced_persistent_volume_claim(
            name=name,
            namespace=_auth_namespace(auth_context),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete PVC {name}: {exc}") from exc


def _wait_for_pvc_absence(name: str, auth_context=None, timeout_seconds: int = 60):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            _core_api(auth_context).read_namespaced_persistent_volume_claim(
                name=name,
                namespace=_auth_namespace(auth_context),
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise NodeLauncherError(f"Failed to confirm PVC deletion for {name}: {exc}") from exc
        time.sleep(1)
    raise NodeLauncherError(f"Timed out waiting for PVC {name} to be deleted.")


def _ensure_workload(node: ManagedNode, auth_context=None) -> str:
    image = _resolve_node_image(node)
    namespace = _auth_namespace(auth_context)
    _create_or_replace_configmap(node, auth_context)
    _create_service_if_missing(
        node,
        _build_service(node, peer_service_name(node), "p2p", node.p2p_port, headless=True),
        auth_context,
    )
    _create_service_if_missing(
        node,
        _build_service(node, rpc_service_name(node), "rpc", node.api_port),
        auth_context,
    )

    apps = _apps_api(auth_context)
    body = _build_statefulset(node, image)
    body.metadata.namespace = namespace
    try:
        apps.create_namespaced_stateful_set(namespace=namespace, body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise NodeLauncherError(f"Failed to create workload for {node.name}: {exc}") from exc

        apps.patch_namespaced_stateful_set(
            name=workload_name(node),
            namespace=namespace,
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


def launch_node(node: ManagedNode, auth_context=None) -> ManagedNode:
    image = _ensure_workload(node, auth_context)
    node.image = image
    node.status = ManagedNode.STATUS_PENDING
    node.last_error = ""
    node.stopped_at = None
    node.last_status_at = timezone.now()
    node.save(update_fields=["image", "status", "last_error", "stopped_at", "last_status_at", "updated_at"])
    return refresh_node(node, auth_context)


def stop_node(node: ManagedNode, auth_context=None) -> ManagedNode:
    statefulset = _read_statefulset(node, auth_context)
    if statefulset is None:
        node.status = ManagedNode.STATUS_STOPPED
        node.stopped_at = timezone.now()
        node.last_status_at = timezone.now()
        node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
        return node

    _apps_api(auth_context).patch_namespaced_stateful_set(
        name=workload_name(node),
        namespace=_auth_namespace(auth_context),
        body={"spec": {"replicas": 0}},
    )
    node.status = ManagedNode.STATUS_STOPPED
    node.stopped_at = timezone.now()
    node.last_status_at = timezone.now()
    node.save(update_fields=["status", "stopped_at", "last_status_at", "updated_at"])
    return refresh_node(node, auth_context)


def restart_node(node: ManagedNode, auth_context=None) -> ManagedNode:
    statefulset = _read_statefulset(node, auth_context)
    if statefulset is None:
        return launch_node(node, auth_context)

    replicas = getattr(statefulset.spec, "replicas", 0) or 0
    if replicas == 0:
        _apps_api(auth_context).patch_namespaced_stateful_set(
            name=workload_name(node),
            namespace=_auth_namespace(auth_context),
            body={"spec": {"replicas": 1}},
        )
    else:
        _delete_pod(pod_name(node), auth_context)

    node.stopped_at = None
    node.last_error = ""
    node.last_status_at = timezone.now()
    node.save(update_fields=["stopped_at", "last_error", "last_status_at", "updated_at"])
    return refresh_node(node, auth_context)


def delete_container(node: ManagedNode, auth_context=None) -> ManagedNode:
    stop_node(node, auth_context)
    node.container_name = ""
    node.container_id = ""
    node.save(update_fields=["container_name", "container_id", "updated_at"])
    return node


def delete_deployment(node: ManagedNode, auth_context=None):
    workload = workload_name(node)

    try:
        _apps_api(auth_context).patch_namespaced_stateful_set(
            name=workload,
            namespace=_auth_namespace(auth_context),
            body={"spec": {"replicas": 0}},
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to scale down workload for {node.name}: {exc}") from exc

    _delete_pod(pod_name(node), auth_context)

    try:
        _apps_api(auth_context).delete_namespaced_stateful_set(
            name=workload,
            namespace=_auth_namespace(auth_context),
            propagation_policy="Foreground",
        )
    except ApiException as exc:
        if exc.status != 404:
            raise NodeLauncherError(f"Failed to delete workload for {node.name}: {exc}") from exc

    _wait_for_statefulset_absence(workload, auth_context)

    _delete_named_service(rpc_service_name(node), auth_context)
    _delete_named_service(peer_service_name(node), auth_context)
    _delete_named_configmap(configmap_name(node), auth_context)

    for pvc in _list_managed_pvcs(node, auth_context):
        _delete_named_pvc(pvc, auth_context)
    for pvc in _list_managed_pvcs(node, auth_context):
        _wait_for_pvc_absence(pvc, auth_context)

    node.delete()


def restart_runtime_container(container_id: str, auth_context=None):
    _delete_pod(container_id, auth_context)
    return True


def delete_runtime_container(container_id: str, auth_context=None):
    _delete_pod(container_id, auth_context)
    return True


def tail_logs(node: ManagedNode, lines: int = 120, auth_context=None) -> str:
    pod = _read_pod(node, auth_context)
    if pod is None:
        return node.last_logs or ""

    try:
        output = _core_api(auth_context).read_namespaced_pod_log(
            name=pod.metadata.name,
            namespace=_auth_namespace(auth_context),
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


def refresh_node(node: ManagedNode, auth_context=None) -> ManagedNode:
    statefulset = _read_statefulset(node, auth_context)
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

    pod = _read_pod(node, auth_context)
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
            _core_api(auth_context).read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=_auth_namespace(auth_context),
                tail_lines=120,
            )
            or ""
        )[-12000:]
    except ApiException:
        pass

    node.save()
    return node


def list_runtime_containers(auth_context=None):
    try:
        pods = _core_api(auth_context).list_namespaced_pod(
            namespace=_auth_namespace(auth_context),
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
