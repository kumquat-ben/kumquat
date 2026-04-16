#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${KUMQUAT_TEST_ENV_FILE:-${REPO_ROOT}/.env.testnet}"
DRY_RUN=0

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  echo "Copy ${REPO_ROOT}/.env.testnet.example to ${ENV_FILE} and fill in the required values." >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Required env var is missing: ${name}" >&2
    exit 1
  fi
}

require_command helm
require_command kubectl
require_command mktemp
require_command base64

if [[ -n "${KUMQUAT_KUBECONFIG_PATH:-}" ]]; then
  if [[ ! -f "${KUMQUAT_KUBECONFIG_PATH}" ]]; then
    echo "Kubeconfig does not exist: ${KUMQUAT_KUBECONFIG_PATH}" >&2
    exit 1
  fi
  export KUBECONFIG="${KUMQUAT_KUBECONFIG_PATH}"
elif [[ -n "${KUMQUAT_KUBE_API_SERVER:-}" || -n "${KUMQUAT_KUBE_BEARER_TOKEN:-}" || -n "${KUMQUAT_KUBE_CA_CERT_B64:-}" ]]; then
  require_var KUMQUAT_KUBE_API_SERVER
  require_var KUMQUAT_KUBE_BEARER_TOKEN
  require_var KUMQUAT_KUBE_CA_CERT_B64

  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "${TMP_DIR}"' EXIT
  printf '%s' "${KUMQUAT_KUBE_CA_CERT_B64}" | base64 --decode > "${TMP_DIR}/ca.crt"
  cat > "${TMP_DIR}/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: kumquat-test
    cluster:
      certificate-authority: ${TMP_DIR}/ca.crt
      server: ${KUMQUAT_KUBE_API_SERVER}
contexts:
  - name: kumquat-test
    context:
      cluster: kumquat-test
      user: kumquat-test
current-context: kumquat-test
users:
  - name: kumquat-test
    user:
      token: ${KUMQUAT_KUBE_BEARER_TOKEN}
EOF
  export KUBECONFIG="${TMP_DIR}/kubeconfig"
else
  echo "No Kubernetes auth configured. Set KUMQUAT_KUBECONFIG_PATH or direct API auth vars." >&2
  exit 1
fi

require_var KUMQUAT_HELM_RELEASE
require_var KUMQUAT_NAMESPACE
require_var KUMQUAT_HELM_CHART
require_var KUMQUAT_BLOCKCHAIN_IMAGE_REPOSITORY
require_var KUMQUAT_BLOCKCHAIN_IMAGE_TAG
require_var KUMQUAT_NETWORK_NAME
require_var KUMQUAT_REPLICA_COUNT
require_var KUMQUAT_ENABLE_MINING
require_var KUMQUAT_MINING_THREADS
require_var KUMQUAT_TARGET_BLOCK_TIME
require_var KUMQUAT_INITIAL_DIFFICULTY
require_var KUMQUAT_DIFFICULTY_ADJUSTMENT_INTERVAL
require_var KUMQUAT_CHAIN_ID
require_var KUMQUAT_HYBRID_ACTIVATION_HEIGHT
require_var KUMQUAT_P2P_PORT
require_var KUMQUAT_RPC_PORT
require_var KUMQUAT_METRICS_PORT
require_var KUMQUAT_GENESIS_TIMESTAMP
require_var KUMQUAT_GENESIS_ACCOUNT_1_ADDRESS
require_var KUMQUAT_GENESIS_ACCOUNT_1_BALANCE
require_var KUMQUAT_GENESIS_ACCOUNT_2_ADDRESS
require_var KUMQUAT_GENESIS_ACCOUNT_2_BALANCE

VALUES_FILE="$(mktemp)"
trap 'rm -f "${VALUES_FILE}"' EXIT

cat > "${VALUES_FILE}" <<EOF
replicaCount: ${KUMQUAT_REPLICA_COUNT}

image:
  repository: ${KUMQUAT_BLOCKCHAIN_IMAGE_REPOSITORY}
  tag: ${KUMQUAT_BLOCKCHAIN_IMAGE_TAG}
  pullPolicy: IfNotPresent
  pullSecrets:
    - ${KUMQUAT_IMAGE_PULL_SECRET:-ecr-pull-secret}

networkName: ${KUMQUAT_NETWORK_NAME}

ports:
  p2p: ${KUMQUAT_P2P_PORT}
  rpc: ${KUMQUAT_RPC_PORT}
  metrics: ${KUMQUAT_METRICS_PORT}

config:
  chainId: ${KUMQUAT_CHAIN_ID}
  enableMining: ${KUMQUAT_ENABLE_MINING}
  miningThreads: ${KUMQUAT_MINING_THREADS}
  targetBlockTime: ${KUMQUAT_TARGET_BLOCK_TIME}
  initialDifficulty: ${KUMQUAT_INITIAL_DIFFICULTY}
  difficultyAdjustmentInterval: ${KUMQUAT_DIFFICULTY_ADJUSTMENT_INTERVAL}
  hybridActivationHeight: ${KUMQUAT_HYBRID_ACTIVATION_HEIGHT}
  bootstrapNodes:
EOF

IFS=',' read -r -a BOOTSTRAP_ARRAY <<< "${KUMQUAT_BOOTSTRAP_NODES:-}"
for bootstrap in "${BOOTSTRAP_ARRAY[@]}"; do
  trimmed="$(echo "${bootstrap}" | xargs)"
  if [[ -n "${trimmed}" ]]; then
    echo "    - ${trimmed}" >> "${VALUES_FILE}"
  fi
done

cat >> "${VALUES_FILE}" <<EOF

genesis:
  chainId: ${KUMQUAT_CHAIN_ID}
  timestamp: ${KUMQUAT_GENESIS_TIMESTAMP}
  initialDifficulty: ${KUMQUAT_INITIAL_DIFFICULTY}
  initialAccounts:
    - address: "${KUMQUAT_GENESIS_ACCOUNT_1_ADDRESS}"
      balance: ${KUMQUAT_GENESIS_ACCOUNT_1_BALANCE}
      accountType: User
    - address: "${KUMQUAT_GENESIS_ACCOUNT_2_ADDRESS}"
      balance: ${KUMQUAT_GENESIS_ACCOUNT_2_BALANCE}
      accountType: User
EOF

if [[ -n "${KUMQUAT_GENESIS_ACCOUNT_3_ADDRESS:-}" && -n "${KUMQUAT_GENESIS_ACCOUNT_3_BALANCE:-}" ]]; then
  cat >> "${VALUES_FILE}" <<EOF
    - address: "${KUMQUAT_GENESIS_ACCOUNT_3_ADDRESS}"
      balance: ${KUMQUAT_GENESIS_ACCOUNT_3_BALANCE}
      accountType: User
EOF
fi

if [[ "${DRY_RUN}" -eq 1 || "${KUMQUAT_DEPLOY_DRY_RUN:-false}" == "true" ]]; then
  helm template \
    "${KUMQUAT_HELM_RELEASE}" \
    "${REPO_ROOT}/${KUMQUAT_HELM_CHART}" \
    --namespace "${KUMQUAT_NAMESPACE}" \
    -f "${VALUES_FILE}"
  exit 0
fi

kubectl cluster-info >/dev/null
kubectl get nodes
kubectl get namespace "${KUMQUAT_NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${KUMQUAT_NAMESPACE}"

helm upgrade --install \
  "${KUMQUAT_HELM_RELEASE}" \
  "${REPO_ROOT}/${KUMQUAT_HELM_CHART}" \
  --namespace "${KUMQUAT_NAMESPACE}" \
  --create-namespace \
  -f "${VALUES_FILE}"

kubectl rollout status "statefulset/${KUMQUAT_HELM_RELEASE}" -n "${KUMQUAT_NAMESPACE}" --timeout=180s
kubectl get pods -n "${KUMQUAT_NAMESPACE}"
kubectl get svc -n "${KUMQUAT_NAMESPACE}"
