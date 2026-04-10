#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BLOCKCHAIN_DIR="${REPO_ROOT}/blockchain"

AWS_REGION="${AWS_REGION:-us-west-2}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-}"
IMAGE_TAG="${IMAGE_TAG:-blockchain-$(date -u +%Y%m%d-%H%M%S)}"
PLATFORM="${PLATFORM:-linux/amd64}"
PUSH_IMAGE="${PUSH_IMAGE:-true}"

if [[ -z "${IMAGE_REPOSITORY}" ]]; then
  echo "IMAGE_REPOSITORY must be set to the full ECR repository URL." >&2
  exit 1
fi

ACCOUNT_REGISTRY="$(printf '%s\n' "${IMAGE_REPOSITORY}" | cut -d/ -f1)"
IMAGE_REF="${IMAGE_REPOSITORY}:${IMAGE_TAG}"

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ACCOUNT_REGISTRY}"

BUILD_ARGS=(
  buildx build
  --platform "${PLATFORM}"
  -t "${IMAGE_REF}"
)

if [[ "${PUSH_IMAGE}" == "true" ]]; then
  BUILD_ARGS+=(--push)
else
  BUILD_ARGS+=(--load)
fi

BUILD_ARGS+=("${BLOCKCHAIN_DIR}")

echo "Building ${IMAGE_REF}"
docker "${BUILD_ARGS[@]}"

echo "Image ready: ${IMAGE_REF}"
