#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WIKI_SRC_DIR="$ROOT_DIR/wiki"
TMP_DIR="$(mktemp -d)"
REMOTE_URL="https://x-access-token:${GITHUB_TOKEN:?GITHUB_TOKEN is required}@github.com/kumquatben/kumquat.wiki.git"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

git clone "$REMOTE_URL" "$TMP_DIR/wiki" >/dev/null 2>&1 || {
  echo "Failed to clone the wiki repository."
  echo "If the wiki has not been initialized yet, open https://github.com/kumquatben/kumquat/wiki and create the first page once."
  exit 1
}

find "$TMP_DIR/wiki" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
cp -R "$WIKI_SRC_DIR"/. "$TMP_DIR/wiki"/

cd "$TMP_DIR/wiki"

if git diff --quiet --exit-code && git diff --cached --quiet --exit-code; then
  echo "No wiki changes to publish."
  exit 0
fi

git add .
git -c user.name="Codex" -c user.email="codex@users.noreply.github.com" commit -m "Update wiki"
git push origin master

echo "Wiki published."
