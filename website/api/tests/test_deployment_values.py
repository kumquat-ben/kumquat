from pathlib import Path
from urllib.parse import urlparse

import yaml
from django.test import SimpleTestCase


REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_VALUES_PATH = (
    REPO_ROOT / "infra" / "aws-secure-platform" / "helm" / "apps" / "kumquat-backend" / "values.yaml"
)
BLOCKCHAIN_VALUES_PATH = (
    REPO_ROOT / "infra" / "aws-secure-platform" / "helm" / "apps" / "kumquat-blockchain" / "values.yaml"
)


class DeploymentValuesTests(SimpleTestCase):
    @staticmethod
    def _load_yaml(path: Path):
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def test_backend_launcher_seed_points_at_shared_blockchain_headless_service(self):
        backend_values = self._load_yaml(BACKEND_VALUES_PATH)
        common_env = backend_values["env"]["common"]

        self.assertEqual(
            common_env["NODE_LAUNCHER_GENESIS_SEED_HOST"],
            "kumquat-blockchain-headless.kumquat.svc.cluster.local",
        )
        self.assertEqual(common_env["NODE_LAUNCHER_GENESIS_SEED_PORT"], "30333")
        self.assertEqual(common_env["NODE_LAUNCHER_GENESIS_SEED_SERVICE_NAME"], "kumquat-blockchain-headless")

    def test_backend_explorer_and_launcher_both_target_same_blockchain_release(self):
        backend_values = self._load_yaml(BACKEND_VALUES_PATH)
        common_env = backend_values["env"]["common"]

        explorer_host = urlparse(common_env["EXPLORER_API_URL"]).hostname
        self.assertEqual(explorer_host, "kumquat-blockchain.kumquat.svc.cluster.local")
        self.assertEqual(
            common_env["NODE_LAUNCHER_GENESIS_SEED_HOST"],
            "kumquat-blockchain-headless.kumquat.svc.cluster.local",
        )

    def test_backend_launcher_chain_id_matches_blockchain_chart_chain_id(self):
        backend_values = self._load_yaml(BACKEND_VALUES_PATH)
        blockchain_values = self._load_yaml(BLOCKCHAIN_VALUES_PATH)
        common_env = backend_values["env"]["common"]

        self.assertEqual(
            int(common_env["NODE_LAUNCHER_CHAIN_ID"]),
            int(blockchain_values["config"]["chainId"]),
        )
        self.assertRegex(common_env["NODE_LAUNCHER_GENESIS_HASH"], r"^[0-9a-f]{64}$")
