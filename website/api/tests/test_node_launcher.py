from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase, TestCase, override_settings

from api.models import ManagedNode
from api.node_launcher import NodeLauncherError, render_config, render_genesis


class NodeLauncherGenesisTests(SimpleTestCase):
    def _node(self, **overrides):
        values = {
            "name": "node-1",
            "display_name": "Node 1",
            "image": "example.com/kumquat:blockchain",
            "network_name": "dev",
            "chain_id": 1337,
            "reward_address": "",
            "enable_mining": True,
            "mining_threads": 1,
            "api_port": 18545,
            "p2p_port": 30380,
            "metrics_port": 19100,
        }
        values.update(overrides)
        return ManagedNode(**values)

    def test_render_genesis_uses_shared_genesis_file_contents(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_text = "chain_id = 1337\ntimestamp = 1744067299\ninitial_difficulty = 100\n"
            genesis_path.write_text(genesis_text, encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                self.assertEqual(render_genesis(self._node()), genesis_text)

    def test_render_config_pins_shared_genesis_hash(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(self._node())

            self.assertIn('genesis_hash = "1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95"', config_text)
            self.assertIn("chain_id = 1337", config_text)

    def test_render_config_rejects_chain_id_mismatch(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                with self.assertRaisesMessage(NodeLauncherError, "launcher is pinned to shared chain_id=1337"):
                    render_config(self._node(chain_id=2))


class NodeLauncherBootstrapTests(TestCase):
    def _make_node(self, **overrides):
        payload = {
            "name": "node-1",
            "display_name": "Node 1",
            "image": "example.com/kumquat:blockchain",
            "network_name": "dev",
            "chain_id": 1337,
            "reward_address": "",
            "enable_mining": False,
            "mining_threads": 1,
            "api_port": 18545,
            "p2p_port": 30380,
            "metrics_port": 19100,
        }
        payload.update(overrides)
        return ManagedNode.objects.create(**payload)

    def test_render_config_bootstraps_non_genesis_nodes_to_genesis_service(self):
        self._make_node(
            name="genesis",
            display_name="Genesis",
            enable_mining=True,
        )
        follower = self._make_node(
            name="node-2",
            display_name="Node 2",
            api_port=18546,
            p2p_port=30381,
            metrics_port=19101,
        )

        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(follower)

        self.assertIn(
            'bootstrap_nodes = [\n  "genesis-peer.kumquat.svc.cluster.local:30380"\n]',
            config_text,
        )

    def test_render_config_uses_configured_genesis_seed_host(self):
        self._make_node(
            name="genesis",
            display_name="Genesis",
            enable_mining=True,
        )
        follower = self._make_node(
            name="node-2",
            display_name="Node 2",
            api_port=18546,
            p2p_port=30381,
            metrics_port=19101,
        )

        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
                NODE_LAUNCHER_GENESIS_SEED_HOST="genesis.node.kumquat.info",
                NODE_LAUNCHER_GENESIS_SEED_PORT=30333,
            ):
                config_text = render_config(follower)

        self.assertIn(
            'bootstrap_nodes = [\n  "genesis.node.kumquat.info:30333"\n]',
            config_text,
        )

    def test_render_config_falls_back_to_cluster_seed_service(self):
        follower = self._make_node(name="node-2", display_name="Node 2")

        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
                NODE_LAUNCHER_GENESIS_SEED_SERVICE_NAME="kumquat-blockchain-headless",
                NODE_LAUNCHER_GENESIS_SEED_PORT=30333,
            ):
                config_text = render_config(follower)

        self.assertIn(
            'bootstrap_nodes = [\n  "kumquat-blockchain-headless.kumquat.svc.cluster.local:30333"\n]',
            config_text,
        )

    def test_render_config_keeps_genesis_node_seedless(self):
        genesis = self._make_node(
            name="genesis",
            display_name="Genesis",
            enable_mining=True,
        )

        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(genesis)

        self.assertIn("bootstrap_nodes = [\n\n]", config_text)
