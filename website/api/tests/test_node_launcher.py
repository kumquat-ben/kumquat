import json
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase, TestCase, override_settings

from api.models import ManagedNode
from api.node_launcher import NodeLauncherError, render_config, render_genesis


class NodeLauncherGenesisTests(SimpleTestCase):
    @staticmethod
    def _write_ceremony(tmpdir: str, *, chain_id: int = 1337, genesis_hash: str = "49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f"):
        genesis_path = Path(tmpdir) / "genesis.toml"
        ceremony_path = Path(tmpdir) / "genesis.ceremony.json"
        genesis_text = "chain_id = 1337\ntimestamp = 1744067299\ninitial_difficulty = 100\n"
        genesis_path.write_text(genesis_text, encoding="utf-8")
        ceremony_path.write_text(
            json.dumps(
                {
                    "chain_id": chain_id,
                    "genesis_config_path": "genesis.toml",
                    "genesis_hash": genesis_hash,
                    "chain_identity": f"chain-{chain_id}:{genesis_hash}",
                    "state_root": "f" * 64,
                    "timestamp": 1744067299,
                    "initial_difficulty": 100,
                    "accounts": [],
                }
            ),
            encoding="utf-8",
        )
        return genesis_path, ceremony_path, genesis_text

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
            _, ceremony_path, genesis_text = self._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                self.assertEqual(render_genesis(self._node()), genesis_text)

    def test_render_config_pins_shared_genesis_hash_from_ceremony(self):
        with TemporaryDirectory() as tmpdir:
            _, ceremony_path, _ = self._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(self._node())

            self.assertIn(
                'genesis_hash = "49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f"',
                config_text,
            )
            self.assertIn("chain_id = 1337", config_text)

    def test_render_config_rejects_chain_id_mismatch_against_ceremony(self):
        with TemporaryDirectory() as tmpdir:
            _, ceremony_path, _ = self._write_ceremony(tmpdir, chain_id=1337)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
                NODE_LAUNCHER_CHAIN_ID=2,
            ):
                with self.assertRaisesMessage(
                    NodeLauncherError, "does not match ceremony chain_id=1337"
                ):
                    render_config(self._node())

    def test_render_config_supports_legacy_file_and_hash_fallback(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE="",
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(self._node())

            self.assertIn(
                'genesis_hash = "1b10e3582554ec4b197368743568f977db91110fd642c7f5c59ed17f83c9ca95"',
                config_text,
            )


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
            _, ceremony_path, _ = NodeLauncherGenesisTests._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
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
            _, ceremony_path, _ = NodeLauncherGenesisTests._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
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
            _, ceremony_path, _ = NodeLauncherGenesisTests._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
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
            _, ceremony_path, _ = NodeLauncherGenesisTests._write_ceremony(tmpdir)

            with override_settings(
                NODE_LAUNCHER_GENESIS_CEREMONY_FILE=str(ceremony_path),
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(genesis)

        self.assertIn("bootstrap_nodes = [\n\n]", config_text)
