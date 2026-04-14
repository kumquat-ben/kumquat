from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase, override_settings

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
                NODE_LAUNCHER_GENESIS_HASH="49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                self.assertEqual(render_genesis(self._node()), genesis_text)

    def test_render_config_pins_shared_genesis_hash(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                config_text = render_config(self._node())

            self.assertIn('genesis_hash = "49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f"', config_text)
            self.assertIn("chain_id = 1337", config_text)

    def test_render_config_rejects_chain_id_mismatch(self):
        with TemporaryDirectory() as tmpdir:
            genesis_path = Path(tmpdir) / "genesis.toml"
            genesis_path.write_text("chain_id = 1337\n", encoding="utf-8")

            with override_settings(
                NODE_LAUNCHER_GENESIS_FILE=str(genesis_path),
                NODE_LAUNCHER_GENESIS_HASH="49be8808fea37733de5e619af4fa5745141c8edd63dc8ddf37deebf907d7c22f",
                NODE_LAUNCHER_CHAIN_ID=1337,
            ):
                with self.assertRaisesMessage(NodeLauncherError, "launcher is pinned to shared chain_id=1337"):
                    render_config(self._node(chain_id=2))
