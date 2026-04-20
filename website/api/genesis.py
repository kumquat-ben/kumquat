import json
from dataclasses import dataclass
from pathlib import Path

from django.conf import settings


class GenesisCeremonyError(Exception):
    pass


@dataclass(frozen=True)
class SharedGenesisMaterial:
    ceremony_path: Path
    genesis_config_path: Path
    genesis_hash: str
    chain_id: int
    chain_identity: str
    genesis_contents: str


def _configured_ceremony_file() -> Path | None:
    ceremony_path = (getattr(settings, "NODE_LAUNCHER_GENESIS_CEREMONY_FILE", "") or "").strip()
    if not ceremony_path:
        return None

    ceremony_file = Path(ceremony_path)
    if not ceremony_file.exists():
        raise GenesisCeremonyError(f"Genesis ceremony file does not exist: {ceremony_file}")
    if not ceremony_file.is_file():
        raise GenesisCeremonyError(f"Genesis ceremony path is not a file: {ceremony_file}")
    return ceremony_file


def _load_shared_genesis_from_ceremony(ceremony_file: Path) -> SharedGenesisMaterial:
    try:
        ceremony = json.loads(ceremony_file.read_text(encoding="utf-8"))
    except OSError as exc:
        raise GenesisCeremonyError(f"Failed to read genesis ceremony file: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise GenesisCeremonyError(f"Failed to parse genesis ceremony file: {exc}") from exc

    for field in ("chain_id", "genesis_hash", "chain_identity", "genesis_config_path"):
        if field not in ceremony:
            raise GenesisCeremonyError(
                f"Genesis ceremony file is missing required field '{field}': {ceremony_file}"
            )

    genesis_config_path = ceremony_file.parent / str(ceremony["genesis_config_path"])
    if not genesis_config_path.exists():
        raise GenesisCeremonyError(
            f"Genesis file referenced by ceremony does not exist: {genesis_config_path}"
        )
    if not genesis_config_path.is_file():
        raise GenesisCeremonyError(
            f"Genesis path referenced by ceremony is not a file: {genesis_config_path}"
        )

    try:
        genesis_contents = genesis_config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise GenesisCeremonyError(f"Failed to read genesis file from ceremony: {exc}") from exc

    return SharedGenesisMaterial(
        ceremony_path=ceremony_file,
        genesis_config_path=genesis_config_path,
        genesis_hash=str(ceremony["genesis_hash"]).strip().lower(),
        chain_id=int(ceremony["chain_id"]),
        chain_identity=str(ceremony["chain_identity"]).strip(),
        genesis_contents=genesis_contents,
    )


def _load_shared_genesis_legacy() -> SharedGenesisMaterial:
    genesis_path = (getattr(settings, "NODE_LAUNCHER_GENESIS_FILE", "") or "").strip()
    if not genesis_path:
        raise GenesisCeremonyError(
            "Neither NODE_LAUNCHER_GENESIS_CEREMONY_FILE nor NODE_LAUNCHER_GENESIS_FILE is configured."
        )

    genesis_file = Path(genesis_path)
    if not genesis_file.exists():
        raise GenesisCeremonyError(f"Shared genesis file does not exist: {genesis_file}")
    if not genesis_file.is_file():
        raise GenesisCeremonyError(f"Shared genesis path is not a file: {genesis_file}")

    genesis_hash = (getattr(settings, "NODE_LAUNCHER_GENESIS_HASH", "") or "").strip().lower()
    if not genesis_hash:
        raise GenesisCeremonyError(
            "NODE_LAUNCHER_GENESIS_HASH is not configured for legacy genesis loading."
        )

    try:
        genesis_contents = genesis_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise GenesisCeremonyError(f"Failed to read shared genesis file: {exc}") from exc

    chain_id = int(getattr(settings, "NODE_LAUNCHER_CHAIN_ID", 1337))
    return SharedGenesisMaterial(
        ceremony_path=genesis_file,
        genesis_config_path=genesis_file,
        genesis_hash=genesis_hash,
        chain_id=chain_id,
        chain_identity=f"chain-{chain_id}:{genesis_hash}",
        genesis_contents=genesis_contents,
    )


def load_shared_genesis_material() -> SharedGenesisMaterial:
    ceremony_file = _configured_ceremony_file()
    material = (
        _load_shared_genesis_from_ceremony(ceremony_file)
        if ceremony_file is not None
        else _load_shared_genesis_legacy()
    )

    configured_chain_id = int(getattr(settings, "NODE_LAUNCHER_CHAIN_ID", material.chain_id))
    if configured_chain_id != material.chain_id:
        raise GenesisCeremonyError(
            f"NODE_LAUNCHER_CHAIN_ID={configured_chain_id} does not match ceremony chain_id={material.chain_id}."
        )

    return material
