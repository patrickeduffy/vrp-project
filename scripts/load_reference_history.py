from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Mapping

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.reference_history.artifacts import (  # noqa: E402
    ContentAddressedArtifactStore,
    prepare_history,
)
from vrp.reference_history.canonical import sha256_file  # noqa: E402
from vrp.reference_history.service import execute_reference_history_load  # noqa: E402
from vrp.reference_history.sources import (  # noqa: E402
    normalize_sofr_csv,
    normalize_spy_daily_files,
)
from vrp.storage.postgres import connect_from_environment  # noqa: E402
from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    assert_no_unresolved_eod_finalizations,
    completed_eod_finalization_evidence,
    require_canonical_eod_runtime_config,
    resolve_eod_audit_root,
)
from vrp.orchestration.eod_lock import exclusive_eod_writer_lock  # noqa: E402
from vrp.storage.finalization_coordination import (  # noqa: E402
    delegated_finalization_child_lease,
    has_delegated_finalization_lease,
    standalone_finalization_lease,
)
from vrp.storage.finalization_continuity import (  # noqa: E402
    verify_database_finalization_continuity,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and load immutable SOFR and SPY daily reference history into PostgreSQL."
        )
    )
    parser.add_argument(
        "dataset",
        choices=("sofr", "spy-daily", "all"),
        help="Reference history to validate or load.",
    )
    parser.add_argument("--project-root", type=Path, default=REPOSITORY_ROOT)
    parser.add_argument("--runtime-config", type=Path, default=None)
    parser.add_argument(
        "--sofr-source",
        type=Path,
        default=None,
        help="Optional exact SOFR CSV snapshot instead of the mutable canonical cache.",
    )
    parser.add_argument(
        "--expected-sofr-source-sha256",
        default=None,
        help="Required raw-file SHA-256 assertion when --sofr-source is supplied.",
    )
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--environment", default="local")
    parser.add_argument("--code-version", default=None)
    parser.add_argument("--generation", type=int, default=0)
    parser.add_argument("--requested-by", default=None)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Reconcile source formulas without writing files or connecting to PostgreSQL.",
    )
    return parser.parse_args(argv)


def _absolute(path: Path, project_root: Path) -> Path:
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def _runtime_sources(
    project_root: Path,
    runtime_config: Path | None,
    sofr_source: Path | None = None,
) -> dict[str, Path]:
    config_path = _absolute(
        runtime_config or Path("config/vrp_hybrid_v2_eod_runtime_config.json"),
        project_root,
    )
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    canonical = payload.get("canonical")
    if not isinstance(canonical, dict):
        raise ValueError("runtime config must contain a canonical path object")
    required = ("sofr_cache", "spy_eod", "wilder_rsi", "rv21d")
    missing = [name for name in required if not canonical.get(name)]
    if missing:
        raise ValueError(f"runtime config is missing canonical paths: {missing}")
    paths = {
        name: _absolute(Path(str(canonical[name])), project_root) for name in required
    }
    if sofr_source is not None:
        paths["sofr_cache"] = _absolute(sofr_source.expanduser(), project_root)
    return paths


def _dataset_sources(name: str, paths: Mapping[str, Path]) -> dict[str, Path]:
    if name == "sofr":
        return {"sofr": paths["sofr_cache"]}
    if name == "spy-daily":
        return {
            "spy_eod": paths["spy_eod"],
            "wilder_rsi": paths["wilder_rsi"],
            "rv21d": paths["rv21d"],
        }
    raise ValueError(f"unsupported reference-history dataset: {name}")


def _normalize(name: str, paths: Mapping[str, Path]):
    if name == "sofr":
        return normalize_sofr_csv(paths["sofr"])
    return normalize_spy_daily_files(
        paths["spy_eod"],
        paths["wilder_rsi"],
        paths["rv21d"],
    )


def _stable_validate(name: str, paths: Mapping[str, Path]):
    before = {key: sha256_file(path) for key, path in paths.items()}
    history = _normalize(name, paths)
    after = {key: sha256_file(path) for key, path in paths.items()}
    if before != after:
        raise RuntimeError("a reference-history source changed during validation")
    return history


def _code_version(project_root: Path, explicit: str | None) -> str:
    candidate = explicit or os.environ.get("VRP_CODE_VERSION")
    if candidate and candidate.strip():
        return candidate.strip()
    try:
        result = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(
            "code version is required; pass --code-version or set VRP_CODE_VERSION"
        ) from exc
    value = result.stdout.strip()
    if not value:
        raise ValueError("Git did not return a code version")
    return value


def _history_summary(history) -> dict[str, object]:
    return {
        "content_sha256": history.content_sha256,
        "dataset_key": history.dataset_key,
        "definition_content_sha256": (
            None if history.definition is None else history.definition.content_sha256
        ),
        "end_date": history.end_date.isoformat(),
        "row_count": len(history.rows),
        "start_date": history.start_date.isoformat(),
        "status": "VALID",
    }


def run(args: argparse.Namespace) -> list[dict[str, object]]:
    if args.generation < 0:
        raise ValueError("generation must be a non-negative integer")
    project_root = args.project_root.expanduser().resolve()
    runtime_paths = _runtime_sources(
        project_root,
        args.runtime_config,
        args.sofr_source,
    )
    datasets = ("sofr", "spy-daily") if args.dataset == "all" else (args.dataset,)
    expected_sofr_source_sha256 = args.expected_sofr_source_sha256
    if args.sofr_source is not None and expected_sofr_source_sha256 is None:
        raise ValueError(
            "--expected-sofr-source-sha256 is required with --sofr-source"
        )
    if expected_sofr_source_sha256 is not None:
        if args.sofr_source is None:
            raise ValueError(
                "--expected-sofr-source-sha256 requires --sofr-source"
            )
        if re.fullmatch(r"[0-9a-f]{64}", expected_sofr_source_sha256) is None:
            raise ValueError(
                "--expected-sofr-source-sha256 must be a lowercase SHA-256"
            )
        if "sofr" not in datasets:
            raise ValueError("SOFR source assertions require the sofr dataset")
        if sha256_file(runtime_paths["sofr_cache"]) != expected_sofr_source_sha256:
            raise ValueError("the exact SOFR source does not match its pinned SHA-256")

    if args.validate_only:
        return [
            _history_summary(
                _stable_validate(name, _dataset_sources(name, runtime_paths))
            )
            for name in datasets
        ]

    require_canonical_eod_runtime_config(project_root, args.runtime_config)

    artifact_root = _absolute(
        args.artifact_root or Path("data/reference_history"), project_root
    )
    store = ContentAddressedArtifactStore(artifact_root)
    code_version = _code_version(project_root, args.code_version)
    requested_by = args.requested_by or os.environ.get("USERNAME") or os.environ.get("USER")
    environment = args.environment
    if (
        not isinstance(environment, str)
        or not environment.strip()
        or environment != environment.strip()
    ):
        raise ValueError("--environment must be canonical non-empty text")
    results: list[dict[str, object]] = []

    def load_all(connection) -> None:
        for name in datasets:
            sources = _dataset_sources(name, runtime_paths)
            frozen = store.freeze_inputs(sources)
            if (
                name == "sofr"
                and expected_sofr_source_sha256 is not None
                and frozen.inputs["sofr"].content_sha256
                != expected_sofr_source_sha256
            ):
                raise RuntimeError(
                    "the frozen SOFR source does not match its pinned SHA-256"
                )
            frozen_paths = {key: item.path for key, item in frozen.inputs.items()}
            history = _normalize(name, frozen_paths)
            store.verify_frozen_inputs(frozen)
            prepared = prepare_history(
                history,
                frozen_inputs=frozen,
                store=store,
            )
            result = execute_reference_history_load(
                connection,
                prepared,
                artifact_store=store,
                environment=environment,
                code_version=code_version,
                generation=args.generation,
                requested_by=requested_by,
            )
            rendered = asdict(result)
            rendered["pipeline_run_id"] = str(result.pipeline_run_id)
            rendered["reference_data_release_id"] = str(
                result.reference_data_release_id
            )
            results.append(rendered)

    if has_delegated_finalization_lease():
        connection = connect_from_environment()
        try:
            with delegated_finalization_child_lease(connection):
                load_all(connection)
        finally:
            connection.close()
    else:
        with exclusive_eod_writer_lock(project_root):
            audit_root = resolve_eod_audit_root(project_root, None)
            assert_no_unresolved_eod_finalizations(audit_root)
            connection = connect_from_environment()
            try:
                with standalone_finalization_lease(connection):
                    assert_no_unresolved_eod_finalizations(audit_root)
                    prior_evidence = completed_eod_finalization_evidence(
                        audit_root,
                        environment=environment,
                    )
                    verify_database_finalization_continuity(
                        connection,
                        prior_evidence,
                    )
                    # End the read-only continuity transaction before the
                    # reference service begins its own atomic write.
                    connection.rollback()
                    load_all(connection)
            finally:
                connection.close()
    return results


def main(argv: list[str] | None = None) -> int:
    try:
        output = run(parse_args(argv))
    except Exception as exc:
        print(f"Reference-history load failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
