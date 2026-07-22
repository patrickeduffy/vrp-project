"""Pure append/correction planning for immutable reference-data rows."""

from __future__ import annotations

from collections.abc import Iterable

from vrp.storage.reference_data import DailyMarketFeature, ReferenceRateObservation

from .canonical import history_row_date
from .models import ChangePlan, CurrentRow, HistoryRow, NormalizedHistory, RowChange


def _row_sha256(row: HistoryRow) -> str:
    if isinstance(row, (ReferenceRateObservation, DailyMarketFeature)):
        return row.row_sha256
    raise TypeError(f"unsupported history row: {type(row).__name__}")


def plan_changes(
    candidate: NormalizedHistory,
    current_rows: Iterable[CurrentRow],
) -> ChangePlan:
    """Diff normalized rows against current leaves without permitting deletions."""

    current: dict[object, CurrentRow] = {}
    for row in current_rows:
        if row.natural_key in current:
            raise ValueError(f"database current view has a duplicate key: {row.natural_key}")
        current[row.natural_key] = row

    candidates = {history_row_date(row): row for row in candidate.rows}
    removed = sorted(set(current).difference(candidates))
    if removed:
        raise ValueError(
            "candidate history removes dates that are already current; "
            f"first_removed={removed[:5]}"
        )

    changes: list[RowChange] = []
    unchanged = 0
    for natural_key in sorted(candidates):
        row = candidates[natural_key]
        existing = current.get(natural_key)
        if existing is None:
            changes.append(
                RowChange(
                    natural_key=natural_key,
                    row=row,
                    supersedes_record_id=None,
                )
            )
        elif existing.row_sha256 == _row_sha256(row):
            unchanged += 1
        else:
            changes.append(
                RowChange(
                    natural_key=natural_key,
                    row=row,
                    supersedes_record_id=existing.record_id,
                )
            )
    return ChangePlan(
        changes=tuple(changes),
        unchanged_count=unchanged,
        current_count=len(current),
    )


def assert_current_matches(
    candidate: NormalizedHistory,
    current_rows: Iterable[CurrentRow],
) -> None:
    current: dict[object, str] = {}
    for row in current_rows:
        if row.natural_key in current:
            raise RuntimeError(f"database current view has a duplicate key: {row.natural_key}")
        current[row.natural_key] = row.row_sha256
    expected = {history_row_date(row): _row_sha256(row) for row in candidate.rows}
    if current != expected:
        missing = sorted(set(expected).difference(current))[:5]
        extra = sorted(set(current).difference(expected))[:5]
        changed = sorted(
            key for key in set(current).intersection(expected) if current[key] != expected[key]
        )[:5]
        raise RuntimeError(
            "database current rows do not match normalized history: "
            f"missing={missing} extra={extra} changed={changed}"
        )
