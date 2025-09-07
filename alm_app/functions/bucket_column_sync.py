# services/bucket_column_sync.py
from __future__ import annotations

import re
from typing import Iterable, Tuple, List

from django.apps import apps
from django.db import connection, transaction

# ── models / helpers ──
from ..models import Process          # <─ NEW: to fetch the default process

def _get_default_process():
    """Return the first Process row or *None* if none exist."""
    return Process.objects.first()

def _quote_regclass(name: str) -> str:
    """Return an identifier literal that to_regclass() will resolve."""
    return name if re.fullmatch(r"[a-z0-9_]+", name) else '"' + name.replace('"', '""') + '"'

def _get_bucket_cols_pg(table_name: str) -> List[str]:
    """
    List every physical bucket_* column on *table_name* (PostgreSQL only),
    using pg_attribute so CamelCase identifiers are never missed.
    """
    rc_text = _quote_regclass(table_name)
    sql = """
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = to_regclass(%s)
          AND attnum  > 0
          AND NOT attisdropped
          AND attname LIKE %s
        ORDER BY attnum;
    """
    with connection.cursor() as cur:
        cur.execute(sql, [rc_text, "bucket_%"])
        return [row[0] for row in cur.fetchall()]

def _drop_columns(table_name: str, cols: Iterable[str]) -> None:
    """Drop each column individually (avoids multi‑DROP syntax‑errors)."""
    with connection.cursor() as cur:
        for c in cols:
            cur.execute(
                f'ALTER TABLE "{table_name}" '
                f'DROP COLUMN IF EXISTS "{c}" CASCADE;'
            )

# ───────────────────────── public API ──────────────────────
def sync_bucket_columns(
    table_name: str = "Report_Contractual_Base",
    rebuild: bool = False,
    numeric_precision: Tuple[int, int] = (20, 2),
    *,
    verbose: bool = False,
) -> None:
    """
    Ensure *table_name* has exactly the bucket_* columns defined for the single
    `Process` configured in the system.

    • Always rebuild when the table is Report_Contractual_Base,
      when its name fits Report_Contractual_<YYYYMMDD>, or when rebuild=True.
    • Otherwise, only append missing columns.
    """
    process = _get_default_process()
    process_name = process.name if process else ""

    TBM = apps.get_model("alm_app", "TimeBucketMaster")
    numeric_sql = f"NUMERIC({numeric_precision[0]},{numeric_precision[1]})"

    always_rebuild = {"report_contractual_base"}
    is_snapshot   = bool(re.fullmatch(r"report_contractual_\d{8}", table_name.lower()))
    must_rebuild  = rebuild or is_snapshot or table_name.lower() in always_rebuild

    with transaction.atomic():
        # ➊ DROP unneeded columns
        if must_rebuild:
            old_cols = _get_bucket_cols_pg(table_name)
            if verbose:
                print(f"[bucket_sync] dropping: {old_cols}")
            _drop_columns(table_name, old_cols)

        # ➋ ADD / ensure required columns
        for bucket in (
            TBM.objects
            .filter(process_name=process_name)
            .order_by("bucket_number")
        ):
            col_name = bucket_column_name(bucket)  # imported lazily below
            if verbose:
                print(f"[bucket_sync] ensuring {col_name}")
            with connection.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{table_name}" '
                    f'ADD COLUMN IF NOT EXISTS "{col_name}" {numeric_sql};'
                )

# Convenience wrapper (kept for symmetry / readability)
def sync_buckets_for_run(
    table_name: str,
    numeric_precision: Tuple[int, int] = (20, 2),
    *,
    verbose: bool = False,
) -> None:
    """Auto‑detects if a full rebuild is needed and runs the sync."""
    sync_bucket_columns(
        table_name=table_name,
        numeric_precision=numeric_precision,
        verbose=verbose,
    )

# delayed import to avoid circularity
from ..functions_view.report_buckets import bucket_column_name  # noqa: E402
