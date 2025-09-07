# services/report_behavioural_loader.py
from datetime import date, datetime
from typing    import Union, Dict
from decimal   import Decimal, ROUND_HALF_UP

from django.db import connection, transaction
from django.apps import apps
from .report_loader import _to_date


CONTRACTUAL_BASE  = "Report_Contractual_Base"   # structural template
BEHAVIOURAL_BASE  = CONTRACTUAL_BASE


# ─────────────────────────────────────────────────────────────────────────────
# 1️⃣  create / recreate Report_behavioural_<YYYYMMDD>
# ─────────────────────────────────────────────────────────────────────────────
def create_report_behavioural_table(fic_mis_date: Union[date, datetime, str]) -> str:
    fic_mis_date = _to_date(fic_mis_date)
    tbl          = f"Report_behavioural_{fic_mis_date:%Y%m%d}"

    with connection.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{tbl}" CASCADE;')
        cur.execute(f'CREATE TABLE "{tbl}" (LIKE "{BEHAVIOURAL_BASE}" INCLUDING ALL);')
    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# 2️⃣  spread contractual amounts according to behavioural patterns
# ─────────────────────────────────────────────────────────────────────────────
def load_report_behavioural(
    fic_mis_date: Union[date, datetime, str],
) -> int:
    """
    Reads Report_Contractual_Cons<YYYYMMDD>.
    For every row whose v_prod_type has a behavioural pattern, allocates each
    bucket amount across the pattern’s bucket percentages and inserts the
    result into Report_behavioural_<YYYYMMDD>.

    Rows with no defined pattern are skipped.
    """
    fic_mis_date = _to_date(fic_mis_date)
    src_tbl      = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
    dst_tbl      = f"Report_behavioural_{fic_mis_date:%Y%m%d}"

    create_report_behavioural_table(fic_mis_date)   # fresh table each run

    # ------------------------------------------------------------------ #
    #  Load behavioural patterns  →  {prod_type: {bucket_num: pct/100}}
    # ------------------------------------------------------------------ #
    Pattern      = apps.get_model("alm_app", "BehavioralPattern")
    PatternSplit = apps.get_model("alm_app", "BehavioralPatternSplit")

    patterns: Dict[str, Dict[int, Decimal]] = {}
    for pat in Pattern.objects.prefetch_related("splits"):
        splits = {
            s.bucket_number: (s.percentage / Decimal("100"))
            for s in pat.splits.all()
        }
        if splits:                 # ignore if no splits
            patterns[pat.v_prod_type] = splits

    if not patterns:
        return 0   # nothing to do

    pattern_types = tuple(patterns.keys())

    # ------------------------------------------------------------------ #
    #  Discover bucket_* column names and map → bucket_number
    # ------------------------------------------------------------------ #
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name LIKE 'bucket_%%'
            """,
            [src_tbl],
        )
        bucket_cols = [r[0] for r in cur.fetchall()]

    # map e.g. 'bucket_001_20240831_20240907' → 1
    bucket_map: Dict[int, str] = {}
    for col in bucket_cols:
        parts = col.split("_")
        try:
            bucket_no = int(parts[1].lstrip("0") or "0")
        except ValueError:
            continue
        bucket_map[bucket_no] = col

    # ordered destination columns (excluding id) for INSERT
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            [src_tbl],
        )
        ordered_cols = [c for (c,) in cur.fetchall() if c != "id"]

    insert_cols_sql = ", ".join(f'"{c}"' for c in ordered_cols)

    # ------------------------------------------------------------------ #
    #  Iterate over source rows, spread amounts, insert
    # ------------------------------------------------------------------ #
    rows_inserted = 0
    with transaction.atomic(), connection.cursor() as cur_src, connection.cursor() as cur_dst:
        # server-side cursor keeps memory low
        cur_src.execute(
            f'SELECT {", ".join(ordered_cols)} FROM "{src_tbl}" '
            f'WHERE v_prod_type IN %s',
            [pattern_types],
        )

        while True:
            batch = cur_src.fetchmany(500)
            if not batch:
                break

            for row in batch:
                row_dict = dict(zip(ordered_cols, row))
                prod_type = row_dict["v_prod_type"]

                splits = patterns.get(prod_type)
                if not splits:
                    continue      # safety: skip if pattern vanished

                # ---------- spread amounts ----------------------------
                new_bucket_vals = {col: Decimal("0") for col in bucket_cols}
                for src_bucket_no, col_name in bucket_map.items():
                    amt = row_dict[col_name] or Decimal("0")
                    if amt == 0:
                        continue
                    for dst_bucket_no, pct in splits.items():
                        dst_col = bucket_map.get(dst_bucket_no)
                        if dst_col:
                            new_bucket_vals[dst_col] += (amt * pct)

                # round to 2dp (same as NUMERIC(20,2))
                for k, v in new_bucket_vals.items():
                    new_bucket_vals[k] = v.quantize(Decimal("0.01"), ROUND_HALF_UP)

                # ---------- build insert tuple ------------------------
                values = []
                for col in ordered_cols:
                    if col in bucket_cols:
                        values.append(new_bucket_vals[col])
                    else:
                        values.append(row_dict[col])

                placeholders = ", ".join(["%s"] * len(values))
                cur_dst.execute(
                    f'INSERT INTO "{dst_tbl}" ({insert_cols_sql}) VALUES ({placeholders})',
                    values,
                )
                rows_inserted += 1

    return rows_inserted
