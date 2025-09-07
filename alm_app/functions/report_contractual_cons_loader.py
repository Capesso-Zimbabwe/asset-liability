# services/report_contractual_cons_loader.py
from datetime import date, datetime
from typing import Union, Dict
from decimal import Decimal

from django.db import connection
from django.apps import apps
from .report_loader import _to_date   # helper you already have

# --------------------------------------------------------------------------- #
# constants & models
# --------------------------------------------------------------------------- #
BASE_TABLE   = "Report_Contractual_Base"
SRC_PATTERN  = "Report_Contractual_{:%Y%m%d}"
TGT_PATTERN  = "Report_Contractual_Cons_{:%Y%m%d}"

FX = apps.get_model("alm_app", "Stg_Exchange_Rate")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _create_cons_table(run_date: date) -> str:
    tgt = TGT_PATTERN.format(run_date)
    with connection.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{tgt}" CASCADE;')
        cur.execute(f'CREATE TABLE "{tgt}" (LIKE "{BASE_TABLE}" INCLUDING ALL);')
    return tgt


def _fx_map(as_of: date, to_ccy: str) -> Dict[str, Decimal]:
    rows = (
        FX.objects
        .filter(v_to_ccy_code=to_ccy.upper(), fic_mis_date__lte=as_of)
        .order_by("v_from_ccy_code", "-fic_mis_date")
    )
    seen, fx = set(), {}
    for r in rows:
        if r.v_from_ccy_code in seen:
            continue
        seen.add(r.v_from_ccy_code)
        fx[r.v_from_ccy_code] = r.n_exchange_rate
    return fx            # { 'EUR': Decimal('0.915000'), ... }


def _bucket_columns(src_table: str) -> list[str]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name LIKE 'bucket_%%'
            ORDER BY ordinal_position
            """,
            [src_table],
        )
        cols = [row[0] for row in cur.fetchall()]
    cols.append("n_adjusted_cash_flow_amount")
    return cols


# --------------------------------------------------------------------------- #
# main loader
# --------------------------------------------------------------------------- #
def load_report_contractual_cons(
    fic_mis_date: Union[date, datetime, str],
    reporting_ccy: str = "USD",
) -> int:
    """
    Copy Report_Contractual_<YYYYMMDD> → Report_Contractual_Cons<YYYYMMDD>,
    converting every cash-flow amount AND changing v_ccy_code to *reporting_ccy*.
    """
    fic_mis_date  = _to_date(fic_mis_date)
    src_table     = SRC_PATTERN.format(fic_mis_date)
    tgt_table     = _create_cons_table(fic_mis_date)
    reporting_ccy = reporting_ccy.upper()

    fx_map      = _fx_map(fic_mis_date, reporting_ccy)
    bucket_cols = _bucket_columns(src_table)

    # ordered list of all columns except serial id
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            [src_table],
        )
        ordered_cols = [c for (c,) in cur.fetchall() if c != "id"]

    # build SELECT list matching ordered_cols
    select_parts = []
    for col in ordered_cols:
        if col == "v_ccy_code":
            select_parts.append(f"'{reporting_ccy}' AS \"v_ccy_code\"")
        elif col in bucket_cols:
            select_parts.append(f'{col} * COALESCE(fx.fx_rate,1) AS "{col}"')
        else:
            select_parts.append(f'r."{col}"')

    select_sql      = ",\n            ".join(select_parts)
    insert_cols_sql = ", ".join(f'"{c}"' for c in ordered_cols)

    fx_values_sql = (
        ", ".join(f"('{ccy}', {rate})" for ccy, rate in fx_map.items())
        or "('XXX',1)"
    )

    insert_sql = f"""
        INSERT INTO "{tgt_table}" ({insert_cols_sql})
        SELECT
            {select_sql}
        FROM "{src_table}" r
        LEFT JOIN (VALUES {fx_values_sql}) AS fx(from_ccy, fx_rate)
               ON fx.from_ccy = r.v_ccy_code;
    """

    with connection.cursor() as cur:
        cur.execute(insert_sql)
        rows_inserted = cur.rowcount

    return rows_inserted
