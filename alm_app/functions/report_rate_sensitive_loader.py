# services/report_rate_sensitive_loader.py
from datetime import date, datetime
from typing    import Union

from django.db import connection
from django.apps import apps
from .report_loader import _to_date                      # existing helper


# ---------- constants ---------- #
CONTRACTUAL_BASE = "Report_Contractual_Base"             # structure template
RATE_BASE        = CONTRACTUAL_BASE                      # same layout

SPM = apps.get_model("alm_app", "Stg_Product_Master")    # product master


# ──────────────────────────────────────────────────────────────────────────
# 1️⃣  (Re)create Report_rate_sensitive_<YYYYMMDD>
# ──────────────────────────────────────────────────────────────────────────
def create_report_rate_table(fic_mis_date: Union[date, datetime, str]) -> str:
    """
    Drops (if exists) and recreates
        Report_rate_sensitive_<YYYYMMDD>
    cloned from Report_Contractual_Base.
    """
    fic_mis_date = _to_date(fic_mis_date)
    tbl          = f"Report_rate_sensitive_{fic_mis_date:%Y%m%d}"

    with connection.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{tbl}" CASCADE;')
        cur.execute(
            f'CREATE TABLE "{tbl}" '
            f'(LIKE "{RATE_BASE}" INCLUDING ALL);'
        )
    return tbl


# ──────────────────────────────────────────────────────────────────────────
# 2️⃣  Copy rate-sensitive rows into that table
# ──────────────────────────────────────────────────────────────────────────
def load_report_rate_sensitive(
    fic_mis_date: Union[date, datetime, str],
) -> int:
    """
    Reads Report_Contractual_Cons<YYYYMMDD>.
    Inserts only those rows whose v_prod_type is marked rate-sensitive
    (f_prod_rate_sensitivity = 'Y' in Stg_Product_Master) into
    Report_rate_sensitive_<YYYYMMDD>.

    Returns number of rows inserted.
    """
    fic_mis_date = _to_date(fic_mis_date)

    src_tbl = f"Report_Contractual_Cons_{fic_mis_date:%Y%m%d}"
    dst_tbl = f"Report_rate_sensitive_{fic_mis_date:%Y%m%d}"

    create_report_rate_table(fic_mis_date)          # ensure empty table

    # ---- find sensitive product types ----------------------------------
    sens_types = (
        SPM.objects
        .filter(f_prod_rate_sensitivity="Y")
        .values_list("v_prod_type", flat=True)
        .distinct()
    )
    sens_types = tuple(sens_types)                  # SQL IN (...) needs tuple

    if not sens_types:
        return 0                                    # nothing to copy

    # ---- perform the copy ----------------------------------------------
    with connection.cursor() as cur:
        cur.execute(
            f'INSERT INTO "{dst_tbl}" '
            f'SELECT * FROM "{src_tbl}" '
            f'WHERE v_prod_type IN %s;',
            [sens_types],
        )
        rows_inserted = cur.rowcount

    return rows_inserted
