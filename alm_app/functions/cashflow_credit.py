"""
cashflows.py · ALM helper  (one‑time same‑level interest)

Each level ('Q1' … 'Qn') gets its cumulative interest recorded
**once** on its first row dated ≥ fic_mis_date.
Subsequent rows of that level carry 0 interest.
"""

from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Set

from django.db import transaction
from django.db.models import Max
from django.utils.timezone import now

from staging.models import CreditLine, LoanPaymentSchedule
from ..models import Arranged_cashflows


# ───────── helpers ───────── #

def _norm_lvl(qstr: str) -> str:
    """Normalise to uppercase Q‑string, empty if None."""
    return qstr.upper().strip() if qstr else ""


def _suffix_principal(rows: List[LoanPaymentSchedule]) -> List[Decimal]:
    """Suffix sum of remaining principal aligned with rows."""
    out, running = [], Decimal("0")
    for r in reversed(rows):
        running += r.n_principal_payment_amnt or Decimal("0")
        out.append(running)
    out.reverse()
    return out


# ───────── flow builder ───────── #

def _build_flows(
    credit: CreditLine,
    all_rows: List[LoanPaymentSchedule],
    snapshot: date,
) -> List[Arranged_cashflows]:
    # 1️⃣ cumulative interest per level up to snapshot
    interest_sum: Dict[str, Decimal] = {}
    for r in all_rows:
        if r.d_next_payment_date > snapshot:
            break
        lvl = _norm_lvl(r.n_level)
        interest_sum[lvl] = interest_sum.get(lvl, Decimal("0")) \
                            + (r.n_interest_payment_amt or Decimal("0"))

    # 2️⃣ rows to write (≥ snapshot)
    rows_kept = [r for r in all_rows if r.d_next_payment_date >= snapshot]
    if not rows_kept:
        return []

    balance_suffix = _suffix_principal(rows_kept)

    # 3️⃣ build flows: only first row of each level gets interest
    level_written: Set[str] = set()
    flows: List[Arranged_cashflows] = []

    for idx, (row, bal) in enumerate(zip(rows_kept, balance_suffix), 1):
        lvl = _norm_lvl(row.n_level)
        prin = row.n_principal_payment_amnt or Decimal("0")
        if lvl not in level_written:
            intr = interest_sum.get(lvl, Decimal("0"))
            level_written.add(lvl)
        else:
            intr = Decimal("0")

        flows.append(
            Arranged_cashflows(
                fic_mis_date              = credit.fic_mis_date,
                v_account_number          = credit.v_account_number,
                v_prod_code               = credit.v_prod_code,
                v_loan_type               = "CREDIT",
                v_cash_flow_type          = "PAYMENT_SCHEDULE",
                n_cash_flow_bucket        = idx,
                d_cashflow_date           = row.d_next_payment_date,
                n_total_cash_flow_amount  = (prin + intr).quantize(
                                                Decimal("0.01"), ROUND_HALF_UP),
                n_total_principal_payment = prin,
                n_total_interest_payment  = intr,
                n_total_balance           = bal,
                v_ccy_code                = credit.v_ccy_code,
                record_count              = 1,
            )
        )
    return flows


# ───────── orchestrator ───────── #

@transaction.atomic
def cashflow_credit_line(fic_mis_date) -> None:
    """Generate arranged cash‑flows for snapshot `fic_mis_date`."""
    if isinstance(fic_mis_date, str):
        fic_mis_date = datetime.fromisoformat(fic_mis_date).date()

    Arranged_cashflows.objects.filter(fic_mis_date=fic_mis_date).delete()

    credits = list(CreditLine.objects.filter(fic_mis_date=fic_mis_date))
    if not credits:
        print("[WARN] no CreditLine rows for snapshot")
        return

    out_rows: List[Arranged_cashflows] = []

    for c in credits:
        latest_sched = (
            LoanPaymentSchedule.objects.filter(
                v_account_number=c.v_account_number,
                v_instrument_type_cd="CREDITLINES",
            ).aggregate(latest=Max("fic_mis_date"))["latest"]
        )
        if not latest_sched:
            continue

        sched_all = list(
            LoanPaymentSchedule.objects.filter(
                fic_mis_date=latest_sched,
                v_account_number=c.v_account_number,
                v_instrument_type_cd="CREDITLINES",
            ).order_by("d_next_payment_date")
        )
        if not sched_all:
            continue

        out_rows.extend(_build_flows(c, sched_all, fic_mis_date))

    if out_rows:
        Arranged_cashflows.objects.bulk_create(out_rows, batch_size=1000)
        print(f"[DONE] inserted {len(out_rows)} rows for {fic_mis_date}")
    else:
        print("[WARN] no eligible rows to insert")
