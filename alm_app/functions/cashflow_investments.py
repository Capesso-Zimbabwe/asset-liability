# core/alm_app/functions/cashflow_loaders.py
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Tuple, Union

from django.db import transaction

from staging.models import FirstDayProduct, Investment
from ..models import Arranged_cashflows  # adjust import path if needed


# ---------- Helpers ----------

def _normalize_date(value: Union[str, date, datetime]) -> date:
    """
    Accepts an ISO 'YYYY-MM-DD' string, date, or datetime and returns a date.
    Raises TypeError/ValueError for anything else.
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"fic_mis_date string must be ISO 'YYYY-MM-DD'. Got: {value!r}"
            ) from exc
    raise TypeError(f"fic_mis_date must be str/date/datetime, got {type(value)}")


# ---------- Day-1 cashflows ----------

@transaction.atomic
def cashflow_first_day(fic_mis_date: Union[str, date, datetime]) -> Tuple[int, int]:
    """
    Load Day-1 cashflows from Stg_First_Day_Bucket (FirstDayProduct) into Fsi_Arranged_cashflows.

    Behaviour:
      • Deletes any existing rows for this fic_mis_date with
        v_loan_type='first_day', bucket 1, cash_flow_type='DAY1'.
      • Inserts fresh rows for fic_mis_date + 1 day.

    Returns:
      (deleted_count, created_count)
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    cf_date = fic_mis_date + timedelta(days=1)
    zero2 = Decimal("0.00")

    src_qs = FirstDayProduct.objects.filter(fic_mis_date=fic_mis_date)

    # Always clear out previous run rows to avoid dupes
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="first_day",
        n_cash_flow_bucket=1,
        v_cash_flow_type="DAY1",
        d_cashflow_date=cf_date,
    ).delete()

    if not src_qs.exists():
        return deleted_count, 0

    to_create = []
    for s in src_qs.iterator():
        principal = s.n_eop_bal or zero2
        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=s.v_account_number,
                v_prod_code=s.v_prod_code,
                v_loan_type="first_day",
                v_party_type_code=None,
                v_cash_flow_type="DAY1",
                n_cash_flow_bucket=1,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=principal,
                n_total_principal_payment=principal,
                n_total_interest_payment=zero2,
                n_total_balance=zero2,
                v_ccy_code=(s.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
    return deleted_count, len(created_objs)


# ---------- Investment cashflows ----------

@transaction.atomic
def cashflow_investments(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 2,
    cashflow_type: str = "SCHD",
) -> Tuple[int, int]:
    """
    Load Investment rows into Fsi_Arranged_cashflows.

    Rules:
      v_loan_type                 = 'investments'
      n_cash_flow_bucket          = bucket_no (default 2)
      d_cashflow_date             = d_maturity_date
      n_total_principal_payment   = n_eop_bal
      n_total_interest_payment    = n_accr_int_amt (fallback 0)
      n_total_cash_flow_amount    = principal + interest
      n_total_balance             = 0
      v_cash_flow_type            = cashflow_type ('SCHD')
      record_count                = 1

    Behaviour:
      • Deletes any existing rows for this fic_mis_date, loan_type='investments',
        bucket_no and cashflow_type to avoid duplicates before inserting.

    Returns:
      (deleted_count, created_count)
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")

    src_qs = Investment.objects.filter(fic_mis_date=fic_mis_date)

    # Clear previous run rows
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="investments",
        n_cash_flow_bucket=bucket_no,
        v_cash_flow_type=cashflow_type,
    ).delete()

    if not src_qs.exists():
        return deleted_count, 0

    to_create = []
    for s in src_qs.iterator():
        principal = s.n_eop_bal or zero2
        interest = getattr(s, "n_accr_int_amt", zero2) or zero2
        cf_date = s.d_maturity_date
        if isinstance(cf_date, datetime):
            cf_date = cf_date.date()

        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=s.v_account_number,
                v_prod_code=s.v_prod_code,
                v_loan_type="investments",
                v_party_type_code=None,
                v_cash_flow_type=cashflow_type,
                n_cash_flow_bucket=bucket_no,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=principal + interest,
                n_total_principal_payment=principal,
                n_total_interest_payment=interest,
                n_total_balance=zero2,
                v_ccy_code=(s.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
    return deleted_count, len(created_objs)
