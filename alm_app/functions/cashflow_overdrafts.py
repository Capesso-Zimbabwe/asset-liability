# core/alm_app/functions/cashflow_overdrafts.py

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Tuple, Union

from django.db import transaction

from staging.models import OverdraftContract
from ..models import Arranged_cashflows


def _normalize_date(value: Union[str, date, datetime]) -> date:
    """
    Accepts an ISO 'YYYY-MM-DD' string, date, or datetime and returns a date.
    Raises TypeError for anything else.
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


@transaction.atomic
def cashflow_overdrafts(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 2,
    cashflow_type: str = "OVERDRAFT"
) -> Tuple[int, int]:
    """
    Load overdraft cashflows from Stg_Overdraft_Contracts (OverdraftContract) into Fsi_Arranged_cashflows.

    Behaviour:
      • Deletes any existing overdraft rows for this fic_mis_date to avoid duplicates.
      • Inserts fresh rows based on overdraft contract data.

    Rules:
      v_loan_type                 = 'overdraft'
      n_cash_flow_bucket          = bucket_no (default 2)
      d_cashflow_date             = d_next_payment_date or fic_mis_date + 1 day
      n_total_principal_payment   = n_eop_bal (outstanding balance)
      n_total_interest_payment    = calculated based on interest rate
      n_total_cash_flow_amount    = principal + interest
      n_total_balance             = n_eop_bal
      v_cash_flow_type            = cashflow_type (default 'OVERDRAFT')
      record_count                = 1

    Returns:
      (deleted_count, created_count)
    """

    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")

    # Delete ALL existing overdraft records for this fic_mis_date (not just specific bucket)
    # This ensures complete cleanup when function is rerun
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="overdraft"
    ).delete()
    
    print(f"🧹 Deleted {deleted_count} existing overdraft records for {fic_mis_date}")

    # Source rows
    src_qs = OverdraftContract.objects.filter(fic_mis_date=fic_mis_date)
    if not src_qs.exists():
        print(f"⚠️  No overdraft contracts found for {fic_mis_date}")
        return deleted_count, 0

    to_create = []
    for contract in src_qs.iterator():
        # Use next payment date if available, otherwise fic_mis_date + 1 day
        cf_date = contract.d_next_payment_date or (fic_mis_date + timedelta(days=1))
        
        # Principal amount (outstanding balance)
        principal = contract.n_eop_bal or zero2
        
        # Calculate interest payment (simple calculation)
        interest_rate = contract.n_curr_interest_rate or zero2
        # Assuming daily interest calculation
        interest_payment = (principal * interest_rate / Decimal("100")) / Decimal("365")
        
        # Total cashflow amount
        total_amount = principal + interest_payment
        
        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=contract.v_account_number,
                v_prod_code=contract.v_prod_code,
                v_loan_type="overdraft",
                v_party_type_code=None,
                v_cash_flow_type=cashflow_type,
                n_cash_flow_bucket=bucket_no,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=total_amount,
                n_total_principal_payment=principal,
                n_total_interest_payment=interest_payment,
                n_total_balance=principal,
                v_ccy_code=(contract.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
    created_count = len(created_objs)
    
    print(f"✓ Created {created_count} new overdraft cashflow records")
    
    return deleted_count, created_count


@transaction.atomic
def cashflow_overdrafts_future(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 3,
    cashflow_type: str = "OVERDRAFT_FUTURE"
) -> Tuple[int, int]:
    """
    Generate future overdraft cashflows based on maturity dates.
    
    Similar to cashflow_overdrafts but focuses on future payment schedules
    using d_maturity_date instead of d_next_payment_date.
    """
    
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")

    # Delete ALL existing overdraft records for this fic_mis_date (not just specific bucket)
    # This ensures complete cleanup when function is rerun
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="overdraft"
    ).delete()
    
    print(f"🧹 Deleted {deleted_count} existing overdraft records for {fic_mis_date}")

    # Source rows with maturity dates in the future
    src_qs = OverdraftContract.objects.filter(
        fic_mis_date=fic_mis_date,
        d_maturity_date__gt=fic_mis_date
    )
    
    if not src_qs.exists():
        print(f"⚠️  No overdraft contracts with future maturity dates found for {fic_mis_date}")
        return deleted_count, 0

    to_create = []
    for contract in src_qs.iterator():
        # Use maturity date for future cashflows
        cf_date = contract.d_maturity_date
        
        principal = contract.n_eop_bal or zero2
        interest_rate = contract.n_curr_interest_rate or zero2
        
        # Calculate days to maturity for interest
        days_to_maturity = (cf_date - fic_mis_date).days
        interest_payment = (principal * interest_rate / Decimal("100")) * Decimal(str(days_to_maturity)) / Decimal("365")
        
        total_amount = principal + interest_payment
        
        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=contract.v_account_number,
                v_prod_code=contract.v_prod_code,
                v_loan_type="overdraft",
                v_party_type_code=None,
                v_cash_flow_type=cashflow_type,
                n_cash_flow_bucket=bucket_no,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=total_amount,
                n_total_principal_payment=principal,
                n_total_interest_payment=interest_payment,
                n_total_balance=zero2,  # Balance becomes zero at maturity
                v_ccy_code=(contract.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
    created_count = len(created_objs)
    
    print(f"✓ Created {created_count} new future overdraft cashflow records")
    
    return deleted_count, created_count