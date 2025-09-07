# core/alm_app/functions/cashflow_loaders.py
import logging
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Union

from django.db import transaction
from django.db.models import Max

from .cashflow_investments import _normalize_date
from staging.models import LoanContract, CreditLine, LoanPaymentSchedule
from ..models import Arranged_cashflows  # adjust import path if needed

# Configure logging
log = logging.getLogger(__name__)


# ---------- Helpers ----------
def _simple_interest(principal: Decimal, rate_pct: Decimal) -> Decimal:
    """
    Return principal × (rate / 100) rounded to two decimals.
    """
    if principal is None or rate_pct is None:
        return Decimal("0.00")
    return (principal * (rate_pct / Decimal("100"))).quantize(
        Decimal("0.01"), ROUND_HALF_UP
    )

# ----‑‑‑ Main loan loader  -------------------------------------------------------
@transaction.atomic
def cashflow_loan_contracts(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 2,
    cashflow_type: str = "SCHD",
) -> Tuple[int, int]:
    """
    Load LoanContract rows into Arranged_cashflows.

    Updated to include:
    - Future payment schedule processing when available
    - Fallback to maturity date with n_eop_bal when no payment schedule
    - Only future payments (d_next_payment_date >= fic_mis_date)
    - No interest calculation for fallback cases
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")
    
    # Enhanced logging
    print(f"\n{'='*80}")
    print(f"STARTING LOAN CONTRACTS CASHFLOW PROCESSING")
    print(f"FIC MIS Date: {fic_mis_date}")
    print(f"Cashflow Type: {cashflow_type}")
    print(f"{'='*80}")

    src_qs = LoanContract.objects.filter(fic_mis_date=fic_mis_date)
    total_loans = src_qs.count()
    
    print(f"\nTotal Loan Contracts found: {total_loans}")

    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="loans",
        n_cash_flow_bucket=bucket_no,
        v_cash_flow_type=cashflow_type,
    ).delete()
    
    print(f"Deleted existing cashflows: {deleted_count}")

    if not src_qs.exists():
        print("No loan contracts found. Exiting.")
        return deleted_count, 0

    to_create = []
    accounts_with_schedule = 0
    accounts_without_schedule = 0
    total_future_payments = 0
    accounts_with_schedule_list = []
    accounts_without_schedule_list = []
    
    print(f"\n{'='*60}")
    print("PROCESSING INDIVIDUAL LOAN CONTRACTS")
    print(f"{'='*60}")
    
    for idx, s in enumerate(src_qs.iterator(), 1):
        account_num = s.v_account_number
        print(f"\n[{idx}/{total_loans}] Processing Account: {account_num}")
        
        # Check for payment schedule first
        latest_sched_date = (
            LoanPaymentSchedule.objects.filter(
                v_account_number=s.v_account_number,
                v_instrument_type_cd="LOANS",
            ).aggregate(latest=Max("fic_mis_date"))["latest"]
        )
        
        if latest_sched_date:
            print(f"  ✓ Payment schedule found (latest date: {latest_sched_date})")
            
            # Get future payment schedules only (>= fic_mis_date)
            future_payments = (
                LoanPaymentSchedule.objects.filter(
                    fic_mis_date=latest_sched_date,
                    v_account_number=s.v_account_number,
                    v_instrument_type_cd="LOANS",
                    d_next_payment_date__gte=fic_mis_date,
                ).order_by("d_next_payment_date")
            )
            
            future_count = future_payments.count()
            print(f"  ✓ Future payments found: {future_count}")
            
            if future_count > 0:
                accounts_with_schedule += 1
                accounts_with_schedule_list.append(account_num)
                total_future_payments += future_count
                
                # Calculate running balance (remaining principal after each payment)
                payment_list = list(future_payments)
                remaining_balance = s.n_eop_bal or zero2
                
                print(f"  ✓ Starting balance: {remaining_balance}")
                
                for bucket_idx, payment in enumerate(payment_list, 1):
                    principal = payment.n_principal_payment_amnt or zero2
                    interest = payment.n_interest_payment_amt or zero2
                    
                    # Calculate remaining balance after this payment
                    remaining_balance = max(zero2, remaining_balance - principal)
                    
                    cf_date = payment.d_next_payment_date
                    if isinstance(cf_date, datetime):
                        cf_date = cf_date.date()
                    
                    print(f"    Payment {bucket_idx}: Principal={principal}, Interest={interest}, Remaining={remaining_balance}, Date={cf_date}")
                    
                    to_create.append(
                        Arranged_cashflows(
                            fic_mis_date=fic_mis_date,
                            v_account_number=s.v_account_number,
                            v_prod_code=s.v_prod_code,
                            v_loan_type="loans",
                            v_party_type_code=None,
                            v_cash_flow_type=cashflow_type,
                            n_cash_flow_bucket=bucket_idx,
                            d_cashflow_date=cf_date,
                            n_total_cash_flow_amount=(principal + interest).quantize(
                                Decimal("0.01"), ROUND_HALF_UP
                            ),
                            n_total_principal_payment=principal,
                            n_total_interest_payment=interest,
                            n_total_balance=remaining_balance,
                            v_ccy_code=(s.v_ccy_code or "").upper()[:10],
                            record_count=1,
                        )
                    )
                
                continue  # Move to next loan contract
            else:
                print(f"  ⚠ Payment schedule exists but no future payments found")
        
        # Fallback: Account not in payment schedule or no future payments
        # Use original logic with n_eop_bal and maturity date
        print(f"  ⚠ Using fallback - EOP balance at maturity")
        
        accounts_without_schedule += 1
        accounts_without_schedule_list.append(account_num)
        
        principal = s.n_eop_bal or zero2
        interest = zero2  # No interest calculation for fallback as requested
        
        print(f"  ✓ Fallback: Principal={principal}, Interest={interest} (maturity date: {s.d_maturity_date})")
        
        cf_date = s.d_maturity_date
        if isinstance(cf_date, datetime):
            cf_date = cf_date.date()

        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=s.v_account_number,
                v_prod_code=s.v_prod_code,
                v_loan_type="loans",
                v_party_type_code=None,
                v_cash_flow_type=cashflow_type,
                n_cash_flow_bucket=bucket_no,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=principal,
                n_total_principal_payment=principal,
                n_total_interest_payment=interest,
                n_total_balance=zero2,
                v_ccy_code=(s.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    # Summary logging
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total Loan Contracts Processed: {total_loans}")
    print(f"Accounts with Payment Schedule: {accounts_with_schedule}")
    print(f"Accounts without Payment Schedule (Fallback): {accounts_without_schedule}")
    print(f"Total Future Payments Found: {total_future_payments}")
    print(f"Total Cashflow Records to Create: {len(to_create)}")
    
    # Show sample accounts for each category
    if accounts_with_schedule_list:
        sample_with_schedule = accounts_with_schedule_list[:5]
        print(f"\nSample accounts WITH payment schedule: {sample_with_schedule}")
        if len(accounts_with_schedule_list) > 5:
            print(f"  ... and {len(accounts_with_schedule_list) - 5} more")
    
    if accounts_without_schedule_list:
        sample_without_schedule = accounts_without_schedule_list[:5]
        print(f"\nSample accounts WITHOUT payment schedule (fallback): {sample_without_schedule}")
        if len(accounts_without_schedule_list) > 5:
            print(f"  ... and {len(accounts_without_schedule_list) - 5} more")
    
    # Bulk create cashflows
    print(f"\n{'='*60}")
    print("CREATING CASHFLOW RECORDS")
    print(f"{'='*60}")
    
    if to_create:
        print(f"Creating {len(to_create)} cashflow records...")
        
        created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
        created_count = len(created_objs)
        
        print(f"✓ Successfully created {created_count} cashflow records")
    else:
        created_count = 0
        print("⚠ No cashflow records to create")
    
    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETED")
    print(f"Deleted: {deleted_count} | Created: {created_count}")
    print(f"{'='*80}\n")
    
    return deleted_count, created_count

# ----‑‑‑ New Credit Lines loader  -----------------------------------------------
@transaction.atomic
def cashflow_credit_lines_future(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 2,
    cashflow_type: str = "PAYMENT_SCHEDULE",
) -> Tuple[int, int]:
    """
    Load future CreditLine payment schedules into Arranged_cashflows.
    
    Key differences from credit line accumulation:
    - Only processes FUTURE payments (d_next_payment_date >= fic_mis_date)
    - Takes actual interest and principal amounts from payment schedule
    - No accumulation of past due amounts
    - Filters by v_instrument_type_cd="CREDITLINES"
    - For accounts not in payment schedule: uses n_eop_bal only, no interest
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")
    
    # Force output to appear in Django terminal using print statements
    print(f"\n{'='*80}")
    print(f"STARTING CREDIT LINES FUTURE CASHFLOW PROCESSING")
    print(f"FIC MIS Date: {fic_mis_date}")
    print(f"Cashflow Type: {cashflow_type}")
    print(f"{'='*80}")

    # Get credit lines for the snapshot date
    credit_lines = CreditLine.objects.filter(fic_mis_date=fic_mis_date)
    total_credit_lines = credit_lines.count()
    
    print(f"\nTotal Credit Lines found: {total_credit_lines}")
    
    # Delete existing credit line cashflows
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="CREDIT",
        v_cash_flow_type=cashflow_type,
    ).delete()
    
    print(f"Deleted existing cashflows: {deleted_count}")

    if not credit_lines.exists():
        print("No credit lines found. Exiting.")
        return deleted_count, 0

    to_create = []
    accounts_with_schedule = 0
    accounts_without_schedule = 0
    total_future_payments = 0
    accounts_with_schedule_list = []
    accounts_without_schedule_list = []
    
    print(f"\n{'='*60}")
    print("PROCESSING INDIVIDUAL CREDIT LINES")
    print(f"{'='*60}")
    
    for idx, credit in enumerate(credit_lines.iterator(), 1):
        account_num = credit.v_account_number
        print(f"\n[{idx}/{total_credit_lines}] Processing Account: {account_num}")
        
        # Get the latest payment schedule for this credit line
        latest_sched_date = (
            LoanPaymentSchedule.objects.filter(
                v_account_number=credit.v_account_number,
                v_instrument_type_cd="CREDITLINES",
            ).aggregate(latest=Max("fic_mis_date"))["latest"]
        )
        
        if latest_sched_date:
            print(f"  ✓ Payment schedule found (latest date: {latest_sched_date})")
            
            # Get future payment schedules only (>= fic_mis_date)
            future_payments = (
                LoanPaymentSchedule.objects.filter(
                    fic_mis_date=latest_sched_date,
                    v_account_number=credit.v_account_number,
                    v_instrument_type_cd="CREDITLINES",
                    d_next_payment_date__gte=fic_mis_date,  # FUTURE ONLY
                ).order_by("d_next_payment_date")
            )
            
            # Calculate remaining balance for each payment
            payments_list = list(future_payments)
            payment_count = len(payments_list)
            
            if payments_list:
                print(f"  ✓ Found {payment_count} future payments")
                
                accounts_with_schedule += 1
                accounts_with_schedule_list.append(account_num)
                total_future_payments += payment_count
                
                # Calculate suffix balances (remaining principal after each payment)
                remaining_balances = []
                total_remaining = sum(
                    (p.n_principal_payment_amnt or zero2) for p in payments_list
                )
                
                print(f"  ✓ Total principal in schedule: {total_remaining}")
                
                for payment in payments_list:
                    remaining_balances.append(total_remaining)
                    total_remaining -= (payment.n_principal_payment_amnt or zero2)
                
                # Create cashflow records for each future payment
                for payment_idx, (payment, balance) in enumerate(zip(payments_list, remaining_balances), 1):
                    principal = payment.n_principal_payment_amnt or zero2
                    interest = payment.n_interest_payment_amt or zero2
                    
                    print(f"    Payment {payment_idx}: Date={payment.d_next_payment_date}, Principal={principal}, Interest={interest}")
                    
                    # Ensure date is properly formatted
                    cf_date = payment.d_next_payment_date
                    if isinstance(cf_date, datetime):
                        cf_date = cf_date.date()
                    
                    to_create.append(
                        Arranged_cashflows(
                            fic_mis_date=fic_mis_date,
                            v_account_number=credit.v_account_number,
                            v_prod_code=credit.v_prod_code,
                            v_loan_type="CREDIT",
                            v_party_type_code=None,
                            v_cash_flow_type=cashflow_type,
                            n_cash_flow_bucket=payment_idx,
                            d_cashflow_date=cf_date,
                            n_total_cash_flow_amount=(principal + interest).quantize(
                                Decimal("0.01"), ROUND_HALF_UP
                            ),
                            n_total_principal_payment=principal,
                            n_total_interest_payment=interest,
                            n_total_balance=balance,
                            v_ccy_code=(credit.v_ccy_code or "").upper()[:10],
                            record_count=1,
                        )
                    )
                continue  # Move to next credit line
            else:
                print(f"  ⚠ Payment schedule exists but no future payments found")
        
        # Fallback: Account not in payment schedule - use n_eop_bal only, no interest
        print(f"  ⚠ No payment schedule found - using fallback (EOP balance only)")
        
        accounts_without_schedule += 1
        accounts_without_schedule_list.append(account_num)
        
        principal = credit.n_eop_bal or zero2
        interest = zero2  # No interest calculation as requested
        
        print(f"  ✓ Fallback: Principal={principal}, Interest={interest} (maturity date: {credit.d_maturity_date})")
        
        # Use maturity date as cashflow date
        cf_date = credit.d_maturity_date
        if isinstance(cf_date, datetime):
            cf_date = cf_date.date()
        
        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=credit.v_account_number,
                v_prod_code=credit.v_prod_code,
                v_loan_type="CREDIT",
                v_party_type_code=None,
                v_cash_flow_type="MATURITY_FALLBACK",
                n_cash_flow_bucket=1,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=principal.quantize(
                    Decimal("0.01"), ROUND_HALF_UP
                ),
                n_total_principal_payment=principal,
                n_total_interest_payment=interest,
                n_total_balance=zero2,
                v_ccy_code=(credit.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    # Summary logging
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total Credit Lines Processed: {total_credit_lines}")
    print(f"Accounts with Payment Schedule: {accounts_with_schedule}")
    print(f"Accounts without Payment Schedule (Fallback): {accounts_without_schedule}")
    print(f"Total Future Payments Found: {total_future_payments}")
    print(f"Total Cashflow Records to Create: {len(to_create)}")
    
    # Show sample accounts for each category
    if accounts_with_schedule_list:
        sample_with_schedule = accounts_with_schedule_list[:5]
        print(f"\nSample accounts WITH payment schedule: {sample_with_schedule}")
        if len(accounts_with_schedule_list) > 5:
            print(f"  ... and {len(accounts_with_schedule_list) - 5} more")
    
    if accounts_without_schedule_list:
        sample_without_schedule = accounts_without_schedule_list[:5]
        print(f"\nSample accounts WITHOUT payment schedule (fallback): {sample_without_schedule}")
        if len(accounts_without_schedule_list) > 5:
            print(f"  ... and {len(accounts_without_schedule_list) - 5} more")
    
    # Bulk create cashflows
    print(f"\n{'='*60}")
    print("CREATING CASHFLOW RECORDS")
    print(f"{'='*60}")
    
    if to_create:
        print(f"Creating {len(to_create)} cashflow records...")
        
        created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
        created_count = len(created_objs)
        
        print(f"✓ Successfully created {created_count} cashflow records")
    else:
        created_count = 0
        print("⚠ No cashflow records to create")
    
    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETED")
    print(f"Deleted: {deleted_count} | Created: {created_count}")
    print(f"{'='*80}\n")
    
    return deleted_count, created_count

# ----‑‑‑ New Loans Future loader  -----------------------------------------------
@transaction.atomic
def cashflow_loans_future(
    fic_mis_date: Union[str, date, datetime],
    bucket_no: int = 2,
    cashflow_type: str = "PAYMENT_SCHEDULE",
) -> Tuple[int, int]:
    """
    Load future Loan payment schedules into Arranged_cashflows.
    
    Key differences from standard loan processing:
    - Only processes FUTURE payments (d_next_payment_date >= fic_mis_date)
    - Takes actual interest and principal amounts from payment schedule
    - No accumulation of past due amounts
    - Filters by v_instrument_type_cd="LOANS"
    - For accounts not in payment schedule: uses n_eop_bal only, no interest
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")
    
    # Force output to appear in Django terminal using print statements
    print(f"\n{'='*80}")
    print(f"STARTING LOANS FUTURE CASHFLOW PROCESSING")
    print(f"FIC MIS Date: {fic_mis_date}")
    print(f"Cashflow Type: {cashflow_type}")
    print(f"{'='*80}")

    # Get loan contracts for the snapshot date
    loan_contracts = LoanContract.objects.filter(fic_mis_date=fic_mis_date)
    total_loans = loan_contracts.count()
    
    print(f"\nTotal Loan Contracts found: {total_loans}")
    
    # Delete existing loan cashflows
    deleted_count, _ = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_loan_type="loans",
        v_cash_flow_type=cashflow_type,
    ).delete()
    
    print(f"Deleted existing cashflows: {deleted_count}")

    if not loan_contracts.exists():
        print("No loan contracts found. Exiting.")
        return deleted_count, 0

    to_create = []
    accounts_with_schedule = 0
    accounts_without_schedule = 0
    total_future_payments = 0
    accounts_with_schedule_list = []
    accounts_without_schedule_list = []
    
    print(f"\n{'='*60}")
    print("PROCESSING INDIVIDUAL LOAN CONTRACTS")
    print(f"{'='*60}")
    
    for idx, loan in enumerate(loan_contracts.iterator(), 1):
        account_num = loan.v_account_number
        print(f"\n[{idx}/{total_loans}] Processing Account: {account_num}")
        
        # Get the latest payment schedule for this loan
        latest_sched_date = (
            LoanPaymentSchedule.objects.filter(
                v_account_number=loan.v_account_number,
                v_instrument_type_cd="LOANS",
            ).aggregate(latest=Max("fic_mis_date"))["latest"]
        )
        
        if latest_sched_date:
            print(f"  ✓ Payment schedule found (latest date: {latest_sched_date})")
            
            # Get future payment schedules only (>= fic_mis_date)
            future_payments = (
                LoanPaymentSchedule.objects.filter(
                    fic_mis_date=latest_sched_date,
                    v_account_number=loan.v_account_number,
                    v_instrument_type_cd="LOANS",
                    d_next_payment_date__gte=fic_mis_date,
                ).order_by("d_next_payment_date")
            )
            
            future_count = future_payments.count()
            print(f"  ✓ Future payments found: {future_count}")
            
            if future_count > 0:
                accounts_with_schedule += 1
                accounts_with_schedule_list.append(account_num)
                total_future_payments += future_count
                
                # Calculate running balance (remaining principal after each payment)
                payment_list = list(future_payments)
                remaining_balance = loan.n_eop_bal or zero2
                
                print(f"  ✓ Starting balance: {remaining_balance}")
                
                for bucket_idx, payment in enumerate(payment_list, 1):
                    principal = payment.n_principal_payment_amnt or zero2
                    interest = payment.n_interest_payment_amt or zero2
                    
                    # Calculate remaining balance after this payment
                    remaining_balance = max(zero2, remaining_balance - principal)
                    
                    cf_date = payment.d_next_payment_date
                    if isinstance(cf_date, datetime):
                        cf_date = cf_date.date()
                    
                    print(f"    Payment {bucket_idx}: Principal={principal}, Interest={interest}, Remaining={remaining_balance}, Date={cf_date}")
                    
                    to_create.append(
                        Arranged_cashflows(
                            fic_mis_date=fic_mis_date,
                            v_account_number=loan.v_account_number,
                            v_prod_code=loan.v_prod_code,
                            v_loan_type="loans",
                            v_party_type_code=None,
                            v_cash_flow_type=cashflow_type,
                            n_cash_flow_bucket=bucket_idx,
                            d_cashflow_date=cf_date,
                            n_total_cash_flow_amount=(principal + interest).quantize(
                                Decimal("0.01"), ROUND_HALF_UP
                            ),
                            n_total_principal_payment=principal,
                            n_total_interest_payment=interest,
                            n_total_balance=remaining_balance,
                            v_ccy_code=(loan.v_ccy_code or "").upper()[:10],
                            record_count=1,
                        )
                    )
                
                continue  # Move to next loan contract
            else:
                print(f"  ⚠ Payment schedule exists but no future payments found")
        
        # Fallback: Account not in payment schedule - use n_eop_bal only, no interest
        print(f"  ⚠ No payment schedule found - using fallback (EOP balance only)")
        
        accounts_without_schedule += 1
        accounts_without_schedule_list.append(account_num)
        
        principal = loan.n_eop_bal or zero2
        interest = zero2  # No interest calculation as requested
        
        print(f"  ✓ Fallback: Principal={principal}, Interest={interest} (maturity date: {loan.d_maturity_date})")
        
        # Use maturity date as cashflow date
        cf_date = loan.d_maturity_date
        if isinstance(cf_date, datetime):
            cf_date = cf_date.date()
        
        to_create.append(
            Arranged_cashflows(
                fic_mis_date=fic_mis_date,
                v_account_number=loan.v_account_number,
                v_prod_code=loan.v_prod_code,
                v_loan_type="loans",
                v_party_type_code=None,
                v_cash_flow_type="MATURITY_FALLBACK",
                n_cash_flow_bucket=1,
                d_cashflow_date=cf_date,
                n_total_cash_flow_amount=principal.quantize(
                    Decimal("0.01"), ROUND_HALF_UP
                ),
                n_total_principal_payment=principal,
                n_total_interest_payment=interest,
                n_total_balance=zero2,
                v_ccy_code=(loan.v_ccy_code or "").upper()[:10],
                record_count=1,
            )
        )

    # Summary logging
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total Loan Contracts Processed: {total_loans}")
    print(f"Accounts with Payment Schedule: {accounts_with_schedule}")
    print(f"Accounts without Payment Schedule (Fallback): {accounts_without_schedule}")
    print(f"Total Future Payments Found: {total_future_payments}")
    print(f"Total Cashflow Records to Create: {len(to_create)}")
    
    # Show sample accounts for each category
    if accounts_with_schedule_list:
        sample_with_schedule = accounts_with_schedule_list[:5]
        print(f"\nSample accounts WITH payment schedule: {sample_with_schedule}")
        if len(accounts_with_schedule_list) > 5:
            print(f"  ... and {len(accounts_with_schedule_list) - 5} more")
    
    if accounts_without_schedule_list:
        sample_without_schedule = accounts_without_schedule_list[:5]
        print(f"\nSample accounts WITHOUT payment schedule (fallback): {sample_without_schedule}")
        if len(accounts_without_schedule_list) > 5:
            print(f"  ... and {len(accounts_without_schedule_list) - 5} more")
    
    # Bulk create cashflows
    print(f"\n{'='*60}")
    print("CREATING CASHFLOW RECORDS")
    print(f"{'='*60}")
    
    if to_create:
        print(f"Creating {len(to_create)} cashflow records...")
        
        created_objs = Arranged_cashflows.objects.bulk_create(to_create, batch_size=2000)
        created_count = len(created_objs)
        
        print(f"✓ Successfully created {created_count} cashflow records")
    else:
        created_count = 0
        print("⚠ No cashflow records to create")
    
    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETED")
    print(f"Deleted: {deleted_count} | Created: {created_count}")
    print(f"{'='*80}\n")
    
    return deleted_count, created_count
