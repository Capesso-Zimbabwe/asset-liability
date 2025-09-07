# core/alm_app/functions/cashflow_loaders.py
import logging
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Union

from django.db import transaction
from django.db.models import Max, Sum, Q

from .cashflow_investments import _normalize_date
from staging.models import LoanContract, CreditLine, LoanPaymentSchedule
from ..models import Arranged_cashflows, ProductBalance

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

# Add these imports at the top with existing imports
from django.db.models import Sum, Q
from ..models import ProductBalance

# Add this function at the end of the file
@transaction.atomic
def balance_cashflows_to_target(
    fic_mis_date: Union[str, date, datetime]
) -> Tuple[int, Decimal]:
    """
    Balance loan cashflows to match ProductBalance target where v_prod_type="Loans".
    
    This function:
    1. Retrieves target balance from ProductBalance for Loans
    2. Calculates current total from Arranged_cashflows for loan types
    3. Distributes any difference equally across all loan accounts
    4. Applies adjustments to each account's last cashflow date
    5. Provides comprehensive logging of the balancing process
    
    Args:
        fic_mis_date: The reporting date to process
        
    Returns:
        Tuple of (records_updated, total_adjustment_applied)
    """
    fic_mis_date = _normalize_date(fic_mis_date)
    zero2 = Decimal("0.00")
    
    print(f"\n{'='*80}")
    print("🎯 BALANCE CASHFLOWS TO TARGET")
    print(f"{'='*80}")
    print(f"📅 Processing date: {fic_mis_date}")
    
    # Step 1: Get target balance from ProductBalance
    print(f"\n{'─'*60}")
    print("📊 RETRIEVING TARGET BALANCE")
    print(f"{'─'*60}")
    
    try:
        product_balance = ProductBalance.objects.filter(
            fic_mis_date=fic_mis_date,
            v_prod_type__iexact="Loans"
        ).first()
        
        if not product_balance:
            print("❌ ERROR: No ProductBalance record found for Loans")
            print(f"   Date: {fic_mis_date}")
            print(f"   Product Type: Loans (case-insensitive)")
            log.error("No ProductBalance found for fic_mis_date=%s, v_prod_type=Loans", fic_mis_date)
            return 0, zero2
            
        target_balance = product_balance.n_balance
        print(f"✅ Target balance found:")
        print(f"   Product Code: {product_balance.v_prod_code}")
        print(f"   Product Name: {product_balance.v_prod_name}")
        print(f"   Product Type: {product_balance.v_prod_type}")
        print(f"   Target Balance: {target_balance:,.2f}")
        
    except Exception as e:
        print(f"❌ ERROR retrieving ProductBalance: {str(e)}")
        log.error("Error retrieving ProductBalance: %s", str(e))
        return 0, zero2
    
    # Step 2: Calculate current loan cashflows total
    print(f"\n{'─'*60}")
    print("💰 CALCULATING CURRENT CASHFLOWS")
    print(f"{'─'*60}")
    
    try:
        # Get current total with case-insensitive loan type matching
        current_total_result = Arranged_cashflows.objects.filter(
            Q(v_loan_type__iexact="loans") | Q(v_loan_type__iexact="Loans"),
            fic_mis_date=fic_mis_date
        ).aggregate(total=Sum('n_total_cash_flow_amount'))
        
        current_total = current_total_result['total'] or zero2
        
        # Get sample cashflows for display
        sample_cashflows = Arranged_cashflows.objects.filter(
            Q(v_loan_type__iexact="loans") | Q(v_loan_type__iexact="Loans"),
            fic_mis_date=fic_mis_date
        ).values(
            'd_cashflow_date', 'n_total_cash_flow_amount'
        ).order_by('d_cashflow_date')[:10]
        
        print(f"✅ Current cashflows analysis:")
        print(f"   Total Amount: {current_total:,.2f}")
        print(f"   Sample cashflows by date:")
        for cf in sample_cashflows:
            print(f"     {cf['d_cashflow_date']}: {cf['n_total_cash_flow_amount']:,.2f}")
        if len(sample_cashflows) == 10:
            print("     ... (showing first 10 records)")
            
    except Exception as e:
        print(f"❌ ERROR calculating current cashflows: {str(e)}")
        log.error("Error calculating current cashflows: %s", str(e))
        return 0, zero2
    
    # Step 3: Calculate difference and determine if adjustment needed
    print(f"\n{'─'*60}")
    print("⚖️  BALANCE COMPARISON")
    print(f"{'─'*60}")
    
    difference = target_balance - current_total
    print(f"Target Balance:    {target_balance:,.2f}")
    print(f"Current Total:     {current_total:,.2f}")
    print(f"Difference:        {difference:,.2f}")
    
    if difference == zero2:
        print("✅ RESULT: Already perfectly balanced - NO ADJUSTMENT NEEDED")
        print(f"   Final total: {current_total:,.2f} (unchanged)")
        print("   Matches ProductBalance: YES")
        log.info("Cashflows already balanced for %s", fic_mis_date)
        return 0, zero2
    
    print(f"🔧 ADJUSTMENT REQUIRED")
    print(f"   Will distribute {difference:,.2f} equally across all loan accounts")
    
    # Step 4: Get all loan accounts for adjustment
    print(f"\n{'─'*60}")
    print("🏦 ANALYZING LOAN ACCOUNTS")
    print(f"{'─'*60}")
    
    try:
        # Get unique accounts with their last cashflow dates
        loan_accounts = Arranged_cashflows.objects.filter(
            Q(v_loan_type__iexact="loans") | Q(v_loan_type__iexact="Loans"),
            fic_mis_date=fic_mis_date
        ).values('v_account_number').annotate(
            last_date=Max('d_cashflow_date'),
            account_total=Sum('n_total_cash_flow_amount')
        ).order_by('v_account_number')
        
        account_count = len(loan_accounts)
        
        if account_count == 0:
            print("❌ ERROR: No loan accounts found for adjustment")
            log.error("No loan accounts found for fic_mis_date=%s", fic_mis_date)
            return 0, zero2
            
        print(f"✅ Found {account_count} loan accounts")
        
        # Show sample accounts
        sample_accounts = list(loan_accounts[:10])
        print(f"   Sample accounts:")
        for acc in sample_accounts:
            print(f"     {acc['v_account_number']}: Last date {acc['last_date']}, Total {acc['account_total']:,.2f}")
        if account_count > 10:
            print(f"     ... and {account_count - 10} more accounts")
            
    except Exception as e:
        print(f"❌ ERROR analyzing loan accounts: {str(e)}")
        log.error("Error analyzing loan accounts: %s", str(e))
        return 0, zero2
    
    # Step 5: Calculate equal adjustment per account
    print(f"\n{'─'*60}")
    print("🧮 CALCULATING ADJUSTMENTS")
    print(f"{'─'*60}")
    
    adjustment_per_account = (difference / account_count).quantize(
        Decimal("0.01"), ROUND_HALF_UP
    )
    
    print(f"Total difference to distribute: {difference:,.2f}")
    print(f"Number of accounts: {account_count}")
    print(f"Adjustment per account: {adjustment_per_account:,.2f}")
    print(f"Total adjustment planned: {adjustment_per_account * account_count:,.2f}")
    
    # Check for rounding variance
    total_planned = adjustment_per_account * account_count
    rounding_variance = difference - total_planned
    if rounding_variance != zero2:
        print(f"⚠️  Rounding variance: {rounding_variance:,.2f}")
        print("   (Will be absorbed in the adjustment process)")
    
    # Step 6: Apply adjustments
    print(f"\n{'─'*60}")
    print("🔄 APPLYING ADJUSTMENTS")
    print(f"{'─'*60}")
    
    records_updated = 0
    total_adjustment_applied = zero2
    failed_updates = []
    
    print(f"Updating cashflows for {account_count} accounts...")
    print("\nFirst 10 account adjustments:")
    print(f"{'Account':<20} {'Last Date':<12} {'Before':<15} {'Adjustment':<12} {'After':<15}")
    print(f"{'-'*20} {'-'*12} {'-'*15} {'-'*12} {'-'*15}")
    
    for i, account in enumerate(loan_accounts):
        try:
            account_number = account['v_account_number']
            last_date = account['last_date']
            
            # Get the specific cashflow record to update
            cashflow_record = Arranged_cashflows.objects.filter(
                Q(v_loan_type__iexact="loans") | Q(v_loan_type__iexact="Loans"),
                fic_mis_date=fic_mis_date,
                v_account_number=account_number,
                d_cashflow_date=last_date
            ).first()
            
            if not cashflow_record:
                failed_updates.append({
                    'account': account_number,
                    'error': 'Cashflow record not found'
                })
                continue
                
            # Store original amount
            original_amount = cashflow_record.n_total_cash_flow_amount
            
            # Apply adjustment
            new_amount = (original_amount + adjustment_per_account).quantize(
                Decimal("0.01"), ROUND_HALF_UP
            )
            
            cashflow_record.n_total_cash_flow_amount = new_amount
            cashflow_record.save()
            
            records_updated += 1
            total_adjustment_applied += adjustment_per_account
            
            # Display first 10 adjustments
            if i < 10:
                print(f"{account_number:<20} {last_date} {original_amount:>13,.2f} {adjustment_per_account:>10,.2f} {new_amount:>13,.2f}")
                
        except Exception as e:
            failed_updates.append({
                'account': account_number,
                'error': str(e)
            })
            log.error("Failed to update account %s: %s", account_number, str(e))
    
    if account_count > 10:
        print(f"... and {account_count - 10} more accounts updated")
    
    # Step 7: Report results
    print(f"\n{'─'*60}")
    print("📈 ADJUSTMENT SUMMARY")
    print(f"{'─'*60}")
    
    print(f"✅ Successfully updated: {records_updated} records")
    print(f"💰 Total adjustment applied: {total_adjustment_applied:,.2f}")
    
    if failed_updates:
        print(f"❌ Failed updates: {len(failed_updates)}")
        for failure in failed_updates[:5]:  # Show first 5 failures
            print(f"   {failure['account']}: {failure['error']}")
        if len(failed_updates) > 5:
            print(f"   ... and {len(failed_updates) - 5} more failures")
    
    # Step 8: Final verification
    print(f"\n{'─'*60}")
    print("🔍 FINAL VERIFICATION")
    print(f"{'─'*60}")
    
    try:
        # Recalculate total after adjustments
        final_total_result = Arranged_cashflows.objects.filter(
            Q(v_loan_type__iexact="loans") | Q(v_loan_type__iexact="Loans"),
            fic_mis_date=fic_mis_date
        ).aggregate(total=Sum('n_total_cash_flow_amount'))
        
        final_total = final_total_result['total'] or zero2
        final_difference = target_balance - final_total
        
        print(f"Target Balance:     {target_balance:,.2f}")
        print(f"Final Total:        {final_total:,.2f}")
        print(f"Remaining Diff:     {final_difference:,.2f}")
        
        if abs(final_difference) <= Decimal("0.01"):
            print("✅ SUCCESS: Cashflows now balanced to target!")
        else:
            print(f"⚠️  WARNING: Remaining difference of {final_difference:,.2f}")
            
    except Exception as e:
        print(f"❌ ERROR in final verification: {str(e)}")
        log.error("Error in final verification: %s", str(e))
    
    print(f"\n{'='*80}")
    print(f"🎯 BALANCE ADJUSTMENT COMPLETED")
    print(f"Records Updated: {records_updated} | Total Adjustment: {total_adjustment_applied:,.2f}")
    print(f"{'='*80}\n")
    
    log.info(
        "Balance adjustment completed for %s: %d records updated, %s total adjustment",
        fic_mis_date, records_updated, total_adjustment_applied
    )
    
    return records_updated, total_adjustment_applied
