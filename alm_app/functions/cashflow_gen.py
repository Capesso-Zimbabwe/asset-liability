
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import timedelta, date
from ..models import *
import traceback
from dateutil.relativedelta import relativedelta
from django.db import transaction, models

BATCH_SIZE = 5000

def get_payment_interval(v_amrt_term_unit, day_count_ind):
    """Determine the payment interval in days based on repayment type and day count convention."""
    # If either v_amrt_term_unit or day_count_ind is missing, default to monthly
    if not v_amrt_term_unit or not day_count_ind:
        return timedelta(days=30)

    if day_count_ind == '30/360':
        return {
            'D': timedelta(days=1),
            'W': timedelta(weeks=1),
            'M': timedelta(days=30),
            'Q': timedelta(days=90),
            'H': timedelta(days=180),
            'Y': timedelta(days=360)
        }.get(v_amrt_term_unit, timedelta(days=30))
    elif day_count_ind == '30/365':
        return {
            'D': timedelta(days=1),
            'W': timedelta(weeks=1),
            'M': timedelta(days=30),
            'Q': timedelta(days=91),
            'H': timedelta(days=182),
            'Y': timedelta(days=365)
        }.get(v_amrt_term_unit, timedelta(days=30))
    else:
        return timedelta(days=30)


logger = logging.getLogger(__name__)

def calculate_cash_flows_for_instrument(instrument):
    """
    Calculates projected cash flows for a single financial instrument.
    If a payment schedule exists, uses it directly; otherwise computes a generic schedule
    based on day count conventions, maturity dates, and interest methods.
    Results are saved to FSI_Expected_Cashflow, and logs are recorded in the Log table.
    """
    logger.info(
        f"Accessed calculate_cash_flows_for_instrument for account_number='{instrument.v_account_number}', fic_mis_date='{instrument.fic_mis_date}'."
    )
    # Log.objects.create(
    #     function_name='calculate_cash_flows_for_instrument',
    #     log_level='INFO',
    #     message=(
    #         f"Accessed calculate_cash_flows_for_instrument for account_number='{instrument.v_account_number}', "
    #         f"fic_mis_date='{instrument.fic_mis_date}'."
    #     ),
    #     status='SUCCESS'
    # )

    try:
        with transaction.atomic():
            # 1) Fetch customer info
            customer_info = stg_party_master.objects.filter(
                v_party_id=instrument.v_cust_ref_code,
                fic_mis_date__lte=instrument.fic_mis_date
            ).order_by('-fic_mis_date').first()

            party_type_code = customer_info.v_party_type_code if customer_info else None
            logger.debug(
                f"Fetched customer_info for v_cust_ref_code='{instrument.v_cust_ref_code}', party_type_code='{party_type_code}'."
            )
            # Log.objects.create(
            #     function_name='calculate_cash_flows_for_instrument',
            #     log_level='DEBUG',
            #     message=(
            #         f"Fetched customer_info for v_cust_ref_code='{instrument.v_cust_ref_code}', "
            #         f"party_type_code='{party_type_code}'."
            #     ),
            #     status='SUCCESS'
            # )

            # 2) Get latest Payment Schedule date if available
            latest_date = stg_payment_schedule.objects.filter(
                v_account_number=instrument.v_account_number
            ).aggregate(latest_date=models.Max('fic_mis_date'))['latest_date']

            payment_schedule = stg_payment_schedule.objects.filter(
                v_account_number=instrument.v_account_number,
                fic_mis_date=latest_date
            ).order_by('d_payment_date')

            logger.debug(
                f"Retrieved payment schedule for account_number='{instrument.v_account_number}', latest_date='{latest_date}', "
                f"count='{payment_schedule.count()}'."
            )
            # Log.objects.create(
            #     function_name='calculate_cash_flows_for_instrument',
            #     log_level='DEBUG',
            #     message=(
            #         f"Payment schedule for account_number='{instrument.v_account_number}', "
            #         f"fic_mis_date='{latest_date}', entries='{payment_schedule.count()}'."
            #     ),
            #     status='SUCCESS'
            # )

            # 3) Initialize variables
            balance = float(instrument.n_eop_bal) if instrument.n_eop_bal else 0.0
            starting_balance = balance

            if instrument.d_next_payment_date:
                current_date = instrument.d_next_payment_date
            else:
                if instrument.d_last_payment_date:
                    current_date = instrument.d_last_payment_date
                    if current_date < instrument.d_start_date:
                        current_date = instrument.d_start_date
                else:
                    current_date = instrument.d_start_date

            if current_date < instrument.d_start_date:
                current_date = instrument.d_start_date

            fixed_interest_rate = float(instrument.n_curr_interest_rate) if instrument.n_curr_interest_rate else 0.0
            withholding_tax = float(instrument.n_wht_percent) if instrument.n_wht_percent else 0.0
            management_fee_rate = float(instrument.v_management_fee_rate) if instrument.v_management_fee_rate else 0.0
            v_amrt_term_unit = instrument.v_amrt_term_unit
            repayment_type = instrument.v_amrt_repayment_type
            v_day_count_ind = instrument.v_day_count_ind
            d_maturity_date = instrument.d_maturity_date or (current_date + timedelta(days=360))
            cashflow_bucket = 1

            # 4) Get or create default interest method
            interest_method = Fsi_Interest_Method.objects.first()
            if not interest_method:
                interest_method = Fsi_Interest_Method.objects.create(
                    v_interest_method='Simple',
                    description="Default Simple Interest Method"
                )

            # 5) Calculate the payment interval
            payment_interval = get_payment_interval(v_amrt_term_unit, v_day_count_ind)

            # 6) Prepare management fee date with relativedelta for yearly increments
            management_fee_date = instrument.d_start_date + relativedelta(years=1)

            # We'll accumulate records in this list before bulk_create
            cashflows_to_create = []

            # 7) If a payment schedule exists, use it
            if payment_schedule.exists():
                bucket = 1
                for schedule in payment_schedule:
                    principal_payment = schedule.n_principal_payment_amt or 0.0
                    interest_payment = schedule.n_interest_payment_amt or 0.0
                    total_payment = principal_payment + interest_payment

                    balance -= principal_payment

                    cashflows_to_create.append(
                        FSI_Expected_Cashflow(
                            fic_mis_date=instrument.fic_mis_date,
                            v_account_number=instrument.v_account_number,
                            n_cash_flow_bucket=bucket,
                            d_cash_flow_date=schedule.d_payment_date,
                            n_principal_payment=principal_payment,
                            n_interest_payment=interest_payment,
                            n_cash_flow_amount=total_payment,
                            n_balance=balance,
                            v_ccy_code=instrument.v_ccy_code,
                            v_party_type_code=party_type_code,
                            v_cash_flow_type=repayment_type
                        )
                    )
                    bucket += 1

                logger.debug(
                    f"Used existing payment schedule for account_number='{instrument.v_account_number}'. "
                    f"Final balance after schedule is {balance}."
                )
                # Log.objects.create(
                #     function_name='calculate_cash_flows_for_instrument',
                #     log_level='DEBUG',
                #     message=(
                #         f"Used existing payment schedule for account_number='{instrument.v_account_number}'. "
                #         f"Final balance={balance}."
                #     ),
                #     status='SUCCESS'
                # )

            else:
                # 8) No Payment Schedule: compute based on day count, maturity, etc.
                logger.debug(
                    f"No payment schedule found for account_number='{instrument.v_account_number}'. Computing generic schedule."
                )
                # Log.objects.create(
                #     function_name='calculate_cash_flows_for_instrument',
                #     log_level='DEBUG',
                #     message=(
                #         f"No payment schedule found for account_number='{instrument.v_account_number}'. "
                #         f"Computing generic schedule."
                #     ),
                #     status='SUCCESS'
                # )

                days_to_maturity = (d_maturity_date - current_date).days
                interval_days = payment_interval.days or 1
                periods = (days_to_maturity // interval_days) + 1
                if periods < 1:
                    periods = 1

                fixed_principal_payment = round(starting_balance / periods, 2)
                total_principal_paid = 0

                while current_date <= d_maturity_date:
                    if v_day_count_ind == '30/360':
                        day_count_factor = 360
                    else:
                        day_count_factor = 365

                    # Determine interest payment
                    interest_payment = 0.0
                    if interest_method.v_interest_method == 'Simple':
                        interest_payment = balance * fixed_interest_rate * (interval_days / day_count_factor)
                    elif interest_method.v_interest_method == 'Compound':
                        rate_per_period = fixed_interest_rate / (day_count_factor / interval_days)
                        interest_payment = balance * ((1 + rate_per_period) ** 1 - 1)
                    elif interest_method.v_interest_method == 'Amortized':
                        interest_rate_per_period = fixed_interest_rate / (day_count_factor / interval_days)
                        if interest_rate_per_period == 0 or periods <= 0:
                            raise ValueError("Invalid interest rate or periods for amortized calculation")

                        total_payment_for_period = starting_balance * (
                            interest_rate_per_period
                            / (1 - (1 + interest_rate_per_period) ** -periods)
                        )
                        interest_payment = balance * interest_rate_per_period
                        principal_payment = total_payment_for_period - interest_payment
                    else:
                        # Floating or default to Simple
                        if interest_method.v_interest_method == 'Floating':
                            variable_rate = (
                                instrument.n_curr_interest_rate
                                + getattr(instrument, 'n_variable_rate_margin', 0)
                            )
                            interest_payment = balance * variable_rate * (interval_days / day_count_factor)
                        else:
                            interest_payment = balance * fixed_interest_rate * (interval_days / day_count_factor)

                    # Withholding tax
                    wht_payment = interest_payment * withholding_tax
                    interest_payment_net = interest_payment - wht_payment

                    # Determine principal payment if not amortized above
                    if interest_method.v_interest_method != 'Amortized':
                        if repayment_type.lower() == 'bullet':
                            if periods == 1:
                                principal_payment = balance
                            else:
                                principal_payment = 0.0
                        elif repayment_type.lower() == 'amortized':
                            principal_payment = fixed_principal_payment
                        else:
                            principal_payment = 0.0

                    total_principal_paid += principal_payment

                    # Management fee
                    management_fee_net = 0.0
                    if (
                        current_date.year == management_fee_date.year
                        and current_date.month == management_fee_date.month
                        and management_fee_rate
                    ):
                        raw_fee = balance * management_fee_rate
                        wht_fee = raw_fee * withholding_tax
                        management_fee_net = raw_fee - wht_fee
                        management_fee_date += relativedelta(years=1)

                    total_payment = principal_payment + interest_payment_net + management_fee_net

                    cashflows_to_create.append(
                        FSI_Expected_Cashflow(
                            fic_mis_date=instrument.fic_mis_date,
                            v_account_number=instrument.v_account_number,
                            n_cash_flow_bucket=cashflow_bucket,
                            d_cash_flow_date=current_date,
                            n_principal_payment=principal_payment,
                            n_interest_payment=interest_payment + management_fee_net,
                            n_cash_flow_amount=total_payment,
                            n_balance=balance - principal_payment,
                            v_cash_flow_type=repayment_type,
                            management_fee_added=management_fee_net,
                            v_ccy_code=instrument.v_ccy_code,
                            v_loan_type=instrument.v_loan_type,
                            v_cust_ref_code=instrument.v_cust_ref_code,
                            v_party_type_code=party_type_code
                        )
                    )

                    balance -= principal_payment
                    current_date += payment_interval
                    cashflow_bucket += 1
                    periods -= 1

            # 9) Bulk-create in batches
            BATCH_SIZE = 1000
            for i in range(0, len(cashflows_to_create), BATCH_SIZE):
                FSI_Expected_Cashflow.objects.bulk_create(cashflows_to_create[i : i + BATCH_SIZE])

            logger.info(
                f"Finished calculating cash flows for account_number='{instrument.v_account_number}'. "
                f"Final balance={balance}."
            )
            # Log.objects.create(
            #     function_name='calculate_cash_flows_for_instrument',
            #     log_level='INFO',
            #     message=(
            #         f"Finished calculating cash flows for account_number='{instrument.v_account_number}'. "
            #         f"Final balance={balance}."
            #     ),
            #     status='SUCCESS'
            # )

    except Exception as e:
        error_details = traceback.format_exc()
        error_message = (
            f"Account {instrument.v_account_number} skipped due to error: {str(e)}"
        )
        
        



logger = logging.getLogger(__name__)

def project_cash_flows(fic_mis_date):
    """
    Projects cash flows for all instruments on a given fic_mis_date.
    Uses a ThreadPoolExecutor for parallel processing.
    Logs actions to the Log table but does not integrate audit trails.
    """
    logger.info(f"Accessed project_cash_flows for fic_mis_date='{fic_mis_date}'.")
    
    try:
        # Step 1: Clear existing cash flows for this date
        deleted_count = FSI_Expected_Cashflow.objects.filter(fic_mis_date=fic_mis_date).delete()
        logger.debug(f"Deleted existing cash flows for fic_mis_date='{fic_mis_date}': {deleted_count}.")
        

        # Step 2: Fetch all instruments for the date
        instruments = FSI_Loans_Processing.objects.filter(fic_mis_date=fic_mis_date)
        if not instruments.exists():
            error_message = f"No loans found for the given fic_mis_date: {fic_mis_date}"
            logger.warning(error_message)
            
            print(f"ERROR: {error_message}")
            return 0

        logger.info(
            f"Found {instruments.count()} instruments to process for fic_mis_date='{fic_mis_date}'."
        )
        

        # Step 3: Parallel execution of cash flow calculations
        num_threads = min(10, len(instruments))
        logger.debug(
            f"Initializing ThreadPoolExecutor with max_workers='{num_threads}'."
        )
        

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(calculate_cash_flows_for_instrument, instr) for instr in instruments]
            for future in futures:
                future.result()  # This blocks until the function is done

        # Step 4: Count the total projected cash flows
        total_cash_flows = FSI_Expected_Cashflow.objects.filter(fic_mis_date=fic_mis_date).count()
        success_message = (
            f"Total of {total_cash_flows} cash flows projected for {instruments.count()} loans "
            f"for MIS date='{fic_mis_date}'."
        )
        logger.info(success_message)
        
        print(f"INFO: {success_message}")
        return 1

    except Exception as e:
        error_details = traceback.format_exc()
        error_message = f"ERROR: Error occurred in project_cash_flows for fic_mis_date='{fic_mis_date}': {str(e)}"
        logger.error(error_message)
        
        print(error_message)
        return 0