from datetime import timedelta, datetime
from decimal import Decimal
from dateutil.relativedelta import relativedelta  # To handle months and years
from django.db.models import Sum, Max
from ..models import *

def filter_products(process_name, fic_mis_date):
    """
    Retrieve products from Ldn_Product_Master based on filters associated with the process.
    Consider the highest fic_mis_date for each product code, then apply filters on f_prod_rate_sensitivity.
    """
    # Get the process and associated filters
    
    try:
        process = Process.objects.get(name=process_name)
        filters = process.filters.all()
        print(f"Filters for process '{process_name}': {[f'{f.field_name} {f.condition} {f.value}' for f in filters]}")
    except Process.DoesNotExist:
        print(f"Process '{process_name}' does not exist.")
        return []

    # Step 1: Get the latest fic_mis_date per product code
    latest_dates_subquery = (
        Stg_Product_Master.objects
        .values('v_prod_code')
        .annotate(max_fic_mis_date=Max('fic_mis_date'))
    )

    # Step 2: Use this subquery to get the latest entries for each product code
    latest_products = Stg_Product_Master.objects.filter(
        v_prod_code__in=[item['v_prod_code'] for item in latest_dates_subquery],
        fic_mis_date__in=[item['max_fic_mis_date'] for item in latest_dates_subquery]
    )

    # Step 3: Apply the process-specific filter on f_prod_rate_sensitivity and other fields
    filtered_products = latest_products
    for filter in filters:
        field = filter.field_name
        condition = filter.condition
        value = filter.value

        print(f"Applying filter - Field: {field}, Condition: {condition}, Value: {value}")

        if condition == 'equals':
            filtered_products = filtered_products.filter(**{f"{field}__iexact": value})
        elif condition == 'contains':
            filtered_products = filtered_products.filter(**{f"{field}__icontains": value})
        elif condition == 'greater_than':
            filtered_products = filtered_products.filter(**{f"{field}__gt": value})
        elif condition == 'less_than':
            filtered_products = filtered_products.filter(**{f"{field}__lt": value})

    # Step 4: Match these products in product_level_cashflows
    final_product_codes = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_prod_code__in=filtered_products.values_list('v_prod_code', flat=True)
    ).values_list('v_prod_code', flat=True).distinct()

    print(f"Filtered product codes with highest fic_mis_date where conditions are met: {list(final_product_codes)}")
    return final_product_codes



def calculate_time_buckets_and_spread(process_name, fic_mis_date):
    """
    Aggregate cashflow data from product_level_cashflows table and spread amounts across time buckets.
    """
    if isinstance(fic_mis_date, str):
        fic_mis_date = datetime.strptime(fic_mis_date, "%Y-%m-%d").date()

    # Step 1: Reset the record_count for all rows in product_level_cashflows for the given fic_mis_date
    reset_count = Arranged_cashflows.objects.filter(fic_mis_date=fic_mis_date).update(record_count=0)
    print(f"Reset 'record_count' for {reset_count} rows in product_level_cashflows for fic_mis_date {fic_mis_date}")

    # Step 2: Get filtered product codes
    filtered_product_codes = filter_products(process_name, fic_mis_date)
    
    # Step 3: Update product_level_cashflows based on filtered product codes
    updated_rows = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_prod_code__in=filtered_product_codes
    ).update(record_count=1)
    print(f"Updated 'record_count' for {updated_rows} rows in product_level_cashflows for fic_mis_date {fic_mis_date}")

    # Step 4: Fetch process information and delete existing data
    try:
        process = Process.objects.get(name=process_name)
    except Process.DoesNotExist:
        print(f"Process '{process_name}' does not exist.")
        return

    Aggregated_Acc_CashflowByBuckets.objects.filter(fic_mis_date=fic_mis_date, process_name=process_name).delete()
    TimeBucketMaster.objects.filter(process_name=process_name).delete()
    print(f"Deleted existing records for process '{process_name}' and fic_mis_date {fic_mis_date}")

    # Step 5: Fetch TimeBuckets and product level cashflows
    time_buckets = TimeBuckets.objects.all().order_by('serial_number')
    if not time_buckets.exists():
        print("No time buckets found.")
        return

    # Only process cashflows with record_count set to 1
    cashflows = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        v_prod_code__in=filtered_product_codes,
        record_count=1
    )
    if not cashflows.exists():
        print(f"No cashflows found for fic_mis_date: {fic_mis_date}")
        return

    # Process each cashflow item
    for cashflow in cashflows:
        account_number = cashflow.v_account_number
        product_code = cashflow.v_prod_code
        currency_code = cashflow.v_ccy_code
        v_loan_type=cashflow.v_loan_type
        v_party_type_code=cashflow.v_party_type_code

        financial_elements = ['n_total_cash_flow_amount', 'n_total_principal_payment', 'n_total_interest_payment']

        for element in financial_elements:
            bucket_data = {f"bucket_{i+1}": 0 for i in range(len(time_buckets))}
            bucket_start_date = fic_mis_date

            for idx, bucket in enumerate(time_buckets):
                if bucket.multiplier == 'Days':
                    bucket_end_date = bucket_start_date + timedelta(days=bucket.frequency)
                elif bucket.multiplier == 'Months':
                    bucket_end_date = bucket_start_date + relativedelta(months=bucket.frequency)
                elif bucket.multiplier == 'Years':
                    bucket_end_date = bucket_start_date + relativedelta(years=bucket.frequency)
                else:
                    raise ValueError(f"Unsupported multiplier: {bucket.multiplier}")

                cashflows_in_bucket = cashflows.filter(
                    v_account_number=account_number,
                    d_cashflow_date__gte=bucket_start_date,
                    d_cashflow_date__lte=bucket_end_date
                )
                cashflows_aggregated = cashflows_in_bucket.aggregate(Sum(element))
                bucket_data[f"bucket_{idx+1}"] += cashflows_aggregated[f"{element}__sum"] or 0

                # Update TimeBucketMaster for reference
                TimeBucketMaster.objects.update_or_create(
                    process_name=process_name,
                    bucket_number=idx+1,
                    defaults={
                        'start_date': bucket_start_date,
                        'end_date': bucket_end_date,
                    }
                )
                bucket_start_date = bucket_end_date + timedelta(days=1)

            # Store the aggregated bucket data
            time_bucket_master = TimeBucketMaster.objects.filter(process_name=process_name).first()
            Aggregated_Acc_CashflowByBuckets.objects.update_or_create(
                fic_mis_date=fic_mis_date,
                process_name=process_name,
                v_account_number=account_number,
                v_prod_code=product_code,
                v_ccy_code=currency_code,
                v_loan_type=v_loan_type,
                v_party_type_code=v_party_type_code,
                financial_element=element,
                defaults={
                    **bucket_data,
                    'time_bucket_master': time_bucket_master
                }
            )

    print(f"Successfully aggregated cashflows for process '{process_name}' and fic_mis_date {fic_mis_date}.")








def calculate_behavioral_pattern_distribution(process_name, fic_mis_date):
    """
    Distribute amounts across time buckets based on behavioral patterns.
    Deletes previous entries if the process is rerun for the same fic_mis_date.
    """
    if isinstance(fic_mis_date, str):
        fic_mis_date = datetime.strptime(fic_mis_date, "%Y-%m-%d").date()

    # Step 1: Delete previous entries for the process_name and fic_mis_date
    Aggregated_Acc_CashflowByBuckets.objects.filter(
        process_name=process_name, 
        fic_mis_date=fic_mis_date
    ).delete()

    print(f"Deleted existing records for process '{process_name}' and fic_mis_date {fic_mis_date}")

    # Fetch behavioral patterns
    behavioral_patterns = BehavioralPatternConfig.objects.all()

    if not behavioral_patterns.exists():
        print("No behavioral patterns found.")
        return

    # Fetch the time buckets associated with the process
    time_buckets = TimeBuckets.objects.all().order_by('serial_number')

    if not time_buckets.exists():
        print("No time buckets found.")
        return

    total_time_buckets = time_buckets.count()

    for pattern in behavioral_patterns:
        print(f"Applying behavioral pattern for product type '{pattern.v_prod_type}'.")

        # Fetch the entries associated with this pattern
        entries = BehavioralPatternEntry.objects.filter(pattern_id=pattern.id).order_by('order')

        total_entries = entries.count()

        # Check if the number of behavioral entries matches the number of time buckets
        if total_entries != total_time_buckets:
            print(f"Mismatch between behavioral entries ({total_entries}) and time buckets ({total_time_buckets}). Skipping pattern '{pattern.v_prod_type}'.")
            continue  # Skip this pattern since it doesn't match the time buckets

        # Fetch product level cashflows that match the product type
        cashflows = Arranged_cashflows.objects.filter(
            fic_mis_date=fic_mis_date,
            v_prod_code__in=Stg_Product_Master.objects.filter(v_prod_type=pattern.v_prod_type).values_list('v_prod_code', flat=True)
        )

        if not cashflows.exists():
            print(f"No cashflows found for fic_mis_date: {fic_mis_date} and product type: {pattern.v_prod_type}")
            continue

        # Process each cashflow item and distribute amounts based on percentage
        for cashflow in cashflows:
            account_number = cashflow.v_account_number
            product_code = cashflow.v_prod_code
            currency_code = cashflow.v_ccy_code
            v_loan_type=cashflow.v_loan_type
            v_party_type_code=cashflow.v_party_type_code

            financial_elements = ['n_total_cash_flow_amount', 'n_total_principal_payment', 'n_total_interest_payment']

            for element in financial_elements:
                total_value = getattr(cashflow, element)

                # Edge case: skip if total_value is None or zero
                if total_value is None or total_value == 0:
                    continue

                # Distribute across the entries based on percentage
                for entry, time_bucket in zip(entries, time_buckets):
                    percentage = entry.percentage / Decimal(100.0)  # Convert percentage to a ratio
                    distributed_value = total_value * percentage

                    # Store the distribution in TimeBucketMaster (optional for record keeping)
                    time_bucket_master, _ = TimeBucketMaster.objects.update_or_create(
                        process_name=process_name,
                        bucket_number=time_bucket.serial_number,  # Use 'serial_number' from time buckets
                        defaults={
                            'start_date': time_bucket.start_date,
                            'end_date': time_bucket.end_date,
                        }
                    )

                    # Fetch existing value in the bucket and accumulate
                    aggregated_entry, created = Aggregated_Acc_CashflowByBuckets.objects.get_or_create(
                        fic_mis_date=fic_mis_date,
                        process_name=process_name,
                        v_account_number=account_number,
                        v_prod_code=product_code,
                        v_ccy_code=currency_code,
                        v_loan_type=v_loan_type,
                        v_party_type_code=v_party_type_code,
                        financial_element=element,
                    )

                    # Fetch the current value, and default to Decimal(0) if it is None
                    current_value = getattr(aggregated_entry, f'bucket_{time_bucket.serial_number}', None)
                    if current_value is None:
                        current_value = Decimal(0)

                    # Accumulate the value in the bucket
                    new_value = current_value + distributed_value

                    # Update the entry with the new accumulated value
                    setattr(aggregated_entry, f'bucket_{time_bucket.serial_number}', new_value)
                    aggregated_entry.time_bucket_master = time_bucket_master  # Correctly assign the TimeBucketMaster instance
                    aggregated_entry.save()

    print(f"Successfully distributed amounts using behavioral patterns for process '{process_name}' and fic_mis_date {fic_mis_date}.")