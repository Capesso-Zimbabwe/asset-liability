import concurrent.futures
from django.db import transaction
from django.db.models import Sum
from ..models import Arranged_cashflows, FSI_Expected_Cashflow, FSI_Loans_Processing
import traceback



# Function to handle insertion of records in chunks
def insert_product_level_cashflows_chunk(records_chunk):
    with transaction.atomic():
        for record in records_chunk:
            try:
                Arranged_cashflows.objects.create(
                    fic_mis_date=record['fic_mis_date'],
                    v_prod_code=record['v_prod_code'],
                    v_account_number=record['v_account_number'],  # Add v_account_number to insert it
                    d_cashflow_date=record['d_cashflow_date'],
                    n_cash_flow_bucket=record['n_cash_flow_bucket'],
                    n_total_cash_flow_amount=record['n_total_cash_flow_amount'],
                    n_total_principal_payment=record['n_total_principal_payment'],
                    n_total_interest_payment=record['n_total_interest_payment'],
                    n_total_balance=record['n_total_balance'],
                    v_ccy_code=record['v_ccy_code'],
                    v_party_type_code=record['v_party_type_code'],
                    v_loan_type=record['v_loan_type'],  # Insert loan type
                    v_cash_flow_type=record['v_cash_flow_type'],  # Insert cash flow type
                )
            except Exception as e:
                print(f"Error inserting record for product {record['v_prod_code']}: {str(e)}")


def aggregate_cashflows_to_product_level(fic_mis_date, chunk_size=100):
    """
    Aggregate cashflows from FSI_Expected_Cashflow to the product level by v_prod_code and d_cashflow_date,
    then insert the aggregated results into product_level_cashflows in chunks using multi-threading.
    The data will be inserted in ascending order of n_cash_flow_bucket.
    """
    inserted_rows = 0
    skipped_rows = 0

    # Step 1: Delete existing product-level cashflows for the same fic_mis_date
    Arranged_cashflows.objects.filter(fic_mis_date=fic_mis_date).delete()

    # Step 2: Aggregate the cashflows by product code, account number, and cashflow date
    cashflow_data = (
        FSI_Expected_Cashflow.objects.filter(fic_mis_date=fic_mis_date)
        .values('v_account_number', 'fic_mis_date', 'd_cash_flow_date', 'n_cash_flow_bucket', 'v_ccy_code', 'v_loan_type','v_party_type_code', 'v_cash_flow_type')  # Include v_loan_type and V_CASH_FLOW_TYPE
        .annotate(
            n_total_cash_flow_amount=Sum('n_cash_flow_amount'),
            n_total_principal_payment=Sum('n_principal_payment'),
            n_total_interest_payment=Sum('n_interest_payment'),
            n_total_balance=Sum('n_balance')
        )
        .order_by('n_cash_flow_bucket')  # Order by n_cash_flow_bucket in ascending order
    )

    if not cashflow_data.exists():
        print(f"No data found for aggregation for fic_mis_date: {fic_mis_date}")
        return None

    # Step 3: Join with Ldn_Financial_Instrument to get v_prod_code using v_account_number
    aggregated_records = []
    for cashflow in cashflow_data:
        try:
            # Get the corresponding product code from Ldn_Financial_Instrument
            product_data = FSI_Loans_Processing.objects.filter(
                v_account_number=cashflow['v_account_number']
            ).values('v_prod_code').first()

            if product_data:
                v_prod_code = product_data['v_prod_code']
                aggregated_records.append({
                    'fic_mis_date': cashflow['fic_mis_date'],
                    'v_prod_code': v_prod_code,
                    'v_account_number': cashflow['v_account_number'],  # Include v_account_number
                    'd_cashflow_date': cashflow['d_cash_flow_date'],
                    'n_cash_flow_bucket': cashflow['n_cash_flow_bucket'],  # Include the cash flow bucket
                    'n_total_cash_flow_amount': cashflow['n_total_cash_flow_amount'],
                    'n_total_principal_payment': cashflow['n_total_principal_payment'],
                    'n_total_interest_payment': cashflow['n_total_interest_payment'],
                    'n_total_balance': cashflow['n_total_balance'],
                    'v_ccy_code': cashflow['v_ccy_code'],
                    'v_party_type_code': cashflow['v_party_type_code'],
                

                    'v_loan_type': cashflow['v_loan_type'],  # Include loan type from FSI_Expected_Cashflow
                    'v_cash_flow_type': cashflow['v_cash_flow_type'],  # Include cash flow type from FSI_Expected_Cashflow
                })
            else:
                print(f"No matching product found for account number: {cashflow['v_account_number']}")
                skipped_rows += 1

        except Exception as e:
            print(f"Error processing row for account {cashflow['v_account_number']}: {str(e)}")
            skipped_rows += 1

    # Step 4: Split the aggregated records into chunks for multi-threading
    total_records = len(aggregated_records)
    record_chunks = [aggregated_records[i:i + chunk_size] for i in range(0, total_records, chunk_size)]

    # Step 5: Use multi-threading to insert records concurrently
    if record_chunks:  # Ensure there are records to process
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(insert_product_level_cashflows_chunk, record_chunks)

        inserted_rows = total_records - skipped_rows
        print(f"{inserted_rows} records for {fic_mis_date} inserted successfully into product_level_cashflows.")
    else:
        print(f"No aggregated records found for {fic_mis_date}.")

    return inserted_rows, skipped_rows