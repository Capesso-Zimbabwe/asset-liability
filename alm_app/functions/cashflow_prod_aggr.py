

from datetime import datetime
from django.db.models import Sum
from .bucket_column_sync import sync_bucket_columns
from ..models import *

# ─────────────────────────────────────────────────────────────────────────────
# 3. AGGREGATE BY PRODUCT CODE
# ─────────────────────────────────────────────────────────────────────────────

from django.db import transaction
from django.db.models import Sum

# …(imports above stay unchanged, including _get_default_process)…

def aggregate_by_prod_code(fic_mis_date):
    """
    Summarise bucket‑spread cashflows into a product‑code level base table.

    Parameters
    ----------
    fic_mis_date : str | datetime.date
        FIC‑MIS run‑date of the data to aggregate.

    Notes
    -----
    * Detects the single ``Process`` row automatically; no `process_name`
      argument is needed (or allowed) any more.
    * Remains **idempotent** — existing rows for the same run‑date are deleted
      before fresh ones are bulk‑created inside an atomic transaction.
    """

    # ── normalise date input
    if isinstance(fic_mis_date, str):
        fic_mis_date = datetime.strptime(fic_mis_date, "%Y-%m-%d").date()

    

    process_name = 'contractual'

    try:
        with transaction.atomic():
            # 1) wipe any prior aggregation for this run
            deleted = Aggregated_Prod_Cashflow_Base.objects.filter(
                fic_mis_date=fic_mis_date,
                process_name=process_name,
            ).delete()[0]
            if deleted:
                print(
                    f"[prod_agg] Cleared {deleted} existing rows for "
                    f"{fic_mis_date} / '{process_name}'."
                )

            # 2) get the bucket‑level rows produced earlier
            bucket_qs = Aggregated_Acc_CashflowByBuckets.objects.filter(
                fic_mis_date=fic_mis_date,
                process_name=process_name,
            )
            if not bucket_qs.exists():
                print(
                    f"[prod_agg] No bucket data found for "
                    f"{fic_mis_date} / '{process_name}'."
                )
                return

            # 3) roll‑up by product‑code & dimensions
            grouped = bucket_qs.values(
                "v_prod_code",
                "v_ccy_code",
                "v_loan_type",
                "v_party_type_code",
                "financial_element",
            ).annotate(**{f"bucket_{i}": Sum(f"bucket_{i}") for i in range(1, 51)})

            # 4) foreign‑key look‑ups (safe if *None*)
            tb_master = TimeBucketMaster.objects.filter(
                process_name=process_name
            ).first()

            # 5) bulk‑create product‑level rows
            objs = []
            for rec in grouped:
                rec_buckets = {
                    f"bucket_{i}": rec.get(f"bucket_{i}") for i in range(1, 51)
                }
                cf_bucket_ref = bucket_qs.filter(
                    v_prod_code=rec["v_prod_code"],
                    v_ccy_code=rec["v_ccy_code"],
                    v_loan_type=rec["v_loan_type"],
                    v_party_type_code=rec["v_party_type_code"],
                    financial_element=rec["financial_element"],
                ).first()

                objs.append(
                    Aggregated_Prod_Cashflow_Base(
                        fic_mis_date=fic_mis_date,
                        process_name=process_name,
                        v_prod_code=rec["v_prod_code"],
                        v_ccy_code=rec["v_ccy_code"],
                        v_loan_type=rec["v_loan_type"],
                        v_party_type_code=rec["v_party_type_code"],
                        financial_element=rec["financial_element"],
                        cashflow_by_bucket=cf_bucket_ref,
                        time_bucket_master=tb_master,
                        **rec_buckets,
                    )
                )

            Aggregated_Prod_Cashflow_Base.objects.bulk_create(objs, batch_size=1000)
            print(
                f"[prod_agg] ✅  Aggregated {len(objs)} rows for "
                f"{fic_mis_date} / '{process_name}'."
            )

    except Exception as exc:
        print(f"[prod_agg] ❌  Error: {exc}")
        return

    # ── 6) keep the reporting table in‑sync with time‑buckets ──
    sync_bucket_columns(verbose=True)          # default table = Report_Contractual_Base

# def aggregate_by_prod_code(fic_mis_date, process_name):
#     """
#     This function groups data from AggregatedCashflowByBuckets by v_prod_code,
#     sums the bucket values, and stores the result in Aggregated_Prod_Cashflow_Base.
#     """

#     try:
#         # Step 1: Delete any existing records in Aggregated_Prod_Cashflow_Base for the same fic_mis_date and process_name
#         Aggregated_Prod_Cashflow_Base.objects.filter(fic_mis_date=fic_mis_date, process_name=process_name).delete()
#         print(f"Deleted existing records for fic_mis_date: {fic_mis_date} and process_name: {process_name}")

#         # Step 2: Fetch all records from AggregatedCashflowByBuckets for the given fic_mis_date and process_name
#         cashflow_buckets = Aggregated_Acc_CashflowByBuckets.objects.filter(fic_mis_date=fic_mis_date, process_name=process_name)

#         if not cashflow_buckets.exists():
#             print(f"No cashflows found for fic_mis_date: {fic_mis_date} and process_name: {process_name}")
#             return

#         # Step 3: Group the data by v_prod_code and sum the bucket values
#         grouped_data = cashflow_buckets.values('v_prod_code', 'v_ccy_code', 'v_loan_type', 'v_party_type_code','financial_element').annotate(
#             bucket_1=Sum('bucket_1'),
#             bucket_2=Sum('bucket_2'),
#             bucket_3=Sum('bucket_3'),
#             bucket_4=Sum('bucket_4'),
#             bucket_5=Sum('bucket_5'),
#             bucket_6=Sum('bucket_6'),
#             bucket_7=Sum('bucket_7'),
#             bucket_8=Sum('bucket_8'),
#             bucket_9=Sum('bucket_9'),
#             bucket_10=Sum('bucket_10'),
#             bucket_11=Sum('bucket_11'),
#             bucket_12=Sum('bucket_12'),
#             bucket_13=Sum('bucket_13'),
#             bucket_14=Sum('bucket_14'),
#             bucket_15=Sum('bucket_15'),
#             bucket_16=Sum('bucket_16'),
#             bucket_17=Sum('bucket_17'),
#             bucket_18=Sum('bucket_18'),
#             bucket_19=Sum('bucket_19'),
#             bucket_20=Sum('bucket_20'),
#             bucket_21=Sum('bucket_21'),
#             bucket_22=Sum('bucket_22'),
#             bucket_23=Sum('bucket_23'),
#             bucket_24=Sum('bucket_24'),
#             bucket_25=Sum('bucket_25'),
#             bucket_26=Sum('bucket_26'),
#             bucket_27=Sum('bucket_27'),
#             bucket_28=Sum('bucket_28'),
#             bucket_29=Sum('bucket_29'),
#             bucket_30=Sum('bucket_30'),
#             bucket_31=Sum('bucket_31'),
#             bucket_32=Sum('bucket_32'),
#             bucket_33=Sum('bucket_33'),
#             bucket_34=Sum('bucket_34'),
#             bucket_35=Sum('bucket_35'),
#             bucket_36=Sum('bucket_36'),
#             bucket_37=Sum('bucket_37'),
#             bucket_38=Sum('bucket_38'),
#             bucket_39=Sum('bucket_39'),
#             bucket_40=Sum('bucket_40'),
#             bucket_41=Sum('bucket_41'),
#             bucket_42=Sum('bucket_42'),
#             bucket_43=Sum('bucket_43'),
#             bucket_44=Sum('bucket_44'),
#             bucket_45=Sum('bucket_45'),
#             bucket_46=Sum('bucket_46'),
#             bucket_47=Sum('bucket_47'),
#             bucket_48=Sum('bucket_48'),
#             bucket_49=Sum('bucket_49'),
#             bucket_50=Sum('bucket_50')
#         )

#         # Step 4: Insert the aggregated data into Aggregated_Prod_Cashflow_Base
#         for record in grouped_data:
#             try:
#                 # Get the corresponding AggregatedCashflowByBucket and TimeBucketMaster record
#                 cashflow_by_bucket = Aggregated_Acc_CashflowByBuckets.objects.filter(
#                     v_prod_code=record['v_prod_code'],
#                     fic_mis_date=fic_mis_date,
#                     process_name=process_name,
#                     v_loan_type=record ['v_loan_type'],
#                     v_party_type_code=record['v_party_type_code'],
#                     financial_element=record['financial_element']
#                 ).first()

#                 time_bucket_master = TimeBucketMaster.objects.filter(
#                     process_name=process_name
#                 ).first()

#                 Aggregated_Prod_Cashflow_Base.objects.create(
#                     fic_mis_date=fic_mis_date,
#                     process_name=process_name,
#                     v_prod_code=record['v_prod_code'],
#                     v_ccy_code=record['v_ccy_code'],
#                     v_loan_type=record['v_loan_type'],
#                     v_party_type_code=record['v_party_type_code'],
#                     financial_element=record['financial_element'],
#                     cashflow_by_bucket=cashflow_by_bucket,  # Link to AggregatedCashflowByBucket
#                     time_bucket_master=time_bucket_master,  # Link to TimeBucketMaster
#                     bucket_1=record['bucket_1'],
#                     bucket_2=record['bucket_2'],
#                     bucket_3=record['bucket_3'],
#                     bucket_4=record['bucket_4'],
#                     bucket_5=record['bucket_5'],
#                     bucket_6=record['bucket_6'],
#                     bucket_7=record['bucket_7'],
#                     bucket_8=record['bucket_8'],
#                     bucket_9=record['bucket_9'],
#                     bucket_10=record['bucket_10'],
#                     bucket_11=record['bucket_11'],
#                     bucket_12=record['bucket_12'],
#                     bucket_13=record['bucket_13'],
#                     bucket_14=record['bucket_14'],
#                     bucket_15=record['bucket_15'],
#                     bucket_16=record['bucket_16'],
#                     bucket_17=record['bucket_17'],
#                     bucket_18=record['bucket_18'],
#                     bucket_19=record['bucket_19'],
#                     bucket_20=record['bucket_20'],
#                     bucket_21=record['bucket_21'],
#                     bucket_22=record['bucket_22'],
#                     bucket_23=record['bucket_23'],
#                     bucket_24=record['bucket_24'],
#                     bucket_25=record['bucket_25'],
#                     bucket_26=record['bucket_26'],
#                     bucket_27=record['bucket_27'],
#                     bucket_28=record['bucket_28'],
#                     bucket_29=record['bucket_29'],
#                     bucket_30=record['bucket_30'],
#                     bucket_31=record['bucket_31'],
#                     bucket_32=record['bucket_32'],
#                     bucket_33=record['bucket_33'],
#                     bucket_34=record['bucket_34'],
#                     bucket_35=record['bucket_35'],
#                     bucket_36=record['bucket_36'],
#                     bucket_37=record['bucket_37'],
#                     bucket_38=record['bucket_38'],
#                     bucket_39=record['bucket_39'],
#                     bucket_40=record['bucket_40'],
#                     bucket_41=record['bucket_41'],
#                     bucket_42=record['bucket_42'],
#                     bucket_43=record['bucket_43'],
#                     bucket_44=record['bucket_44'],
#                     bucket_45=record['bucket_45'],
#                     bucket_46=record['bucket_46'],
#                     bucket_47=record['bucket_47'],
#                     bucket_48=record['bucket_48'],
#                     bucket_49=record['bucket_49'],
#                     bucket_50=record['bucket_50']
#                 )
#             except Exception as e:
#                 print(f"Error inserting record for v_prod_code: {record['v_prod_code']}, Error: {e}")

#         print(f"Successfully aggregated cashflows by product code for process '{process_name}' and fic_mis_date {fic_mis_date}.")

#     except Exception as e:
#         print(f"Error during aggregation: {e}")