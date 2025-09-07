# services/cashflow_processing.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Tuple
import logging
import time

from dateutil.relativedelta import relativedelta
from django.db import transaction, connection
from django.db.models import (
    Sum, Case, When, F, DecimalField, Value,
    Max, Q,
)

from ..models import (
    Arranged_cashflows,
    TimeBuckets,
    TimeBucketMaster,
    Aggregated_Acc_CashflowByBuckets,
)

# Configure logging
log = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────

@transaction.atomic
def calculate_time_buckets_and_spread(fic_mis_date):
    """
    1. Reset & flag participating Arranged_cashflows rows
    2. Generate / refresh TimeBucketMaster for *contractual* process
    3. Aggregate each financial element into bucket_1 … bucket_N
       and write to Aggregated_Acc_CashflowByBuckets.

    The heavy lifting is done in a *single* GROUP‑BY query per
    financial element; no nested Python loops.
    """
    # Configure console logging to ensure output appears in terminal
    import sys
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    
    # Remove existing handlers to avoid duplicates
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    
    log.addHandler(console_handler)
    log.setLevel(logging.WARNING)
    
    t0_total = time.perf_counter()
    print(f"➡️  Start aggregation run for {fic_mis_date}")

    if isinstance(fic_mis_date, str):
        fic_mis_date = datetime.strptime(fic_mis_date, "%Y-%m-%d").date()
    if not isinstance(fic_mis_date, (datetime, date)):
        raise TypeError("fic_mis_date must be datetime.date or ISO‑string")

    process = "contractual"
    
    print(f"\n🔍 CHECKING FOR MISSING VALUES IN CASHFLOW DATA ({fic_mis_date})")
    print("=" * 60)

    # ── 1. housekeeping: reset then flag rows
    t_reset = time.perf_counter()
    Arranged_cashflows.objects.filter(fic_mis_date=fic_mis_date).update(record_count=0)
    flagged_count = Arranged_cashflows.objects.filter(fic_mis_date=fic_mis_date).update(record_count=1)
    print(f"📊 Records flagged: {flagged_count}  (took {(time.perf_counter()-t_reset):.2f}s)")

    # ── Check for missing v_prod_code and v_loan_type values
    missing_prod_code = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        record_count=1,
        v_prod_code__isnull=True
    ).count()
    
    missing_loan_type = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        record_count=1,
        v_loan_type__isnull=True
    ).count()
    
    empty_prod_code = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        record_count=1,
        v_prod_code=''
    ).count()
    
    empty_loan_type = Arranged_cashflows.objects.filter(
        fic_mis_date=fic_mis_date,
        record_count=1,
        v_loan_type=''
    ).count()
    
    # Always show the check results, even if zero
    print(f"\n📋 MISSING VALUE ANALYSIS:")
    print(f"   v_prod_code (NULL): {missing_prod_code} records")
    print(f"   v_prod_code (empty): {empty_prod_code} records")
    print(f"   v_loan_type (NULL): {missing_loan_type} records")
    print(f"   v_loan_type (empty): {empty_loan_type} records")
    
    if missing_prod_code > 0 or empty_prod_code > 0:
        print(f"\n⚠️  MISSING v_prod_code VALUES DETECTED: {missing_prod_code} NULL, {empty_prod_code} empty")
        log.warning("⚠️  MISSING v_prod_code VALUES DETECTED:")
        log.warning("   NULL v_prod_code: %d rows", missing_prod_code)
        log.warning("   Empty v_prod_code: %d rows", empty_prod_code)
    
    if missing_loan_type > 0 or empty_loan_type > 0:
        print(f"\n⚠️  MISSING v_loan_type VALUES DETECTED: {missing_loan_type} NULL, {empty_loan_type} empty")
        log.warning("⚠️  MISSING v_loan_type VALUES DETECTED:")
        log.warning("   NULL v_loan_type: %d rows", missing_loan_type)
        log.warning("   Empty v_loan_type: %d rows", empty_loan_type)
    
    # Log specific records with missing values
    if missing_prod_code > 0 or empty_prod_code > 0:
        print(f"\n📋 Sample records with missing v_prod_code (showing first 10):")
        missing_records = Arranged_cashflows.objects.filter(
            fic_mis_date=fic_mis_date,
            record_count=1
        ).filter(
            Q(v_prod_code__isnull=True) | Q(v_prod_code='')
        ).values('id', 'v_account_number', 'v_ccy_code', 'v_loan_type')[:10]
        
        for record in missing_records:
            print(f"   ID: {record['id']}, Account: {record['v_account_number']}, CCY: {record['v_ccy_code']}, Loan Type: {record['v_loan_type']}")
            log.warning("   ID: %s, Account: %s, CCY: %s, Loan Type: %s", 
                       record['id'], record['v_account_number'], 
                       record['v_ccy_code'], record['v_loan_type'])
    
    if missing_loan_type > 0 or empty_loan_type > 0:
        print(f"\n📋 Sample records with missing v_loan_type (showing first 10):")
        missing_records = Arranged_cashflows.objects.filter(
            fic_mis_date=fic_mis_date,
            record_count=1
        ).filter(
            Q(v_loan_type__isnull=True) | Q(v_loan_type='')
        ).values('id', 'v_account_number', 'v_prod_code', 'v_ccy_code')[:10]
        
        for record in missing_records:
            print(f"   ID: {record['id']}, Account: {record['v_account_number']}, Prod Code: {record['v_prod_code']}, CCY: {record['v_ccy_code']}")
            log.warning("   ID: %s, Account: %s, Prod Code: %s, CCY: %s", 
                       record['id'], record['v_account_number'], 
                       record['v_prod_code'], record['v_ccy_code'])
    
    # Show summary
    total_missing = missing_prod_code + empty_prod_code + missing_loan_type + empty_loan_type
    if total_missing == 0:
        print(f"\n✅ NO MISSING VALUES DETECTED - All records have valid v_prod_code and v_loan_type")
    else:
        print(f"\n⚠️  TOTAL MISSING VALUES: {total_missing}")
    
    print("=" * 60)

    # ── 2. rebuild TimeBucketMaster for this process (scoped to fic_mis_date)
    t_tb = time.perf_counter()
    # If TimeBuckets has fic_mis_date, filter by it; otherwise, fall back to all
    try:
        tb_qs = TimeBuckets.objects.filter(fic_mis_date=fic_mis_date)
    except Exception:
        tb_qs = TimeBuckets.objects.all()
    tb_meta = list(tb_qs.order_by("serial_number"))
    if not tb_meta:
        raise RuntimeError("No rows in TimeBuckets; cannot bucketise.")

    bucket_ranges = _build_bucket_ranges(fic_mis_date, tb_meta)
    _refresh_timebucket_master(process, bucket_ranges, fic_mis_date)
    print(f"🧱 Time buckets prepared for {fic_mis_date}: {len(bucket_ranges)} (took {(time.perf_counter()-t_tb):.2f}s)")

    # wipe previous aggregation for that date / process
    t_wipe = time.perf_counter()
    Aggregated_Acc_CashflowByBuckets.objects.filter(
        fic_mis_date=fic_mis_date,
        process_name=process).delete()
    print(f"🧹 Cleared previous aggregates for {fic_mis_date} (took {(time.perf_counter()-t_wipe):.2f}s)")

    # ── 3. aggregate with raw SQL INSERT ... SELECT per element
    base_fields = [
        "v_account_number",
        "v_prod_code",
        "v_ccy_code",
        "v_loan_type",
        "v_party_type_code",
    ]

    financial_elements = (
        "n_total_cash_flow_amount",
        "n_total_principal_payment",
        "n_total_interest_payment",
    )

    tb_master_ref = TimeBucketMaster.objects.filter(process_name=process, fic_mis_date=fic_mis_date).first()
    tb_master_id = tb_master_ref.id if tb_master_ref else None

    # Quote source and target tables to preserve case-sensitive identifiers
    arr_table = f'"{Arranged_cashflows._meta.db_table}"'
    # Use the exact table name provided, quoted for case-sensitive identifiers
    aggr_table = '"Fsi_Aggregated_Acc_Cashflow"'

    # Determine how many bucket columns actually exist on the target table
    bucket_field_names = [
        f.name for f in Aggregated_Acc_CashflowByBuckets._meta.get_fields()
        if hasattr(f, 'attname') and isinstance(getattr(Aggregated_Acc_CashflowByBuckets, f.name, None), property) is False and f.name.startswith('bucket_')
    ]
    # Sort by numeric suffix
    def _bucket_index(nm: str) -> int:
        try:
            return int(nm.split('_')[1])
        except Exception:
            return 10**6
    bucket_field_names = sorted(bucket_field_names, key=_bucket_index)

    max_target_buckets = len(bucket_field_names)
    use_bucket_count = min(len(bucket_ranges), max_target_buckets)

    print(f"📊 Bucket columns available on target: {max_target_buckets}; will use: {use_bucket_count}")
    print(f"📦 Using tables -> source: {arr_table}, target: {aggr_table}")

    print(f"🚀 Aggregating elements {list(financial_elements)} with {use_bucket_count} buckets using raw SQL...")

    for element in financial_elements:
        t_elem = time.perf_counter()
        # Columns for insert
        bucket_cols = ", ".join([f"bucket_{i+1}" for i in range(use_bucket_count)])
        insert_cols = (
            "fic_mis_date, process_name, v_account_number, v_prod_code, v_ccy_code, "
            "v_loan_type, v_party_type_code, financial_element, time_bucket_master_id" +
            (", " + bucket_cols if bucket_cols else "")
        )

        # SELECT list: constants + keys + element + fk + bucket sums
        select_consts = "%s AS fic_mis_date, %s AS process_name"
        select_keys = ", ".join(base_fields)
        select_elem = "%s AS financial_element, %s AS time_bucket_master_id"

        # CASE WHEN per bucket
        bucket_selects = []
        params: List = [
            fic_mis_date, process,  # consts
        ]
        # keys are selected directly
        params.extend([element, tb_master_id])
        for idx in range(use_bucket_count):
            start_d, end_d = bucket_ranges[idx]
            alias = f"bucket_{idx+1}"
            bucket_selects.append(
                f"COALESCE(SUM(CASE WHEN d_cashflow_date >= %s AND d_cashflow_date <= %s THEN {element} ELSE 0 END), 0) AS {alias}"
            )
        # add dates for each used bucket (2 per bucket)
        for idx in range(use_bucket_count):
            start_d, end_d = bucket_ranges[idx]
            params.extend([start_d, end_d])
        # WHERE params
        params.append(fic_mis_date)

        select_bucket_sql = (", " + ", ".join(bucket_selects)) if bucket_selects else ""
        select_sql = (
            f"SELECT {select_consts}, {select_keys}, {select_elem}{select_bucket_sql} "
            f"FROM {arr_table} "
            f"WHERE fic_mis_date = %s AND record_count = 1 "
            f"GROUP BY {select_keys}"
        )

        sql = f"INSERT INTO {aggr_table} ({insert_cols}) {select_sql}"

        with connection.cursor() as cur:
            cur.execute(sql, params)

        # Verification: count rows inserted for this element
        with connection.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {aggr_table} WHERE fic_mis_date=%s AND process_name=%s AND financial_element=%s",
                [fic_mis_date, process, element]
            )
            cnt = cur.fetchone()[0]
        print(f"✅ Element '{element}' inserted rows: {cnt} in {(time.perf_counter()-t_elem):.2f}s")

    print(f"[bucket_spread] ✅ aggregated for {fic_mis_date} in {(time.perf_counter()-t0_total):.2f}s")
    
    # Final summary of missing values
    total_missing = missing_prod_code + empty_prod_code + missing_loan_type + empty_loan_type
    if total_missing > 0:
        print(f"⚠️  SUMMARY: {total_missing} total missing values detected")
        log.warning("📊 MISSING VALUES SUMMARY:")
        log.warning("   v_prod_code missing: %d", missing_prod_code + empty_prod_code)
        log.warning("   v_loan_type missing: %d", missing_loan_type + empty_loan_type)


# ───────────────────────────────────────────────────────────
# helpers
# ───────────────────────────────────────────────────────────

def _build_bucket_ranges(
    anchor_date: date,
    tb_rows: List[TimeBuckets],
) -> List[Tuple[date, date]]:
    """
    Convert TimeBuckets metadata into a list of (start, end) absolute dates
    anchored to *anchor_date* (fic_mis_date).
    """
    ranges = []
    start = anchor_date
    for tb in tb_rows:
        if tb.multiplier == "Days":
            end = start + timedelta(days=tb.frequency)
        elif tb.multiplier == "Months":
            end = start + relativedelta(months=tb.frequency)
        elif tb.multiplier == "Years":
            end = start + relativedelta(years=tb.frequency)
        else:
            raise ValueError(f"Unsupported multiplier: {tb.multiplier}")
        ranges.append((start, end))
        start = end + timedelta(days=1)
    return ranges


def _refresh_timebucket_master(
    process: str,
    ranges: List[Tuple[date, date]],
    fic_mis_date: date,
) -> None:
    """
    Delete & recreate TimeBucketMaster rows for this *process* in one go,
    scoped to the provided fic_mis_date.
    """
    TimeBucketMaster.objects.filter(process_name=process, fic_mis_date=fic_mis_date).delete()

    objs = [
        TimeBucketMaster(
            process_name=process,
            bucket_number=i + 1,
            start_date=start,
            end_date=end,
            fic_mis_date=fic_mis_date,
        )
        for i, (start, end) in enumerate(ranges)
    ]
    TimeBucketMaster.objects.bulk_create(objs)


