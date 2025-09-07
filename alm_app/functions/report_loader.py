



# services/report_loader.py
from datetime import date, datetime
from typing import Union
import logging
import sys

from django.db import connection, transaction
from django.apps import apps

# helper that maps a TimeBucketMaster row -> legal SQL column name
from ..functions_view.report_buckets import bucket_column_name

# Configure logging
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#  Lazy model look-ups (avoid circular imports)
# --------------------------------------------------------------------------- #
APB = apps.get_model("alm_app", "Aggregated_Prod_Cashflow_Base")
TBM = apps.get_model("alm_app", "TimeBucketMaster")
SPM = apps.get_model("alm_app", "Stg_Product_Master")
SCC = apps.get_model("alm_app", "Stg_Common_Coa_Master")

BASE_TABLE = "Report_Contractual_Base"   # template defined in migrations


# --------------------------------------------------------------------------- #
#  Utility – normalise fic_mis_date to datetime.date
# --------------------------------------------------------------------------- #
def _to_date(value: Union[date, datetime, str]) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise TypeError("fic_mis_date must be date, datetime, or 'YYYY-MM-DD' str")


# --------------------------------------------------------------------------- #
# 1️⃣  CREATE / RE-CREATE THE PER-RUN TABLE
# --------------------------------------------------------------------------- #
def create_report_contractual_table(fic_mis_date: Union[date, datetime, str]) -> str:
    """
    Drop (if exists) and recreate  Report_Contractual_<YYYYMMDD>
    as a copy of Report_Contractual_Base.
    """
    fic_mis_date = _to_date(fic_mis_date)
    table_name   = f"Report_Contractual_{fic_mis_date:%Y%m%d}"

    with connection.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
        cur.execute(
            f'CREATE TABLE "{table_name}" '
            f'(LIKE "{BASE_TABLE}" INCLUDING ALL);'
        )
    return table_name


# --------------------------------------------------------------------------- #
# 2️⃣  LOAD ONE (fic_mis_date, process_name) SLICE INTO THAT TABLE
# --------------------------------------------------------------------------- #



def report_contractual_load(
    fic_mis_date: Union[date, datetime, str],
    buckets: range = range(1, 51),
) -> int:
    """
    Load rows from Aggregated_Prod_Cashflow_Base where
    financial_element = 'n_total_cash_flow_amount' into
    Report_Contractual_<YYYYMMDD>, populating product meta and flow_type.
    """
    # Ensure logging appears in terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    
    # Remove existing handlers to avoid duplicates
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    
    log.addHandler(console_handler)
    log.setLevel(logging.WARNING)
    
    process_name = 'contractual'

    fic_mis_date = _to_date(fic_mis_date)
    target       = f'Report_Contractual_{fic_mis_date:%Y%m%d}'

    # {bucket_number: TimeBucketMaster}
    bucket_meta = {
        tb.bucket_number: tb
        for tb in TBM.objects.filter(process_name=process_name)
    }

    # source rows – ONLY cash-flow total element
    src_qs = (
        APB.objects
        .filter(
            fic_mis_date=fic_mis_date,
            process_name=process_name,
            financial_element="n_total_cash_flow_amount",
        )
        .select_related("time_bucket_master")
    )

    # Track missing product codes
    missing_in_spm = set()  # Missing in Stg_Product_Master
    missing_in_scc = set()  # Missing in Stg_Common_Coa_Master
    missing_coa_codes = set()  # Product codes with missing v_common_coa_code
    
    # Sample records for detailed logging (first 10 of each type)
    sample_missing_spm = []
    sample_missing_scc = []
    sample_missing_coa = []
    
    processed = 0
    with transaction.atomic(), connection.cursor() as cur:
        print("\n" + "="*80)
        print("🔍 STARTING REPORT CONTRACTUAL LOAD - MASTER DATA VALIDATION")
        print(f"📅 Date: {fic_mis_date}")
        print(f"📋 Target Table: {target}")
        print("="*80)
        
        for src in src_qs.iterator(chunk_size=500):

            # ── product & account look-ups ───────────────────────────────
            prod = (
                SPM.objects
                .filter(v_prod_code=src.v_prod_code)
                .order_by("-fic_mis_date")
                .first()
            )

            # Track missing product codes in Stg_Product_Master
            if not prod:
                missing_in_spm.add(src.v_prod_code)
                if len(sample_missing_spm) < 10:
                    sample_missing_spm.append({
                        'v_prod_code': src.v_prod_code,
                        'v_ccy_code': src.v_ccy_code,
                        'v_loan_type': src.v_loan_type,
                        'cashflow_id': src.id
                    })
                # Real-time warning for missing product
                print(f"⚠️  WARNING: v_prod_code '{src.v_prod_code}' not found in Stg_Product_Master")
                log.warning("⚠️  WARNING: v_prod_code '%s' not found in Stg_Product_Master", src.v_prod_code)

            v_product_name   = prod.v_prod_name        if prod and prod.v_prod_name        else ""
            v_prod_type      = prod.v_prod_type        if prod and prod.v_prod_type        else ""
            v_prod_type_desc = prod.v_prod_type_desc   if prod and prod.v_prod_type_desc   else ""
            account_type     = ""
            
            # Check for missing v_common_coa_code
            if prod and not prod.v_common_coa_code:
                missing_coa_codes.add(src.v_prod_code)
                if len(sample_missing_coa) < 10:
                    sample_missing_coa.append({
                        'v_prod_code': src.v_prod_code,
                        'v_ccy_code': src.v_ccy_code,
                        'v_loan_type': src.v_loan_type,
                        'cashflow_id': src.id
                    })
                print(f"⚠️  WARNING: v_prod_code '{src.v_prod_code}' has missing v_common_coa_code in Stg_Product_Master")
                log.warning("⚠️  WARNING: v_prod_code '%s' has missing v_common_coa_code in Stg_Product_Master", src.v_prod_code)
            
            if prod and prod.v_common_coa_code:
                coa = (
                    SCC.objects
                    .filter(v_common_coa_code=prod.v_common_coa_code)
                    .order_by("-fic_mis_date")
                    .first()
                )
                
                # Track missing COA codes in Stg_Common_Coa_Master
                if not coa:
                    missing_in_scc.add(prod.v_common_coa_code)
                    if len(sample_missing_scc) < 10:
                        sample_missing_scc.append({
                            'v_prod_code': src.v_prod_code,
                            'v_common_coa_code': prod.v_common_coa_code,
                            'v_ccy_code': src.v_ccy_code,
                            'cashflow_id': src.id
                        })
                    print(f"⚠️  WARNING: v_common_coa_code '{prod.v_common_coa_code}' not found in Stg_Common_Coa_Master")
                    log.warning("⚠️  WARNING: v_common_coa_code '%s' not found in Stg_Common_Coa_Master", prod.v_common_coa_code)
                
                account_type = coa.v_account_type if coa and coa.v_account_type else ""

            # flow_type derivation
            if account_type in ("EARNINGASSETS", "OTHERASSET"):
                flow_type = "inflow"
            elif account_type in ("INTBEARINGLIABS", "OTHERLIABS"):
                flow_type = "outflow"
            else:
                flow_type = ""

            # ── 1. static INSERT (idempotent) ───────────────────────────
            cur.execute(
                f'''
                INSERT INTO "{target}" (
                    fic_mis_date, process_name,
                    v_loan_type, v_party_type_code,
                    v_prod_code, v_ccy_code, financial_element,
                    v_product_name, v_prod_type, v_prod_type_desc, account_type,
                    flow_type,
                    cashflow_by_bucket_id, time_bucket_master_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                ''',
                [
                    src.fic_mis_date, src.process_name,
                    src.v_loan_type, src.v_party_type_code,
                    src.v_prod_code, src.v_ccy_code,
                    src.financial_element,          # always 'n_total_cash_flow_amount'
                    v_product_name, v_prod_type, v_prod_type_desc, account_type,
                    flow_type,
                    src.id,
                    src.time_bucket_master_id,
                ],
            )

            # ── 2. dynamic bucket columns ────────────────────────────────
            dyn = {}
            for n in buckets:
                amt = getattr(src, f"bucket_{n}", None)
                if amt in (None, 0):
                    continue
                meta = bucket_meta.get(n)
                if meta:
                    dyn[bucket_column_name(meta)] = amt

            if dyn:
                set_clause = ", ".join(f'"{c}" = %s' for c in dyn)
                where_pk   = (
                    "fic_mis_date = %s AND process_name = %s "
                    "AND v_prod_code = %s AND v_ccy_code = %s "
                    "AND financial_element = %s"
                )
                cur.execute(
                    f'UPDATE "{target}" SET {set_clause} WHERE {where_pk};',
                    list(dyn.values()) + [
                        src.fic_mis_date, src.process_name,
                        src.v_prod_code,  src.v_ccy_code,
                        src.financial_element,
                    ],
                )

            processed += 1

    # Final summary logging
    print("\n" + "="*80)
    print("📊 MASTER DATA VALIDATION SUMMARY")
    print("="*80)
    
    # Missing in Stg_Product_Master
    print(f"❌ MISSING IN STG_PRODUCT_MASTER: {len(missing_in_spm)} unique v_prod_code(s)")
    log.warning("❌ MISSING IN STG_PRODUCT_MASTER: %d unique v_prod_code(s)", len(missing_in_spm))
    
    if missing_in_spm:
        print("   📋 Missing Product Codes:")
        for prod_code in sorted(missing_in_spm):
            print(f"      - {prod_code}")
        
        if sample_missing_spm:
            print("   📝 Sample Records (first 10):")
            for i, record in enumerate(sample_missing_spm, 1):
                print(f"      {i}. Product: {record['v_prod_code']}, Currency: {record['v_ccy_code']}, Loan Type: {record['v_loan_type']}, Cashflow ID: {record['cashflow_id']}")
    
    # Missing v_common_coa_code
    print(f"\n❌ MISSING V_COMMON_COA_CODE: {len(missing_coa_codes)} unique v_prod_code(s)")
    log.warning("❌ MISSING V_COMMON_COA_CODE: %d unique v_prod_code(s)", len(missing_coa_codes))
    
    if missing_coa_codes:
        print("   📋 Product Codes with Missing COA Code:")
        for prod_code in sorted(missing_coa_codes):
            print(f"      - {prod_code}")
        
        if sample_missing_coa:
            print("   📝 Sample Records (first 10):")
            for i, record in enumerate(sample_missing_coa, 1):
                print(f"      {i}. Product: {record['v_prod_code']}, Currency: {record['v_ccy_code']}, Loan Type: {record['v_loan_type']}, Cashflow ID: {record['cashflow_id']}")
    
    # Missing in Stg_Common_Coa_Master
    print(f"\n❌ MISSING IN STG_COMMON_COA_MASTER: {len(missing_in_scc)} unique v_common_coa_code(s)")
    log.warning("❌ MISSING IN STG_COMMON_COA_MASTER: %d unique v_common_coa_code(s)", len(missing_in_scc))
    
    if missing_in_scc:
        print("   📋 Missing COA Codes:")
        for coa_code in sorted(missing_in_scc):
            print(f"      - {coa_code}")
        
        if sample_missing_scc:
            print("   📝 Sample Records (first 10):")
            for i, record in enumerate(sample_missing_scc, 1):
                print(f"      {i}. Product: {record['v_prod_code']}, COA Code: {record['v_common_coa_code']}, Currency: {record['v_ccy_code']}, Cashflow ID: {record['cashflow_id']}")
    
    print(f"\n✅ TOTAL RECORDS PROCESSED: {processed}")
    print("="*80)
    
    return processed