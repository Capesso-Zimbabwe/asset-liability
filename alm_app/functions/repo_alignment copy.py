# --------------------------------------------------------------------------- #
#  ALIGN BUCKET TOTALS TO MIS BALANCE – COMPLETE WORKING VERSION
# --------------------------------------------------------------------------- #
from decimal import Decimal, ROUND_HALF_UP
import logging

from django.apps import apps
from django.db import connection, transaction
from ..functions_view.report_base import _to_date

log = logging.getLogger(__name__)
ProductBalance = apps.get_model("alm_app", "ProductBalance")


def _bucket_columns(cur, table: str) -> list[str]:
    """Find all bucket columns in the specified table"""
    cur.execute(
        """
        SELECT column_name
        FROM   information_schema.columns
        WHERE  table_schema = current_schema()
          AND  LOWER(table_name) = LOWER(%s)
          AND (
                 column_name ILIKE 'bucket_%'
              OR column_name ILIKE 'c_%'
              OR column_name ~ '^[Bb][0-9]+$'
              OR column_name ILIKE 'b[0-9]%'
          )
        ORDER  BY ordinal_position;
        """,
        [table],
    )
    columns = [r[0] for r in cur.fetchall()]
    log.info("🔍 Found bucket columns in %s: %s", table, columns)
    return columns


def align_buckets_to_balance(
    fic_mis_date,
    *,
    process_name: str | None = None,
    tolerance: Decimal = Decimal("0.01"),
):
    """
    STEP-BY-STEP ALIGNMENT PROCESS:
    
    1. Find all rows in Report_Contractual_<date> 
    2. Calculate sum of all bucket columns for each row
    3. Compare with corresponding ProductBalance.n_balance
    4. If difference > tolerance: adjust the largest bucket
    5. Ensure final sum equals n_balance exactly
    """
    
    fic_mis_date = _to_date(fic_mis_date)
    tbl = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
    
    log.info("=" * 80)
    log.info("🚀 STARTING BUCKET ALIGNMENT")
    log.info("📅 Date: %s", fic_mis_date)
    log.info("📋 Table: %s", tbl)
    log.info("🎯 Process: %s", process_name or "ALL")
    log.info("⚖️  Tolerance: %s", tolerance)
    log.info("=" * 80)

    stats = {
        'processed': 0,
        'adjusted': 0, 
        'missing_pb': 0,
        'within_tolerance': 0,
        'errors': 0
    }
    
    adjusted_rows = []
    missing_rows = []
    error_rows = []

    with transaction.atomic():
        with connection.cursor() as cur:
            
            # 1. VERIFY TABLE EXISTS
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = current_schema() 
                    AND table_name = %s
                )
            """, [tbl])
            
            if not cur.fetchone()[0]:
                log.error("❌ Table %s not found!", tbl)
                return {"error": f"Table {tbl} not found"}

            # 2. GET BUCKET COLUMNS
            bucket_cols = _bucket_columns(cur, tbl)
            if not bucket_cols:
                log.error("❌ No bucket columns found!")
                return {"error": "No bucket columns found"}

            log.info("✅ Found %d bucket columns", len(bucket_cols))
            
            # 3. BUILD WHERE CLAUSE
            where_conditions = ["rc.fic_mis_date = %s"]
            params = [fic_mis_date]
            
            if process_name:
                where_conditions.append("rc.process_name = %s") 
                params.append(process_name)
            
            where_clause = " AND ".join(where_conditions)

            # 4. MAIN QUERY - GET ALL DATA
            bucket_cols_sql = ", ".join([f'COALESCE(rc."{col}", 0)::numeric AS "{col}"' for col in bucket_cols])
            bucket_sum_sql = " + ".join([f'COALESCE(rc."{col}", 0)::numeric' for col in bucket_cols])

            main_query = f"""
                SELECT 
                    rc.id,
                    rc.v_prod_code,
                    rc.v_ccy_code,
                    rc.v_prod_type,
                    rc.process_name,
                    {bucket_cols_sql},
                    ({bucket_sum_sql}) AS current_bucket_sum,
                    pb.n_balance::numeric AS target_balance
                FROM "{tbl}" rc
                LEFT JOIN product_balance pb ON (
                    pb.fic_mis_date = rc.fic_mis_date 
                    AND pb.v_prod_code = rc.v_prod_code 
                    AND pb.v_ccy_code = rc.v_ccy_code
                )
                WHERE {where_clause}
                ORDER BY rc.id
            """
            
            log.info("🔍 Executing main query...")
            log.debug("Query: %s", main_query)
            log.debug("Params: %s", params)
            
            cur.execute(main_query, params)
            
            # 5. PROCESS EACH ROW
            for row in cur.fetchall():
                try:
                    stats['processed'] += 1
                    
                    # Calculate expected row length
                    expected_columns = 5 + len(bucket_cols) + 2  # 5 base + buckets + current_sum + target_balance
                    if len(row) < expected_columns:
                        raise IndexError(
                            f"Row has only {len(row)} columns, expected {expected_columns}. "
                            "Possible missing data in ProductBalance or SQL structure mismatch."
                        )
                    
                    # Extract row data
                    row_id = row[0]
                    prod_code = row[1] 
                    ccy_code = row[2]
                    prod_type = row[3]
                    process = row[4]
                    
                    # Extract bucket values (positions 5 to 5+len(bucket_cols)-1)
                    bucket_values = []
                    for i in range(len(bucket_cols)):
                        val = row[5 + i]
                        bucket_values.append(Decimal(str(val or 0)).quantize(tolerance, ROUND_HALF_UP))
                    
                    # Extract calculated fields
                    current_sum = Decimal(str(row[5 + len(bucket_cols)] or 0)).quantize(tolerance, ROUND_HALF_UP)
                    target_balance = row[5 + len(bucket_cols) + 1]
                    
                    log.debug("📝 Row %d: %s/%s - Sum: %s", row_id, prod_code, ccy_code, current_sum)
                    
                    # Handle missing ProductBalance
                    if target_balance is None:
                        log.warning("⚠️  Row %d: No ProductBalance found for %s/%s", 
                                  row_id, prod_code, ccy_code)
                        stats['missing_pb'] += 1
                        missing_rows.append({
                            'id': row_id,
                            'prod_code': prod_code,
                            'ccy_code': ccy_code,
                            'current_sum': current_sum,
                            'target_balance': None
                        })
                        continue
                    
                    target_balance = Decimal(str(target_balance)).quantize(tolerance, ROUND_HALF_UP)
                    difference = target_balance - current_sum
                    
                    # Check if adjustment needed
                    if abs(difference) <= tolerance:
                        log.debug("✅ Row %d: Already balanced (diff: %s)", row_id, difference)
                        stats['within_tolerance'] += 1
                        continue
                    
                    log.info("🎯 Row %d: %s/%s - NEEDS ADJUSTMENT", row_id, prod_code, ccy_code)
                    log.info("   Current sum: %s", current_sum)
                    log.info("   Target balance: %s", target_balance) 
                    log.info("   Difference: %s", difference)
                    
                    # FIND BEST BUCKET TO ADJUST
                    best_bucket_idx = None
                    
                    # Strategy 1: Find largest bucket with same sign as difference
                    same_sign_candidates = []
                    for i, bucket_val in enumerate(bucket_values):
                        if (difference > 0 and bucket_val >= 0) or (difference < 0 and bucket_val < 0):
                            same_sign_candidates.append((i, abs(bucket_val)))
                    
                    if same_sign_candidates:
                        # Pick largest same-sign bucket
                        best_bucket_idx = max(same_sign_candidates, key=lambda x: x[1])[0]
                        strategy = "same-sign largest"
                    else:
                        # Strategy 2: Pick overall largest bucket
                        best_bucket_idx = max(range(len(bucket_values)), key=lambda i: abs(bucket_values[i]))
                        strategy = "overall largest"
                    
                    target_col = bucket_cols[best_bucket_idx]
                    old_value = bucket_values[best_bucket_idx]
                    new_value = (old_value + difference).quantize(tolerance, ROUND_HALF_UP)
                    
                    log.info("   Selected bucket: %s (%s)", target_col, strategy)
                    log.info("   Old value: %s -> New value: %s", old_value, new_value)
                    
                    # EXECUTE UPDATE
                    update_sql = f'UPDATE "{tbl}" SET "{target_col}" = %s WHERE id = %s'
                    cur.execute(update_sql, [new_value, row_id])
                    
                    # VERIFY UPDATE WORKED
                    verify_sql = f"""
                        SELECT ({" + ".join([f'COALESCE("{col}", 0)::numeric' for col in bucket_cols])}) 
                        FROM "{tbl}" WHERE id = %s
                    """
                    cur.execute(verify_sql, [row_id])
                    new_sum = Decimal(str(cur.fetchone()[0])).quantize(tolerance, ROUND_HALF_UP)
                    
                    verification_diff = abs(new_sum - target_balance)
                    if verification_diff <= tolerance:
                        log.info("✅ SUCCESS: New sum %s matches target %s", new_sum, target_balance)
                        stats['adjusted'] += 1
                        adjusted_rows.append({
                            'id': row_id,
                            'prod_code': prod_code,
                            'ccy_code': ccy_code, 
                            'old_sum': current_sum,
                            'new_sum': new_sum,
                            'target': target_balance,
                            'adjusted_column': target_col,
                            'old_value': old_value,
                            'new_value': new_value,
                            'difference_applied': difference
                        })
                    else:
                        log.error("❌ VERIFICATION FAILED: Expected %s, got %s", target_balance, new_sum)
                        error_rows.append({
                            'id': row_id,
                            'error': 'Verification failed',
                            'expected': target_balance,
                            'actual': new_sum
                        })
                        stats['errors'] += 1
                    
                except Exception as e:
                    log.error("❌ Error processing row %s: %s", row_id if 'row_id' in locals() else '?', str(e))
                    stats['errors'] += 1
                    error_rows.append({
                        'id': row_id if 'row_id' in locals() else None,
                        'error': str(e)
                    })
                    continue

    # FINAL SUMMARY
    log.info("=" * 80)
    log.info("🏁 ALIGNMENT COMPLETE")
    log.info("📊 STATISTICS:")
    log.info("   Rows processed: %d", stats['processed'])
    log.info("   Successfully adjusted: %d", stats['adjusted']) 
    log.info("   Already within tolerance: %d", stats['within_tolerance'])
    log.info("   Missing ProductBalance: %d", stats['missing_pb'])
    log.info("   Errors: %d", stats['errors'])
    log.info("=" * 80)
    
    if adjusted_rows:
        log.info("🔧 ADJUSTED ROWS:")
        for adj in adjusted_rows:
            log.info("   %s/%s: %s -> %s (diff: %s, col: %s)", 
                    adj['prod_code'], adj['ccy_code'], 
                    adj['old_sum'], adj['new_sum'],
                    adj['difference_applied'], adj['adjusted_column'])
    
    if missing_rows:
        log.warning("⚠️  MISSING PRODUCTBALANCE ROWS:")
        for missing in missing_rows:
            log.warning("   %s/%s: sum=%s (no target balance)", 
                       missing['prod_code'], missing['ccy_code'], missing['current_sum'])
    
    if error_rows:
        log.error("❌ ERROR ROWS:")
        for error in error_rows:
            log.error("   Row %s: %s", error.get('id', 'Unknown'), error['error'])
    
    return {
        'success': True,
        'statistics': stats,
        'adjusted_rows': adjusted_rows,
        'missing_rows': missing_rows, 
        'error_rows': error_rows
    }


# HELPER FUNCTION FOR TESTING/DEBUGGING
def test_alignment_results(fic_mis_date, process_name=None, tolerance=Decimal("0.01")):
    """
    Test function to verify alignment worked correctly.
    Run this after align_buckets_to_balance() to double-check results.
    """
    fic_mis_date = _to_date(fic_mis_date)
    tbl = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
    
    log.info("🧪 TESTING ALIGNMENT RESULTS FOR %s", tbl)
    
    mismatches = []
    
    with connection.cursor() as cur:
        bucket_cols = _bucket_columns(cur, tbl)
        if not bucket_cols:
            return {"error": "No bucket columns found"}
        
        bucket_sum_sql = " + ".join([f'COALESCE("{col}", 0)::numeric' for col in bucket_cols])
        
        where_conditions = ["rc.fic_mis_date = %s"]
        params = [fic_mis_date]
        
        if process_name:
            where_conditions.append("rc.process_name = %s")
            params.append(process_name)
        
        where_clause = " AND ".join(where_conditions)
        
        test_query = f"""
            SELECT 
                rc.id,
                rc.v_prod_code,
                rc.v_ccy_code,
                ({bucket_sum_sql}) AS bucket_sum,
                pb.n_balance::numeric AS target_balance,
                ABS(pb.n_balance::numeric - ({bucket_sum_sql})) AS difference
            FROM "{tbl}" rc
            LEFT JOIN product_balance pb ON (
                pb.fic_mis_date = rc.fic_mis_date 
                AND pb.v_prod_code = rc.v_prod_code 
                AND pb.v_ccy_code = rc.v_ccy_code
            )
            WHERE {where_clause}
              AND pb.n_balance IS NOT NULL
              AND ABS(pb.n_balance::numeric - ({bucket_sum_sql})) > %s
            ORDER BY difference DESC
        """
        
        cur.execute(test_query, params + [tolerance])
        
        for row in cur.fetchall():
            row_id, prod_code, ccy_code, bucket_sum, target_balance, diff = row
            mismatches.append({
                'id': row_id,
                'prod_code': prod_code,
                'ccy_code': ccy_code,
                'bucket_sum': Decimal(str(bucket_sum)).quantize(tolerance, ROUND_HALF_UP),
                'target_balance': Decimal(str(target_balance)).quantize(tolerance, ROUND_HALF_UP),
                'difference': Decimal(str(diff)).quantize(tolerance, ROUND_HALF_UP)
            })
    
    if mismatches:
        log.error("❌ FOUND %d REMAINING MISMATCHES:", len(mismatches))
        for mismatch in mismatches:
            log.error("   Row %s (%s/%s): sum=%s, target=%s, diff=%s", 
                     mismatch['id'], mismatch['prod_code'], mismatch['ccy_code'],
                     mismatch['bucket_sum'], mismatch['target_balance'], mismatch['difference'])
    else:
        log.info("✅ ALL ROWS ARE NOW PROPERLY ALIGNED!")
    
    return {
        'success': len(mismatches) == 0,
        'remaining_mismatches': mismatches,
        'total_mismatches': len(mismatches)
    }



















# # --------------------------------------------------------------------------- #
# #  ALIGN BUCKET TOTALS TO MIS BALANCE  – uses the behavioural-loader trick
# # --------------------------------------------------------------------------- #
# from decimal import Decimal
# import logging

# from django.apps import apps
# from django.db import connection, transaction
# from ..functions_view.report_base import _to_date

# log = logging.getLogger(__name__)
# ProductBalance = apps.get_model("alm_app", "ProductBalance")

# # ── helper: discover buckets exactly like behavioural loader  --------------- #
# def _bucket_columns(cur, table: str) -> list[str]:
#     """
#     Return every physical bucket column name in *table*.

#     • Matches long names  'bucket_001_20250430_20250507'
#     • Matches legacy 'c_…'
#     • Matches short   'b1', 'b2', …  (case-insensitive)
#     """
#     cur.execute(
#         """
#         SELECT column_name
#         FROM   information_schema.columns
#         WHERE  LOWER(table_name) = LOWER(%s)
#           AND (
#                  column_name LIKE 'bucket_%%'
#               OR column_name LIKE 'c_%%'
#               OR column_name ~ '^[Bb][0-9]+$'
#           )
#         ORDER  BY ordinal_position;
#         """,
#         [table],
#     )
#     return [r[0] for r in cur.fetchall()]

# # ── main -------------------------------------------------------------------- #
# def align_buckets_to_balance(
#     fic_mis_date,
#     *,
#     process_name: str | None = None,     # None = all processes
#     tolerance: Decimal = Decimal("0.01"),
# ):
#     fic_mis_date = _to_date(fic_mis_date)
#     tbl          = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
#     log.info("[BAL-ALIGN] table=%s date=%s process=%s",
#              tbl, fic_mis_date, process_name or "<ALL>")

#     adjusted, missing, mismatched = 0, [], []

#     with transaction.atomic(), connection.cursor() as cur:
#         # 1️⃣  discover bucket columns (behavioural-loader style)
#         bucket_cols = _bucket_columns(cur, tbl)
#         if not bucket_cols:
#             log.warning("No bucket_* / b# / c_* columns in %s – abort", tbl)
#             return {"adjusted": 0, "missing": [], "mismatched": []}

#         # 2️⃣  build dynamic list / SUM expression
#         bucket_sum = " + ".join(f'COALESCE(rc."{c}",0)' for c in bucket_cols)
#         static_cols = ['v_prod_code', 'v_ccy_code', 'v_prod_type']  # present in every table
#         static_sql  = ", ".join(f'rc."{c}"' for c in static_cols)

#         where = ["rc.fic_mis_date = %s"]
#         params = [fic_mis_date]
#         if process_name is not None:
#             where.append("rc.process_name = %s")
#             params.append(process_name)
#         where_sql = " AND ".join(where)

#         cur.execute(
#             f"""
#             SELECT rc.id,
#                    {static_sql},
#                    {bucket_sum} AS bucket_total,
#                    pb.n_balance
#             FROM   "{tbl}" rc
#             LEFT   JOIN product_balance pb
#                    ON pb.fic_mis_date = rc.fic_mis_date
#                   AND pb.v_prod_code  = rc.v_prod_code
#                   AND pb.v_ccy_code   = rc.v_ccy_code
#             WHERE  {where_sql};
#             """,
#             params,
#         )

#         rows = cur.fetchall()
#         log.debug("Fetched %d rows", len(rows))

#         for row in rows:
#             row_id, code, ccy, ptype, b_total_raw, bal_db = row
#             bucket_total = Decimal(b_total_raw or 0).quantize(tolerance)

#             # ---- no balance row ---------------------------------------
#             if bal_db is None:
#                 missing.append(
#                     dict(v_prod_code=code, v_ccy_code=ccy, v_prod_type=ptype,
#                          bucket_total=bucket_total, n_balance=None,
#                          diff=-bucket_total)
#                 )
#                 continue

#             n_balance = Decimal(bal_db).quantize(tolerance)
#             diff      = n_balance - bucket_total
#             if abs(diff) <= tolerance:
#                 continue   # already matches

#             mismatched.append(
#                 dict(v_prod_code=code, v_ccy_code=ccy, v_prod_type=ptype,
#                      bucket_total=bucket_total, n_balance=n_balance, diff=diff)
#             )

#             # ---- push diff into the largest bucket --------------------
#             # find which bucket column is largest for this row
#             cur.execute(
#                 f"""
#                 SELECT {", ".join(f'"{c}"' for c in bucket_cols)}
#                 FROM   "{tbl}"
#                 WHERE  id = %s;
#                 """,
#                 [row_id],
#             )
#             bucket_vals = cur.fetchone()
#             max_idx = max(range(len(bucket_cols)),
#                           key=lambda i: abs(bucket_vals[i] or 0))
#             tgt_col = bucket_cols[max_idx]
#             new_val = (bucket_vals[max_idx] or Decimal(0)) + diff

#             cur.execute(
#                 f'UPDATE "{tbl}" SET "{tgt_col}" = %s WHERE id = %s',
#                 [new_val, row_id],
#             )
#             adjusted += 1

#     log.info("[BAL-ALIGN summary] adjusted=%d  missing=%d  mismatched=%d",
#              adjusted, len(missing), len(mismatched))
#     return {"adjusted": adjusted, "missing": missing, "mismatched": mismatched}
