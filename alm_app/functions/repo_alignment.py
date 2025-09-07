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
    try:
        cur.execute(
            """
            SELECT column_name
            FROM   information_schema.columns
            WHERE  table_schema = current_schema()
              AND  LOWER(table_name) = LOWER(%s)
              AND (
                     (column_name ILIKE 'bucket_%%' AND column_name != 'cashflow_by_bucket_id')
                  OR column_name ILIKE 'c_%%'
                  OR column_name ~ '^[Bb][0-9]+$'
                  OR column_name ILIKE 'b[0-9]%%'
              )
            ORDER  BY ordinal_position;
            """,
            [table],
        )
        columns = [r[0] for r in cur.fetchall()]
        log.info("🔍 Found bucket columns in %s: %s", table, columns)
        return columns
    except Exception as e:
        log.error("❌ Error finding bucket columns in %s: %s", table, str(e))
        return []


def align_buckets_to_balance(
    fic_mis_date,
    *,
    process_name: str | None = None,
):
    # Ensure logging appears in terminal
    import sys
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    
    # Remove existing handlers to avoid duplicates
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    
    log.addHandler(console_handler)
    log.setLevel(logging.INFO)
    
    try:
        fic_mis_date = _to_date(fic_mis_date)
    except Exception as e:
        log.error("❌ Invalid date format: %s", str(e))
        return {"error": f"Invalid date format: {str(e)}"}
    
    tbl = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
    
    # Fixed precision for all decimal operations
    precision = Decimal("0.01")
    
    log.info("=" * 80)
    log.info("🚀 STARTING BUCKET ALIGNMENT")
    log.info("📅 Date: %s", fic_mis_date)
    log.info("📋 Table: %s", tbl)
    log.info("🎯 Process: %s", process_name or "ALL")
    log.info("⚖️  Mode: ADJUST ALL DIFFERENCES")
    log.info("=" * 80)

    stats = {
        'processed': 0,
        'adjusted': 0, 
        'missing_pb': 0,
        'within_tolerance': 0,  # This will always be 0 now
        'errors': 0
    }
    
    adjusted_rows = []
    missing_rows = []
    error_rows = []

    with transaction.atomic():
        with connection.cursor() as cur:
            
            # 1. VERIFY TABLE EXISTS
            try:
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
            except Exception as e:
                log.error("❌ Error checking table existence: %s", str(e))
                return {"error": f"Error checking table existence: {str(e)}"}

            # 2. GET BUCKET COLUMNS
            bucket_cols = _bucket_columns(cur, tbl)
            if not bucket_cols:
                log.error("❌ No bucket columns found!")
                return {"error": "No bucket columns found"}
            
            # ADDITIONAL SAFEGUARD: Remove cashflow_by_bucket_id if it somehow got included
            bucket_cols = [col for col in bucket_cols if col != 'cashflow_by_bucket_id']
            
            log.info("✅ Found %d bucket columns", len(bucket_cols))
            
            # 3. BUILD WHERE CLAUSE
            where_conditions = ["rc.fic_mis_date = %s"]
            params = [fic_mis_date]
            
            if process_name:
                where_conditions.append("rc.process_name = %s") 
                params.append(process_name)
            
            where_clause = " AND ".join(where_conditions)

            # 4. MAIN QUERY - GET ALL DATA
            try:
                # Safely construct bucket columns SQL
                if bucket_cols:
                    bucket_cols_sql = ", ".join([f'COALESCE(rc."{col}", 0)::numeric AS "{col}"' for col in bucket_cols])
                    bucket_sum_sql = " + ".join([f'COALESCE(rc."{col}", 0)::numeric' for col in bucket_cols])
                else:
                    # Fallback if no bucket columns (shouldn't happen due to earlier check)
                    bucket_cols_sql = "0 AS dummy_bucket"
                    bucket_sum_sql = "0"

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
                
            except Exception as e:
                log.error("❌ Error executing main query: %s", str(e))
                return {"error": f"Error executing main query: {str(e)}"}
            
            # 5. PROCESS EACH ROW
            try:
                rows = cur.fetchall()
                log.info("📊 Retrieved %d rows to process", len(rows))
                
                for row in rows:
                    try:
                        stats['processed'] += 1
                        
                        # Safety check for empty row
                        if not row:
                            log.error("❌ Empty row encountered")
                            stats['errors'] += 1
                            continue
                        
                        # Calculate expected row length
                        expected_columns = 5 + len(bucket_cols) + 2  # 5 base + buckets + current_sum + target_balance
                        if len(row) < expected_columns:
                            log.error("❌ Row has only %d columns, expected %d. Row: %s", 
                                     len(row), expected_columns, row[:5])  # Log first 5 elements safely
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row[0] if len(row) > 0 else None,
                                'error': f'Row has only {len(row)} columns, expected {expected_columns}'
                            })
                            continue
                        
                        # Extract row data safely
                        row_id = row[0]
                        prod_code = row[1] 
                        ccy_code = row[2]
                        prod_type = row[3]
                        process = row[4]
                        
                        # Extract bucket values (positions 5 to 5+len(bucket_cols)-1)
                        bucket_values = []
                        for i in range(len(bucket_cols)):
                            try:
                                val = row[5 + i]
                                bucket_values.append(Decimal(str(val or 0)).quantize(precision, ROUND_HALF_UP))
                            except (IndexError, ValueError) as e:
                                log.error("❌ Error extracting bucket value at index %d for row %d: %s", 
                                         5 + i, row_id, str(e))
                                bucket_values.append(Decimal('0').quantize(precision, ROUND_HALF_UP))
                        
                        # Extract calculated fields safely
                        try:
                            db_calculated_sum = Decimal(str(row[5 + len(bucket_cols)] or 0)).quantize(precision, ROUND_HALF_UP)
                            target_balance = row[5 + len(bucket_cols) + 1]
                        except IndexError as e:
                            log.error("❌ Error extracting calculated fields for row %d: %s", row_id, str(e))
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row_id,
                                'error': f'Error extracting calculated fields: {str(e)}'
                            })
                            continue
                        
                        # MANUAL CALCULATION from individual bucket values
                        manual_sum = sum(bucket_values)
                        
                        # Handle missing ProductBalance
                        if target_balance is None:
                            log.warning("⚠️  Row %d: No ProductBalance found for %s/%s", 
                                      row_id, prod_code, ccy_code)
                            log.info("   📋 Individual bucket values: %s", [str(bv) for bv in bucket_values])
                            log.info("   ➕ Total across all buckets: %s", manual_sum)
                            log.info("   🎯 ProductBalance.n_balance: MISSING")
                            log.info("   📏 Cannot calculate difference - ProductBalance missing")
                            stats['missing_pb'] += 1
                            missing_rows.append({
                                'id': row_id,
                                'prod_code': prod_code,
                                'ccy_code': ccy_code,
                                'prod_type': prod_type,
                                'current_sum': manual_sum,
                                'target_balance': None
                            })
                            continue
                        
                        target_balance = Decimal(str(target_balance)).quantize(precision, ROUND_HALF_UP)
                        
                        # Use MANUAL sum for difference calculation
                        difference = target_balance - manual_sum
                        
                        # COMPREHENSIVE LOGGING - Show ALL details for EVERY row
                        separator = "="*80
                        print(separator)
                        print(f"📊 ROW {row_id} ANALYSIS: {prod_code}/{ccy_code}")
                        print(separator)
                        print("📋 INDIVIDUAL BUCKET VALUES:")
                        for i, (col, val) in enumerate(zip(bucket_cols, bucket_values)):
                            print(f"   {col}: {val}")
                        print(f"➕ TOTAL ACROSS ALL BUCKETS: {manual_sum}")
                        print(f"🗄️  DATABASE CALCULATED SUM: {db_calculated_sum}")
                        print(f"🎯 PRODUCTBALANCE.N_BALANCE: {target_balance}")
                        print(f"📏 DIFFERENCE (Target - Current): {difference}")
                        
                        # Keep existing log statements too
                        log.info(separator)
                        log.info("📊 ROW %d ANALYSIS: %s/%s", row_id, prod_code, ccy_code)
                        log.info(separator)
                        log.info("📋 INDIVIDUAL BUCKET VALUES:")
                        for i, (col, val) in enumerate(zip(bucket_cols, bucket_values)):
                            log.info("   %s: %s", col, val)
                        log.info("➕ TOTAL ACROSS ALL BUCKETS: %s", manual_sum)
                        log.info("🗄️  DATABASE CALCULATED SUM: %s", db_calculated_sum)
                        log.info("🎯 PRODUCTBALANCE.N_BALANCE: %s", target_balance)
                        log.info("📏 DIFFERENCE (Target - Current): %s", difference)
                        
                        # Check for discrepancy between manual and DB calculation
                        calc_difference = abs(manual_sum - db_calculated_sum)
                        if calc_difference > Decimal('0.01'):
                            print("⚠️  CALCULATION MISMATCH DETECTED!")
                            print(f"   Manual sum: {manual_sum}")
                            print(f"   DB calculated sum: {db_calculated_sum}")
                            print(f"   Difference: {calc_difference}")
                            log.warning("⚠️  CALCULATION MISMATCH DETECTED!")
                            log.warning("   Manual sum: %s", manual_sum)
                            log.warning("   DB calculated sum: %s", db_calculated_sum)
                            log.warning("   Difference: %s", calc_difference)
                        
                        # REMOVED TOLERANCE CHECK - Now adjust ANY difference
                        if difference == 0:
                            print("✅ RESULT: Already perfectly balanced - NO ADJUSTMENT NEEDED")
                            print(f"   Final bucket total: {manual_sum} (unchanged)")
                            print("   Matches ProductBalance: YES")
                            log.info("✅ RESULT: Already perfectly balanced - NO ADJUSTMENT NEEDED")
                            log.info("   Final bucket total: %s (unchanged)", manual_sum)
                            log.info("   Matches ProductBalance: YES")
                            stats['within_tolerance'] += 1
                            continue
                        
                        print(f"🔧 ADJUSTMENT REQUIRED - Difference: {difference}")
                        print("   Will adjust to match ProductBalance exactly")
                        log.info("🔧 ADJUSTMENT REQUIRED - Difference: %s", difference)
                        log.info("   Will adjust to match ProductBalance exactly")
                        
                        # FIND BEST BUCKET TO ADJUST
                        best_bucket_idx = None
                        
                        # Check if we have any bucket values to work with
                        if not bucket_values:
                            log.error("❌ No bucket values found for row %d", row_id)
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row_id,
                                'error': 'No bucket values found'
                            })
                            continue
                        
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
                            if bucket_values:  # Additional safety check
                                best_bucket_idx = max(range(len(bucket_values)), key=lambda i: abs(bucket_values[i]))
                                strategy = "overall largest"
                            else:
                                log.error("❌ No bucket values available for adjustment on row %d", row_id)
                                stats['errors'] += 1
                                error_rows.append({
                                    'id': row_id,
                                    'error': 'No bucket values available for adjustment'
                                })
                                continue
                        
                        # Additional safety check for bucket_cols access
                        if best_bucket_idx is None or best_bucket_idx >= len(bucket_cols):
                            log.error("❌ Invalid bucket index %s for row %d (max: %d)", 
                                     best_bucket_idx, row_id, len(bucket_cols) - 1)
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row_id,
                                'error': f'Invalid bucket index {best_bucket_idx}'
                            })
                            continue
                        
                        target_col = bucket_cols[best_bucket_idx]
                        old_value = bucket_values[best_bucket_idx]
                        new_value = (old_value + difference).quantize(precision, ROUND_HALF_UP)
                        
                        log.info("   Selected bucket: %s (%s)", target_col, strategy)
                        log.info("   Old value: %s -> New value: %s", old_value, new_value)
                        
                        # EXECUTE UPDATE
                        try:
                            update_sql = f'UPDATE "{tbl}" SET "{target_col}" = %s WHERE id = %s'
                            cur.execute(update_sql, [new_value, row_id])
                        except Exception as e:
                            log.error("❌ Error updating row %d: %s", row_id, str(e))
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row_id,
                                'error': f'Update failed: {str(e)}'
                            })
                            continue
                        
                        # VERIFY UPDATE WORKED (around line 330-340)
                        try:
                            verify_sql = f"""
                                SELECT ({" + ".join([f'COALESCE("{col}", 0)::numeric' for col in bucket_cols])}) 
                                FROM "{tbl}" WHERE id = %s
                            """
                            cur.execute(verify_sql, [row_id])
                            verify_result = cur.fetchone()
                            
                            if verify_result:
                                final_sum = Decimal(str(verify_result[0])).quantize(precision, ROUND_HALF_UP)
                                final_difference = target_balance - final_sum
                                
                                log.info("="*80)
                                log.info("✅ ADJUSTMENT COMPLETED FOR ROW %d", row_id)
                                log.info("="*80)
                                log.info("📊 BEFORE ADJUSTMENT:")
                                log.info("   Original bucket total: %s", manual_sum)
                                log.info("   ProductBalance target: %s", target_balance)
                                log.info("   Original difference: %s", difference)
                                log.info("🔧 ADJUSTMENT MADE:")
                                log.info("   Adjusted bucket: %s", target_col)
                                log.info("   Old value: %s -> New value: %s", old_value, new_value)
                                log.info("   Adjustment amount: %s", difference)
                                log.info("📊 AFTER ADJUSTMENT:")
                                log.info("   Final bucket total: %s", final_sum)
                                log.info("   ProductBalance target: %s", target_balance)
                                log.info("   Final difference: %s", final_difference)
                                
                                if abs(final_difference) < Decimal('0.01'):
                                    log.info("   ✅ SUCCESS: Buckets now match ProductBalance!")
                                else:
                                    log.warning("   ⚠️  WARNING: Final total still doesn't match target!")
                                    log.warning("   Expected: %s, Got: %s, Off by: %s", 
                                               target_balance, final_sum, final_difference)
                            else:
                                log.error("❌ Could not verify adjustment - no result returned")
                        except Exception as e:
                            log.error("❌ Error verifying update for row %d: %s", row_id, str(e))
                            stats['errors'] += 1
                            error_rows.append({
                                'id': row_id,
                                'error': f'Verification failed: {str(e)}'
                            })
                            continue
                        
                        # Check if the new sum matches target exactly
                        if final_sum == target_balance:
                            log.info("✅ SUCCESS: New sum %s matches target %s exactly", final_sum, target_balance)
                            stats['adjusted'] += 1
                            adjusted_rows.append({
                                'id': row_id,
                                'prod_code': prod_code,
                                'ccy_code': ccy_code, 
                                'old_sum': manual_sum,
                                'new_sum': final_sum,
                                'target': target_balance,
                                'adjusted_column': target_col,
                                'old_value': old_value,
                                'new_value': new_value,
                                'difference_applied': difference
                            })
                        else:
                            log.error("❌ VERIFICATION FAILED: Expected %s, got %s", target_balance, final_sum)
                            error_rows.append({
                                'id': row_id,
                                'error': 'Verification failed',
                                'expected': target_balance,
                                'actual': final_sum
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
                        
            except Exception as e:
                log.error("❌ Error processing rows: %s", str(e))
                return {"error": f"Error processing rows: {str(e)}"}

    # FINAL SUMMARY
    log.info("=" * 80)
    log.info("🏁 ALIGNMENT COMPLETE")
    log.info("📊 STATISTICS:")
    log.info("   Rows processed: %d", stats['processed'])
    log.info("   Successfully adjusted: %d", stats['adjusted']) 
    log.info("   Already perfectly balanced: %d", stats['within_tolerance'])
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
            log.warning("   %s/%s (%s): sum=%s (no target balance)", 
                       missing['prod_code'], missing['ccy_code'], missing['prod_type'], missing['current_sum'])
    
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
def test_alignment_results(fic_mis_date, process_name=None):
    """
    Test function to verify alignment worked correctly.
    Run this after align_buckets_to_balance() to double-check results.
    """
    try:
        fic_mis_date = _to_date(fic_mis_date)
    except Exception as e:
        return {"error": f"Invalid date format: {str(e)}"}
        
    tbl = f"Report_Contractual_{fic_mis_date:%Y%m%d}"
    
    log.info("🧪 TESTING ALIGNMENT RESULTS FOR %s", tbl)
    
    mismatches = []
    
    try:
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
                  AND pb.n_balance::numeric != ({bucket_sum_sql})
                ORDER BY difference DESC
            """
            
            cur.execute(test_query, params)
            
            for row in cur.fetchall():
                row_id, prod_code, ccy_code, bucket_sum, target_balance, difference = row
                mismatches.append({
                    'id': row_id,
                    'prod_code': prod_code,
                    'ccy_code': ccy_code,
                    'bucket_sum': float(bucket_sum),
                    'target_balance': float(target_balance),
                    'difference': float(difference)
                })
            
            if mismatches:
                log.warning("⚠️  Found %d mismatches after alignment!", len(mismatches))
                for mismatch in mismatches[:5]:  # Show first 5
                    log.warning("   %s/%s: bucket_sum=%s, target=%s, diff=%s", 
                               mismatch['prod_code'], mismatch['ccy_code'],
                               mismatch['bucket_sum'], mismatch['target_balance'], 
                               mismatch['difference'])
            else:
                log.info("✅ All rows are perfectly aligned!")
                
    except Exception as e:
        log.error("❌ Error testing alignment results: %s", str(e))
        return {"error": f"Error testing alignment results: {str(e)}"}
    
    return {
        'success': True,
        'mismatches_found': len(mismatches),
        'mismatches': mismatches
    }


