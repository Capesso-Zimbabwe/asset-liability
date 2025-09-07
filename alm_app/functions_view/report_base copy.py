from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.db import connection
from django.apps import apps
from django.http import JsonResponse
from django.contrib import messages
from datetime import datetime, date, timedelta
from typing import Union, Dict, List, Any
from decimal import Decimal
import logging
from collections import defaultdict
from ..functions_view.report_buckets import bucket_column_name

# Set up logging
logger = logging.getLogger(__name__)

# Lazy model lookups
TBM = apps.get_model("alm_app", "TimeBucketMaster")


def _to_date(value: Union[date, datetime, str]) -> date:
    """
    Normalize fic_mis_date to datetime.date with better error handling
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        # Try different date formats
        formats = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y%m%d"]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Unable to parse date: {value}")
    raise TypeError(f"fic_mis_date must be date, datetime, or string, got {type(value)}")


def get_table_name(fic_mis_date: Union[date, datetime, str]) -> str:
    """
    Generate the correct table name from the date
    """
    date_obj = _to_date(fic_mis_date)
    table_name = f"Report_Contractual_{date_obj.strftime('%Y%m%d')}"
    logger.info(f"Generated table name: {table_name} from date: {date_obj}")
    return table_name


def get_available_report_tables() -> List[Dict[str, Any]]:
    """
    Get list of available Report_Contractual tables from the database
    PostgreSQL compatible version
    """
    with connection.cursor() as cursor:
        # PostgreSQL stores table names in lowercase in information_schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name LIKE 'report_contractual_%'
            ORDER BY table_name DESC
        """)
        tables = cursor.fetchall()
        
        available_tables = []
        for (table_name,) in tables:
            # Extract date from table name
            try:
                # PostgreSQL returns lowercase table names
                date_part = table_name.split('_')[-1]  # Get the date part
                if len(date_part) == 8 and date_part.isdigit():
                    date_obj = datetime.strptime(date_part, '%Y%m%d').date()
                    # Store the actual table name as it appears in the database
                    available_tables.append({
                        'table_name': table_name,
                        'actual_table_name': f"Report_Contractual_{date_part}",  # Original case
                        'date': date_obj,
                        'formatted_date': date_obj.strftime('%Y-%m-%d'),
                        'display_date': date_obj.strftime('%d %b %Y')
                    })
            except Exception as e:
                logger.warning(f"Could not parse date from table name {table_name}: {e}")
                continue
        
        return available_tables


def format_time_range(start_date: date, end_date: date, base_date: date = None) -> str:
    """
    Format time range into meaningful display (e.g., "91-180 days", "1-2 years", "Over 5 years")
    """
    if base_date is None:
        base_date = start_date
    
    # Calculate days from base date
    start_days = (start_date - base_date).days
    end_days = (end_date - base_date).days
    
    # Handle immediate/overnight
    if start_days == 0 and end_days <= 1:
        return "Overnight"
    elif start_days == 0 and end_days <= 7:
        return f"0-{end_days} days"
    
    # Convert to years for longer periods
    start_years = start_days / 365.25
    end_years = end_days / 365.25
    
    # Format based on duration
    if end_days <= 30:
        return f"{start_days}-{end_days} days"
    elif end_days <= 90:
        return f"{start_days}-{end_days} days"
    elif end_days <= 180:
        return f"{start_days}-{end_days} days"
    elif end_days <= 365:
        return f"{start_days}-{end_days} days"
    elif end_years <= 2:
        if start_years < 1:
            return f"{start_days} days-{end_years:.1f} years"
        else:
            return f"{start_years:.1f}-{end_years:.1f} years"
    elif end_years <= 5:
        return f"{start_years:.1f}-{end_years:.1f} years"
    else:
        if start_years >= 5:
            return "Over 5 years"
        else:
            return f"{start_years:.1f} years+"


def get_bucket_columns(process_name: str, base_date: date = None) -> List[Dict[str, Any]]:
    """Get bucket column information for the given process with enhanced formatting"""
    buckets = TBM.objects.filter(process_name=process_name).order_by('bucket_number')
    bucket_columns = []
    
    # If no base_date provided, use the first bucket's start date
    if base_date is None and buckets.exists():
        base_date = buckets.first().start_date
    
    for bucket in buckets:
        column_name = bucket_column_name(bucket)
        
        # Create time range display
        time_range = format_time_range(bucket.start_date, bucket.end_date, base_date)
        
        # Create short display for headers
        days_diff = (bucket.end_date - bucket.start_date).days
        if days_diff <= 7:
            short_display = f"{days_diff}D"
        elif days_diff <= 30:
            short_display = f"{days_diff}D"
        elif days_diff <= 90:
            short_display = f"{days_diff}D"
        elif days_diff <= 365:
            short_display = f"{days_diff}D"
        else:
            years = days_diff / 365.25
            short_display = f"{years:.1f}Y"
        
        bucket_columns.append({
            'column_name': column_name,
            'bucket_number': bucket.bucket_number,
            'start_date': bucket.start_date,
            'end_date': bucket.end_date,
            'display_name': time_range,
            'short_display': short_display,
            'header_display': bucket.start_date.strftime('%d-%b-%Y'),
            'days_from_base': (bucket.start_date - base_date).days if base_date else 0,
            'duration_days': days_diff,
            'is_long_term': days_diff > 365
        })
    
    return bucket_columns


def aggregate_products_by_type(products: List[Dict], bucket_columns: List[Dict]) -> Dict[str, Dict]:
    """
    Aggregate products by product type for three-level drill-down functionality:
    Level 1: Product Type
    Level 2: Product Name  
    Level 3: Product Splits (if v_product_splits is not null) - aggregated by split name
    """
    aggregated = defaultdict(lambda: {
        'v_prod_type': '',
        'v_prod_type_desc': '',
        'account_types': set(),
        'product_count': 0,
        'products': [],
        'products_by_name': defaultdict(lambda: {
            'v_product_name': '',
            'products': [],
            'splits_by_name': defaultdict(lambda: {
                'v_product_splits': '',
                'products': [],
                'bucket_values': defaultdict(lambda: Decimal('0')),
                'adjusted_amount': Decimal('0'),
                'total_amount': Decimal('0'),
                'total_after_adjustment': Decimal('0'),
                'account_types': set()
            }),
            'non_split_products': [],  # Products without splits
            'bucket_values': defaultdict(lambda: Decimal('0')),
            'adjusted_amount': Decimal('0'),
            'total_amount': Decimal('0'),
            'total_after_adjustment': Decimal('0'),
            'has_splits': False,
            'split_count': 0
        }),
        'bucket_values': defaultdict(lambda: Decimal('0')),
        'adjusted_amount': Decimal('0'),
        'total_amount': Decimal('0'),
        'total_after_adjustment': Decimal('0')
    })
    
    for product in products:
        prod_type = product['v_prod_type'] or 'Unknown'
        prod_name = product['v_product_name'] or 'Unknown'
        prod_split = product.get('v_product_splits', '').strip()
        
        # Add to aggregated data by type
        agg = aggregated[prod_type]
        agg['v_prod_type'] = prod_type
        agg['v_prod_type_desc'] = product['v_prod_type_desc'] or ''
        agg['account_types'].add(product['account_type'])
        agg['product_count'] += 1
        agg['products'].append(product)
        
        # Add to products by name within type
        prod_by_name = agg['products_by_name'][prod_name]
        prod_by_name['v_product_name'] = prod_name
        prod_by_name['products'].append(product)
        
        # Handle product splits vs non-split products
        if prod_split:
            # This product has splits - aggregate by split name
            prod_by_name['has_splits'] = True
            
            # Aggregate splits by split name
            split_agg = prod_by_name['splits_by_name'][prod_split]
            split_agg['v_product_splits'] = prod_split
            split_agg['products'].append(product)
            split_agg['account_types'].add(product['account_type'])
            
            # Sum bucket values for this split
            for col in bucket_columns:
                split_agg['bucket_values'][col['column_name']] += product['bucket_values'][col['column_name']]
            
            # Sum totals for this split
            split_agg['adjusted_amount'] += product['adjusted_amount']
            split_agg['total_amount'] += product['total_amount']  # This is bucket values only
            split_agg['total_after_adjustment'] += product['total_after_adjustment']  # This includes adjusted
        else:
            # This product doesn't have splits
            prod_by_name['non_split_products'].append(product)
        
        # Sum bucket values for type level
        for col in bucket_columns:
            agg['bucket_values'][col['column_name']] += product['bucket_values'][col['column_name']]
            prod_by_name['bucket_values'][col['column_name']] += product['bucket_values'][col['column_name']]
        
        # Sum totals for type level
        agg['adjusted_amount'] += product['adjusted_amount']
        agg['total_amount'] += product['total_amount']
        agg['total_after_adjustment'] += product['total_after_adjustment']
        
        # Sum totals for product name level
        prod_by_name['adjusted_amount'] += product['adjusted_amount']
        prod_by_name['total_amount'] += product['total_amount']
        prod_by_name['total_after_adjustment'] += product['total_after_adjustment']
    
    # Convert sets to strings and format
    for prod_type, agg in aggregated.items():
        agg['account_types'] = ', '.join(sorted(agg['account_types']))
        agg['display_name'] = f"{prod_type} ({agg['product_count']} products)"
        # Convert defaultdict to regular dict for template compatibility
        agg['bucket_values'] = dict(agg['bucket_values'])
        
        # Process products by name
        for prod_name, prod_data in agg['products_by_name'].items():
            prod_data['bucket_values'] = dict(prod_data['bucket_values'])
            
            # Process splits by name and convert to list
            splits_list = []
            for split_name, split_data in prod_data['splits_by_name'].items():
                split_data['bucket_values'] = dict(split_data['bucket_values'])
                split_data['account_types'] = ', '.join(sorted(split_data['account_types']))
                split_data['product_count'] = len(split_data['products'])
                splits_list.append(split_data)
            
            # Sort splits by name for consistent display
            splits_list.sort(key=lambda x: x['v_product_splits'])
            prod_data['splits'] = splits_list
            prod_data['split_count'] = len(splits_list)
            
            # Create display name based on whether it has splits
            if prod_data['has_splits']:
                prod_data['display_name'] = f"{prod_name} ({prod_data['split_count']} splits)"
            else:
                non_split_count = len(prod_data['non_split_products'])
                prod_data['display_name'] = f"{prod_name} ({non_split_count} items)"
            
            # Remove the defaultdict to avoid template issues
            del prod_data['splits_by_name']
        
        # Convert defaultdict to regular dict
        agg['products_by_name'] = dict(agg['products_by_name'])
    
    return dict(aggregated)


def get_available_processes():
    """Get list of available processes from TimeBucketMaster"""
    processes = TBM.objects.values_list('process_name', flat=True).distinct().order_by('process_name')
    return list(processes)


def check_table_exists(table_name: str) -> bool:
    """
    Check if the report table exists - PostgreSQL compatible
    """
    with connection.cursor() as cursor:
        # Check both with original case and lowercase
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND (table_name = %s OR table_name = %s)
            );
        """, [table_name.lower(), table_name])
        exists = cursor.fetchone()[0]
        logger.info(f"Table {table_name} exists: {exists}")
        return exists


def get_actual_table_name(table_name: str) -> str:
    """
    Get the actual table name as it exists in the database
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND (table_name = %s OR table_name = %s)
            LIMIT 1
        """, [table_name.lower(), table_name])
        result = cursor.fetchone()
        if result:
            actual_name = result[0]
            # If it's lowercase in the database, we need to use the original case for queries
            if actual_name.islower():
                return table_name  # Use the original case
            return actual_name
        return table_name


def get_table_row_count(table_name: str) -> int:
    """Get the number of rows in the table"""
    try:
        actual_table_name = get_actual_table_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{actual_table_name}"')
            count = cursor.fetchone()[0]
            logger.info(f"Table {actual_table_name} has {count} rows")
            return count
    except Exception as e:
        logger.error(f"Error getting row count for {table_name}: {e}")
        return 0


def validate_table_structure(table_name: str) -> Dict[str, Any]:
    """
    Validate that the table has the required structure for the report
    PostgreSQL compatible version
    """
    try:
        with connection.cursor() as cursor:
            # Check if required columns exist
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                  AND (table_name = %s OR table_name = %s)
                ORDER BY ordinal_position
            """, [table_name.lower(), table_name])
            
            columns = [row[0] for row in cursor.fetchall()]
            
            required_columns = [
                'fic_mis_date', 'process_name', 'v_product_name', 
                'flow_type', 'financial_element', 'account_type',
                'n_adjusted_cash_flow_amount'
            ]
            
            missing_columns = [col for col in required_columns if col not in columns]
            bucket_columns = [col for col in columns if col.startswith('bucket_')]
            
            return {
                'valid': len(missing_columns) == 0,
                'missing_columns': missing_columns,
                'total_columns': len(columns),
                'bucket_columns': len(bucket_columns),
                'all_columns': columns
            }
    except Exception as e:
        logger.error(f"Error validating table structure for {table_name}: {e}")
        return {
            'valid': False,
            'error': str(e)
        }


def debug_table_info(table_name: str) -> Dict[str, Any]:
    """
    Debug function to get detailed table information
    """
    debug_info = {
        'requested_table': table_name,
        'tables_found': [],
        'exact_match': False,
        'case_variations': []
    }
    
    try:
        with connection.cursor() as cursor:
            # Get all tables that might match
            cursor.execute("""
                SELECT table_name, table_schema 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name LIKE %s
                ORDER BY table_name
            """, [f"%{table_name.lower().replace('report_contractual_', '')}%"])
            
            tables = cursor.fetchall()
            debug_info['tables_found'] = [{'name': t[0], 'schema': t[1]} for t in tables]
            
            # Check exact matches
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name = %s
            """, [table_name.lower()])
            
            exact_match = cursor.fetchone()
            debug_info['exact_match'] = exact_match is not None
            if exact_match:
                debug_info['exact_match_name'] = exact_match[0]
            
            # Try different case variations
            variations = [
                table_name,
                table_name.lower(),
                table_name.upper(),
            ]
            
            for variation in variations:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                      AND table_name = %s
                """, [variation])
                
                result = cursor.fetchone()
                if result:
                    debug_info['case_variations'].append({
                        'variation': variation,
                        'found': result[0]
                    })
    
    except Exception as e:
        debug_info['error'] = str(e)
    
    return debug_info


def get_available_currencies(table_name: str, process_name: str) -> List[str]:
    """
    Get list of available currencies from the report table
    """
    try:
        actual_table_name = get_actual_table_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(f'''
                SELECT DISTINCT v_ccy_code 
                FROM "{actual_table_name}"
                WHERE financial_element = 'n_total_cash_flow_amount'
                  AND flow_type IN ('inflow', 'outflow')
                  AND process_name = %s
                  AND v_ccy_code IS NOT NULL
                ORDER BY v_ccy_code
            ''', [process_name])
            
            currencies = [row[0] for row in cursor.fetchall()]
            logger.info(f"Available currencies in {actual_table_name}: {currencies}")
            return currencies
    except Exception as e:
        logger.error(f"Error getting currencies from {table_name}: {e}")
        return []


@login_required
def get_currencies_api(request, fic_mis_date: Union[str, date], process_name: str):
    """
    API endpoint to get available currencies for a given date and process
    """
    try:
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist.'}, status=404)
        
        # Get available currencies
        currencies = get_available_currencies(table_name, process_name)
        
        return JsonResponse({
            'currencies': currencies,
            'date': date_obj.strftime('%Y-%m-%d'),
            'process_name': process_name,
            'table_name': table_name
        })
        
    except Exception as e:
        logger.error(f"Error getting currencies: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
def contractual_gap_report_form(request):
    """
    Display form for users to input date and process name for the contractual gap report
    """
    available_processes = get_available_processes()
    available_tables = get_available_report_tables()
    
    if request.method == 'POST':
        fic_mis_date = request.POST.get('fic_mis_date')
        process_name = request.POST.get('process_name')
        selected_currency = request.POST.get('currency', 'all')
        
        if not fic_mis_date or not process_name:
            messages.error(request, 'Please provide both date and process name.')
            return render(request, 'reports/contractual_gap_form.html', {
                'available_processes': available_processes,
                'available_tables': available_tables
            })
        
        try:
            # Validate and convert date
            date_obj = _to_date(fic_mis_date)
            table_name = get_table_name(date_obj)
            
            logger.info(f"Processing request for date: {date_obj}, process: {process_name}, table: {table_name}")
            
            # Debug table information
            debug_info = debug_table_info(table_name)
            logger.info(f"Debug info for table {table_name}: {debug_info}")
            
            # Check if table exists
            if not check_table_exists(table_name):
                available_dates = [t['display_date'] for t in available_tables]
                messages.error(request, 
                    f'Report table "{table_name}" for date {date_obj.strftime("%d %b %Y")} does not exist. '
                    f'Available dates: {", ".join(available_dates) if available_dates else "None"}. '
                    f'Debug info: Found {len(debug_info["tables_found"])} similar tables.')
                return render(request, 'reports/contractual_gap_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name,
                    'debug_info': debug_info
                })
            
            # Validate table structure
            validation = validate_table_structure(table_name)
            if not validation['valid']:
                messages.error(request, 
                    f'Table "{table_name}" has invalid structure. '
                    f'Missing columns: {", ".join(validation.get("missing_columns", []))}')
                return render(request, 'reports/contractual_gap_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name
                })
            
            # Check if table has data
            row_count = get_table_row_count(table_name)
            if row_count == 0:
                messages.warning(request, 
                    f'Table "{table_name}" exists but contains no data. '
                    f'Please ensure data has been loaded for {date_obj.strftime("%d %b %Y")}.')
                return render(request, 'reports/contractual_gap_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name
                })
            
            # Check if process has bucket configuration
            bucket_columns = get_bucket_columns(process_name, date_obj)
            if not bucket_columns:
                messages.error(request, 
                    f'No bucket configuration found for process: "{process_name}". '
                    f'Please ensure TimeBucketMaster is configured for this process.')
                return render(request, 'reports/contractual_gap_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name
                })
            
            # Get available currencies
            available_currencies = get_available_currencies(table_name, process_name)
            if not available_currencies:
                messages.warning(request, 
                    f'No currency data found for process "{process_name}". '
                    f'Please ensure currency codes are properly set in the data.')
                return render(request, 'reports/contractual_gap_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name,
                    'available_currencies': available_currencies
                })
            
            # All validations passed - redirect to the report with currency parameter
            messages.success(request, f'Report generated successfully for {date_obj.strftime("%d %b %Y")}')
            
            # Build URL with currency parameter
            if selected_currency == 'all':
                return redirect('alm_app:contractual_gap_report', 
                              fic_mis_date=date_obj.strftime('%Y-%m-%d'), 
                              process_name=process_name)
            else:
                return redirect('alm_app:contractual_gap_report_currency', 
                              fic_mis_date=date_obj.strftime('%Y-%m-%d'), 
                              process_name=process_name,
                              currency=selected_currency)
            
        except Exception as e:
            logger.error(f"Error processing report request: {e}")
            messages.error(request, f'Error processing request: {str(e)}')
            return render(request, 'reports/contractual_gap_form.html', {
                'available_processes': available_processes,
                'available_tables': available_tables,
                'fic_mis_date': fic_mis_date,
                'process_name': process_name
            })
    
    # GET request - show the form
    return render(request, 'reports/contractual_gap_form.html', {
        'available_processes': available_processes,
        'available_tables': available_tables
    })


@login_required
def contractual_gap_report(request, fic_mis_date: Union[str, date], process_name: str, currency: str = 'all'):
    """
    Generate the Contractual Gap report from Report_Contractual_<YYYYMMDD> table.
    
    The report shows:
    - Total Inflows (flow_type = 'inflow')
    - Total Outflows (flow_type = 'outflow')  
    - Net Liquidity Gap (Inflows - Outflows)
    - Net Gap % of Total Outflows
    - Cumulative Gap (running sum)
    
    Args:
        currency: Currency code to filter by, or 'all' for all currencies
    """
    try:
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        logger.info(f"Generating report for date: {date_obj}, process: {process_name}, table: {table_name}, actual: {actual_table_name}, currency: {currency}")
        
        # Check if table exists
        if not check_table_exists(table_name):
            messages.error(request, f'Report table "{table_name}" does not exist.')
            return redirect('alm_app:contractual_gap_report_form')
        
        # Get available currencies for filtering
        available_currencies = get_available_currencies(table_name, process_name)
        
        # Get bucket columns for this process with enhanced formatting
        bucket_columns = get_bucket_columns(process_name, date_obj)
        
        if not bucket_columns:
            messages.error(request, f'No bucket columns found for process: {process_name}')
            return redirect('alm_app:contractual_gap_report_form')
        
        # Build the dynamic SQL query with currency filtering
        bucket_column_names = [col['column_name'] for col in bucket_columns]
        bucket_select_clause = ', '.join([f'"{col}"' for col in bucket_column_names])
        
        # Build currency filter condition
        currency_condition = ""
        query_params = [process_name]
        
        if currency != 'all':
            currency_condition = "AND v_ccy_code = %s"
            query_params.append(currency)
        
        with connection.cursor() as cursor:
            # First check if there's any data for this process, date, and currency
            count_query = f'''
                SELECT COUNT(*) FROM "{actual_table_name}"
                WHERE financial_element = 'n_total_cash_flow_amount'
                  AND flow_type IN ('inflow', 'outflow')
                  AND process_name = %s
                  {currency_condition}
            '''
            
            cursor.execute(count_query, query_params)
            record_count = cursor.fetchone()[0]
            
            if record_count == 0:
                currency_msg = f' and currency "{currency}"' if currency != 'all' else ''
                messages.warning(request, 
                    f'No data found in "{actual_table_name}" for process "{process_name}"{currency_msg} '
                    f'with the specified criteria.')
                return redirect('alm_app:contractual_gap_report_form')
            
            # Query for inflows and outflows data - now including currency
            query = f"""
            SELECT 
                flow_type,
                v_product_name,
                v_product_splits,
                v_prod_type,
                v_prod_type_desc,
                account_type,
                v_ccy_code,
                n_adjusted_cash_flow_amount,
                {bucket_select_clause}
            FROM "{actual_table_name}"
            WHERE financial_element = 'n_total_cash_flow_amount'
              AND flow_type IN ('inflow', 'outflow')
              AND process_name = %s
              {currency_condition}
            ORDER BY v_ccy_code ASC, flow_type DESC, v_prod_type ASC, v_product_name ASC, v_product_splits ASC
            """
            
            logger.info(f"Executing query with params: {query_params}")
            cursor.execute(query, query_params)
            columns = [desc[0] for desc in cursor.description]
            raw_data = cursor.fetchall()
            
            logger.info(f"Query returned {len(raw_data)} rows")
        
        # Group data by currency if showing all currencies
        currency_data = {}
        
        for row in raw_data:
            row_dict = dict(zip(columns, row))
            row_currency = row_dict['v_ccy_code'] or 'Unknown'
            
            if row_currency not in currency_data:
                currency_data[row_currency] = {
                    'inflows': [],
                    'outflows': [],
                    'currency_code': row_currency
                }
            
            # Calculate bucket totals for this row
            bucket_total = Decimal('0')
            bucket_values = {}
            
            for col in bucket_columns:
                value = row_dict.get(col['column_name']) or Decimal('0')
                bucket_values[col['column_name']] = value
                bucket_total += value
            
            # Add adjusted amount
            adjusted_amount = row_dict.get('n_adjusted_cash_flow_amount') or Decimal('0')
            total_amount = bucket_total  # Total = bucket values only (excludes adjusted)
            total_after_adjustment = bucket_total + adjusted_amount  # Total after adjustment includes adjusted
            
            product_data = {
                'v_product_name': row_dict['v_product_name'] or '',
                'v_product_splits': row_dict['v_product_splits'] or '',
                'v_prod_type': row_dict['v_prod_type'] or '',
                'v_prod_type_desc': row_dict['v_prod_type_desc'] or '',
                'account_type': row_dict['account_type'] or '',
                'v_ccy_code': row_currency,
                'bucket_values': bucket_values,
                'adjusted_amount': adjusted_amount,
                'total_amount': total_amount,
                'total_after_adjustment': total_after_adjustment
            }
            
            if row_dict['flow_type'] == 'inflow':
                currency_data[row_currency]['inflows'].append(product_data)
            else:
                currency_data[row_currency]['outflows'].append(product_data)
        
        # Process each currency's data
        currency_reports = {}
        
        for curr_code, curr_data in currency_data.items():
            inflows = curr_data['inflows']
            outflows = curr_data['outflows']
            
            logger.info(f"Processing currency {curr_code}: {len(inflows)} inflows and {len(outflows)} outflows")
            
            # Aggregate products by type for drill-down functionality
            inflow_aggregated = aggregate_products_by_type(inflows, bucket_columns)
            outflow_aggregated = aggregate_products_by_type(outflows, bucket_columns)
            
            # Calculate column totals
            inflow_totals = {}
            outflow_totals = {}
            
            # Initialize totals
            for col in bucket_columns:
                inflow_totals[col['column_name']] = Decimal('0')
                outflow_totals[col['column_name']] = Decimal('0')
            
            inflow_totals['adjusted_amount'] = Decimal('0')
            inflow_totals['total_amount'] = Decimal('0')
            inflow_totals['total_after_adjustment'] = Decimal('0')
            
            outflow_totals['adjusted_amount'] = Decimal('0')
            outflow_totals['total_amount'] = Decimal('0')
            outflow_totals['total_after_adjustment'] = Decimal('0')
            
            # Sum inflows
            for product in inflows:
                for col in bucket_columns:
                    inflow_totals[col['column_name']] += product['bucket_values'][col['column_name']]
                inflow_totals['adjusted_amount'] += product['adjusted_amount']
                inflow_totals['total_amount'] += product['total_amount']
                inflow_totals['total_after_adjustment'] += product['total_after_adjustment']
            
            # Sum outflows
            for product in outflows:
                for col in bucket_columns:
                    outflow_totals[col['column_name']] += product['bucket_values'][col['column_name']]
                outflow_totals['adjusted_amount'] += product['adjusted_amount']
                outflow_totals['total_amount'] += product['total_amount']
                outflow_totals['total_after_adjustment'] += product['total_after_adjustment']
            
            # Calculate Net Liquidity Gap (Inflows - Outflows)
            net_gap = {}
            net_gap_percent = {}
            cumulative_gap = {}
            running_total = Decimal('0')
            
            for col in bucket_columns:
                gap_value = inflow_totals[col['column_name']] - outflow_totals[col['column_name']]
                net_gap[col['column_name']] = gap_value
                
                # Calculate percentage (avoid division by zero)
                if outflow_totals[col['column_name']] != 0:
                    net_gap_percent[col['column_name']] = (gap_value / outflow_totals[col['column_name']]) * 100
                else:
                    net_gap_percent[col['column_name']] = Decimal('0')
                
                # Calculate cumulative gap
                running_total += gap_value
                cumulative_gap[col['column_name']] = running_total
            
            # Net gap for adjusted and total columns
            net_gap['adjusted_amount'] = inflow_totals['adjusted_amount'] - outflow_totals['adjusted_amount']
            net_gap['total_amount'] = inflow_totals['total_amount'] - outflow_totals['total_amount']
            net_gap['total_after_adjustment'] = inflow_totals['total_after_adjustment'] - outflow_totals['total_after_adjustment']
            
            # Net gap percentage for totals
            if outflow_totals['total_amount'] != 0:
                net_gap_percent['total_amount'] = (net_gap['total_amount'] / outflow_totals['total_amount']) * 100
            else:
                net_gap_percent['total_amount'] = Decimal('0')
            
            # Store currency report data
            currency_reports[curr_code] = {
                'currency_code': curr_code,
                'inflows': inflows,
                'outflows': outflows,
                'inflow_aggregated': inflow_aggregated,
                'outflow_aggregated': outflow_aggregated,
                'inflow_totals': inflow_totals,
                'outflow_totals': outflow_totals,
                'net_gap': net_gap,
                'net_gap_percent': net_gap_percent,
                'cumulative_gap': cumulative_gap,
                'record_count': len(inflows) + len(outflows)
            }
        
        # Prepare context for template
        context = {
            'fic_mis_date': date_obj,
            'process_name': process_name,
            'bucket_columns': bucket_columns,
            'currency_reports': currency_reports,
            'available_currencies': available_currencies,
            'selected_currency': currency,
            'show_all_currencies': currency == 'all',
            'table_name': actual_table_name,
            'total_record_count': record_count
        }
        
        return render(request, 'reports/contractual_gap_report.html', context)
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        messages.error(request, f'Error generating report: {str(e)}')
        return redirect('alm_app:contractual_gap_report_form')


@login_required
def contractual_gap_report_api(request, fic_mis_date: Union[str, date], process_name: str):
    """
    API endpoint that returns the contractual gap report data as JSON
    """
    try:
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist.'}, status=404)
        
        # Get bucket columns for this process
        bucket_columns = get_bucket_columns(process_name, date_obj)
        
        if not bucket_columns:
            return JsonResponse({'error': f'No bucket columns found for process: {process_name}'}, status=400)
        
        # Build the dynamic SQL query
        bucket_column_names = [col['column_name'] for col in bucket_columns]
        bucket_select_clause = ', '.join([f'"{col}"' for col in bucket_column_names])
        
        with connection.cursor() as cursor:
            # Query for inflows and outflows data - now including v_product_splits
            query = f"""
            SELECT 
                flow_type,
                v_product_name,
                v_product_splits,
                v_prod_type,
                v_prod_type_desc,
                account_type,
                n_adjusted_cash_flow_amount,
                {bucket_select_clause}
            FROM "{actual_table_name}"
            WHERE financial_element = 'n_total_cash_flow_amount'
              AND flow_type IN ('inflow', 'outflow')
              AND process_name = %s
            ORDER BY flow_type DESC, v_prod_type ASC, v_product_name ASC, v_product_splits ASC
            """
            
            cursor.execute(query, [process_name])
            columns = [desc[0] for desc in cursor.description]
            raw_data = cursor.fetchall()
        
        # Convert Decimal values to float for JSON serialization
        def decimal_to_float(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            return obj
        
        # Process the data (similar to above but with JSON serialization)
        result_data = {
            'fic_mis_date': date_obj.strftime('%Y-%m-%d'),
            'process_name': process_name,
            'table_name': actual_table_name,
            'bucket_columns': bucket_columns,
            'raw_data': []
        }
        
        for row in raw_data:
            row_dict = dict(zip(columns, row))
            # Convert Decimal values to float
            for key, value in row_dict.items():
                row_dict[key] = decimal_to_float(value)
            result_data['raw_data'].append(row_dict)
        
        return JsonResponse(result_data)
        
    except Exception as e:
        logger.error(f"API error: {e}")
        return JsonResponse({'error': str(e)}, status=500)
