import logging
from datetime import date, datetime
from typing import Union, List, Dict, Any
from decimal import Decimal

from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import connection, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json

from .report_base import (
    _to_date, 
    get_table_name, 
    get_actual_table_name, 
    check_table_exists,
    get_available_processes,
    get_available_report_tables,
    get_available_currencies,
    get_bucket_columns
)

logger = logging.getLogger(__name__)


@login_required
def adjustments_form(request):
    """
    Display form for users to select date, process, and table for adjustments
    """
    available_processes = get_available_processes()
    available_tables = get_available_report_tables()
    
    if request.method == 'POST':
        fic_mis_date = request.POST.get('fic_mis_date')
        process_name = request.POST.get('process_name')
        
        if not fic_mis_date or not process_name:
            messages.error(request, 'Please provide both date and process name.')
            return render(request, 'reports/adjustments_form.html', {
                'available_processes': available_processes,
                'available_tables': available_tables
            })
        
        try:
            # Validate and convert date
            date_obj = _to_date(fic_mis_date)
            table_name = get_table_name(date_obj)
            
            logger.info(f"Processing adjustments request for date: {date_obj}, process: {process_name}, table: {table_name}")
            
            # Check if table exists
            if not check_table_exists(table_name):
                available_dates = [t['display_date'] for t in available_tables]
                messages.error(request, 
                    f'Report table "{table_name}" for date {date_obj.strftime("%d %b %Y")} does not exist. '
                    f'Available dates: {", ".join(available_dates) if available_dates else "None"}.')
                return render(request, 'reports/adjustments_form.html', {
                    'available_processes': available_processes,
                    'available_tables': available_tables,
                    'fic_mis_date': fic_mis_date,
                    'process_name': process_name
                })
            
            # Redirect to adjustments management page
            return redirect('adjustments_manage', 
                          fic_mis_date=date_obj.strftime('%Y-%m-%d'), 
                          process_name=process_name)
            
        except Exception as e:
            logger.error(f"Error processing adjustments form: {e}")
            messages.error(request, f'Error processing request: {str(e)}')
            return render(request, 'reports/adjustments_form.html', {
                'available_processes': available_processes,
                'available_tables': available_tables,
                'fic_mis_date': fic_mis_date,
                'process_name': process_name
            })
    
    return render(request, 'reports/adjustments_form.html', {
        'available_processes': available_processes,
        'available_tables': available_tables
    })


@login_required
def adjustments_manage(request, fic_mis_date: Union[str, date], process_name: str):
    """
    Main adjustments management page - shows existing adjustments and allows adding new ones
    """
    try:
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        logger.info(f"Managing adjustments for date: {date_obj}, process: {process_name}, table: {table_name}")
        
        # Check if table exists
        if not check_table_exists(table_name):
            messages.error(request, f'Report table "{table_name}" does not exist.')
            return redirect('adjustments_form')
        
        # Get available currencies and product types
        available_currencies = get_available_currencies(table_name, process_name)
        product_types = get_product_types(table_name, process_name)
        
        # Get existing adjustments
        existing_adjustments = get_existing_adjustments(table_name, process_name)
        
        # Get bucket columns for display
        bucket_columns = get_bucket_columns(process_name, date_obj)
        
        context = {
            'fic_mis_date': date_obj,
            'process_name': process_name,
            'table_name': actual_table_name,
            'available_currencies': available_currencies,
            'product_types': product_types,
            'existing_adjustments': existing_adjustments,
            'bucket_columns': bucket_columns,
        }
        
        return render(request, 'reports/adjustments_manage.html', context)
        
    except Exception as e:
        logger.error(f"Error managing adjustments: {e}")
        messages.error(request, f'Error managing adjustments: {str(e)}')
        return redirect('adjustments_form')


def get_product_types(table_name: str, process_name: str) -> List[Dict[str, Any]]:
    """
    Get available product types for the given table and process
    """
    actual_table_name = get_actual_table_name(table_name)
    
    with connection.cursor() as cursor:
        query = f"""
        SELECT DISTINCT 
            v_prod_type,
            v_prod_type_desc,
            v_ccy_code,
            flow_type,
            COUNT(*) as record_count
        FROM "{actual_table_name}"
        WHERE financial_element = 'n_total_cash_flow_amount'
          AND process_name = %s
          AND flow_type IN ('inflow', 'outflow')
        GROUP BY v_prod_type, v_prod_type_desc, v_ccy_code, flow_type
        ORDER BY v_ccy_code, flow_type DESC, v_prod_type
        """
        
        cursor.execute(query, [process_name])
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        product_types = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            product_types.append(row_dict)
        
        return product_types


def get_existing_adjustments(table_name: str, process_name: str) -> List[Dict[str, Any]]:
    """
    Get existing adjustments (records with non-zero n_adjusted_cash_flow_amount)
    """
    actual_table_name = get_actual_table_name(table_name)
    
    with connection.cursor() as cursor:
        query = f"""
        SELECT 
            v_prod_type,
            v_prod_type_desc,
            v_product_name,
            v_ccy_code,
            flow_type,
            n_adjusted_cash_flow_amount,
            account_type
        FROM "{actual_table_name}"
        WHERE financial_element = 'n_total_cash_flow_amount'
          AND process_name = %s
          AND n_adjusted_cash_flow_amount IS NOT NULL
          AND n_adjusted_cash_flow_amount != 0
        ORDER BY v_ccy_code, flow_type DESC, v_prod_type, v_product_name
        """
        
        cursor.execute(query, [process_name])
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        adjustments = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            # Convert Decimal to float for JSON serialization
            if row_dict['n_adjusted_cash_flow_amount']:
                row_dict['n_adjusted_cash_flow_amount'] = float(row_dict['n_adjusted_cash_flow_amount'])
            adjustments.append(row_dict)
        
        return adjustments


@login_required
@require_http_methods(["GET"])
def get_product_names_for_type(request, fic_mis_date: str, process_name: str):
    """
    Get product names for a specific product type, currency, and flow type
    """
    try:
        v_prod_type = request.GET.get('v_prod_type')
        v_ccy_code = request.GET.get('v_ccy_code')
        flow_type = request.GET.get('flow_type')
        
        logger.info(f"Getting product names for: prod_type={v_prod_type}, currency={v_ccy_code}, flow_type={flow_type}")
        
        if not all([v_prod_type, v_ccy_code, flow_type]):
            missing_params = []
            if not v_prod_type: missing_params.append('v_prod_type')
            if not v_ccy_code: missing_params.append('v_ccy_code')
            if not flow_type: missing_params.append('flow_type')
            return JsonResponse({'error': f'Missing required parameters: {", ".join(missing_params)}'}, status=400)
        
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        logger.info(f"Using table: {actual_table_name}")
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist'}, status=404)
        
        with connection.cursor() as cursor:
            query = f"""
            SELECT DISTINCT
                REGEXP_REPLACE(v_product_name, ' \\[ADJ: [^\\]]*\\]', '', 'g') as v_product_name,
                v_product_splits,
                account_type,
                n_adjusted_cash_flow_amount,
                COUNT(*) as record_count
            FROM "{actual_table_name}"
            WHERE financial_element = 'n_total_cash_flow_amount'
              AND process_name = %s
              AND v_prod_type = %s
              AND v_ccy_code = %s
              AND flow_type = %s
            GROUP BY REGEXP_REPLACE(v_product_name, ' \\[ADJ: [^\\]]*\\]', '', 'g'), v_product_splits, account_type, n_adjusted_cash_flow_amount
            ORDER BY REGEXP_REPLACE(v_product_name, ' \\[ADJ: [^\\]]*\\]', '', 'g'), v_product_splits
            """
            
            logger.info(f"Executing query with params: {[process_name, v_prod_type, v_ccy_code, flow_type]}")
            
            cursor.execute(query, [process_name, v_prod_type, v_ccy_code, flow_type])
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            logger.info(f"Found {len(rows)} product names")
            
            product_names = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Convert Decimal to float for JSON serialization
                if row_dict['n_adjusted_cash_flow_amount']:
                    row_dict['n_adjusted_cash_flow_amount'] = float(row_dict['n_adjusted_cash_flow_amount'])
                product_names.append(row_dict)
        
        return JsonResponse({
            'success': True,
            'product_names': product_names
        })
        
    except Exception as e:
        logger.error(f"Error getting product names: {e}")
        return JsonResponse({'error': f'Error getting product names: {str(e)}'}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["POST"])
def add_adjustment(request):
    """
    Add or update an adjustment for a specific product name
    """
    try:
        data = json.loads(request.body)
        
        fic_mis_date = data.get('fic_mis_date')
        process_name = data.get('process_name')
        v_prod_type = data.get('v_prod_type')
        v_product_name = data.get('v_product_name')
        v_ccy_code = data.get('v_ccy_code')
        flow_type = data.get('flow_type')
        adjustment_amount = data.get('adjustment_amount')
        adjustment_description = data.get('adjustment_description')
        
        # Validate required fields
        if not all([fic_mis_date, process_name, v_prod_type, v_product_name, v_ccy_code, flow_type]):
            return JsonResponse({'error': 'Missing required fields'}, status=400)
        
        # Validate adjustment amount
        try:
            adjustment_amount = Decimal(str(adjustment_amount))
        except (ValueError, TypeError):
            return JsonResponse({'error': 'Invalid adjustment amount'}, status=400)
        
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist'}, status=404)
        
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Update existing records for this specific product name
                update_query = f"""
                UPDATE "{actual_table_name}"
                SET n_adjusted_cash_flow_amount = %s,
                    v_product_name = CASE 
                        WHEN %s IS NOT NULL AND %s != '' 
                        THEN REGEXP_REPLACE(
                            COALESCE(v_product_name, ''), 
                            ' \\[ADJ: [^\\]]*\\]', 
                            '', 
                            'g'
                        ) || ' [ADJ: ' || %s || ']'
                        ELSE REGEXP_REPLACE(
                            COALESCE(v_product_name, ''), 
                            ' \\[ADJ: [^\\]]*\\]', 
                            '', 
                            'g'
                        )
                    END
                WHERE financial_element = 'n_total_cash_flow_amount'
                  AND process_name = %s
                  AND v_prod_type = %s
                  AND v_product_name = %s
                  AND v_ccy_code = %s
                  AND flow_type = %s
                """
                
                cursor.execute(update_query, [
                    adjustment_amount,
                    adjustment_description,
                    adjustment_description,
                    adjustment_description,
                    process_name,
                    v_prod_type,
                    v_product_name,
                    v_ccy_code,
                    flow_type
                ])
                
                updated_rows = cursor.rowcount
                
                logger.info(f"Updated {updated_rows} rows with adjustment: {adjustment_amount} for {v_product_name} ({v_prod_type}, {v_ccy_code}, {flow_type})")
        
        return JsonResponse({
            'success': True,
            'message': f'Adjustment applied successfully to {updated_rows} records',
            'updated_rows': updated_rows
        })
        
    except Exception as e:
        logger.error(f"Error adding adjustment: {e}")
        return JsonResponse({'error': f'Error adding adjustment: {str(e)}'}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["POST"])
def remove_adjustment(request):
    """
    Remove an adjustment for a specific product name
    """
    try:
        data = json.loads(request.body)
        
        fic_mis_date = data.get('fic_mis_date')
        process_name = data.get('process_name')
        v_prod_type = data.get('v_prod_type')
        v_product_name = data.get('v_product_name')
        v_ccy_code = data.get('v_ccy_code')
        flow_type = data.get('flow_type')
        
        # Validate required fields
        if not all([fic_mis_date, process_name, v_prod_type, v_product_name, v_ccy_code, flow_type]):
            return JsonResponse({'error': 'Missing required fields'}, status=400)
        
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist'}, status=404)
        
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Remove adjustment and clean up product name
                update_query = f"""
                UPDATE "{actual_table_name}"
                SET n_adjusted_cash_flow_amount = NULL,
                    v_product_name = REGEXP_REPLACE(
                        COALESCE(v_product_name, ''), 
                        ' \\[ADJ: [^\\]]*\\]', 
                        '', 
                        'g'
                    )
                WHERE financial_element = 'n_total_cash_flow_amount'
                  AND process_name = %s
                  AND v_prod_type = %s
                  AND v_product_name = %s
                  AND v_ccy_code = %s
                  AND flow_type = %s
                """
                
                cursor.execute(update_query, [
                    process_name,
                    v_prod_type,
                    v_product_name,
                    v_ccy_code,
                    flow_type
                ])
                
                updated_rows = cursor.rowcount
                
                logger.info(f"Removed adjustment from {updated_rows} rows for {v_product_name} ({v_prod_type}, {v_ccy_code}, {flow_type})")
        
        return JsonResponse({
            'success': True,
            'message': f'Adjustment removed successfully from {updated_rows} records',
            'updated_rows': updated_rows
        })
        
    except Exception as e:
        logger.error(f"Error removing adjustment: {e}")
        return JsonResponse({'error': f'Error removing adjustment: {str(e)}'}, status=500)


@login_required
@require_http_methods(["GET"])
def get_product_type_details(request, fic_mis_date: str, process_name: str):
    """
    Get detailed information about a specific product type for adjustments
    """
    try:
        v_prod_type = request.GET.get('v_prod_type')
        v_ccy_code = request.GET.get('v_ccy_code')
        flow_type = request.GET.get('flow_type')
        
        if not all([v_prod_type, v_ccy_code, flow_type]):
            return JsonResponse({'error': 'Missing required parameters'}, status=400)
        
        date_obj = _to_date(fic_mis_date)
        table_name = get_table_name(date_obj)
        actual_table_name = get_actual_table_name(table_name)
        
        # Check if table exists
        if not check_table_exists(table_name):
            return JsonResponse({'error': f'Report table "{table_name}" does not exist'}, status=404)
        
        with connection.cursor() as cursor:
            query = f"""
            SELECT 
                v_product_name,
                v_product_splits,
                account_type,
                n_adjusted_cash_flow_amount,
                COUNT(*) as record_count
            FROM "{actual_table_name}"
            WHERE financial_element = 'n_total_cash_flow_amount'
              AND process_name = %s
              AND v_prod_type = %s
              AND v_ccy_code = %s
              AND flow_type = %s
            GROUP BY v_product_name, v_product_splits, account_type, n_adjusted_cash_flow_amount
            ORDER BY v_product_name, v_product_splits
            """
            
            cursor.execute(query, [process_name, v_prod_type, v_ccy_code, flow_type])
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            details = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Convert Decimal to float for JSON serialization
                if row_dict['n_adjusted_cash_flow_amount']:
                    row_dict['n_adjusted_cash_flow_amount'] = float(row_dict['n_adjusted_cash_flow_amount'])
                details.append(row_dict)
        
        return JsonResponse({
            'success': True,
            'details': details
        })
        
    except Exception as e:
        logger.error(f"Error getting product type details: {e}")
        return JsonResponse({'error': f'Error getting details: {str(e)}'}, status=500)
