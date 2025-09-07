from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Sum, Max, Min
from django.db import connection
from decimal import Decimal
from datetime import datetime, date
from collections import defaultdict
import logging

# Import staging models (removed CASA, Guarantee, Borrowing, Card)
from staging.models import (
    LoanContract, OverdraftContract, Investment, 
    FirstDayProduct, CreditLine
)

# Import ALM app models
from ..models import Stg_Exchange_Rate

# Set up logging
logger = logging.getLogger(__name__)

def get_available_report_tables_by_pattern(pattern: str) -> list:
    """
    Get list of available report tables from the database by pattern
    PostgreSQL compatible version - similar to behavioral report approach
    """
    with connection.cursor() as cursor:
        # PostgreSQL stores table names in lowercase in information_schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name LIKE %s
            ORDER BY table_name DESC
        """, [pattern])
        tables = cursor.fetchall()
        
        available_tables = []
        for (table_name,) in tables:
            # Extract date from table name
            try:
                # PostgreSQL returns lowercase table names
                # Handle different table name patterns
                if 'contractual_cons' in table_name:
                    date_part = table_name.split('_')[-1]  # Get the date part
                elif 'behavioural' in table_name:
                    date_part = table_name.split('_')[-1]  # Get the date part
                elif 'rate_sensitive' in table_name:
                    date_part = table_name.split('_')[-1]  # Get the date part
                elif 'contractual' in table_name and 'cons' not in table_name:
                    date_part = table_name.split('_')[-1]  # Get the date part
                else:
                    continue
                    
                if len(date_part) == 8 and date_part.isdigit():
                    date_obj = datetime.strptime(date_part, '%Y%m%d').date()
                    # Store the actual table name as it appears in the database
                    available_tables.append({
                        'table_name': table_name,
                        'actual_table_name': table_name,  # Use as-is from database
                        'date': date_obj,
                        'formatted_date': date_obj.strftime('%Y-%m-%d'),
                        'display_date': date_obj.strftime('%d %b %Y')
                    })
            except Exception as e:
                logger.warning(f"Could not parse date from table name {table_name}: {e}")
                continue
        
        return available_tables

@login_required
def dashboard(request):
    """
    Dashboard view displaying data summaries from staging models per fic_mis_date,
    exchange rate information, and report table summaries grouped by product type and currency.
    """
    context = {}
    
    try:
        # Get all unique fic_mis_dates from staging models
        fic_dates = set()
        
        # Collect dates from remaining staging models
        loan_dates = LoanContract.objects.values_list('fic_mis_date', flat=True).distinct()
        overdraft_dates = OverdraftContract.objects.values_list('fic_mis_date', flat=True).distinct()
        investment_dates = Investment.objects.values_list('fic_mis_date', flat=True).distinct()
        first_day_dates = FirstDayProduct.objects.values_list('fic_mis_date', flat=True).distinct()
        credit_line_dates = CreditLine.objects.values_list('fic_mis_date', flat=True).distinct()
        
        # Combine all dates
        all_dates = list(loan_dates) + list(overdraft_dates) + list(investment_dates) + \
                   list(first_day_dates) + list(credit_line_dates)
        
        fic_dates = sorted(set(filter(None, all_dates)), reverse=True)
        context['available_dates'] = fic_dates
        
        # Get selected date from request or use latest
        selected_date = request.GET.get('fic_mis_date')
        if selected_date:
            try:
                # Convert string to date if needed
                if isinstance(selected_date, str):
                    selected_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
                context['selected_date'] = selected_date
            except (ValueError, TypeError):
                selected_date = fic_dates[0] if fic_dates else None
                context['selected_date'] = selected_date
        else:
            selected_date = fic_dates[0] if fic_dates else None
            context['selected_date'] = selected_date
        
        # Get data summaries per fic_mis_date (show 10 most recent)
        dashboard_data = []
        
        for fic_date in fic_dates[:10]:  # Show last 10 dates
            date_summary = {
                'fic_mis_date': fic_date,
                'loan_contracts': {
                    'count': LoanContract.objects.filter(fic_mis_date=fic_date).count(),
                    'total_balance': LoanContract.objects.filter(
                        fic_mis_date=fic_date, n_eop_bal__isnull=False
                    ).aggregate(total=Sum('n_eop_bal'))['total'] or Decimal('0.00')
                },
                'overdraft_contracts': {
                    'count': OverdraftContract.objects.filter(fic_mis_date=fic_date).count(),
                    'total_balance': OverdraftContract.objects.filter(
                        fic_mis_date=fic_date, n_eop_bal__isnull=False
                    ).aggregate(total=Sum('n_eop_bal'))['total'] or Decimal('0.00')
                },
                'investments': {
                    'count': Investment.objects.filter(fic_mis_date=fic_date).count(),
                    'total_balance': Investment.objects.filter(
                        fic_mis_date=fic_date, n_eop_bal__isnull=False
                    ).aggregate(total=Sum('n_eop_bal'))['total'] or Decimal('0.00')
                },
                'first_day_products': {
                    'count': FirstDayProduct.objects.filter(fic_mis_date=fic_date).count(),
                    'total_balance': FirstDayProduct.objects.filter(
                        fic_mis_date=fic_date, n_eop_bal__isnull=False
                    ).aggregate(total=Sum('n_eop_bal'))['total'] or Decimal('0.00')
                },
                'credit_lines': {
                    'count': CreditLine.objects.filter(fic_mis_date=fic_date).count(),
                    'total_balance': CreditLine.objects.filter(
                        fic_mis_date=fic_date, n_eop_bal__isnull=False
                    ).aggregate(total=Sum('n_eop_bal'))['total'] or Decimal('0.00')
                }
            }
            dashboard_data.append(date_summary)
        
        context['dashboard_data'] = dashboard_data
        
        # Get Exchange Rate Summary
        exchange_rate_summary = []
        exchange_dates = Stg_Exchange_Rate.objects.values_list('fic_mis_date', flat=True).distinct().order_by('-fic_mis_date')[:5]
        
        for ex_date in exchange_dates:
            rate_data = {
                'fic_mis_date': ex_date,
                'total_rates': Stg_Exchange_Rate.objects.filter(fic_mis_date=ex_date).count(),
                'currency_pairs': Stg_Exchange_Rate.objects.filter(fic_mis_date=ex_date).count(),
                'rates': Stg_Exchange_Rate.objects.filter(fic_mis_date=ex_date).order_by('v_from_ccy_code', 'v_to_ccy_code')[:10]  # Show first 10 rates
            }
            exchange_rate_summary.append(rate_data)
        
        context['exchange_rate_summary'] = exchange_rate_summary
        
        # Get Report Tables Summary grouped by product type and currency
        def get_report_summary_by_product_and_currency(table_list, table_prefix):
            """Get summary data grouped by product type and currency for report tables"""
            summary_data = []
            
            for table_info in table_list[:3]:  # Show last 3 tables
                table_name = table_info.get('actual_table_name', table_info['table_name'])
                
                try:
                    with connection.cursor() as cursor:
                        # Query to get product type and currency summaries
                        query = f"""
                        SELECT 
                            v_prod_type,
                            v_ccy_code,
                            COUNT(*) as record_count,
                            SUM(COALESCE(n_adjusted_cash_flow_amount, 0)) as total_amount
                        FROM "{table_name}"
                        WHERE financial_element = 'n_total_cash_flow_amount'
                        GROUP BY v_prod_type, v_ccy_code
                        ORDER BY v_prod_type, v_ccy_code
                        """
                        
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        # Group by product type
                        product_summary = defaultdict(lambda: {'currencies': {}, 'total_records': 0, 'total_amount': Decimal('0')})
                        
                        for prod_type, ccy_code, count, amount in results:
                            prod_type = prod_type or 'Unknown'
                            ccy_code = ccy_code or 'Unknown'
                            amount = Decimal(str(amount)) if amount else Decimal('0')
                            
                            product_summary[prod_type]['currencies'][ccy_code] = {
                                'count': count,
                                'amount': amount
                            }
                            product_summary[prod_type]['total_records'] += count
                            product_summary[prod_type]['total_amount'] += amount
                        
                        summary_data.append({
                            'table_info': table_info,
                            'product_summary': dict(product_summary)
                        })
                        
                except Exception as e:
                    # If query fails, add empty summary
                    summary_data.append({
                        'table_info': table_info,
                        'product_summary': {},
                        'error': str(e)
                    })
            
            return summary_data
        
        try:
            # Get available report tables from database using dynamic discovery
            base_tables = get_available_report_tables_by_pattern('Report_Contractual_%')
            behavioral_tables = get_available_report_tables_by_pattern('Report_behavioural_%')
            cons_tables = get_available_report_tables_by_pattern('Report_Contractual_Cons_%')
            rate_tables = get_available_report_tables_by_pattern('Report_rate_sensitive_%')
            
            # Get summaries for each report type
            context['report_summaries'] = {
                'base_reports': get_report_summary_by_product_and_currency(base_tables, 'Report_Contractual_{fic_mis_date:%Y%m%d}'),
                'behavioral_reports': get_report_summary_by_product_and_currency(behavioral_tables, 'Report_behavioural_{fic_mis_date:%Y%m%d}'),
                'cons_reports': get_report_summary_by_product_and_currency(cons_tables, 'Report_Contractual_Cons_{fic_mis_date:%Y%m%d}'),
                'rate_reports': get_report_summary_by_product_and_currency(rate_tables, 'Report_rate_sensitive_{fic_mis_date:%Y%m%d}')
            }
            
        except Exception as e:
            context['report_tables_error'] = f"Error loading report tables: {str(e)}"
            logger.error(f"Error loading report tables: {e}")
        
        # Get statistics for overview cards based on selected date (only staging data)
        if selected_date:
            context['latest_date'] = selected_date
            context['total_loan_contracts'] = LoanContract.objects.filter(fic_mis_date=selected_date).count()
            context['total_overdraft_contracts'] = OverdraftContract.objects.filter(fic_mis_date=selected_date).count()
            context['total_investments'] = Investment.objects.filter(fic_mis_date=selected_date).count()
            context['total_first_day_products'] = FirstDayProduct.objects.filter(fic_mis_date=selected_date).count()
            context['total_credit_lines'] = CreditLine.objects.filter(fic_mis_date=selected_date).count()
            context['total_exchange_rates'] = Stg_Exchange_Rate.objects.filter(fic_mis_date=selected_date).count()
        
    except Exception as e:
        context['error'] = f"Error loading dashboard data: {str(e)}"
        logger.error(f"Dashboard error: {e}")
    
    return render(request, 'reports/dashboard/index.html', context)