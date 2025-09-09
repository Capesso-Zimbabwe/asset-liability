import os
import pandas as pd
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.contrib import messages
from django.core.exceptions import ValidationError
from django.core.cache import cache

from alm_app.models import ProductBalance, Stg_Common_Coa_Master, Stg_Exchange_Rate, Stg_Product_Master, stg_party_master

from .models import (
    LoanContract, OverdraftContract, LoanPaymentSchedule,
    Investment, FirstDayProduct, CreditLine
)
from datetime import datetime, date
import uuid
import json
from django.db import models
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Generic file handling functions
def handle_uploaded_file(file):
    """Generic function to handle file upload"""
    file_id = str(uuid.uuid4())
    temp_dir = os.path.join(settings.BASE_DIR, 'temp_uploads')
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, f"{file_id}_{file.name}")
    
    try:
        with open(file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)
        return file_id, file_path
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise e

def get_file_path(file_id):
    """Get file path from file ID"""
    temp_dir = os.path.join(settings.BASE_DIR, 'temp_uploads')
    for filename in os.listdir(temp_dir):
        if filename.startswith(file_id):
            return os.path.join(temp_dir, filename)
    return None

def read_file_data(file_path):
    """Read data from file"""
    if file_path.lower().endswith('.csv'):
        return pd.read_csv(file_path)
    return pd.read_excel(file_path)

# Helper utilities for mapping and coercion
def get_model_field_map(model_class):
    """Return a dict of field_name -> field for non auto-created fields."""
    field_map = {}
    for field in model_class._meta.fields:
        if getattr(field, 'auto_created', False):
            continue
        field_map[field.name] = field
    return field_map

def normalize_null(value):
    """Convert empty-like values and NaN to None."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() in ['', 'null', 'None', 'NaN']:
        return None
    return value

def parse_date_flexibly(value):
    """Parse many date representations to a date, avoiding dayfirst warnings.
    Tries day-first first, then month-first, then a set of explicit formats.
    Returns a datetime.date or None.
    """
    value = normalize_null(value)
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # Try pandas with dayfirst=True then False
    try:
        dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass
    try:
        dt = pd.to_datetime(value, errors='coerce', dayfirst=False)
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass
    # Try common explicit formats
    if isinstance(value, str):
        value = value.strip()
        formats = [
            '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d',
            '%d.%m.%Y', '%m-%d-%Y', '%d %b %Y', '%d %B %Y'
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except Exception:
                continue
    return None


def coerce_decimal(field, value):
    """Coerce a value into a Decimal with the field's decimal_places using HALF_UP rounding."""
    value = normalize_null(value)
    if value is None or value == '':
        return None
    try:
        # Allow strings with thousands separators
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        d = Decimal(str(value))
        # Quantize to the field's decimal places
        places = getattr(field, 'decimal_places', None)
        if places is not None:
            quant = Decimal('1').scaleb(-places)
            d = d.quantize(quant, rounding=ROUND_HALF_UP)
        return d
    except (InvalidOperation, ValueError, TypeError):
        return value

def coerce_value_for_field(field, value):
    """Coerce a value into the correct python type for a Django model field."""
    value = normalize_null(value)
    if value is None:
        return None

    try:
        if isinstance(field, models.DateField):
            return parse_date_flexibly(value)
        if isinstance(field, models.DecimalField):
            return coerce_decimal(field, value)
        if isinstance(field, models.IntegerField):
            if isinstance(value, str):
                value = value.replace(',', '').strip()
            return None if value == '' else int(float(value))
        if isinstance(field, models.CharField):
            return str(value)
        # Fallback to Django's to_python
        return field.to_python(value)
    except Exception:
        # On coercion error, return original; validation will catch and we will report row error
        return value

# View generators
def create_upload_view(model_class, template_name):
    """Create a view for file upload page"""
    @login_required
    def view(request):
        return render(request, template_name, {'step': 1})
    return view

def create_file_upload_view(model_class):
    """Create a view for handling file upload"""
    @login_required
    @csrf_protect
    def view(request):
        try:
            if request.method != 'POST':
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid request method. Please use POST.'
                })

            if 'file' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No file was uploaded. Please select a file.'
                })

            uploaded_file = request.FILES['file']
            
            # Validate file type
            if not any(uploaded_file.name.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid file type. Please upload a CSV or Excel file.'
                })

            # Validate file size (500MB limit)
            if uploaded_file.size > 500 * 1024 * 1024:
                return JsonResponse({
                    'success': False,
                    'error': 'File size exceeds 500MB limit.'
                })

            # Save and process file
            file_id, file_path = handle_uploaded_file(uploaded_file)
            
            try:
                # Initialize progress ASAP (before file read)
                cache.set(f'import_progress:{file_id}', {
                    'progress': 1,
                    'total': 0,
                    'success_count': 0,
                    'errors': []
                }, 300)

                df = read_file_data(file_path)
                
                if len(df.columns) < 1:
                    raise ValueError("File contains no columns")
                
                return JsonResponse({
                    'success': True,
                    'file_id': file_id,
                    'row_count': len(df),
                    'message': 'File uploaded successfully'
                })
            except Exception as e:
                if os.path.exists(file_path):
                    os.remove(file_path)
                return JsonResponse({
                    'success': False,
                    'error': f'Error reading file: {str(e)}. Please ensure it is a valid Excel or CSV file.'
                })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error processing file: {str(e)}'
            })
    return view

def create_columns_view(model_class):
    """Create a view for getting columns mapping"""
    @login_required
    def view(request, file_id):
        try:
            file_path = get_file_path(file_id)
            if not file_path:
                return JsonResponse({
                    'success': False,
                    'error': 'File not found. Please upload the file again.'
                })

            try:
                df = read_file_data(file_path)
                
                # Get model fields with descriptions
                model_fields = []
                for field in model_class._meta.fields:
                    if getattr(field, 'auto_created', False):
                        continue
                    field_info = {
                        'value': field.name,
                        'label': field.verbose_name or field.name.replace('_', ' ').title(),
                        'required': not field.null and not field.blank,
                        'unique': field.unique,
                        'type': field.get_internal_type(),
                        'max_length': getattr(field, 'max_length', None),
                        'decimal_places': getattr(field, 'decimal_places', None),
                        'max_digits': getattr(field, 'max_digits', None),
                        'help_text': str(field.help_text) if field.help_text else None
                    }
                    model_fields.append(field_info)

                # Create automatic mapping suggestions
                automatic_mapping = {}
                for file_col in df.columns:
                    # Try exact match first
                    exact_match = next(
                        (field['value'] for field in model_fields if field['value'].lower() == file_col.lower()),
                        None
                    )
                    if exact_match:
                        automatic_mapping[file_col] = exact_match
                        continue

                    # Try matching without spaces and underscores
                    normalized_col = file_col.lower().replace(' ', '').replace('_', '')
                    match = next(
                        (field['value'] for field in model_fields 
                         if field['value'].lower().replace('_', '') == normalized_col),
                        None
                    )
                    if match:
                        automatic_mapping[file_col] = match

                return JsonResponse({
                    'success': True,
                    'columns': list(df.columns),
                    'fields': model_fields,
                    'automatic_mapping': automatic_mapping
                })
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'error': f'Error reading file columns: {str(e)}'
                })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'Error processing columns: {str(e)}'
            })
    return view

def create_preview_view(model_class):
    """Create a view for data preview"""
    @login_required
    @csrf_protect
    def view(request):
        if request.method != 'POST':
            return JsonResponse({
                'success': False,
                'error': 'Invalid request method'
            })

        try:
            data = json.loads(request.body)
            file_id = data.get('file_id')
            mapping = data.get('mapping', {})

            if not file_id:
                return JsonResponse({
                    'success': False,
                    'error': 'No file ID provided'
                })

            file_path = get_file_path(file_id)
            if not file_path:
                return JsonResponse({
                    'success': False,
                    'error': 'File not found'
                })

            try:
                df = read_file_data(file_path)
                
                # Limit to first 5 rows early to keep preview fast
                df = df.head(5)
                
                # Apply mapping and keep only mapped target columns
                mapped_targets = [t for t in mapping.values() if t]
                df_mapped = df.rename(columns=mapping)
                if mapped_targets:
                    df_mapped = df_mapped[[c for c in mapped_targets if c in df_mapped.columns]]

                # Replace NaN with None for JSON
                df_mapped = df_mapped.where(pd.notna(df_mapped), None)

                # Coerce to model field types for better preview formatting
                field_map = get_model_field_map(model_class)
                for col in list(df_mapped.columns):
                    field = field_map.get(col)
                    if field is None:
                        continue
                    df_mapped[col] = df_mapped[col].apply(lambda v: coerce_value_for_field(field, v))

                # Convert dates to ISO strings for preview
                for col in list(df_mapped.columns):
                    field = field_map.get(col)
                    if isinstance(field, models.DateField):
                        df_mapped[col] = df_mapped[col].apply(lambda d: d.isoformat() if d else None)
                
                # Return first 5 rows as preview (already limited)
                preview_data = df_mapped.to_dict('records')
                
                return JsonResponse({
                    'success': True,
                    'preview': preview_data
                })
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'error': str(e)
                })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            })
    return view

def create_import_view(model_class):
    """Create a view for data import"""
    @login_required
    @csrf_protect
    def view(request):
        # Progress polling support
        if request.method == 'GET':
            file_id = request.GET.get('file_id')
            if not file_id:
                return JsonResponse({'success': False, 'error': 'No file ID provided'})
            progress = cache.get(f'import_progress:{file_id}', None)
            if progress is None:
                return JsonResponse({'success': True, 'progress': 0, 'total': 0, 'success_count': 0, 'errors': []})
            return JsonResponse({'success': True, **progress})

        if request.method != 'POST':
            return JsonResponse({
                'success': False,
                'error': 'Invalid request method'
            })

        try:
            data = json.loads(request.body)
            file_id = data.get('file_id')
            mapping = data.get('mapping', {})

            if not file_id:
                return JsonResponse({
                    'success': False,
                    'error': 'No file ID provided'
                })

            file_path = get_file_path(file_id)
            if not file_path:
                return JsonResponse({
                    'success': False,
                    'error': 'File not found'
                })

            try:
                # Initialize progress ASAP (before file read)
                cache.set(f'import_progress:{file_id}', {
                    'progress': 1,
                    'total': 0,
                    'success_count': 0,
                    'errors': []
                }, 300)

                df = read_file_data(file_path)
                
                # Apply mapping and keep only mapped target columns
                mapped_targets = [t for t in mapping.values() if t]
                df_mapped = df.rename(columns=mapping)
                if mapped_targets:
                    df_mapped = df_mapped[[c for c in mapped_targets if c in df_mapped.columns]]

                # Prepare model field map
                field_map = get_model_field_map(model_class)

                # Coerce values per field and handle nulls
                for col in list(df_mapped.columns):
                    field = field_map.get(col)
                    if field is None:
                        # Drop columns not in model
                        df_mapped.drop(columns=[col], inplace=True)
                        continue
                    df_mapped[col] = df_mapped[col].apply(lambda v: coerce_value_for_field(field, v))

                total_rows = len(df_mapped)
                errors = []
                instances_to_save = []

                # Initialize progress in cache
                cache.set(f'import_progress:{file_id}', {
                    'progress': 2,
                    'total': total_rows,
                    'success_count': 0,
                    'errors': []
                }, 300)

                def update_progress(processed: int, success_count: int, errors_list):
                    progress_pct = 0 if total_rows == 0 else min(90, int(processed / total_rows * 90))
                    cache.set(f'import_progress:{file_id}', {
                        'progress': progress_pct,
                        'total': total_rows,
                        'success_count': success_count,
                        'errors': errors_list[:50],
                    }, 300)

                # Validate each row and build instances
                success_count = 0
                for index, row in df_mapped.iterrows():
                    try:
                        instance_data = {}
                        for field_name, field in field_map.items():
                            if field_name in row.index:
                                instance_data[field_name] = row[field_name]
                        instance = model_class(**instance_data)
                        instance.full_clean()
                        instances_to_save.append(instance)
                        success_count += 1
                    except ValidationError as ve:
                        idt_acc = instance_data.get('v_account_number') if 'instance_data' in locals() else None
                        idt_fic = instance_data.get('fic_mis_date') if 'instance_data' in locals() else None
                        idt = []
                        if idt_acc: idt.append(f"Account: {idt_acc}")
                        if idt_fic: idt.append(f"FIC MIS Date: {idt_fic}")
                        idt_text = f" ({' | '.join(idt)})" if idt else ''
                        if hasattr(ve, 'message_dict'):
                            parts = []
                            for fld, msgs in ve.message_dict.items():
                                if isinstance(msgs, (list, tuple)):
                                    msg_text = ', '.join(msgs)
                                else:
                                    msg_text = str(msgs)
                                if fld != '__all__':
                                    val = instance_data.get(fld, None)
                                    val_str = val.isoformat() if hasattr(val, 'isoformat') else (str(val) if val is not None else '')
                                    parts.append(f"{fld}: {msg_text} (value: {val_str})")
                                else:
                                    parts.append(f"{fld}: {msg_text}")
                            msg_text_full = '; '.join(parts)
                        else:
                            msgs = getattr(ve, 'messages', [str(ve)])
                            msg_text_full = '; '.join(msgs)
                        errors.append(f"Row {index + 1}{idt_text}: {msg_text_full}")
                    except Exception as e:
                        errors.append(f"Row {index + 1}: {str(e)}")

                    # Periodically update progress every 200 rows
                    if (index + 1) % 200 == 0:
                        update_progress(index + 1, success_count, errors)

                # Final validation progress update (set to 90%)
                update_progress(total_rows, success_count, errors)

                # Perform save
                if len(errors) == 0:
                    from django.db import transaction
                    try:
                        with transaction.atomic():
                            model_class.objects.bulk_create(instances_to_save, batch_size=1000)
                        # Final progress to 100%
                        cache.set(f'import_progress:{file_id}', {
                            'progress': 100,
                            'total': total_rows,
                            'success_count': total_rows,
                            'errors': []
                        }, 300)
                        success_count = total_rows
                    except Exception as e:
                        errors.append(f"Transaction save error: {str(e)}")
                        cache.set(f'import_progress:{file_id}', {
                            'progress': 100,
                            'total': total_rows,
                            'success_count': 0,
                            'errors': errors[:50]
                        }, 300)
                        success_count = 0
                else:
                    # Errors found; finalize progress
                    cache.set(f'import_progress:{file_id}', {
                        'progress': 100,
                        'total': total_rows,
                        'success_count': 0,
                        'errors': errors[:50]
                    }, 300)

                # Clean up temporary file
                try:
                    os.remove(file_path)
                except Exception:
                    pass

                return JsonResponse({
                    'success': True,
                    'total': total_rows,
                    'success_count': success_count,
                    'errors': errors,
                    'progress': 100
                })
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'error': str(e)
                })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            })
    return view

# Add views for COA and Product Master
@login_required
def add_coa(request):
    """View for adding new Common COA Master data"""
    if request.method == 'POST':
        try:
            # Get form data with null handling
            data = {
                'v_common_coa_code': request.POST.get('v_common_coa_code') or None,
                'v_common_coa_name': request.POST.get('v_common_coa_name') or None,
                'v_common_coa_description': request.POST.get('v_common_coa_description') or None,
                'v_accrual_basis_code': request.POST.get('v_accrual_basis_code') or None,
                'v_account_type': request.POST.get('v_account_type') or None,
                'v_rollup_signage_code': request.POST.get('v_rollup_signage_code') or None,
            }

            # Create new COA instance
            coa = Stg_Common_Coa_Master(**data)

            # Handle date fields
            fic_mis_date = request.POST.get('fic_mis_date', '').strip()
            d_start_date = request.POST.get('d_start_date', '').strip()
            d_end_date = request.POST.get('d_end_date', '').strip()

            # Only parse dates if they're not empty
            coa.fic_mis_date = datetime.strptime(fic_mis_date, '%Y-%m-%d').date() if fic_mis_date else None
            coa.d_start_date = datetime.strptime(d_start_date, '%Y-%m-%d').date() if d_start_date else None
            coa.d_end_date = datetime.strptime(d_end_date, '%Y-%m-%d').date() if d_end_date else None

            # Validate and save
            coa.full_clean()
            coa.save()
            
            messages.success(request, f'COA "{coa.v_common_coa_code}" created successfully.')
            return redirect('staging:view_coa')
            
        except ValidationError as e:
            messages.error(request, f'Validation error: {e}')
        except Exception as e:
            messages.error(request, f'Error creating COA: {e}')
            import traceback
            print(traceback.format_exc())  # Print full traceback for debugging
    
    context = {
        'title': 'Add Common COA Master',
    }
    return render(request, 'staging/coa/add_coa.html', context)

@login_required
def add_product_master(request):
    """View for adding new Product Master data"""
    if request.method == 'POST':
        try:
            # Required fields validation
            v_prod_code = request.POST.get('v_prod_code')
            fic_mis_date = request.POST.get('fic_mis_date')
            
            if not v_prod_code:
                raise ValidationError({'v_prod_code': ['This field is required.']})
            if not fic_mis_date:
                raise ValidationError({'fic_mis_date': ['This field is required.']})

            # Create new Product Master instance with required fields
            product = Stg_Product_Master(
                v_prod_code=v_prod_code,
                fic_mis_date=datetime.strptime(fic_mis_date, '%Y-%m-%d').date()
            )

            # Update optional fields - handle empty strings as None
            product.v_prod_name = request.POST.get('v_prod_name', '').strip() or None
            product.v_prod_type = request.POST.get('v_prod_type', '').strip() or None
            product.v_prod_group_desc = request.POST.get('v_prod_group_desc', '').strip() or None
            product.f_prod_rate_sensitivity = request.POST.get('f_prod_rate_sensitivity', '').strip() or None
            product.v_common_coa_code = request.POST.get('v_common_coa_code', '').strip() or None
            product.v_balance_sheet_category = request.POST.get('v_balance_sheet_category', '').strip() or None
            product.v_balance_sheet_category_desc = request.POST.get('v_balance_sheet_category_desc', '').strip() or None
            product.v_prod_type_desc = request.POST.get('v_prod_type_desc', '').strip() or None
            product.v_load_type = request.POST.get('v_load_type', '').strip() or None
            product.v_lob_code = request.POST.get('v_lob_code', '').strip() or None
            product.v_prod_desc = request.POST.get('v_prod_desc', '').strip() or None

            # Validate and save
            product.full_clean()
            product.save()
            
            messages.success(request, f'Product "{product.v_prod_code}" created successfully.')
            return redirect('staging:view_master')
            
        except ValidationError as e:
            messages.error(request, f'Validation error: {e}')
        except Exception as e:
            messages.error(request, f'Error creating product: {e}')
            import traceback
            print(traceback.format_exc())  # Print full traceback for debugging
    
    context = {
        'title': 'Add Product Master',
    }
    return render(request, 'staging/master/add_master.html', context)

# Create views for each model
# Loan Contract views
load_loans_view = create_upload_view(
    LoanContract,
    'staging/loans/load_loans.html'
)
upload_loan_file = create_file_upload_view(LoanContract)
get_loan_columns = create_columns_view(LoanContract)
preview_loan_data = create_preview_view(LoanContract)
import_loan_data = create_import_view(LoanContract)

# Overdraft Contract views
load_overdraft_view = create_upload_view(
    OverdraftContract,
    'staging/OverdraftContract/load_overdraft.html'
)
upload_overdraft_file = create_file_upload_view(OverdraftContract)
get_overdraft_columns = create_columns_view(OverdraftContract)
preview_overdraft_data = create_preview_view(OverdraftContract)
import_overdraft_data = create_import_view(OverdraftContract)

# Loan Payment Schedule views
load_schedule_view = create_upload_view(
    LoanPaymentSchedule,
    'staging/LoanPaymentSchedule/load_schedule.html'
)
upload_schedule_file = create_file_upload_view(LoanPaymentSchedule)
get_schedule_columns = create_columns_view(LoanPaymentSchedule)
preview_schedule_data = create_preview_view(LoanPaymentSchedule)
import_schedule_data = create_import_view(LoanPaymentSchedule)

# Investment views
load_investment_view = create_upload_view(
    Investment,
    'staging/Investment/load_investments.html'
)
upload_investment_file = create_file_upload_view(Investment)
get_investment_columns = create_columns_view(Investment)
preview_investment_data = create_preview_view(Investment)
import_investment_data = create_import_view(Investment)

# First Day Product views
load_firstday_view = create_upload_view(
    FirstDayProduct,
    'staging/FirstDayProduct/load_first.html'
)
upload_firstday_file = create_file_upload_view(FirstDayProduct)
get_firstday_columns = create_columns_view(FirstDayProduct)
preview_firstday_data = create_preview_view(FirstDayProduct)
import_firstday_data = create_import_view(FirstDayProduct)

# Credit Line views
load_creditline_view = create_upload_view(
    CreditLine,
    'staging/CreditLine/load_credit.html'
)
upload_creditline_file = create_file_upload_view(CreditLine)
get_creditline_columns = create_columns_view(CreditLine)
preview_creditline_data = create_preview_view(CreditLine)
import_creditline_data = create_import_view(CreditLine)

# Product Balance views
load_product_view = create_upload_view(
    ProductBalance,
    'staging/product/load_product.html'
)
upload_product_file = create_file_upload_view(ProductBalance)
get_product_columns = create_columns_view(ProductBalance)
preview_product_data = create_preview_view(ProductBalance)
import_product_data = create_import_view(ProductBalance)

# Product Master views
load_master_view = create_upload_view(
    Stg_Product_Master,
    'staging/master/load_master.html'
)
upload_master_file = create_file_upload_view(Stg_Product_Master)
get_master_columns = create_columns_view(Stg_Product_Master)
preview_master_data = create_preview_view(Stg_Product_Master)
import_master_data = create_import_view(Stg_Product_Master)

# Common COA Master views
load_coa_view = create_upload_view(
    Stg_Common_Coa_Master,
    'staging/coa/load_coa.html'
)
upload_coa_file = create_file_upload_view(Stg_Common_Coa_Master)
get_coa_columns = create_columns_view(Stg_Common_Coa_Master)
preview_coa_data = create_preview_view(Stg_Common_Coa_Master)
import_coa_data = create_import_view(Stg_Common_Coa_Master)

# Exchange Rate views
load_rates_view = create_upload_view(
    Stg_Exchange_Rate,
    'staging/rates/load_rates.html'
)
upload_rates_file = create_file_upload_view(Stg_Exchange_Rate)
get_rates_columns = create_columns_view(Stg_Exchange_Rate)
preview_rates_data = create_preview_view(Stg_Exchange_Rate)
import_rates_data = create_import_view(Stg_Exchange_Rate)

# Customer (Party Master) views
load_customer_view = create_upload_view(
    stg_party_master,
    'staging/customer/load_customer.html'
)
upload_customer_file = create_file_upload_view(stg_party_master)
get_customer_columns = create_columns_view(stg_party_master)
preview_customer_data = create_preview_view(stg_party_master)
import_customer_data = create_import_view(stg_party_master)

# View functions for displaying data
@login_required
def view_coa(request):
    """View for displaying Common COA Master data with filtering and sorting"""
    # Get filter parameters from request
    search_query = request.GET.get('search', '')
    sort_field = request.GET.get('sort', 'v_common_coa_code')
    sort_direction = request.GET.get('direction', 'asc')
    page_number = request.GET.get('page', 1)
    
    # Base queryset
    queryset = Stg_Common_Coa_Master.objects.all()
    
    # Apply search filter if provided
    if search_query:
        queryset = queryset.filter(
            models.Q(v_common_coa_code__icontains=search_query) |
            models.Q(v_common_coa_name__icontains=search_query) |
            models.Q(v_common_coa_description__icontains=search_query) |
            models.Q(v_account_type__icontains=search_query)
        )
    
    # Apply sorting
    if sort_direction == 'desc':
        sort_field = f'-{sort_field}'
    queryset = queryset.order_by(sort_field)
    
    # Pagination
    paginator = Paginator(queryset, 25)  # Show 25 records per page
    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)
    
    context = {
        'page_obj': page_obj,
        'search_query': search_query,
        'sort_field': sort_field.lstrip('-'),
        'sort_direction': sort_direction,
        'total_records': queryset.count(),
    }
    
    return render(request, 'staging/coa/view_coa.html', context)

@login_required
def view_product_master(request):
    """View for displaying Product Master data with filtering and sorting"""
    # Get filter parameters from request
    search_query = request.GET.get('search', '')
    sort_field = request.GET.get('sort', 'v_prod_code')
    sort_direction = request.GET.get('direction', 'asc')
    page_number = request.GET.get('page', 1)
    
    # Base queryset
    queryset = Stg_Product_Master.objects.all()
    
    # Apply search filter if provided
    if search_query:
        queryset = queryset.filter(
            models.Q(v_prod_code__icontains=search_query) |
            models.Q(v_prod_name__icontains=search_query) |
            models.Q(v_prod_type__icontains=search_query) |
            models.Q(v_prod_group_desc__icontains=search_query) |
            models.Q(v_balance_sheet_category__icontains=search_query)
        )
    
    # Apply sorting
    if sort_direction == 'desc':
        sort_field = f'-{sort_field}'
    queryset = queryset.order_by(sort_field)
    
    # Pagination
    paginator = Paginator(queryset, 25)  # Show 25 records per page
    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)
    
    context = {
        'page_obj': page_obj,
        'search_query': search_query,
        'sort_field': sort_field.lstrip('-'),
        'sort_direction': sort_direction,
        'total_records': queryset.count(),
    }
    
    return render(request, 'staging/master/view_master.html', context)

# Edit and Delete views for Product Master
@login_required
def edit_product_master(request, pk):
    """View for editing Product Master data"""
    product = get_object_or_404(Stg_Product_Master, pk=pk)
    
    if request.method == 'POST':
        try:
            # Required fields (null=False)
            v_prod_code = request.POST.get('v_prod_code')
            fic_mis_date = request.POST.get('fic_mis_date')
            
            if not v_prod_code:
                raise ValidationError({'v_prod_code': ['This field is required.']})
            if not fic_mis_date:
                raise ValidationError({'fic_mis_date': ['This field is required.']})
            
            # Update required fields
            product.v_prod_code = v_prod_code
            product.fic_mis_date = datetime.strptime(fic_mis_date, '%Y-%m-%d').date()
            
            # Update optional fields (null=True)
            product.v_prod_name = request.POST.get('v_prod_name') or None
            product.v_prod_type = request.POST.get('v_prod_type') or None
            product.v_prod_group_desc = request.POST.get('v_prod_group_desc') or None
            product.f_prod_rate_sensitivity = request.POST.get('f_prod_rate_sensitivity') or None
            product.v_common_coa_code = request.POST.get('v_common_coa_code') or None
            product.v_balance_sheet_category = request.POST.get('v_balance_sheet_category') or None
            product.v_balance_sheet_category_desc = request.POST.get('v_balance_sheet_category_desc') or None
            product.v_prod_type_desc = request.POST.get('v_prod_type_desc') or None
            product.v_load_type = request.POST.get('v_load_type') or None
            product.v_lob_code = request.POST.get('v_lob_code') or None
            product.v_prod_desc = request.POST.get('v_prod_desc') or None

            # Validate and save
            product.full_clean()
            product.save()
            
            messages.success(request, f'Product "{product.v_prod_code}" updated successfully.')
            return redirect('staging:view_master')
            
        except ValidationError as e:
            messages.error(request, f'Validation error: {e}')
        except Exception as e:
            messages.error(request, f'Error updating product: {e}')
    
    context = {
        'product': product,
        'title': 'Edit Product Master',
    }
    return render(request, 'staging/master/edit_master.html', context)

@login_required
def delete_product_master(request, pk):
    """View for deleting Product Master data"""
    product = get_object_or_404(Stg_Product_Master, pk=pk)
    
    if request.method == 'POST':
        try:
            product_code = product.v_prod_code
            product_name = product.v_prod_name
            product.delete()
            messages.success(request, f'Product "{product_code} - {product_name}" deleted successfully.')
            return redirect('staging:view_master')
        except Exception as e:
            messages.error(request, f'Error deleting product: {e}')
            return redirect('staging:view_master')
    
    context = {
        'product': product,
        'title': 'Delete Product Master',
        'cancel_url': 'staging:view_master',
    }
    return render(request, 'staging/master/delete_master.html', context)

# Edit and Delete views for COA Master
@login_required
def edit_coa(request, pk):
    """View for editing Common COA Master data"""
    coa = get_object_or_404(Stg_Common_Coa_Master, pk=pk)
    
    if request.method == 'POST':
        try:
            # Get form data with null handling
            data = {
                'v_common_coa_code': request.POST.get('v_common_coa_code') or None,
                'v_common_coa_name': request.POST.get('v_common_coa_name') or None,
                'v_common_coa_description': request.POST.get('v_common_coa_description') or None,
                'v_accrual_basis_code': request.POST.get('v_accrual_basis_code') or None,
                'v_account_type': request.POST.get('v_account_type') or None,
                'v_rollup_signage_code': request.POST.get('v_rollup_signage_code') or None,
            }

            # Update text fields
            for field, value in data.items():
                setattr(coa, field, value)

            # Handle date fields
            fic_mis_date = request.POST.get('fic_mis_date', '').strip()
            d_start_date = request.POST.get('d_start_date', '').strip()
            d_end_date = request.POST.get('d_end_date', '').strip()

            # Only parse dates if they're not empty
            coa.fic_mis_date = datetime.strptime(fic_mis_date, '%Y-%m-%d').date() if fic_mis_date else None
            coa.d_start_date = datetime.strptime(d_start_date, '%Y-%m-%d').date() if d_start_date else None
            coa.d_end_date = datetime.strptime(d_end_date, '%Y-%m-%d').date() if d_end_date else None

            # Validate and save
            coa.full_clean()
            coa.save()
            
            messages.success(request, f'COA "{coa.v_common_coa_code}" updated successfully.')
            return redirect('staging:view_coa')
            
        except ValidationError as e:
            messages.error(request, f'Validation error: {e}')
        except Exception as e:
            messages.error(request, f'Error updating COA: {e}')
            import traceback
            print(traceback.format_exc())  # Print full traceback for debugging
    
    context = {
        'coa': coa,
        'title': 'Edit Common COA Master',
    }
    return render(request, 'staging/coa/edit_coa.html', context)

@login_required
def delete_coa(request, pk):
    """View for deleting Common COA Master data"""
    coa = get_object_or_404(Stg_Common_Coa_Master, pk=pk)
    
    if request.method == 'POST':
        try:
            coa_code = coa.v_common_coa_code
            coa_name = coa.v_common_coa_name
            coa.delete()
            messages.success(request, f'COA "{coa_code} - {coa_name}" deleted successfully.')
            return redirect('staging:view_coa')
        except Exception as e:
            messages.error(request, f'Error deleting COA: {e}')
            return redirect('staging/coa/delete_coa.html', context)
    
    context = {
        'coa': coa,
        'title': 'Delete Common COA Master',
        'cancel_url': 'staging:view_coa',
    }
    return render(request, 'staging/coa/delete_coa.html', context)
