from django.shortcuts import render
from django.views.generic import TemplateView, View
from django.contrib.auth.views import LoginView
from django.contrib import messages
from django.urls import reverse_lazy
from django.views.generic.edit import FormView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import transaction
from django.contrib.messages.views import SuccessMessageMixin
import pandas as pd
from .models import HQLATable, CashInflowTable, CashOutflowTable, HQLASection, CashInflowSection, CashOutflowSection, CurrencyAdjustmentSummary, LCRRecord, LCRRun, ASFTable, RSFTable, HQLAItem, CashInflowItem, CashOutflowItem, ASFItem, RSFItem, ASFSection, RSFSection
from .forms import HQLAUploadForm, CashInflowUploadForm, CashOutflowUploadForm, AvailableStableUploadForm, RequiredStableUploadForm, HQLASectionForm, CashInflowSectionForm, CashOutflowSectionForm, HQLAItemForm, CashInflowItemForm, CashOutflowItemForm, ASFSectionForm, ASFItemForm, RSFSectionForm, RSFItemForm
import os
from django.db.models import Case, When, IntegerField, Value, F, Subquery, OuterRef, Sum, DecimalField
from .mixins import CustomOrderGroupedByCurrencyMixin
from django.shortcuts import redirect
from django.http import JsonResponse
from datetime import datetime
from decimal import Decimal
from django import forms
import logging
from django.contrib.contenttypes.models import ContentType
from itertools import groupby
import operator
from django.utils.text import slugify
from django.conf import settings
from django.http import FileResponse, Http404
from django.urls import reverse
from django.test import RequestFactory
from django.contrib.messages.storage.fallback import FallbackStorage
from django.views.generic import ListView
from django.db.models.functions import Coalesce
from django.db.models import F
from django.forms import modelformset_factory
from collections import defaultdict
from django.utils import timezone










logger = logging.getLogger(__name__)

class Index(TemplateView):
    template_name = "pages/index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = "LCR:Home"
        return context



class BaseUploadView(SuccessMessageMixin, FormView):
    """
    A reusable upload view that accepts only Excel or CSV files,
    parses them into a DataFrame, bulk-creates model rows, and shows messages.
    """
    model = None               # override in subclass
    form_class = None          # override in subclass
    template_name = None       # override in subclass
    success_url = None         # override in subclass
    success_message = None     # override in subclass
    sheet_name = None          # override in subclass for Excel

    def form_valid(self, form):
        upload_file = form.cleaned_data['file']
        try:
            df = self._read_file(upload_file)
            instances = self.build_instances(df)
        except Exception as e:
            form.add_error(None, f"Upload failed: {e}")
            return self.form_invalid(form)

        try:
            with transaction.atomic():
                self.model.objects.bulk_create(instances)
        except Exception as e:
            form.add_error(None, f"Save failed: {e}")
            return self.form_invalid(form)

        messages.success(self.request, self.success_message)
        return super().form_valid(form)

    def _read_file(self, upload_file):
        """Read an Excel or CSV into a pandas DataFrame."""
        ext = os.path.splitext(upload_file.name)[1].lower()
        if ext in ('.xls', '.xlsx'):
            if not self.sheet_name:
                raise ValueError("No sheet_name specified for Excel upload")
            return pd.read_excel(upload_file, sheet_name=self.sheet_name)
        elif ext == '.csv':
            return pd.read_csv(upload_file)
        else:
            raise ValueError("Unsupported file type. Please upload .xls, .xlsx or .csv only.")

    def build_instances(self, df):
        """Convert DataFrame rows into model instances. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement build_instances()")


class HQLAUploadView(BaseUploadView):
    model           = HQLATable
    form_class      = HQLAUploadForm
    template_name   = 'pages/upload_hqla.html'
    success_url     = reverse_lazy('LCR:upload_hqla')
    success_message = "HQLA records uploaded successfully!"
    sheet_name      = 'Stock of HQLA'

    def build_instances(self, df):
        # 1) Normalize headers: strip spaces & title-case
        df.columns = (
            df.columns
              .str.strip()   # remove leading/trailing whitespace
              .str.title()   # e.g. "name " → "Name", "item id" → "Item Id"
        )

        # 2) Ensure required columns exist
        required = ['Item Id', 'Name', 'Currency', 'Amount', 'Reporting Date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise forms.ValidationError(
                f"Upload failed: missing columns: {', '.join(missing)}"
            )

        # 3) Build model instances
        instances = []
        for _, row in df.iterrows():
            instances.append(HQLATable(
                item_id        = row['Item Id'],
                name           = row['Name'],
                currency       = row['Currency'],
                amount         = row['Amount'],
                reporting_date = row['Reporting Date'],
            ))
        return instances


class CashInflowUploadView(BaseUploadView):
    model = CashInflowTable
    form_class = CashInflowUploadForm
    template_name = 'pages/upload_cash_inflow.html'
    success_url = reverse_lazy('LCR:upload_cash_inflow')
    success_message = "Cash Inflow records uploaded successfully!"
    sheet_name = 'Cash Inflows'

    def build_instances(self, df):
        # 1) Normalize headers: strip spaces & title-case
        df.columns = (
            df.columns
              .str.strip()   # remove leading/trailing whitespace
              .str.title()   # e.g. "name " → "Name", "item id" → "Item Id"
        )

        # 2) Ensure required columns exist
        required = ['Item Id', 'Name', 'Currency', 'Amount', 'Reporting Date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise forms.ValidationError(
                f"Upload failed: missing columns: {', '.join(missing)}"
            )
        
        # 3) Build model instances
        instances = []
        for _, row in df.iterrows():
            instances.append(HQLATable(
                item_id        = row['Item Id'],
                name           = row['Name'],
                currency       = row['Currency'],
                amount         = row['Amount'],
                reporting_date = row['Reporting Date'],
            ))
        return instances


class CashOutflowUploadView(BaseUploadView):
    model = CashOutflowTable
    form_class = CashOutflowUploadForm
    template_name = 'pages/upload_cash_outflow.html'
    success_url = reverse_lazy('LCR:upload_cash_outflow')
    success_message = "Cash Outflow records uploaded successfully!"
    sheet_name = 'Cash Outflows'

    def build_instances(self, df):
        # 1) Normalize headers: strip spaces & title-case
        df.columns = (
            df.columns
              .str.strip()   # remove leading/trailing whitespace
              .str.title()   # e.g. "name " → "Name", "item id" → "Item Id"
        )

        # 2) Ensure required columns exist
        required = ['Item Id', 'Name', 'Currency', 'Amount', 'Reporting Date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise forms.ValidationError(
                f"Upload failed: missing columns: {', '.join(missing)}"
            )
        
        # 3) Build model instances
        instances = []
        for _, row in df.iterrows():
            instances.append(HQLATable(
                item_id        = row['Item Id'],
                name           = row['Name'],
                currency       = row['Currency'],
                amount         = row['Amount'],
                reporting_date = row['Reporting Date'],
            ))
        return instances
    
class AvailableStableUploadView(BaseUploadView):
    model = ASFTable
    form_class = AvailableStableUploadForm
    template_name = 'pages/upload_asf.html'
    success_url = reverse_lazy('LCR:upload_asf')
    success_message = "Available Stable records uploaded successfully!"
    sheet_name = 'ASF'

    def build_instances(self, df):
        # 1) Normalize headers: strip spaces & title-case
        df.columns = (
            df.columns
              .str.strip()   # remove leading/trailing whitespace
              .str.title()   # e.g. "name " → "Name", "item id" → "Item Id"
        )

        # 2) Ensure required columns exist
        required = ['Item Id', 'Name', 'Currency', 'Amount', 'Reporting Date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise forms.ValidationError(
                f"Upload failed: missing columns: {', '.join(missing)}"
            )
        
        # 3) Build model instances
        instances = []
        for _, row in df.iterrows():
            instances.append(HQLATable(
                item_id        = row['Item Id'],
                name           = row['Name'],
                currency       = row['Currency'],
                amount         = row['Amount'],
                reporting_date = row['Reporting Date'],
            ))
        return instances
    
class RequiredStableUploadView(BaseUploadView):
    model = RSFTable
    form_class = RequiredStableUploadForm
    template_name = 'pages/upload_rsf.html'
    success_url = reverse_lazy('LCR:upload_rsf')
    success_message = "Required Stable records uploaded successfully!"
    sheet_name = 'RSF'

    def build_instances(self, df):
        # 1) Normalize headers: strip spaces & title-case
        df.columns = (
            df.columns
              .str.strip()   # remove leading/trailing whitespace
              .str.title()   # e.g. "name " → "Name", "item id" → "Item Id"
        )

        # 2) Ensure required columns exist
        required = ['Item Id', 'Name', 'Currency', 'Amount', 'Reporting Date']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise forms.ValidationError(
                f"Upload failed: missing columns: {', '.join(missing)}"
            )
        
        # 3) Build model instances
        instances = []
        for _, row in df.iterrows():
            instances.append(HQLATable(
                item_id        = row['Item Id'],
                name           = row['Name'],
                currency       = row['Currency'],
                amount         = row['Amount'],
                reporting_date = row['Reporting Date'],
            ))
        return instances
    



class CashInflowByCurrencyView(CustomOrderGroupedByCurrencyMixin):
    model = CashInflowTable
    template_name = 'pages/cash_inflow_by_currency.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # supply distinct reporting dates for the dropdown
        context['reporting_dates'] = (
            CashInflowTable.objects
            .order_by('reporting_date')
            .values_list('reporting_date', flat=True)
            .distinct()
        )
        return context


class CashOutflowByCurrencyView(CustomOrderGroupedByCurrencyMixin):
    model = CashOutflowTable
    template_name = 'pages/cash_outflow_by_currency.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['reporting_dates'] = (
            CashOutflowTable.objects
            .order_by('reporting_date')
            .values_list('reporting_date', flat=True)
            .distinct()
        )
        return context


class HqlaByCurrencyView(CustomOrderGroupedByCurrencyMixin):
    model = HQLATable
    template_name = 'pages/hqla_by_currency.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['reporting_dates'] = (
            HQLATable.objects
            .order_by('reporting_date')
            .values_list('reporting_date', flat=True)
            .distinct()
        )
        return context






class ApplyAllHaircutsView(View):
    template_name = 'pages/apply_weights.html'
    redirect_url  = reverse_lazy('apply_weights')

    def get(self, request, *args, **kwargs):
        dates = (
            set(HQLATable.objects.values_list('reporting_date', flat=True)) |
            set(CashInflowTable.objects.values_list('reporting_date', flat=True)) |
            set(CashOutflowTable.objects.values_list('reporting_date', flat=True)) |
            set(ASFTable.objects.values_list('reporting_date', flat=True)) |
            set(RSFTable.objects.values_list('reporting_date', flat=True))
        )
        return render(request, self.template_name, {
            'reporting_dates': sorted(dates),
        })

    def post(self, request, *args, **kwargs):
        # 1) parse & validate date
        date_str = request.POST.get('reporting_date')
        try:
            reporting_date = datetime.fromisoformat(date_str).date()
        except (TypeError, ValueError):
            err = "Invalid reporting date selected."
            messages.error(request, err)
            logger.error(err)
            return redirect(self.redirect_url)

        hqla_count = inflow_count = outflow_count = asf_count = rsf_count = 0

        def upsert_lcr(row, record_type, adjusted_value):
            ct_item    = ContentType.objects.get_for_model(row.item)
            ct_section = ContentType.objects.get_for_model(row.item.section)
            LCRRecord.objects.update_or_create(
                reporting_date        = reporting_date,
                currency              = row.currency,
                item_content_type     = ct_item,
                item_object_id        = row.item.pk,
                section_content_type  = ct_section,
                section_object_id     = row.item.section.pk,
                record_type           = record_type,
                defaults = {
                    'amount_before_weights': row.amount,
                    'adjusted_amount':       adjusted_value,
                    'item_name':             row.item.item_name,
                    'section_name':          row.item.section.section_name,
                }
            )

        # 2) HQLA
        for row in HQLATable.objects.filter(reporting_date=reporting_date) \
                                   .annotate(weight=F('item__section__weight')) \
                                   .select_related('item__section'):
            adj = row.amount * row.weight
            row.adjusted_amount = adj
            row.save(update_fields=['adjusted_amount'])
            upsert_lcr(row, LCRRecord.HQLA, adj)
            hqla_count += 1

        # 3) Cash Inflow
        for row in CashInflowTable.objects.filter(reporting_date=reporting_date) \
                                          .annotate(weight=F('item__section__weight')) \
                                          .select_related('item__section'):
            adj = row.amount * row.weight
            row.adjusted_amount = adj
            row.save(update_fields=['adjusted_amount'])
            upsert_lcr(row, LCRRecord.INFLOW, adj)
            inflow_count += 1

        # 4) Cash Outflow
        for row in CashOutflowTable.objects.filter(reporting_date=reporting_date) \
                                           .annotate(runoff=F('item__section__runoff_rate')) \
                                           .select_related('item__section'):
            adj = row.amount * row.runoff
            row.adjusted_amount = adj
            row.save(update_fields=['adjusted_amount'])
            upsert_lcr(row, LCRRecord.OUTFLOW, adj)
            outflow_count += 1

        # 5) ASFTable
        for row in ASFTable.objects.filter(reporting_date=reporting_date) \
                                  .annotate(weight=F('item__weight')) \
                                  .select_related('item', 'item__section'):
            adj = row.amount * row.weight
            row.adjusted_amount = adj
            row.save(update_fields=['adjusted_amount'])
            upsert_lcr(row, LCRRecord.ASF, adj)
            asf_count += 1

        # 6) RSFTable
        for row in RSFTable.objects.filter(reporting_date=reporting_date) \
                                  .annotate(weight=F('item__weight')) \
                                  .select_related('item', 'item__section'):
            adj = row.amount * row.weight
            row.adjusted_amount = adj
            row.save(update_fields=['adjusted_amount'])
            upsert_lcr(row, LCRRecord.RSF, adj)
            rsf_count += 1

        # 7) feedback
        total = hqla_count + inflow_count + outflow_count + asf_count + rsf_count
        msg = (
            f"Applied haircuts for {reporting_date} → "
            f"HQLA: {hqla_count}, Inflows: {inflow_count}, "
            f"Outflows: {outflow_count}, ASF: {asf_count}, RSF: {rsf_count} "
            f"(Total: {total})"
        )
        messages.success(request, msg)
        logger.info(msg)

        dates = (
            set(HQLATable.objects.values_list('reporting_date', flat=True)) |
            set(CashInflowTable.objects.values_list('reporting_date', flat=True)) |
            set(CashOutflowTable.objects.values_list('reporting_date', flat=True)) |
            set(ASFTable.objects.values_list('reporting_date', flat=True)) |
            set(RSFTable.objects.values_list('reporting_date', flat=True))
        )
        return render(request, self.template_name, {
            'reporting_dates': sorted(dates),
            'hqla_count':      hqla_count,
            'inflow_count':    inflow_count,
            'outflow_count':   outflow_count,
            'asf_count':       asf_count,
            'rsf_count':       rsf_count,
            'total_count':     total,
            'selected_date':   reporting_date,
        })
    






class GenerateCurrencyAdjustmentSummaryView(View):
    template_name = 'pages/generate_summaries.html'
    redirect_url  = reverse_lazy('generate_summary')

    def get(self, request, *args, **kwargs):
        # collect all distinct dates across the three tables
        dates = sorted({
            *HQLATable.objects.values_list('reporting_date', flat=True),
            *CashInflowTable.objects.values_list('reporting_date', flat=True),
            *CashOutflowTable.objects.values_list('reporting_date', flat=True),
        })
        return render(request, self.template_name, {
            'reporting_dates': dates
        })

    def post(self, request, *args, **kwargs):
        date_str = request.POST.get('reporting_date')
        try:
            reporting_date = datetime.fromisoformat(date_str).date()
        except (TypeError, ValueError):
            err = "Please select a valid reporting date."
            messages.error(request, err)
            return redirect(self.redirect_url)

        # 1) delete old summaries for that date
        CurrencyAdjustmentSummary.objects.filter(
            reporting_date=reporting_date
        ).delete()

        # 2) helper: rebuild per‐currency summary rows
        def summarize(model, record_type):
            qs = model.objects.filter(reporting_date=reporting_date)
            for row in qs.values('currency').annotate(
                    total_amount=Sum('amount'),
                    total_adjusted_amount=Sum('adjusted_amount')
                ):
                CurrencyAdjustmentSummary.objects.create(
                    reporting_date        = reporting_date,
                    currency              = row['currency'],
                    record_type           = record_type,
                    total_amount          = row['total_amount'] or 0,
                    total_adjusted_amount = row['total_adjusted_amount'] or 0,
                )

        summarize(HQLATable,        CurrencyAdjustmentSummary.HQLA)
        summarize(CashInflowTable,  CurrencyAdjustmentSummary.INFLOW)
        summarize(CashOutflowTable, CurrencyAdjustmentSummary.OUTFLOW)

        # 3) compute consolidated totals across ALL currencies
        totals_qs = CurrencyAdjustmentSummary.objects.filter(
            reporting_date=reporting_date
        ).values('record_type').annotate(
            consolidated_amount=Sum('total_adjusted_amount')
        )
        consolidated_totals = {
            entry['record_type']: entry['consolidated_amount'] or 0
            for entry in totals_qs
        }

        # 4) success feedback
        msg = f"Summaries regenerated for {reporting_date}."
        messages.success(request, msg)

        # 5) re-fetch date list and render template with consolidated totals
        dates = sorted({
            *HQLATable.objects.values_list('reporting_date', flat=True),
            *CashInflowTable.objects.values_list('reporting_date', flat=True),
            *CashOutflowTable.objects.values_list('reporting_date', flat=True),
        })
        return render(request, self.template_name, {
            'reporting_dates':    dates,
            'selected_date':      reporting_date,
            'consolidated_totals': consolidated_totals,
        })
    






    
class LCRByCurrencyView(TemplateView):
    template_name = 'pages/lcr_by_currency.html'

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        # 1) supply date dropdown
        dates = CurrencyAdjustmentSummary.objects \
                  .values_list('reporting_date', flat=True) \
                  .distinct() \
                  .order_by('reporting_date')
        ctx['reporting_dates'] = dates

        # 2) if user selected a date, compute per‐currency LCR
        date_str = self.request.GET.get('reporting_date')
        if date_str:
            try:
                sel_date = datetime.fromisoformat(date_str).date()
            except ValueError:
                ctx['error'] = "Invalid date format."
                return ctx

            qs = CurrencyAdjustmentSummary.objects.filter(reporting_date=sel_date)
            # build a lookup: {(currency, type) -> totals}
            data = {}
            for rec in qs:
                data.setdefault(rec.currency, {})[rec.record_type] = rec.total_adjusted_amount

            lcr_list = []
            for currency, vals in data.items():
                hqla    = vals.get(CurrencyAdjustmentSummary.HQLA,   Decimal(0))
                inflow  = vals.get(CurrencyAdjustmentSummary.INFLOW, Decimal(0))
                outflow = vals.get(CurrencyAdjustmentSummary.OUTFLOW,Decimal(0))

                gross_out = outflow
                net_out   = gross_out - min(inflow, gross_out * Decimal('0.75'))

                if net_out > 0:
                    lcr = (hqla / net_out) * Decimal(100)
                else:
                    lcr = None

                lcr_list.append({
                    'currency': currency,
                    'hqla':      hqla,
                    'inflow':    inflow,
                    'outflow':   outflow,
                    'net_out':   net_out,
                    'lcr':       lcr.quantize(Decimal('0.01')) if lcr is not None else None,
                })

            ctx['lcr_data']      = sorted(lcr_list, key=lambda x: x['currency'])
            ctx['selected_date'] = sel_date

            # 3) consolidated totals across all currencies
            total_hqla    = sum(item['hqla']    for item in ctx['lcr_data'])
            total_inflow  = sum(item['inflow']  for item in ctx['lcr_data'])
            total_outflow = sum(item['outflow'] for item in ctx['lcr_data'])

            gross_out_con = total_outflow
            net_out_con   = gross_out_con - min(total_inflow, gross_out_con * Decimal('0.75'))

            if net_out_con > 0:
                cons_lcr = (total_hqla / net_out_con) * Decimal(100)
                cons_lcr = cons_lcr.quantize(Decimal('0.01'))
            else:
                cons_lcr = None

            ctx.update({
                'consolidated_hqla':    total_hqla,
                'consolidated_inflow':  total_inflow,
                'consolidated_outflow': total_outflow,
                'consolidated_net_out': net_out_con,
                'consolidated_lcr':     cons_lcr,
            })

        return ctx
    







class LCRRecordListView(View):
    template_name = 'pages/lcr_records.html'

    def get(self, request, *args, **kwargs):
        # 1) date dropdown
        reporting_dates = (
            CurrencyAdjustmentSummary.objects
            .values_list('reporting_date', flat=True)
            .distinct()
            .order_by('reporting_date')
        )
        ctx = {'reporting_dates': reporting_dates}

        date_str = request.GET.get('reporting_date')
        if not date_str:
            return render(request, self.template_name, ctx)

        # 2) parse selected date
        try:
            sel_date = datetime.fromisoformat(date_str).date()
        except ValueError:
            messages.error(request, "Invalid date format.")
            return render(request, self.template_name, ctx)
        ctx['selected_date'] = sel_date

        # 3) pull all summaries for that date
        summaries = CurrencyAdjustmentSummary.objects.filter(reporting_date=sel_date)

        # 4) organize into per-currency dict
        data = {}
        for s in summaries:
            data.setdefault(s.currency, {})[s.record_type] = s.total_adjusted_amount or Decimal(0)

        # 5) build per-currency LCR summary list
        lcr_data = []
        for currency in sorted(data):
            vals    = data[currency]
            hqla    = vals.get(CurrencyAdjustmentSummary.HQLA,   Decimal(0))
            inflow  = vals.get(CurrencyAdjustmentSummary.INFLOW, Decimal(0))
            outflow = vals.get(CurrencyAdjustmentSummary.OUTFLOW,Decimal(0))

            gross_out = outflow
            net_out   = gross_out - min(inflow, gross_out * Decimal('0.75'))

            if net_out > 0:
                lcr = (hqla / net_out) * Decimal(100)
                lcr = lcr.quantize(Decimal('0.01'))
            else:
                lcr = None

            lcr_data.append({
                'currency': currency,
                'hqla':      hqla,
                'inflow':    inflow,
                'outflow':   outflow,
                'net_out':   net_out,
                'lcr':       lcr,
            })

        # 6) compute consolidated row
        total_hqla    = sum(item['hqla']    for item in lcr_data)
        total_inflow  = sum(item['inflow']  for item in lcr_data)
        total_outflow = sum(item['outflow'] for item in lcr_data)

        gross_out = total_outflow
        con_net_out = gross_out - min(total_inflow, gross_out * Decimal('0.75'))

        if con_net_out > 0:
            con_lcr = (total_hqla / con_net_out) * Decimal(100)
            con_lcr = con_lcr.quantize(Decimal('0.01'))
        else:
            con_lcr = None

        consolidated = {
            'currency': 'Consolidated',
            'hqla':      total_hqla,
            'inflow':    total_inflow,
            'outflow':   total_outflow,
            'net_out':   con_net_out,
            'lcr':       con_lcr,
        }

        # 7) set context for summary table: all currencies then consolidated last
        ctx['lcr_data'] = lcr_data + [consolidated]

        # 8) prepare detailed records
        qs = (
            LCRRecord.objects
            .filter(reporting_date=sel_date)
            .order_by('currency', 'record_type', 'item_object_id')
        )

        summary_map = {
            (s.currency, s.record_type): s.total_adjusted_amount or Decimal(0)
            for s in summaries
        }

        detailed = []
        for rec in qs:
            # reload item & section objects
            item_model    = rec.item_content_type.model_class()
            section_model = rec.section_content_type.model_class()
            try:
                item_obj    = item_model.objects.get(pk=rec.item_object_id)
                section_obj = section_model.objects.get(pk=rec.section_object_id)
                rec.item_name    = getattr(item_obj, 'item_name', rec.item_name)
                rec.section_name = getattr(section_obj, 'section_name', rec.section_name)
                # extract weight or runoff_rate
                if rec.record_type in (LCRRecord.HQLA, LCRRecord.INFLOW):
                    rec.section_weight = getattr(section_obj, 'weight', None)
                else:
                    rec.section_weight = getattr(section_obj, 'runoff_rate', None)
            except item_model.DoesNotExist:
                rec.section_weight = None

            rec.summary_total = summary_map.get((rec.currency, rec.record_type), Decimal(0))
            detailed.append(rec)

        # 9) group detailed by currency
        currency_groups = []
        for currency, grp in groupby(detailed, key=operator.attrgetter('currency')):
            rows = list(grp)
            types = {r.record_type for r in rows}
            totals_by_type = {
                t: summary_map.get((currency, t), Decimal(0))
                for t in types
            }
            currency_groups.append({
                'currency':       currency,
                'rows':           rows,
                'totals_by_type': totals_by_type,
                'total':          sum(totals_by_type.values()),
            })

        ctx['currency_groups'] = currency_groups
        return render(request, self.template_name, ctx)
    
    








class NSFRRecordListView(View):
    """
    Shows NSFR per-currency summary (ASF, RSF, NSFR%) and detailed rows
    pulled from ASFTable and RSFTable for a selected reporting date.

    Assumptions:
      - ASF/RSF weights live on the item (ASFItem.weight / RSFItem.weight).
      - ASFTable/RSFTable fields: item (FK), currency, amount, adjusted_amount, reporting_date.
      - item.section.section_name and item.item_name exist.
    """
    template_name = 'pages/nsfr_records.html'

    def get(self, request, *args, **kwargs):
        # 1) Date dropdown from both ASF and RSF tables
        reporting_dates = sorted({
            *ASFTable.objects.values_list('reporting_date', flat=True),
            *RSFTable.objects.values_list('reporting_date', flat=True),
        })
        ctx = {'reporting_dates': reporting_dates}

        date_str = request.GET.get('reporting_date')
        if not date_str:
            return render(request, self.template_name, ctx)

        # 2) Parse selected date
        try:
            sel_date = datetime.fromisoformat(date_str).date()
        except ValueError:
            messages.error(request, "Invalid date format.")
            return render(request, self.template_name, ctx)
        ctx['selected_date'] = sel_date

        # Helper to compute an adjusted value with fallback
        def adjusted_or_fallback(row):
            if row.adjusted_amount is not None:
                return row.adjusted_amount
            # fallback if not yet applied: amount * item.weight
            w = getattr(row.item, 'weight', None)
            if w is None:
                return Decimal(0)
            return row.amount * w

        # 3) Per-currency totals for ASF and RSF
        # Use DB totals if adjusted_amount is populated, else compute in Python fallback
        asf_qs = (ASFTable.objects
                  .filter(reporting_date=sel_date)
                  .select_related('item', 'item__section'))
        rsf_qs = (RSFTable.objects
                  .filter(reporting_date=sel_date)
                  .select_related('item', 'item__section'))

        # Try DB aggregation first
        asf_totals_db = asf_qs.values('currency').annotate(total_asf=Sum('adjusted_amount'))
        rsf_totals_db = rsf_qs.values('currency').annotate(total_rsf=Sum('adjusted_amount'))

        asf_map = {r['currency']: (r['total_asf'] or Decimal(0)) for r in asf_totals_db}
        rsf_map = {r['currency']: (r['total_rsf'] or Decimal(0)) for r in rsf_totals_db}

        # If any currency has zero because adjusted_amount is still nulls, compute python-side
        # Sum fallbacks per currency
        if any(v == 0 for v in asf_map.values()) or not asf_map:
            asf_map = {}
            for r in asf_qs:
                asf_map.setdefault(r.currency, Decimal(0))
                asf_map[r.currency] += adjusted_or_fallback(r)

        if any(v == 0 for v in rsf_map.values()) or not rsf_map:
            rsf_map = {}
            for r in rsf_qs:
                rsf_map.setdefault(r.currency, Decimal(0))
                rsf_map[r.currency] += adjusted_or_fallback(r)

        # 4) Build per-currency NSFR summary
        currencies = sorted(set(asf_map.keys()) | set(rsf_map.keys()))
        nsfr_data = []
        for cur in currencies:
            asf = asf_map.get(cur, Decimal(0))
            rsf = rsf_map.get(cur, Decimal(0))
            if rsf and rsf != 0:
                nsfr = (asf / rsf) * Decimal(100)
                nsfr = nsfr.quantize(Decimal('0.01'))
            else:
                nsfr = None
            nsfr_data.append({
                'currency': cur,
                'asf': asf,
                'rsf': rsf,
                'nsfr': nsfr,
            })
        ctx['nsfr_data'] = nsfr_data

        # 5) Detailed rows (ASF + RSF), grouped by currency then type
        detailed_asf = []
        for r in asf_qs:
            detailed_asf.append({
                'currency': r.currency,
                'record_type': 'ASF',
                'section_name': getattr(getattr(r.item, 'section', None), 'section_name', None),
                'item_name': getattr(r.item, 'item_name', None),
                'weight': getattr(r.item, 'weight', None),  # weight from item
                'amount_before_weights': r.amount,
                'adjusted_amount': adjusted_or_fallback(r),
            })

        detailed_rsf = []
        for r in rsf_qs:
            detailed_rsf.append({
                'currency': r.currency,
                'record_type': 'RSF',
                'section_name': getattr(getattr(r.item, 'section', None), 'section_name', None),
                'item_name': getattr(r.item, 'item_name', None),
                'weight': getattr(r.item, 'weight', None),  # weight from item
                'amount_before_weights': r.amount,
                'adjusted_amount': adjusted_or_fallback(r),
            })

        detailed_all = detailed_asf + detailed_rsf
        type_order = {'ASF': 0, 'RSF': 1}
        detailed_all.sort(key=lambda x: (x['currency'], type_order.get(x['record_type'], 99), x['item_name'] or ''))

        # 6) Group and compute per-type subtotals
        currency_groups = []
        for currency, grp in groupby(detailed_all, key=lambda x: x['currency']):
            rows = list(grp)
            totals_by_type = {'ASF': Decimal(0), 'RSF': Decimal(0)}
            for r in rows:
                totals_by_type[r['record_type']] += r['adjusted_amount'] or Decimal(0)

            currency_groups.append({
                'currency':       currency,
                'rows':           rows,
                'totals_by_type': totals_by_type,
                'total':          totals_by_type['ASF'] + totals_by_type['RSF'],
            })

        ctx['currency_groups'] = currency_groups
        return render(request, self.template_name, ctx)
    







def download_log(request, filename):
    """
    Serve a log .txt from MEDIA_ROOT/lcr_logs/ as an attachment.
    """
    safe_name = os.path.basename(filename)  # prevent path traversal
    path = os.path.join(settings.MEDIA_ROOT, 'lcr_logs', safe_name)
    if not os.path.exists(path):
        raise Http404("Log not found")
    # open and return as a text/plain attachment
    response = FileResponse(open(path, 'rb'), content_type='text/plain')
    response['Content-Disposition'] = f'attachment; filename="{safe_name}"'
    return response






class PreviousRunsView(ListView):
    model = LCRRun
    template_name = 'pages/previous_runs.html'
    context_object_name = 'runs'
    ordering = ['-created_at']





class NSFRByCurrencyView(TemplateView):
    template_name = 'pages/nsfr_by_currency.html'

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # 1) Date dropdown (union of ASF & RSF dates)
        dates = sorted({
            *ASFTable.objects.values_list('reporting_date', flat=True),
            *RSFTable.objects.values_list('reporting_date', flat=True),
        })
        ctx['reporting_dates'] = dates

        # 2) If user selected a date, compute per-currency NSFR
        date_str = self.request.GET.get('reporting_date')
        if date_str:
            try:
                sel_date = datetime.fromisoformat(date_str).date()
            except ValueError:
                ctx['error'] = "Invalid date format."
                return ctx

            # Sum adjusted_amount by currency
            asf_rows = (
                ASFTable.objects.filter(reporting_date=sel_date)
                .values('currency')
                .annotate(
                    total_asf=Coalesce(
                        Sum('adjusted_amount'),
                        Value(0, output_field=DecimalField(max_digits=24, decimal_places=2))
                    )
                )
            )
            rsf_rows = (
                RSFTable.objects.filter(reporting_date=sel_date)
                .values('currency')
                .annotate(
                    total_rsf=Coalesce(
                        Sum('adjusted_amount'),
                        Value(0, output_field=DecimalField(max_digits=24, decimal_places=2))
                    )
                )
            )

            asf_map = {r['currency']: r['total_asf'] for r in asf_rows}
            rsf_map = {r['currency']: r['total_rsf'] for r in rsf_rows}

            # union of currencies present in either table
            currencies = sorted(set(asf_map) | set(rsf_map))

            nsfr_list = []
            for cur in currencies:
                asf = Decimal(asf_map.get(cur, 0))
                rsf = Decimal(rsf_map.get(cur, 0))
                nsfr = (asf / rsf * Decimal(100)).quantize(Decimal('0.01')) if rsf > 0 else None

                nsfr_list.append({
                    'currency': cur,
                    'asf': asf,
                    'rsf': rsf,
                    'nsfr': nsfr,
                })

            ctx['nsfr_data'] = nsfr_list
            ctx['selected_date'] = sel_date

        return ctx
    







class RunLCRView(View):
    template_name = 'pages/run_lcr.html'
    redirect_url  = reverse_lazy('lcr_run')

    STEPS = [
        ("Grouping HQLA",         HqlaByCurrencyView,                    'get',
         "Summarize HQLA balances by currency"),
        ("Grouping Inflows",      CashInflowByCurrencyView,              'get',
         "Summarize cash inflows by currency"),
        ("Grouping Outflows",     CashOutflowByCurrencyView,             'get',
         "Summarize cash outflows by currency"),
        ("Apply Haircuts",        ApplyAllHaircutsView,                  'post',
         "Apply section weights to raw amounts"),
        ("Generate Summary",      GenerateCurrencyAdjustmentSummaryView, 'post',
         "Recompute currency adjustment summaries"),
        ("Compute LCR Ratios",    LCRByCurrencyView,                     'get',
         "Calculate Liquidity Coverage Ratios per currency"),
        ("Compute NSFR Ratios",   NSFRByCurrencyView,                    'get',
         "Calculate Net Stable Funding Ratio per currency"),
    ]

    def _all_dates(self):
        """
        Union of reporting_date across HQLA, Inflow, Outflow, ASF, RSF.
        Excludes NULLs and returns a sorted list of date objects.
        """
        dates = set()

        dates |= set(HQLATable.objects
                     .exclude(reporting_date__isnull=True)
                     .values_list('reporting_date', flat=True))
        dates |= set(CashInflowTable.objects
                     .exclude(reporting_date__isnull=True)
                     .values_list('reporting_date', flat=True))
        dates |= set(CashOutflowTable.objects
                     .exclude(reporting_date__isnull=True)
                     .values_list('reporting_date', flat=True))
        dates |= set(ASFTable.objects
                     .exclude(reporting_date__isnull=True)
                     .values_list('reporting_date', flat=True))
        dates |= set(RSFTable.objects
                     .exclude(reporting_date__isnull=True)
                     .values_list('reporting_date', flat=True))

        return sorted(dates)

    def get(self, request, *args, **kwargs):
        ctx = {'reporting_dates': self._all_dates()}
        last_run = LCRRun.objects.order_by('-created_at').first()
        if not last_run:
            return render(request, self.template_name, ctx)

        processes = []
        for name, _, _, description in self.STEPS:
            fname = f"{last_run.run_name}_{slugify(name)}.txt"
            path  = os.path.join(settings.MEDIA_ROOT, 'lcr_logs', fname)

            if os.path.exists(path):
                with open(path, encoding='utf-8') as f:
                    content = f.read()
                status  = 'Failed' if 'Error:' in content else 'Success'
                log_url = reverse('LCR:download_log', args=[fname])
            else:
                status, log_url = 'Not Started', None

            processes.append({
                'name':        name,
                'description': description,
                'status':      status,
                'log_url':     log_url,
            })

        ctx.update({
            'selected_date':  last_run.reporting_date,
            'run_name':       last_run.run_name,
            'processes':      processes,
            'all_successful': all(p['status'] == 'Success' for p in processes),
        })
        return render(request, self.template_name, ctx)

    def post(self, request, *args, **kwargs):
        # 1) Parse date
        date_str = request.POST.get('reporting_date')
        try:
            sel_date = datetime.fromisoformat(date_str).date()
        except (TypeError, ValueError):
            messages.error(request, "Please select a valid reporting date.")
            return redirect(self.redirect_url)

        # 2) Create unique run name
        existing = LCRRun.objects.filter(reporting_date=sel_date).count()
        base     = sel_date.strftime('LCR_%Y%m%d')
        run_name = base if existing == 0 else f"{base}_{existing}"

        # 3) Create run row with status "Running"
        run = LCRRun.objects.create(
            reporting_date=sel_date,
            run_name=run_name,
            status="Running",
        )

        # 4) Prepare logging dir
        log_dir = os.path.join(settings.MEDIA_ROOT, 'lcr_logs')
        os.makedirs(log_dir, exist_ok=True)

        rf = RequestFactory()
        processes = []

        # 5) Execute pipeline steps
        for name, ViewClass, method, description in self.STEPS:
            fname = f"{run_name}_{slugify(name)}.txt"
            path  = os.path.join(log_dir, fname)

            fake_req = rf.post('/', {'reporting_date': date_str}) if method == 'post' else rf.get('/', {'reporting_date': date_str})
            fake_req.user = request.user
            fake_req.session = request.session
            setattr(fake_req, '_messages', FallbackStorage(fake_req))

            status = "Pending"
            logs   = []
            try:
                view = ViewClass()
                view.setup(fake_req, *[], **{})

                if method == 'post':
                    resp = view.post(fake_req, *args, **kwargs)
                    logs.append(f"{name}: HTTP {getattr(resp, 'status_code', 'OK')}")
                else:
                    resp = view.get(fake_req, *args, **kwargs)
                    key_by_view = {
                        HqlaByCurrencyView:        'hqla_data',
                        CashInflowByCurrencyView:  'cash_inflow_data',
                        CashOutflowByCurrencyView: 'cash_outflow_data',
                        LCRByCurrencyView:         'lcr_data',
                        NSFRByCurrencyView:        'nsfr_data',
                    }
                    data_key = key_by_view.get(ViewClass)
                    length = len(getattr(resp, 'context_data', {}).get(data_key, [])) if data_key else 0
                    logs.append(f"{name}: {length} records")
                status = "Success"

            except Exception as e:
                status = "Failed"
                logs.append(f"Error: {e}")
                logger.exception(f"{name} failed for run {run_name}")

            # write step log
            with open(path, 'w', encoding='utf-8') as f:
                f.write("\n".join(logs))

            processes.append({
                'name':        name,
                'description': description,
                'status':      status,
                'log_url':     reverse('LCR:download_log', args=[fname]),
            })

        # 6) Finalize overall status on the run row
        overall_success = all(p['status'] == 'Success' for p in processes)
        run.status = "Success" if overall_success else "Failed"
        run.save(update_fields=['status'])

        # 7) Render page with results
        ctx = {
            'reporting_dates': self._all_dates(),
            'selected_date':   sel_date,
            'run_name':        run_name,
            'processes':       processes,
            'all_successful':  overall_success,
        }
        return render(request, self.template_name, ctx)
    








class ConfigsView(View):
    template_name = "pages/configs.html"
    edit_template_name = "pages/configs_edit.html"

    # NOTE:
    # - For sections that have both "parent" and "weight", "parent" now comes before "weight".
    # - For CASH OUTFLOW SECTIONS (no weight), "parent" now comes before "runoff_rate".
    ENTITY_MAP = {
        "hqla_section":    ("LCR — HQLA Sections",                   HQLASection,        HQLASectionForm,          ["level", "section_name", "parent", "weight"]),
        "hqla_item":       ("LCR — HQLA Items",                      HQLAItem,           HQLAItemForm,             ["section", "item_name"]),

        # Cash In/Out use Category for sections
        "inflow_section":  ("LCR — Cash Inflow Sections",            CashInflowSection,  CashInflowSectionForm,    ["category", "section_name", "parent", "weight"]),
        "inflow_item":     ("LCR — Cash Inflow Items",               CashInflowItem,     CashInflowItemForm,       ["section", "item_name"]),

        # ⬇️ Fixed: parent now before runoff_rate
        "outflow_section": ("LCR — Cash Outflow Sections",           CashOutflowSection, CashOutflowSectionForm,   ["category", "section_name", "parent", "runoff_rate"]),
        "outflow_item":    ("LCR — Cash Outflow Items",              CashOutflowItem,    CashOutflowItemForm,      ["section", "item_name"]),

        "asf_section":     ("NSFR — ASF Sections",                   ASFSection,         ASFSectionForm,           ["level", "section_name", "parent"]),
        "asf_item":        ("NSFR — ASF Items (weights on items)",   ASFItem,            ASFItemForm,              ["section", "item_name", "weight"]),

        "rsf_section":     ("NSFR — RSF Sections",                   RSFSection,         RSFSectionForm,           ["level", "section_name", "parent"]),
        "rsf_item":        ("NSFR — RSF Items (weights on items)",   RSFItem,            RSFItemForm,              ["section", "item_name", "weight"]),
    }

    def _ordered_qs(self, model):
        if hasattr(model, "display_order"):
            return model.objects.all().order_by("display_order")
        if hasattr(model, "category"):
            return model.objects.all().order_by("category", "section_name")
        if hasattr(model, "level"):
            return model.objects.all().order_by("level", "section_name")
        for f in ("section_name", "item_name", "id"):
            if hasattr(model, f):
                return model.objects.all().order_by(f)
        return model.objects.all()

    def _group_data_for_display(self, queryset, entity_key):
        is_section = "_section" in entity_key
        grouped_data = []

        if is_section:
            use_category = entity_key in ("inflow_section", "outflow_section")
            group_field = "category" if use_category else "level"
            header_label = "Category" if use_category else "Level"

            groups = defaultdict(list)
            for item in queryset:
                if group_field == "category":
                    key = getattr(item, "category", None) or "Uncategorized"
                else:
                    key = getattr(item, "level", None) or 1
                groups[key].append(item)

            for key in sorted(groups.keys(), key=lambda x: (str(x).lower() if isinstance(x, str) else x)):
                items = groups[key]
                grouped_data.append({
                    "group_value": key,
                    "items": items,
                    "count": len(items),
                })

            return grouped_data, is_section, group_field, header_label

        section_groups = defaultdict(list)
        for item in queryset:
            section = getattr(item, 'section', None)
            section_name = section.section_name if section else "No Section"
            section_groups[section_name].append(item)

        for section_name in sorted(section_groups.keys()):
            items = section_groups[section_name]
            grouped_data.append({
                "group_value": section_name,
                "items": items,
                "count": len(items)
            })

        return grouped_data, is_section, "section", "Section"

    def _get_table_headers(self, fields, is_section, group_field, group_header_label):
        header_map = {
            'level': 'Level',
            'category': 'Category',
            'section_name': 'Section Name',
            'item_name': 'Item Name',
            'weight': 'Weight',
            'runoff_rate': 'Runoff Rate',
            'parent': 'Parent Section',
            'section': 'Section'
        }

        headers = []
        if is_section:
            headers.append(group_header_label)
            for field in fields:
                if field != group_field:
                    headers.append(header_map.get(field, field.replace('_', ' ').title()))
        else:
            headers.append('Section')
            for field in fields:
                if field != 'section':
                    headers.append(header_map.get(field, field.replace('_', ' ').title()))
        return headers

    def get(self, request, *args, **kwargs):
        entity_key = request.GET.get("entity", "hqla_section")
        mode = request.GET.get("mode", "display")

        if entity_key not in self.ENTITY_MAP:
            messages.error(request, "Unknown configuration entity.")
            entity_key = "hqla_section"

        label, model, form_cls, fields = self.ENTITY_MAP[entity_key]
        queryset = self._ordered_qs(model)

        if mode == "edit":
            Formset = modelformset_factory(model, form=form_cls, extra=3, can_delete=True)
            formset = Formset(queryset=queryset, prefix=entity_key)
            return render(request, self.edit_template_name, {
                "entity_key": entity_key,
                "entity_label": label,
                "fields": fields,
                "formset": formset,
                "entities": [(k, v[0]) for k, v in self.ENTITY_MAP.items()],
            })

        grouped_data, is_section, group_field, group_header_label = self._group_data_for_display(queryset, entity_key)
        table_headers = self._get_table_headers(fields, is_section, group_field, group_header_label)

        return render(request, self.template_name, {
            "entity_key": entity_key,
            "entity_label": label,
            "grouped_data": grouped_data,
            "is_section_entity": is_section,
            "table_headers": table_headers,
            "display_fields": fields,
            "entities": [(k, v[0]) for k, v in self.ENTITY_MAP.items()],
            "group_field": group_field,
            "group_header_label": group_header_label,
        })

    def post(self, request, *args, **kwargs):
        entity_key = request.POST.get("entity_key", "hqla_section")
        if entity_key not in self.ENTITY_MAP:
            messages.error(request, "Unknown configuration entity.")
            return redirect(f"{request.path}?entity=hqla_section")

        label, model, form_cls, fields = self.ENTITY_MAP[entity_key]
        Formset = modelformset_factory(model, form=form_cls, extra=3, can_delete=True)
        formset = Formset(request.POST, queryset=self._ordered_qs(model), prefix=entity_key)

        if formset.is_valid():
            formset.save()
            messages.success(request, f"{label}: changes saved.")
            return redirect(f"{request.path}?entity={entity_key}")

        messages.error(request, f"{label}: please correct the errors below.")
        return render(request, self.edit_template_name, {
            "entity_key": entity_key,
            "entity_label": label,
            "fields": fields,
            "formset": formset,
            "entities": [(k, v[0]) for k, v in self.ENTITY_MAP.items()],
        })






class DashboardView(TemplateView):
    template_name = "pages/home.html"

    def _latest_reporting_date(self):
        # union of dates across all upload tables
        dates = set()
        for Model in (HQLATable, CashInflowTable, CashOutflowTable, ASFTable, RSFTable):
            dates |= set(
                Model.objects.exclude(reporting_date__isnull=True)
                     .values_list("reporting_date", flat=True)
            )
        if not dates:
            return None
        return max(dates)

    def _has_data_for_date(self, Model, d):
        return Model.objects.filter(reporting_date=d).exists()

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        today = timezone.localdate()

        latest_date = self._latest_reporting_date()

        # data completeness for latest date
        completeness = None
        if latest_date:
            checks = {
                "HQLA":        self._has_data_for_date(HQLATable, latest_date),
                "Inflows":     self._has_data_for_date(CashInflowTable, latest_date),
                "Outflows":    self._has_data_for_date(CashOutflowTable, latest_date),
                "ASF":         self._has_data_for_date(ASFTable, latest_date),
                "RSF":         self._has_data_for_date(RSFTable, latest_date),
            }
            completeness = {
                "date": latest_date,
                "checks": checks,
                "ok": all(checks.values()),
                "missing": [k for k, v in checks.items() if not v],
            }

        # basic counts (lifetime)
        totals = {
            "hqla":        HQLATable.objects.count(),
            "inflows":     CashInflowTable.objects.count(),
            "outflows":    CashOutflowTable.objects.count(),
            "asf":         ASFTable.objects.count(),
            "rsf":         RSFTable.objects.count(),
        }

        # recent runs and success rate (last 10)
        recent_runs = list(
            LCRRun.objects.order_by("-created_at").values(
                "run_name", "reporting_date", "created_at", "status"
            )[:10]
        )
        if recent_runs:
            s = sum(1 for r in recent_runs if (r["status"] or "").lower() == "success")
            success_rate = round(100 * s / len(recent_runs))
        else:
            success_rate = None

        # last run (for CTA)
        last_run = LCRRun.objects.order_by("-created_at").first()

        ctx.update({
            "title": "Home",
            "today": today,
            "latest_date": latest_date,
            "totals": totals,
            "completeness": completeness,
            "recent_runs": recent_runs,
            "success_rate": success_rate,
            "last_run": last_run,
        })
        return ctx                                      