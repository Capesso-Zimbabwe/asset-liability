from collections import OrderedDict
from itertools import groupby
from operator import attrgetter
from django.views.generic import ListView
from django.db.models import Case, When, Value, IntegerField
from datetime import datetime

class CustomOrderGroupedByCurrencyMixin(ListView):
    """
    Optionally filters by ?reporting_date=YYYY-MM-DD,
    then orders by a custom currency preference,
    and groups records by currency into context['grouped_data'].
    """
    preferred    = ['USD', 'ZWG']
    group_key    = 'currency'
    date_param   = 'reporting_date'  # GET-param name

    def get_queryset(self):
        qs = super().get_queryset()

        # 1) optional date filter
        date_str = self.request.GET.get(self.date_param)
        if date_str:
            try:
                date = datetime.fromisoformat(date_str).date()
                qs = qs.filter(reporting_date=date)
            except ValueError:
                # invalid date: ignore filter or handle as you prefer
                pass

        # 2) order by currency so groupby will see contiguous blocks
        qs = qs.order_by(self.group_key)

        # 3) custom currency ordering
        whens = [
            When(currency=cur, then=Value(idx))
            for idx, cur in enumerate(self.preferred)
        ]
        qs = qs.annotate(
            currency_order=Case(
                *whens,
                default=Value(len(self.preferred)),
                output_field=IntegerField()
            )
        ).order_by('currency_order', 'currency')

        return qs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()

        # build grouped_data
        grouped = OrderedDict(
            (currency, list(recs))
            for currency, recs in groupby(qs, key=attrgetter(self.group_key))
        )
        context['grouped_data'] = [
            {'currency': currency, 'records': recs}
            for currency, recs in grouped.items()
        ]

        # echo the chosen date into context so your template can re-select it
        context[self.date_param] = self.request.GET.get(self.date_param, '')
        return context
