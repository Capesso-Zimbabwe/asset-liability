from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView
from django.views.generic import TemplateView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required

from .functions.cashflow_overdrafts  import cashflow_overdrafts

from .functions.repo_alignment import align_buckets_to_balance

from .functions.cashflows_loans import balance_cashflows_to_target, cashflow_loan_contracts, cashflow_loans_future

from .functions.cashflow_credit import cashflow_credit_line

from .functions.cashflow_first_day import cashflow_first_day
from .functions.cashflow_investments import cashflow_investments

from .functions.report_rate_sensitive_loader import load_report_rate_sensitive

from .functions.report_contractual_cons_loader import load_report_contractual_cons

from .functions.report_behavioural_loader import load_report_behavioural

from .functions.bucket_column_sync import sync_bucket_columns
from .functions.report_loader import create_report_contractual_table, report_contractual_load
from .functions.cashflow_prod_aggr import aggregate_by_prod_code
from .functions.cashflow_acc_aggr import calculate_time_buckets_and_spread
from .functions.cashflow_arrange import aggregate_cashflows_to_product_level
from .functions.cashflow_gen import project_cash_flows
from django.views import View
from django.contrib.auth import logout

# Create your views here.
class CustomLoginView(LoginView):
    template_name = 'login.html'
    redirect_authenticated_user = True

    def get_success_url(self):
        return reverse_lazy('alm_app:home')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = 'Login'
        return context
    
class LogoutView(View):
    def get(self, request):
        # End the user session
        logout(request)
        # Redirect to login page (or homepage)
        return redirect('alm_app:login')  # change 'login' to your URL name



@login_required
def project_cash_flows_view(request):
    process_name = 'contractual'
    # fic_mis_date = '2024-08-31'
    fic_mis_date = '2025-04-30'

     
    # status= project_cash_flows(fic_mis_date)
    # status= aggregate_cashflows_to_product_level(fic_mis_date)
    # status= cashflow_first_day(fic_mis_date) 
    # status=cashflow_credit_line(fic_mis_date)
   
    # status=cashflow_loan_contracts(fic_mis_date)
   
    # status=balance_cashflows_to_target(fic_mis_date)
    # status=cashflow_overdrafts(fic_mis_date)  
    # status=cashflow_investments(fic_mis_date)
    status= calculate_time_buckets_and_spread(fic_mis_date) 





    # status= aggregate_by_prod_code(fic_mis_date)
  
   
    # status=create_report_contractual_table(fic_mis_date)

    # status= report_contractual_load(fic_mis_date)'

    # status=align_buckets_to_balance(fic_mis_date)
    # status=load_report_contractual_cons(fic_mis_date)

    # status= load_report_behavioural(fic_mis_date)



    # status = load_report_rate_sensitive(fic_mis_date)






    print(status)
    # project_cash_flows(fic_mis_date)
    return render(request, 'project_cash_flows.html')
