from django.urls import path


from .functions_view.monitor import monitor_view, stop_execution, continue_execution, get_execution_status

from .functions_view.home import HomeView

from .functions_view.report_rate import get_currencies_api_rate_sensitive, rate_sensitive_gap_report_api_cons, rate_sensitive_gap_report_cons, rate_sensitive_gap_report_form

from .functions_view.report_behavioral import behavioural_gap_report_api_cons, behavioural_gap_report_cons, behavioural_gap_report_form, get_currencies_api_behavioural

from .functions_view.report_cons import Contractual_Cons_gap_report_api_cons, Contractual_Cons_gap_report_cons, Contractual_Cons_gap_report_form, get_currencies_api_Contractual_Cons
from .views import CustomLoginView, LogoutView,  project_cash_flows_view
from .functions_view.report_base import contractual_gap_report, contractual_gap_report_api, contractual_gap_report_form, get_currencies_api
from .functions_view.execute import execute_view, execution_history, execution_status_api
from .functions_view.adjustments_repo import (
    adjustments_form, 
    adjustments_manage, 
    add_adjustment, 
    remove_adjustment, 
    get_product_type_details,
    get_product_names_for_type
)
from .functions_view.time_buckets import (
    time_bucket_list,
    time_bucket_manage,
    time_bucket_delete
)

from .functions_view.behavoural import (
    BehavioralPatternListView,
    BehavioralPatternCreateView,
    BehavioralPatternUpdateView,
    BehavioralPatternDeleteView,
    BehavioralPatternSplitListView,
    BehavioralPatternSplitCreateView,
    BehavioralPatternSplitUpdateView,
    BehavioralPatternSplitDeleteView,
    pattern_manage,
    pattern_delete_api
)

from .functions_view.dashboard import dashboard


app_name = 'alm_app'


urlpatterns = [
    path('login/', CustomLoginView.as_view(), name='login'),
    path('logout/', LogoutView.as_view(), name='logout'),

    path('home/', HomeView.as_view(), name='home'),

    # Dashboard URL
    path('dashboard/', dashboard, name='dashboard'),
    # Execute URLs
    path('execute/', execute_view, name='execute_view'),
    path('execute/status/', execution_status_api, name='execution_status_api'),
    path('execute/history/', execution_history, name='execution_history'),
    
    # Monitor URLs
    path('monitor/', monitor_view, name='monitor_view'), 
    path('monitor/status/', get_execution_status, name='get_execution_status'),
    path('monitor/stop/', stop_execution, name='stop_execution'),
    path('monitor/continue/', continue_execution, name='continue_execution'),

    # Time Buckets URLs
    path('time-buckets/', time_bucket_list, name='time_bucket_list'),
    path('time-buckets/manage/', time_bucket_manage, name='time_bucket_manage'),
    path('time-buckets/delete/', time_bucket_delete, name='time_bucket_delete'),

    
    path('project_cash_flows/', project_cash_flows_view, name='project_cash_flows'),

    # Contractual Gap BaseReport URLs
    path('reports/contractual-gap/', 
         contractual_gap_report_form, name='contractual_gap_report_form'),
    path('reports/contractual-gap/<str:fic_mis_date>/<str:process_name>/', 
         contractual_gap_report, name='contractual_gap_report'),
    path('reports/contractual-gap/<str:fic_mis_date>/<str:process_name>/<str:currency>/', 
         contractual_gap_report, name='contractual_gap_report_currency'),
    path('api/reports/contractual-gap/<str:fic_mis_date>/<str:process_name>/', 
         contractual_gap_report_api, name='contractual_gap_report_api'),
    path('api/get-currencies/<str:fic_mis_date>/<str:process_name>/', 
         get_currencies_api, name='get_currencies_api'),



     
    
    # Adjustments URLs
    path('adjustments/', 
         adjustments_form, name='adjustments_form'),
    path('adjustments/<str:fic_mis_date>/<str:process_name>/', 
         adjustments_manage, name='adjustments_manage'),
    path('adjustments/add/', 
         add_adjustment, name='add_adjustment'),
    path('adjustments/remove/', 
         remove_adjustment, name='remove_adjustment'),
    path('api/adjustments/<str:fic_mis_date>/<str:process_name>/details/', 
         get_product_type_details, name='get_product_type_details'),
    path('api/adjustments/<str:fic_mis_date>/<str:process_name>/product-names/', 
         get_product_names_for_type, name='get_product_names_for_type'),


     


          # behavioural Gap Cons Report URLs
    path('reports/behavioural-gap', 
         behavioural_gap_report_form, name='behavioural_gap_report_form_cons'),
    path('reports/behavioural-gap/cons/<str:fic_mis_date>/<str:process_name>/', 
         behavioural_gap_report_cons, name='behavioural_gap_report_cons'),
    path('reports/behavioural-gap/cons/<str:fic_mis_date>/<str:process_name>/<str:currency>/', 
         behavioural_gap_report_cons, name='behavioural_gap_report_currency_cons'),
    path('api/reports/behavioural-gap/cons/<str:fic_mis_date>/<str:process_name>/', 
         behavioural_gap_report_api_cons, name='behavioural_gap_report_api_cons'),
    path('api/get-currencies/behavioural/<str:fic_mis_date>/<str:process_name>/', 
         get_currencies_api_behavioural, name='get_currencies_api_behavioural'),


     # behavioural Gap Cons Report URLs
    path('reports/rate_sensitive-gap', 
         rate_sensitive_gap_report_form, name='rate_sensitive_gap_report_form_cons'),
    path('reports/rate_sensitive-gap/cons/<str:fic_mis_date>/<str:process_name>/', 
         rate_sensitive_gap_report_cons, name='rate_sensitive_gap_report_cons'),
    path('reports/rate_sensitive-gap/cons/<str:fic_mis_date>/<str:process_name>/<str:currency>/', 
         rate_sensitive_gap_report_cons, name='rate_sensitive_report_currency_cons'),
    path('api/reports/rate_sensitive-gap/cons/<str:fic_mis_date>/<str:process_name>/', 
         rate_sensitive_gap_report_api_cons, name='rate_sensitive_gap_report_api_cons'),
    path('api/get-currencies/rate_sensitive/<str:fic_mis_date>/<str:process_name>/', 
         get_currencies_api_rate_sensitive, name='get_currencies_api_rate_sensitive'),


     # Consolidated Gap Cons Report URLs
    path('reports/Contractual_Cons-gap', 
         Contractual_Cons_gap_report_form, name='Contractual_Cons_gap_report_form_cons'),
    path('reports/Contractual_Cons/cons/<str:fic_mis_date>/<str:process_name>/', 
         Contractual_Cons_gap_report_cons, name='Contractual_Cons_gap_report_cons'),
    path('reports/Contractual_Cons-gap/cons/<str:fic_mis_date>/<str:process_name>/<str:currency>/', 
         Contractual_Cons_gap_report_cons, name='Contractual_Cons_report_currency_cons'),
    path('api/reports/Contractual_Cons-gap/cons/<str:fic_mis_date>/<str:process_name>/', 
         Contractual_Cons_gap_report_api_cons, name='Contractual_Cons_gap_report_api_cons'),
    path('api/get-currencies/rate_Contractual_Cons/<str:fic_mis_date>/<str:process_name>/', 
         get_currencies_api_Contractual_Cons, name='get_currencies_api_Contractual_Cons'),

    # Behavioral Pattern URLs
    path('behavioral/patterns/', BehavioralPatternListView.as_view(), name='pattern-list'),
    path('behavioral/patterns/create/', BehavioralPatternCreateView.as_view(), name='pattern-create'),
    path('behavioral/patterns/<int:pk>/update/', BehavioralPatternUpdateView.as_view(), name='pattern-update'),
    path('behavioral/patterns/<int:pk>/delete/', BehavioralPatternDeleteView.as_view(), name='pattern-delete'),
    path('api/behavioral/patterns/<int:pk>/delete/', pattern_delete_api, name='pattern-delete-api'),
    
    # Behavioral Pattern Split URLs
    path('behavioral/patterns/<int:pattern_id>/splits/', BehavioralPatternSplitListView.as_view(), name='split-list'),
    path('behavioral/patterns/<int:pattern_id>/splits/create/', BehavioralPatternSplitCreateView.as_view(), name='split-create'),
    path('behavioral/splits/<int:pk>/update/', BehavioralPatternSplitUpdateView.as_view(), name='split-update'),
    path('behavioral/splits/<int:pk>/delete/', BehavioralPatternSplitDeleteView.as_view(), name='split-delete'),
    path('behavioural/manage/', pattern_manage, name='pattern-manage-create'),
    path('behavioural/manage/<int:pk>/', pattern_manage, name='pattern-manage-edit'),
]
