from django.conf import settings
from django.conf.urls.static import static
from django.urls import path
from .views import (Index, LoginView, HQLAUploadView, CashInflowUploadView, CashOutflowUploadView, HqlaByCurrencyView, CashInflowByCurrencyView, CashOutflowByCurrencyView, ApplyAllHaircutsView,
GenerateCurrencyAdjustmentSummaryView, LCRByCurrencyView, LCRRecordListView, RunLCRView, download_log, PreviousRunsView, AvailableStableUploadView, RequiredStableUploadView, NSFRByCurrencyView, NSFRRecordListView, ConfigsView,
DashboardView,
)

app_name = 'LCR'


urlpatterns = [
    
    path('lcr/home', DashboardView.as_view(), name='home_lcr'),
    path('upload/hqla/', HQLAUploadView.as_view(), name='upload_hqla'),
    path('upload/cash-inflow/', CashInflowUploadView.as_view(), name='upload_cash_inflow'),
    path('upload/cash-outflow/', CashOutflowUploadView.as_view(), name='upload_cash_outflow'),
    path('upload/asf', AvailableStableUploadView.as_view(), name='upload_asf'),
    path('upload/rsf', RequiredStableUploadView.as_view(), name='upload_rsf'),
    path('grouped/hqla/', HqlaByCurrencyView.as_view(), name=('grouped-hqla') ),
    path('grouped/cash_outflows/', CashOutflowByCurrencyView.as_view(), name=('grouped-cash_outflow') ),
    path('grouped/cash_inflows/', CashInflowByCurrencyView.as_view(), name=('grouped-cash_inflow') ),
    path('apply-weights/', ApplyAllHaircutsView.as_view(), name='apply_weights'),
    path('summaries/generate/', GenerateCurrencyAdjustmentSummaryView.as_view(), name='generate_summary'),
    path('lcr/', LCRByCurrencyView.as_view(), name='lcr_by_currency'),
    path('lcr_records/', LCRRecordListView.as_view(), name="lcr_records"),
    path('lcr_run/', RunLCRView.as_view(), name="lcr_run"),
    path('lcr_run/logs/<str:filename>/', download_log, name='download_log'),
    path('lcr_run/previous/', PreviousRunsView.as_view(),   name='previous_runs'),
    path('nsfr/', NSFRByCurrencyView.as_view(), name='nsfr_by_currency'),
    path('nsfr_records/', NSFRRecordListView.as_view(), name='nsfr_records'),
    path('configs/', ConfigsView.as_view(), name='configs'),

]



if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)