from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

app_name = 'staging'

router = DefaultRouter()

urlpatterns = [
    # API URLs
    path('api/', include(router.urls)),
    
    # Product Master URLs
    path('master/add/', views.add_product_master, name='add_master'),
    path('master/edit/<int:pk>/', views.edit_product_master, name='edit_master'),
    path('master/delete/<int:pk>/', views.delete_product_master, name='delete_master'),
    path('master/view/', views.view_product_master, name='view_master'),
    path('master/', views.load_master_view, name='load_master'),
    path('master/upload/', views.upload_master_file, name='upload_master'),
    path('master/columns/<str:file_id>/', views.get_master_columns, name='get_master_columns'),
    path('master/preview/', views.preview_master_data, name='preview_master'),
    path('master/import/', views.import_master_data, name='import_master'),

    # Common COA Master URLs
    path('coa/add/', views.add_coa, name='add_coa'),
    path('coa/edit/<int:pk>/', views.edit_coa, name='edit_coa'),
    path('coa/delete/<int:pk>/', views.delete_coa, name='delete_coa'),
    path('coa/view/', views.view_coa, name='view_coa'),
    path('coa/', views.load_coa_view, name='load_coa'),
    path('coa/upload/', views.upload_coa_file, name='upload_coa'),
    path('coa/columns/<str:file_id>/', views.get_coa_columns, name='get_coa_columns'),
    path('coa/preview/', views.preview_coa_data, name='preview_coa'),
    path('coa/import/', views.import_coa_data, name='import_coa'),

    # Loan Contract Upload URLs
    path('loans/', views.load_loans_view, name='load_loans'),
    path('loans/upload/', views.upload_loan_file, name='upload_loan'),
    path('loans/columns/<str:file_id>/', views.get_loan_columns, name='get_loan_columns'),
    path('loans/preview/', views.preview_loan_data, name='preview_loan'),
    path('loans/import/', views.import_loan_data, name='import_loan'),

    # Overdraft Contract Upload URLs
    path('overdraft/', views.load_overdraft_view, name='load_overdraft'),
    path('overdraft/upload/', views.upload_overdraft_file, name='upload_overdraft'),
    path('overdraft/columns/<str:file_id>/', views.get_overdraft_columns, name='get_overdraft_columns'),
    path('overdraft/preview/', views.preview_overdraft_data, name='preview_overdraft'),
    path('overdraft/import/', views.import_overdraft_data, name='import_overdraft'),

    # Loan Payment Schedule Upload URLs
    path('schedule/', views.load_schedule_view, name='load_schedule'),
    path('schedule/upload/', views.upload_schedule_file, name='upload_schedule'),
    path('schedule/columns/<str:file_id>/', views.get_schedule_columns, name='get_schedule_columns'),
    path('schedule/preview/', views.preview_schedule_data, name='preview_schedule'),
    path('schedule/import/', views.import_schedule_data, name='import_schedule'),

    # Investment Upload URLs
    path('investment/', views.load_investment_view, name='load_investment'),
    path('investment/upload/', views.upload_investment_file, name='upload_investment'),
    path('investment/columns/<str:file_id>/', views.get_investment_columns, name='get_investment_columns'),
    path('investment/preview/', views.preview_investment_data, name='preview_investment'),
    path('investment/import/', views.import_investment_data, name='import_investment'),

    # First Day Product Upload URLs
    path('firstday/', views.load_firstday_view, name='load_firstday'),
    path('firstday/upload/', views.upload_firstday_file, name='upload_firstday'),
    path('firstday/columns/<str:file_id>/', views.get_firstday_columns, name='get_firstday_columns'),
    path('firstday/preview/', views.preview_firstday_data, name='preview_firstday'),
    path('firstday/import/', views.import_firstday_data, name='import_firstday'),

    # Credit Line Upload URLs
    path('creditline/', views.load_creditline_view, name='load_creditline'),
    path('creditline/upload/', views.upload_creditline_file, name='upload_creditline'),
    path('creditline/columns/<str:file_id>/', views.get_creditline_columns, name='get_creditline_columns'),
    path('creditline/preview/', views.preview_creditline_data, name='preview_creditline'),
    path('creditline/import/', views.import_creditline_data, name='import_creditline'),

    # Loan Contract Upload URLs
    path('load_product/', views.load_product_view, name='load_product'),
    path('load_product/upload/', views.upload_product_file, name='upload_product'),
    path('load_product/columns/<str:file_id>/', views.get_product_columns, name='get_product_columns'),
    path('load_product/preview/', views.preview_product_data, name='preview_product'),
    path('load_product/import/', views.import_product_data, name='import_product'),
]