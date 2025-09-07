from django.urls import path
from . import views

app_name = 'fsi'

urlpatterns = [
    # Loan Contracts
    path('loan-contracts/', views.loan_contract_list, name='loan-contracts'),
    
    # CASA
    path('casa/', views.casa_list, name='casa'),
    
    # Investments
    path('investments/', views.investment_list, name='investments'),
    
    # Guarantees
    path('guarantees/', views.guarantee_list, name='guarantees'),
    
    # Borrowings
    path('borrowings/', views.borrowing_list, name='borrowings'),
    
    # Cards
    path('cards/', views.card_list, name='cards'),
    
    # Overdrafts
    path('overdrafts/', views.overdraft_list, name='overdrafts'),
]