from django.shortcuts import render
from django.contrib.auth.decorators import login_required

@login_required
def loan_contract_list(request):
    return render(request, 'fsi/loan_contract_list.html')

@login_required
def casa_list(request):
    return render(request, 'fsi/casa_list.html')

@login_required
def investment_list(request):
    return render(request, 'fsi/investment_list.html')

@login_required
def guarantee_list(request):
    return render(request, 'fsi/guarantee_list.html')

@login_required
def borrowing_list(request):
    return render(request, 'fsi/borrowing_list.html')

@login_required
def card_list(request):
    return render(request, 'fsi/card_list.html')

@login_required
def overdraft_list(request):
    return render(request, 'fsi/overdraft_list.html')