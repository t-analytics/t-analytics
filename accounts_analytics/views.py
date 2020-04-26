from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect

# Create your views here.


def accounts(request):
    return render(request, 'accounts_analytics/accounts.html')
