from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect

# Create your views here.


def connections(request):
    return render(request, 'connections_analytics/connections.html')

