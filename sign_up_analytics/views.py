from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from .forms import SignUpForm
# Create your views here.


def sign_up(request):
    return render(request, 'sign_analytics/sign-up.html', context={"SignUpForm": SignUpForm})
