from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from .forms import SignInForm


def sign_in(request):
    return render(request, 'sign_analytics/sign-in.html', context={"SignInForm": SignInForm})


