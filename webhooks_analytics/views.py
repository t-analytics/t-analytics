from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
import requests
# Create your views here.


def webhook_vkontakte(request, client_id):
    https_proxy1 = "https://54.164.133.248:3128"
    https_proxy2 = "https://163.172.219.130:443"
    https_proxy3 = "https://151.253.165.70:8080"

    access_token = "1074300704:AAGCu4LpIOk0Wq2Z9gzkUTiSjbt_wT0MxeQ"

    proxyDict = {
        "https": https_proxy1,
        "https": https_proxy2,
        "https": https_proxy3
    }
    if request.method == 'GET':
        if request.GET.get("type") == 'confirmation':
            return HttpResponse('10a829f5')
        else:
            return HttpResponse('10a829f5')
    elif request.method == 'POST':
        if request.POST.get("type") == 'confirmation':
            return HttpResponse('10a829f5')
        else:
            return HttpResponse('10a829f5')
    else:
        return HttpResponse('10a829f5')
