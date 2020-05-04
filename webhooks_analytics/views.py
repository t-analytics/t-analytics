from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
# Create your views here.


def webhook_vkontakte(request, client_id):
    message = "Not get param"
    if request.method == 'GET':
        message = request.GET.get("hello")
    return HttpResponse(f"{message} - {client_id}")
