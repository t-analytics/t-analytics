from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
import requests
# Create your views here.


def oauth_vkontakte_code(request, client_id):
    return redirect('oauth_vkontakte_token', client_id="hello")


def oauth_vkontakte_token(request, client_id):
    return HttpResponse(client_id)
