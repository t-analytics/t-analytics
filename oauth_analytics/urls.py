from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'oauth_analytics'

urlpatterns = [
    path("vkontakte/<int:client_id>/", views.oauth_vkontakte_code, name="oauth_vkontakte_code"),
    path("vkontakte/bla", views.oauth_vkontakte_token, name="oauth_vkontakte_token")
]