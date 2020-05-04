from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'webhooks_analytics'

urlpatterns = [
    path("vkontakte/<int:client_id>/", views.webhook_vkontakte, name="webhook_vkontakte")
]