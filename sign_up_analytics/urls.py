from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'sign_up_analytics'

urlpatterns = [
    url("^$", views.sign_up, name="sign-up")
]