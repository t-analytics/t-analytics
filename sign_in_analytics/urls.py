from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'sign_in_analytics'

urlpatterns = [
    url("^$", views.sign_in, name="sign-in")
]
