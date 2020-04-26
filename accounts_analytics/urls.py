from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'accounts_analytics'
urlpatterns = [
    url(r"^$", views.accounts, name="accounts")
]