from django.urls import path
from django.conf.urls import url

from . import views

app_name = 'connections_analytics'
urlpatterns = [
    url(r"^$", views.connections, name="connections")
]
