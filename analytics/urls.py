"""analytics URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls.static import static
from django.urls import path, include
from django.conf import settings

urlpatterns = [
    path('admin/', admin.site.urls),
    url(r'^sign-in/', include('sign_in_analytics.urls', namespace="sign_in_analytics")),
    url(r'^sign-up/', include('sign_up_analytics.urls', namespace="sign_up_analytics")),
    url(r'^connections/', include('connections_analytics.urls', namespace="connections_analytics")),
    url(r'^accounts/', include('accounts_analytics.urls', namespace="accounts_analytics")),
    url(r'^webhooks/', include('webhooks_analytics.urls', namespace="webhooks_analytics")),
    url(r'^oauth/', include('oauth_analytics.urls', namespace="oauth_analytics"))
]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
