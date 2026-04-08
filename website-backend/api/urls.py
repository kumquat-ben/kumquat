from django.urls import path

from .views import healthz_view, index_view, messages_view

urlpatterns = [
    path("", index_view, name="api-index"),
    path("healthz", healthz_view, name="api-healthz"),
    path("messages", messages_view, name="api-messages"),
]
