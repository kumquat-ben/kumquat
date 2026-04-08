from django.urls import path

from .views import early_access_signup_view, healthz_view, index_view, messages_view

urlpatterns = [
    path("", index_view, name="api-index"),
    path("early-access", early_access_signup_view, name="api-early-access"),
    path("healthz", healthz_view, name="api-healthz"),
    path("messages", messages_view, name="api-messages"),
]
