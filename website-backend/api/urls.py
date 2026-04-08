from django.urls import path

from .views import (
    auth_logout_view,
    auth_me_view,
    early_access_signup_view,
    google_oauth_callback_view,
    google_oauth_exchange_view,
    google_oauth_start_view,
    healthz_view,
    index_view,
    messages_view,
)

urlpatterns = [
    path("", index_view, name="api-index"),
    path("auth/google/start", google_oauth_start_view, name="api-auth-google-start"),
    path("auth/google/callback", google_oauth_callback_view, name="api-auth-google-callback"),
    path("auth/google/exchange", google_oauth_exchange_view, name="api-auth-google-exchange"),
    path("auth/logout", auth_logout_view, name="api-auth-logout"),
    path("auth/me", auth_me_view, name="api-auth-me"),
    path("early-access", early_access_signup_view, name="api-early-access"),
    path("healthz", healthz_view, name="api-healthz"),
    path("messages", messages_view, name="api-messages"),
]
