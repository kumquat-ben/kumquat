# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.urls import path, re_path
from django.views.generic import RedirectView

from .views import (
    admin_dashboard_view,
    admin_dashboard_page_view,
    admin_node_launch_view,
    admin_node_logs_view,
    admin_node_proxy_view,
    admin_node_stop_view,
    admin_vonage_sms_view,
    admin_vonage_sms_page_view,
    auth_logout_view,
    early_access_signup_view,
    google_oauth_exchange_view,
    google_oauth_callback_view,
    google_oauth_start_view,
    healthz_view,
    home_page_view,
    messages_view,
    sign_in_page_view,
    vonage_sms_callback_view,
)

urlpatterns = [
    path("", home_page_view, name="home"),
    path("auth/sign-in", sign_in_page_view, name="sign-in"),
    path("auth/google/start", google_oauth_start_view, name="auth-google-start"),
    path("auth/google/callback", google_oauth_callback_view, name="auth-google-callback"),
    path("auth/google/exchange", google_oauth_exchange_view, name="auth-google-exchange"),
    path("auth/logout", auth_logout_view, name="auth-logout"),
    path("dashboard", admin_dashboard_page_view, name="dashboard"),
    path("dashboard/data", admin_dashboard_view, name="dashboard-data"),
    path("nodes/launch", admin_node_launch_view, name="node-launch"),
    path("nodes/<int:node_id>/logs", admin_node_logs_view, name="node-logs"),
    path("nodes/<int:node_id>/stop", admin_node_stop_view, name="node-stop"),
    re_path(r"^nodes/(?P<node_id>\d+)/proxy(?:/(?P<subpath>.*))?$", admin_node_proxy_view, name="node-proxy"),
    path("sms", admin_vonage_sms_page_view, name="sms"),
    path("sms/data", admin_vonage_sms_view, name="sms-data"),
    path("early-access", early_access_signup_view, name="early-access"),
    path("healthz", healthz_view, name="healthz"),
    path("messages", messages_view, name="messages"),
    path("webhooks/vonage/sms", vonage_sms_callback_view, name="vonage-sms-callback"),
    path("admin/dashboard", RedirectView.as_view(url="/dashboard", permanent=False)),
    path("admin/vonage/sms", RedirectView.as_view(url="/sms", permanent=False)),
    path("api/auth/logout", RedirectView.as_view(url="/auth/logout", permanent=False)),
    path("api/admin/dashboard", RedirectView.as_view(url="/dashboard/data", permanent=False)),
    path("api/admin/vonage/sms", RedirectView.as_view(url="/sms/data", permanent=False)),
    path("api/admin/nodes/launch", RedirectView.as_view(url="/nodes/launch", permanent=False)),
]
