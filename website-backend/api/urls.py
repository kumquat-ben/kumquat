# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.urls import path, re_path

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
    auth_me_view,
    early_access_signup_view,
    google_oauth_callback_view,
    google_oauth_exchange_view,
    google_oauth_start_view,
    healthz_view,
    index_view,
    messages_view,
    vonage_sms_callback_view,
)

urlpatterns = [
    path("", index_view, name="api-index"),
    path("admin/dashboard-page", admin_dashboard_page_view, name="api-admin-dashboard-page"),
    path("admin/dashboard", admin_dashboard_view, name="api-admin-dashboard"),
    path("admin/nodes/launch", admin_node_launch_view, name="api-admin-node-launch"),
    path("admin/nodes/<int:node_id>/logs", admin_node_logs_view, name="api-admin-node-logs"),
    path("admin/nodes/<int:node_id>/stop", admin_node_stop_view, name="api-admin-node-stop"),
    re_path(r"^admin/nodes/(?P<node_id>\d+)/proxy(?:/(?P<subpath>.*))?$", admin_node_proxy_view, name="api-admin-node-proxy"),
    path("admin/vonage/sms-page", admin_vonage_sms_page_view, name="api-admin-vonage-sms-page"),
    path("admin/vonage/sms", admin_vonage_sms_view, name="api-admin-vonage-sms"),
    path("auth/google/start", google_oauth_start_view, name="api-auth-google-start"),
    path("auth/google/callback", google_oauth_callback_view, name="api-auth-google-callback"),
    path("auth/google/exchange", google_oauth_exchange_view, name="api-auth-google-exchange"),
    path("auth/logout", auth_logout_view, name="api-auth-logout"),
    path("auth/me", auth_me_view, name="api-auth-me"),
    path("early-access", early_access_signup_view, name="api-early-access"),
    path("healthz", healthz_view, name="api-healthz"),
    path("messages", messages_view, name="api-messages"),
    path("vonage/sms/callback", vonage_sms_callback_view, name="api-vonage-sms-callback"),
]
