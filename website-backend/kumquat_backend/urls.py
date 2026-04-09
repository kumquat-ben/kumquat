# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.urls import include, path

from api.views import admin_dashboard_page_view, admin_vonage_sms_page_view

urlpatterns = [
    path("admin/dashboard", admin_dashboard_page_view),
    path("admin/vonage/sms", admin_vonage_sms_page_view),
    path("api/", include("api.urls")),
]
