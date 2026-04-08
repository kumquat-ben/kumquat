# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.urls import include, path

urlpatterns = [
    path("api/", include("api.urls")),
]
