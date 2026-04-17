from django.conf import settings


def asset_context(request):
    return {
        "asset_version": getattr(settings, "STATIC_VERSION", "1"),
    }
