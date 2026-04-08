import json
from datetime import datetime, timezone

from django.db import connection
from django.http import HttpResponseNotAllowed, JsonResponse
from django.views.decorators.csrf import csrf_exempt


def _database_state():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return "ok"
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return f"error: {exc}"


def index_view(_request):
    return JsonResponse(
        {
            "service": "kumquat-website-backend",
            "status": "ok",
            "endpoints": ["/api/healthz", "/api/messages"],
            "database": _database_state(),
        }
    )


def healthz_view(_request):
    database_state = _database_state()
    http_status = 200 if database_state == "ok" else 503
    return JsonResponse(
        {
            "status": "ok" if http_status == 200 else "degraded",
            "database": database_state,
        },
        status=http_status,
    )


@csrf_exempt
def messages_view(request):
    if request.method == "GET":
        return JsonResponse(
            {
                "message": "Send a POST with JSON to have the backend echo it.",
                "database": _database_state(),
            }
        )

    if request.method != "POST":
        return HttpResponseNotAllowed(["GET", "POST"])

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    return JsonResponse(
        {
            "received": payload,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "database": _database_state(),
        },
        status=201,
    )
