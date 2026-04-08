import json
from datetime import datetime, timezone

from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.db import connection
from django.http import HttpResponse, HttpResponseNotAllowed, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .models import EarlyAccessSignup


def _database_state():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return "ok"
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return f"error: {exc}"


def index_view(_request):
    database_state = _database_state()
    return HttpResponse(
        f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Kumquat API</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #fbf6ee;
        --fg: #1f1a17;
        --muted: #6a5f58;
        --card: #fffaf2;
        --border: #eadfce;
        --accent: #db7c26;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #fff8ef 0%, var(--bg) 100%);
        color: var(--fg);
      }}
      main {{
        max-width: 760px;
        margin: 0 auto;
        padding: 48px 24px 72px;
      }}
      .eyebrow {{
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
      }}
      h1 {{
        margin: 12px 0 16px;
        font-size: clamp(40px, 8vw, 72px);
        line-height: 0.94;
      }}
      p {{
        font-size: 18px;
        line-height: 1.6;
        color: var(--muted);
      }}
      .card {{
        margin-top: 32px;
        padding: 24px;
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 18px;
      }}
      code {{
        font-family: "SFMono-Regular", Menlo, monospace;
        font-size: 14px;
      }}
      ul {{
        padding-left: 20px;
        color: var(--muted);
      }}
      a {{
        color: var(--accent);
        text-decoration: none;
      }}
    </style>
  </head>
  <body>
    <main>
      <div class="eyebrow">Kumquat API</div>
      <h1>API documentation is coming.</h1>
      <p>
        This path is reserved for the Kumquat backend. It should not mirror the
        marketing homepage, and it now serves a backend-owned landing page.
      </p>
      <div class="card">
        <p><strong>Current endpoints</strong></p>
        <ul>
          <li><code>/api/early-access</code> - early access signup intake</li>
          <li><code>/api/healthz</code> - backend health and database status</li>
          <li><code>/api/messages</code> - message echo test endpoint</li>
        </ul>
        <p><strong>Database</strong>: <code>{database_state}</code></p>
      </div>
    </main>
  </body>
</html>
""",
        content_type="text/html; charset=utf-8",
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
def early_access_signup_view(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    email = (payload.get("email") or "").strip().lower()
    name = (payload.get("name") or "").strip()

    if not email:
        return JsonResponse({"error": "Email is required."}, status=400)

    try:
        validate_email(email)
    except ValidationError:
        return JsonResponse({"error": "Enter a valid email address."}, status=400)

    signup, created = EarlyAccessSignup.objects.update_or_create(
        email=email,
        defaults={"name": name},
    )

    return JsonResponse(
        {
            "status": "created" if created else "updated",
            "signup": {
                "email": signup.email,
                "name": signup.name,
                "created_at": signup.created_at.isoformat(),
            },
        },
        status=201 if created else 200,
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
