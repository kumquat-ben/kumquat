# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import json
import secrets
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from django.conf import settings
from django.contrib.auth import get_user_model, login, logout
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.db import connection
from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseRedirect, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .models import EarlyAccessSignup

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
GOOGLE_OAUTH_STATE_SESSION_KEY = "google_oauth_state"
GOOGLE_OAUTH_REDIRECT_URI_SESSION_KEY = "google_oauth_redirect_uri"


def _database_state():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return "ok"
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return f"error: {exc}"


def _serialize_user(user):
    full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    return {
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "full_name": full_name or user.username,
        "username": user.username,
        "is_staff": user.is_staff,
        "is_superuser": user.is_superuser,
        "is_active": user.is_active,
        "date_joined": user.date_joined.isoformat() if getattr(user, "date_joined", None) else None,
        "last_login": user.last_login.isoformat() if user.last_login else None,
    }


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


def _oauth_is_configured():
    return bool(
        settings.GOOGLE_OAUTH_CLIENT_ID
        and settings.GOOGLE_OAUTH_CLIENT_SECRET
        and settings.GOOGLE_OAUTH_REDIRECT_URI
    )


def _google_json_request(url, *, method="GET", data=None, headers=None):
    request_headers = {
        "Accept": "application/json",
        **(headers or {}),
    }
    payload = None
    if data is not None:
        payload = urlencode(data).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

    request = Request(url, data=payload, headers=request_headers, method=method)

    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Google OAuth HTTP {exc.code}: {error_body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Google OAuth network error: {exc.reason}") from exc


def _google_redirect_uri_for_request(request):
    configured_redirect_uri = (settings.GOOGLE_OAUTH_REDIRECT_URI or "").strip()
    if configured_redirect_uri:
        return configured_redirect_uri

    for header_name in ("HTTP_ORIGIN", "HTTP_REFERER"):
        value = (request.META.get(header_name) or "").strip()
        if not value:
            continue

        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue

        return f"{parsed.scheme}://{parsed.netloc}/auth/google/callback"

    return "http://localhost:5173/auth/google/callback"


def _build_username(email, google_sub):
    User = get_user_model()
    base_username = (email or "").split("@", 1)[0].strip() or f"google-{google_sub}"
    normalized = "".join(character if character.isalnum() else "-" for character in base_username.lower()).strip("-")
    candidate = normalized[:120] or f"google-{google_sub}"
    suffix = 1

    while User.objects.filter(username=candidate).exists():
        candidate = f"{normalized[:100] or 'google-user'}-{suffix}"
        suffix += 1

    return candidate


def _upsert_google_user(profile):
    User = get_user_model()
    google_sub = profile.get("sub")
    email = (profile.get("email") or "").strip().lower()
    full_name = (profile.get("name") or "").strip()
    given_name = (profile.get("given_name") or "").strip()
    family_name = (profile.get("family_name") or "").strip()

    if not google_sub or not email:
        raise ValueError("Google account response did not include a stable subject and email.")

    user = User.objects.filter(email=email).first()
    created = False

    if user is None:
        user = User.objects.create(
            username=_build_username(email, google_sub),
            email=email,
            first_name=given_name[:150],
            last_name=family_name[:150],
        )
        user.set_unusable_password()
        created = True
    else:
        fields_to_update = []
        if given_name and user.first_name != given_name[:150]:
            user.first_name = given_name[:150]
            fields_to_update.append("first_name")
        if family_name and user.last_name != family_name[:150]:
            user.last_name = family_name[:150]
            fields_to_update.append("last_name")
        if not user.email:
            user.email = email
            fields_to_update.append("email")
        if fields_to_update:
            user.save(update_fields=fields_to_update)

    if created:
        user.save(update_fields=["password"])

    return user, full_name


def google_oauth_start_view(request):
    if not _oauth_is_configured():
        return JsonResponse({"error": "Google OAuth is not configured."}, status=503)

    state = secrets.token_urlsafe(32)
    redirect_uri = _google_redirect_uri_for_request(request)
    request.session[GOOGLE_OAUTH_STATE_SESSION_KEY] = state
    request.session[GOOGLE_OAUTH_REDIRECT_URI_SESSION_KEY] = redirect_uri
    query = urlencode(
        {
            "client_id": settings.GOOGLE_OAUTH_CLIENT_ID,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "access_type": "offline",
            "prompt": "consent",
        }
    )
    return HttpResponseRedirect(f"{GOOGLE_AUTH_URL}?{query}")


def google_oauth_callback_view(request):
    callback_url = "/auth/google/callback"
    query = request.META.get("QUERY_STRING", "")
    if query:
        callback_url = f"{callback_url}?{query}"
    return HttpResponseRedirect(callback_url)


@csrf_exempt
def google_oauth_exchange_view(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if not _oauth_is_configured():
        return JsonResponse({"error": "Google OAuth is not configured."}, status=503)

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    returned_state = (payload.get("state") or "").strip()
    saved_state = request.session.pop(GOOGLE_OAUTH_STATE_SESSION_KEY, "")
    redirect_uri = request.session.pop(
        GOOGLE_OAUTH_REDIRECT_URI_SESSION_KEY,
        settings.GOOGLE_OAUTH_REDIRECT_URI,
    )
    if not saved_state or returned_state != saved_state:
        return JsonResponse({"error": "OAuth state mismatch."}, status=400)

    if payload.get("error"):
        return JsonResponse({"error": payload.get("error")}, status=400)

    code = (payload.get("code") or "").strip()
    if not code:
        return JsonResponse({"error": "Missing Google authorization code."}, status=400)

    try:
        token_payload = _google_json_request(
            GOOGLE_TOKEN_URL,
            method="POST",
            data={
                "client_id": settings.GOOGLE_OAUTH_CLIENT_ID,
                "client_secret": settings.GOOGLE_OAUTH_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            },
        )
        access_token = token_payload.get("access_token")
        if not access_token:
            raise ValueError("Google token response did not include an access token.")
        profile = _google_json_request(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        user, full_name = _upsert_google_user(profile)
    except Exception:
        return JsonResponse({"error": "Google OAuth exchange failed."}, status=502)

    login(request, user, backend="django.contrib.auth.backends.ModelBackend")
    return JsonResponse(
        {
            "status": "ok",
            "user": {
                **_serialize_user(user),
                "full_name": full_name or user.username,
            },
        }
    )


def auth_me_view(request):
    if not request.user.is_authenticated:
        return JsonResponse({"authenticated": False})

    return JsonResponse(
        {
            "authenticated": True,
            "user": _serialize_user(request.user),
        }
    )


@csrf_exempt
def auth_logout_view(request):
    if request.method not in {"POST", "GET"}:
        return HttpResponseNotAllowed(["GET", "POST"])

    logout(request)
    return JsonResponse({"status": "ok"})


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


def admin_dashboard_view(request):
    if not request.user.is_authenticated:
        return JsonResponse({"error": "Authentication required."}, status=401)

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    User = get_user_model()
    users = [_serialize_user(user) for user in User.objects.order_by("-date_joined", "username")]
    signups = [
        {
            "email": signup.email,
            "name": signup.name,
            "created_at": signup.created_at.isoformat(),
            "updated_at": signup.updated_at.isoformat(),
        }
        for signup in EarlyAccessSignup.objects.order_by("-created_at")
    ]

    return JsonResponse(
        {
            "stats": {
                "users": len(users),
                "superusers": sum(1 for user in users if user["is_superuser"]),
                "signups": len(signups),
            },
            "users": users,
            "signups": signups,
        }
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
