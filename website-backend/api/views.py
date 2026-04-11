# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import hashlib
import hmac
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
from django.db.models import Max
from django.utils.dateparse import parse_datetime
from django.utils.text import slugify
from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseRedirect, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .models import EarlyAccessSignup, ManagedNode, VonageInboundSms
from .node_launcher import (
    NodeLauncherError,
    dashboard_proxy_path,
    launch_node,
    launcher_enabled,
    refresh_node,
    stop_node,
    tail_logs,
)

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


def _parse_unix_timestamp(raw_value):
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None

    try:
        return datetime.fromtimestamp(int(raw_value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _parse_vonage_datetime(raw_value):
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None

    parsed = parse_datetime(raw_value)
    if parsed is not None:
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    for pattern in ("%Y-%m-%d %H:%M:%S", "%y%m%d%H%M"):
        try:
            return datetime.strptime(raw_value, pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def _flatten_request_data(request):
    payload = {}
    content_type = (request.content_type or "").lower()

    if "application/json" in content_type:
        try:
            decoded = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            decoded = {}
        if isinstance(decoded, dict):
            payload.update({str(key): value for key, value in decoded.items()})

    for source in (request.POST, request.GET):
        for key in source.keys():
            values = source.getlist(key)
            if not values:
                continue
            payload[key] = values[-1] if len(values) == 1 else values

    return payload


def _sanitize_signature_value(value):
    return str(value).replace("&", "_").replace("=", "_")


def _validate_vonage_signature(payload):
    provided_signature = str(payload.get("sig") or "").strip()
    signature_secret = (settings.VONAGE_SMS_SIGNATURE_SECRET or "").strip()
    account_secret = (settings.VONAGE_ACCOUNT_SECRET or "").strip()
    algorithm = (settings.VONAGE_SMS_SIGNATURE_ALGORITHM or "md5hash").strip().lower()

    if not provided_signature:
        return None, ""

    active_secret = signature_secret or account_secret
    if not active_secret:
        return None, "Signature received but no VONAGE_SMS_SIGNATURE_SECRET or VONAGE_ACCOUNT_SECRET is configured."

    signed_items = []
    for key in sorted(payload.keys()):
        if key == "sig":
            continue
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            normalized = ",".join(_sanitize_signature_value(item) for item in value)
        else:
            normalized = _sanitize_signature_value(value)
        signed_items.append(f"&{key}={normalized}")

    signature_input = "".join(signed_items)

    if algorithm == "md5hash":
        expected_signature = hashlib.md5(
            f"{signature_input}{active_secret}".encode("utf-8")
        ).hexdigest()
    else:
        digest_name = {
            "md5": "md5",
            "sha1": "sha1",
            "sha256": "sha256",
            "sha512": "sha512",
        }.get(algorithm)
        if not digest_name:
            return None, f"Unsupported Vonage signature algorithm: {algorithm}"
        expected_signature = hmac.new(
            active_secret.encode("utf-8"),
            signature_input.encode("utf-8"),
            getattr(hashlib, digest_name),
        ).hexdigest()

    signature_error = ""
    if not signature_secret and account_secret:
        signature_error = "Validated with VONAGE_ACCOUNT_SECRET fallback because VONAGE_SMS_SIGNATURE_SECRET is not configured."

    return hmac.compare_digest(expected_signature.lower(), provided_signature.lower()), signature_error


def _parse_positive_int(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return None

    try:
        parsed = int(raw_value)
    except ValueError:
        return None

    return parsed if parsed >= 0 else None


def _serialize_vonage_sms(message):
    return {
        "id": message.id,
        "api_key": message.api_key,
        "message_id": message.message_id,
        "from_number": message.from_number,
        "to_number": message.to_number,
        "text": message.text,
        "message_type": message.message_type,
        "keyword": message.keyword,
        "message_timestamp": message.message_timestamp.isoformat() if message.message_timestamp else None,
        "message_timestamp_raw": message.message_timestamp_raw,
        "event_timestamp": message.event_timestamp.isoformat() if message.event_timestamp else None,
        "event_timestamp_raw": message.event_timestamp_raw,
        "nonce": message.nonce,
        "signature": message.signature,
        "signature_valid": message.signature_valid,
        "signature_error": message.signature_error,
        "is_concatenated": message.is_concatenated,
        "concat_ref": message.concat_ref,
        "concat_total": message.concat_total,
        "concat_part": message.concat_part,
        "data": message.data,
        "udh": message.udh,
        "content_type": message.content_type,
        "request_method": message.request_method,
        "remote_addr": message.remote_addr,
        "user_agent": message.user_agent,
        "payload": message.payload,
        "raw_body": message.raw_body,
        "received_at": message.received_at.isoformat(),
        "created_at": message.created_at.isoformat(),
        "updated_at": message.updated_at.isoformat(),
    }


def _admin_required_response(request):
    if not request.user.is_authenticated:
        return JsonResponse({"error": "Authentication required."}, status=401)
    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)
    return None


def _serialize_managed_node(node):
    return {
        "id": node.id,
        "name": node.name,
        "display_name": node.display_name,
        "container_name": node.container_name,
        "container_id": node.container_id,
        "image": node.image,
        "network_name": node.network_name,
        "chain_id": node.chain_id,
        "enable_mining": node.enable_mining,
        "mining_threads": node.mining_threads,
        "api_port": node.api_port,
        "p2p_port": node.p2p_port,
        "metrics_port": node.metrics_port,
        "status": node.status,
        "last_error": node.last_error,
        "logs_tail": node.last_logs,
        "dashboard_proxy_url": dashboard_proxy_path(node),
        "launched_by": node.launched_by.email if node.launched_by else "",
        "launched_at": node.launched_at.isoformat() if node.launched_at else None,
        "last_status_at": node.last_status_at.isoformat() if node.last_status_at else None,
        "stopped_at": node.stopped_at.isoformat() if node.stopped_at else None,
    }


def _next_managed_node_ports():
    aggregate = ManagedNode.objects.aggregate(
        max_api_port=Max("api_port"),
        max_p2p_port=Max("p2p_port"),
        max_metrics_port=Max("metrics_port"),
    )
    return {
        "api_port": max(settings.NODE_LAUNCHER_BASE_API_PORT, (aggregate["max_api_port"] or settings.NODE_LAUNCHER_BASE_API_PORT - 1) + 1),
        "p2p_port": max(settings.NODE_LAUNCHER_BASE_P2P_PORT, (aggregate["max_p2p_port"] or settings.NODE_LAUNCHER_BASE_P2P_PORT - 1) + 1),
        "metrics_port": max(settings.NODE_LAUNCHER_BASE_METRICS_PORT, (aggregate["max_metrics_port"] or settings.NODE_LAUNCHER_BASE_METRICS_PORT - 1) + 1),
    }


def _build_managed_node_name(display_name):
    base = slugify(display_name)[:40] or "node"
    candidate = base
    suffix = 1
    while ManagedNode.objects.filter(name=candidate).exists():
        candidate = f"{base[:50]}-{suffix}"
        suffix += 1
    return candidate[:63]


def _load_managed_nodes():
    nodes = list(ManagedNode.objects.select_related("launched_by").order_by("-created_at"))
    hydrated = []
    for node in nodes:
        try:
            hydrated.append(refresh_node(node))
        except NodeLauncherError:
            hydrated.append(node)
    return hydrated


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
          <li><code>/api/vonage/sms/callback</code> - Vonage inbound SMS webhook</li>
        </ul>
        <p><strong>Database</strong>: <code>{database_state}</code></p>
      </div>
    </main>
  </body>
</html>
""",
        content_type="text/html; charset=utf-8",
    )


def _admin_html_shell(*, title, eyebrow, heading, copy, bootstrap_url, back_href="/", back_label="Back home"):
    return HttpResponse(
        f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #faf7f2;
        --card: rgba(255, 255, 255, 0.82);
        --border: rgba(216, 90, 48, 0.18);
        --ink: #1a1208;
        --muted: #6f624d;
        --accent: #d85a30;
        --shadow: 0 28px 80px rgba(74, 27, 12, 0.08);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "DM Sans", system-ui, sans-serif;
        background:
          radial-gradient(circle at top right, rgba(239, 159, 39, 0.12), transparent 30%),
          radial-gradient(circle at left 20%, rgba(216, 90, 48, 0.08), transparent 24%),
          var(--bg);
        color: var(--ink);
      }}
      main {{
        max-width: 1160px;
        margin: 0 auto;
        padding: 56px 20px 72px;
      }}
      .eyebrow {{
        margin: 0 0 14px;
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
      }}
      h1 {{
        margin: 0 0 12px;
        font-family: Georgia, "Times New Roman", serif;
        font-size: clamp(40px, 6vw, 68px);
        line-height: 0.95;
      }}
      .copy {{
        max-width: 760px;
        margin: 0;
        color: var(--muted);
        font-size: 18px;
        line-height: 1.6;
      }}
      .toolbar {{
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 24px;
      }}
      .button {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 44px;
        padding: 0.8rem 1.1rem;
        border: 1px solid var(--border);
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.7);
        color: var(--ink);
        text-decoration: none;
      }}
      .button:hover {{
        border-color: rgba(216, 90, 48, 0.36);
      }}
      .button-primary {{
        border-color: transparent;
        background: var(--ink);
        color: #fffaf2;
      }}
      .grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 16px;
        margin-top: 28px;
      }}
      .card, .panel {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 24px;
        box-shadow: var(--shadow);
        backdrop-filter: blur(10px);
      }}
      .card {{
        padding: 20px;
      }}
      .panel {{
        margin-top: 24px;
        padding: 22px;
      }}
      .label {{
        margin: 0 0 10px;
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
      }}
      .value {{
        margin: 0;
        font-size: 34px;
        font-weight: 700;
      }}
      .status {{
        color: var(--muted);
      }}
      .error {{
        color: #9c2f1a;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        padding: 14px 12px;
        text-align: left;
        border-bottom: 1px solid rgba(216, 90, 48, 0.12);
        vertical-align: top;
      }}
      th {{
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--muted);
      }}
      pre {{
        margin: 0;
        padding: 16px;
        overflow: auto;
        border-radius: 16px;
        background: rgba(26, 18, 8, 0.92);
        color: #fffaf2;
        font-size: 12px;
        line-height: 1.6;
      }}
      .stack {{
        display: grid;
        gap: 16px;
      }}
      @media (max-width: 840px) {{
        main {{
          padding-left: 16px;
          padding-right: 16px;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <p class="eyebrow">{eyebrow}</p>
      <h1>{heading}</h1>
      <p class="copy">{copy}</p>
      <div class="toolbar">
        <a class="button" href="{back_href}">{back_label}</a>
        <a class="button" href="/admin/dashboard">Dashboard</a>
        <a class="button" href="/admin/vonage/sms">SMS Inbox</a>
        <a class="button button-primary" href="/api/auth/logout">Sign out</a>
      </div>
      <div id="app" class="panel">
        <p class="status">Loading…</p>
      </div>
    </main>
    <script>
      window.__KUMQUAT_BOOTSTRAP_URL__ = {json.dumps(bootstrap_url)};
    </script>
    <script>
      const mount = document.getElementById("app");
      const endpoint = window.__KUMQUAT_BOOTSTRAP_URL__;

      function escapeHtml(value) {{
        return String(value ?? "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }}

      function formatDate(value) {{
        if (!value) return "Never";
        const parsed = new Date(value);
        return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
      }}

      fetch(endpoint, {{ credentials: "same-origin" }})
        .then(async (response) => {{
          const payload = await response.json().catch(() => ({{}}));
          if (!response.ok) {{
            throw new Error(payload.error || "Failed to load admin data.");
          }}
          return payload;
        }})
        .then((payload) => {{
          if (endpoint.includes("/api/admin/vonage/sms")) {{
            const rows = (payload.messages || []).map((message) => `
              <tr>
                <td>${{escapeHtml(message.from_number || "Unknown")}}</td>
                <td>${{escapeHtml(message.to_number || "Unknown")}}</td>
                <td>${{escapeHtml(message.text || "No body")}}</td>
                <td>${{escapeHtml(message.signature_valid === true ? "Valid" : message.signature_valid === false ? "Invalid" : message.signature ? "Present" : "None")}}</td>
                <td>${{escapeHtml(formatDate(message.received_at))}}</td>
              </tr>
              <tr>
                <td colspan="5">
                  <div class="stack">
                    <div><strong>Message ID:</strong> ${{escapeHtml(message.message_id || "Not provided")}}</div>
                    <div><strong>Signature error:</strong> ${{escapeHtml(message.signature_error || "None")}}</div>
                    <pre>${{escapeHtml(JSON.stringify(message.payload || {{}}, null, 2))}}</pre>
                  </div>
                </td>
              </tr>
            `).join("");
            mount.innerHTML = `
              <div class="grid">
                <div class="card"><p class="label">Messages</p><p class="value">${{escapeHtml(payload.stats?.messages ?? 0)}}</p></div>
                <div class="card"><p class="label">Signed</p><p class="value">${{escapeHtml(payload.stats?.signed_messages ?? 0)}}</p></div>
                <div class="card"><p class="label">Unsigned</p><p class="value">${{escapeHtml(payload.stats?.unsigned_messages ?? 0)}}</p></div>
                <div class="card"><p class="label">Failed Sig</p><p class="value">${{escapeHtml(payload.stats?.failed_signatures ?? 0)}}</p></div>
              </div>
              <div class="panel">
                <table>
                  <thead>
                    <tr><th>From</th><th>To</th><th>Text</th><th>Signature</th><th>Received</th></tr>
                  </thead>
                  <tbody>${{rows || '<tr><td colspan="5">No inbound SMS records yet.</td></tr>'}}</tbody>
                </table>
              </div>
            `;
            return;
          }}

          const users = (payload.users || []).map((user) => `
            <tr>
              <td>${{escapeHtml(user.full_name)}}</td>
              <td>${{escapeHtml(user.email || user.username)}}</td>
              <td>${{escapeHtml(user.is_superuser ? "Superuser" : user.is_staff ? "Staff" : "User")}}</td>
              <td>${{escapeHtml(formatDate(user.date_joined))}}</td>
            </tr>
          `).join("");
          const signups = (payload.signups || []).map((signup) => `
            <tr>
              <td>${{escapeHtml(signup.name || "Unknown")}}</td>
              <td>${{escapeHtml(signup.email)}}</td>
              <td>${{escapeHtml(formatDate(signup.created_at))}}</td>
            </tr>
          `).join("");
          mount.innerHTML = `
            <div class="grid">
              <div class="card"><p class="label">Users</p><p class="value">${{escapeHtml(payload.stats?.users ?? 0)}}</p></div>
              <div class="card"><p class="label">Superusers</p><p class="value">${{escapeHtml(payload.stats?.superusers ?? 0)}}</p></div>
              <div class="card"><p class="label">Signups</p><p class="value">${{escapeHtml(payload.stats?.signups ?? 0)}}</p></div>
              <div class="card"><p class="label">Inbound SMS</p><p class="value">${{escapeHtml(payload.stats?.inbound_sms ?? 0)}}</p></div>
            </div>
            <div class="panel">
              <p class="label">Users</p>
              <table>
                <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Joined</th></tr></thead>
                <tbody>${{users || '<tr><td colspan="4">No users found.</td></tr>'}}</tbody>
              </table>
            </div>
            <div class="panel">
              <p class="label">Early Signups</p>
              <table>
                <thead><tr><th>Name</th><th>Email</th><th>Created</th></tr></thead>
                <tbody>${{signups || '<tr><td colspan="3">No signups found.</td></tr>'}}</tbody>
              </table>
            </div>
          `;
        }})
        .catch((error) => {{
          mount.innerHTML = `<p class="error">${{escapeHtml(error.message)}}</p>`;
        }});
    </script>
  </body>
</html>
""",
        content_type="text/html; charset=utf-8",
    )


def _oauth_is_configured():
    return bool(
        settings.GOOGLE_OAUTH_CLIENT_ID
        and settings.GOOGLE_OAUTH_CLIENT_SECRET
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

    absolute_callback_url = request.build_absolute_uri("/api/auth/google/callback")
    parsed_callback_url = urlparse(absolute_callback_url)
    if parsed_callback_url.scheme in {"http", "https"} and parsed_callback_url.netloc:
        return absolute_callback_url

    for header_name in ("HTTP_ORIGIN", "HTTP_REFERER"):
        value = (request.META.get(header_name) or "").strip()
        if not value:
            continue

        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue

        return f"{parsed.scheme}://{parsed.netloc}/api/auth/google/callback"

    return "http://localhost:8000/api/auth/google/callback"


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
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

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
    sms_messages = [
        _serialize_vonage_sms(message)
        for message in VonageInboundSms.objects.order_by("-received_at", "-created_at")[:10]
    ]
    managed_nodes = [_serialize_managed_node(node) for node in _load_managed_nodes()]

    return JsonResponse(
        {
            "stats": {
                "users": len(users),
                "superusers": sum(1 for user in users if user["is_superuser"]),
                "signups": len(signups),
                "inbound_sms": VonageInboundSms.objects.count(),
                "managed_nodes": len(managed_nodes),
                "running_nodes": sum(1 for node in managed_nodes if node["status"] == ManagedNode.STATUS_RUNNING),
            },
            "users": users,
            "signups": signups,
            "recent_sms": sms_messages,
            "managed_nodes": managed_nodes,
            "launcher": {
                "enabled": launcher_enabled(),
                "default_image": settings.NODE_LAUNCHER_IMAGE,
                "default_network": settings.NODE_LAUNCHER_NETWORK,
                "default_chain_id": settings.NODE_LAUNCHER_CHAIN_ID,
            },
        }
    )


def admin_dashboard_page_view(request):
    if not request.user.is_authenticated:
        return HttpResponseRedirect("/auth/sign-in")

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    return _admin_html_shell(
        title="Kumquat Admin Dashboard",
        eyebrow="Admin",
        heading="Product release dashboard.",
        copy="Review signed-in users, early access signups, and inbound SMS from the backend-owned admin surface.",
        bootstrap_url="/api/admin/dashboard",
    )


def admin_vonage_sms_view(request):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    messages = [
        _serialize_vonage_sms(message)
        for message in VonageInboundSms.objects.order_by("-received_at", "-created_at")
    ]

    return JsonResponse(
        {
            "stats": {
                "messages": len(messages),
                "signed_messages": sum(1 for message in messages if message["signature_valid"] is True),
                "unsigned_messages": sum(1 for message in messages if not message["signature"]),
                "failed_signatures": sum(1 for message in messages if message["signature_valid"] is False),
            },
            "messages": messages,
        }
    )


@csrf_exempt
def admin_node_launch_view(request):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if not launcher_enabled():
        return JsonResponse({"error": "Node launcher is disabled."}, status=503)

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    display_name = (payload.get("display_name") or payload.get("name") or "").strip()
    if not display_name:
        display_name = f"Managed Node {ManagedNode.objects.count() + 1}"

    ports = _next_managed_node_ports()
    node = ManagedNode.objects.create(
        name=_build_managed_node_name(display_name),
        display_name=display_name[:120],
        image=(payload.get("image") or settings.NODE_LAUNCHER_IMAGE).strip(),
        network_name=(payload.get("network_name") or settings.NODE_LAUNCHER_NETWORK).strip() or settings.NODE_LAUNCHER_NETWORK,
        chain_id=_parse_positive_int(payload.get("chain_id")) or settings.NODE_LAUNCHER_CHAIN_ID,
        enable_mining=bool(payload.get("enable_mining")),
        mining_threads=_parse_positive_int(payload.get("mining_threads")) or 1,
        api_port=ports["api_port"],
        p2p_port=ports["p2p_port"],
        metrics_port=ports["metrics_port"],
        launched_by=request.user,
    )

    try:
        node = launch_node(node)
        return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)}, status=201)
    except NodeLauncherError as exc:
        node.last_error = str(exc)
        node.status = ManagedNode.STATUS_FAILED
        node.save(update_fields=["last_error", "status", "updated_at"])
        return JsonResponse({"error": str(exc), "node": _serialize_managed_node(node)}, status=502)


@csrf_exempt
def admin_node_stop_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        return JsonResponse({"error": "Managed node not found."}, status=404)

    try:
        node = stop_node(node)
        return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)})
    except NodeLauncherError as exc:
        return JsonResponse({"error": str(exc)}, status=502)


def admin_node_logs_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        return JsonResponse({"error": "Managed node not found."}, status=404)

    try:
        node = refresh_node(node)
        logs = tail_logs(node)
    except NodeLauncherError as exc:
        return JsonResponse({"error": str(exc)}, status=502)

    return JsonResponse({"status": "ok", "node": _serialize_managed_node(node), "logs": logs})


def admin_node_proxy_view(request, node_id, subpath=""):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        return JsonResponse({"error": "Managed node not found."}, status=404)

    try:
        node = refresh_node(node)
    except NodeLauncherError as exc:
        return JsonResponse({"error": str(exc)}, status=502)

    if node.status != ManagedNode.STATUS_RUNNING:
        return JsonResponse({"error": "Managed node is not running."}, status=409)

    target_path = "/" + (subpath or "dashboard")
    if request.GET:
        target_path = f"{target_path}?{request.META.get('QUERY_STRING', '')}"

    upstream_url = f"http://127.0.0.1:{node.api_port}{target_path}"

    try:
        with urlopen(
            Request(
                upstream_url,
                method="GET",
                headers={
                    "Accept": request.headers.get("Accept", "*/*"),
                    "User-Agent": request.headers.get("User-Agent", "kumquat-admin-proxy"),
                },
            ),
            timeout=10,
        ) as response:
            proxied = HttpResponse(response.read(), status=response.status)
            for header_name, header_value in response.headers.items():
                if header_name.lower() in {"connection", "transfer-encoding", "content-length"}:
                    continue
                proxied[header_name] = header_value
            proxied["X-Kumquat-Managed-Node"] = node.name
            return proxied
    except HTTPError as exc:
        return HttpResponse(exc.read(), status=exc.code)
    except URLError as exc:
        return JsonResponse({"error": f"Managed node upstream is unavailable: {exc.reason}"}, status=502)


def admin_vonage_sms_page_view(request):
    if not request.user.is_authenticated:
        return HttpResponseRedirect("/auth/sign-in")

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    return _admin_html_shell(
        title="Kumquat Inbound SMS",
        eyebrow="Vonage",
        heading="Inbound SMS inbox.",
        copy="Review webhook deliveries handled by the Django backend, including payload details and signature validation state.",
        bootstrap_url="/api/admin/vonage/sms",
        back_href="/admin/dashboard",
        back_label="Back to dashboard",
    )


@csrf_exempt
def vonage_sms_callback_view(request):
    if request.method not in {"GET", "POST"}:
        return HttpResponseNotAllowed(["GET", "POST"])

    payload = _flatten_request_data(request)
    if not payload:
        return JsonResponse({"error": "Webhook payload is empty."}, status=400)

    signature_valid, signature_error = _validate_vonage_signature(payload)
    message_timestamp_raw = str(payload.get("message-timestamp") or "").strip()
    event_timestamp_raw = str(payload.get("timestamp") or "").strip()
    received_at = _parse_unix_timestamp(event_timestamp_raw) or datetime.now(timezone.utc)

    defaults = {
        "api_key": str(payload.get("api-key") or "").strip(),
        "from_number": str(payload.get("msisdn") or "").strip(),
        "to_number": str(payload.get("to") or "").strip(),
        "text": str(payload.get("text") or "").strip(),
        "message_type": str(payload.get("type") or "").strip(),
        "keyword": str(payload.get("keyword") or "").strip(),
        "message_timestamp": _parse_vonage_datetime(message_timestamp_raw),
        "message_timestamp_raw": message_timestamp_raw,
        "event_timestamp": _parse_unix_timestamp(event_timestamp_raw),
        "event_timestamp_raw": event_timestamp_raw,
        "nonce": str(payload.get("nonce") or "").strip(),
        "signature": str(payload.get("sig") or "").strip(),
        "signature_valid": signature_valid,
        "signature_error": signature_error,
        "is_concatenated": str(payload.get("concat") or "").strip().lower() in {"true", "1"},
        "concat_ref": str(payload.get("concat-ref") or "").strip(),
        "concat_total": _parse_positive_int(payload.get("concat-total")),
        "concat_part": _parse_positive_int(payload.get("concat-part")),
        "data": str(payload.get("data") or "").strip(),
        "udh": str(payload.get("udh") or "").strip(),
        "content_type": request.content_type or "",
        "request_method": request.method,
        "remote_addr": (
            request.META.get("HTTP_X_FORWARDED_FOR", "").split(",", 1)[0].strip()
            or request.META.get("REMOTE_ADDR", "")
        ),
        "user_agent": request.META.get("HTTP_USER_AGENT", "")[:255],
        "payload": payload,
        "raw_body": request.body.decode("utf-8", errors="replace"),
        "received_at": received_at,
    }

    message_id = str(payload.get("messageId") or "").strip()
    if message_id:
        sms_message, created = VonageInboundSms.objects.update_or_create(
            message_id=message_id,
            defaults=defaults,
        )
    else:
        sms_message = VonageInboundSms.objects.create(
            message_id="",
            **defaults,
        )
        created = True

    return JsonResponse(
        {
            "status": "created" if created else "updated",
            "id": sms_message.id,
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
