# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import hashlib
import hmac
import json
import re
import secrets
import base64
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model, login, logout
from django.core.exceptions import ValidationError
from django.core.paginator import Paginator
from django.core.validators import validate_email
from django.db import IntegrityError, connection
from django.db.models import Max
from django.templatetags.static import static
from django.urls import reverse
from django.utils.dateparse import parse_datetime
from django.utils.text import slugify
from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.csrf import csrf_exempt

from .models import EarlyAccessSignup, ManagedNode, SearchCrawlTarget, UserWallet, VonageInboundSms
from .models import CompanyProfile, JobListing
from .address_codec import AddressCodecError, encode_address, normalize_address
from .node_launcher import (
    delete_container,
    delete_deployment,
    delete_runtime_container,
    dashboard_subdomain_url,
    list_runtime_containers,
    NodeLauncherError,
    dashboard_proxy_path,
    launch_node,
    launcher_enabled,
    refresh_node,
    restart_runtime_container,
    restart_node,
    stop_node,
    tail_logs,
    upstream_rpc_url,
)
from .search import SearchCrawlerError, normalize_crawl_url, search_documents
from .tasks import schedule_crawl_search_target

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
GOOGLE_OAUTH_STATE_SESSION_KEY = "google_oauth_state"
GOOGLE_OAUTH_REDIRECT_URI_SESSION_KEY = "google_oauth_redirect_uri"
EARLY_ACCESS_SIGNUP_SESSION_KEY = "early_access_signup"
EARLY_ACCESS_SIGNUP_SUCCESS_SESSION_KEY = "early_access_signup_success"
EARLY_ACCESS_SIGNUP_ERROR_SESSION_KEY = "early_access_signup_error"
WALLET_PRIVATE_KEY_SESSION_KEY = "wallet_private_key"
WALLET_GENERATION_ERROR_SESSION_KEY = "wallet_generation_error"
WALLET_GENERATION_SUCCESS_SESSION_KEY = "wallet_generation_success"
WALLET_GENERATION_STATUS_SESSION_KEY = "wallet_generation_status"
DEFAULT_SEO_IMAGE_PATH = "website/img/og-card.svg"
DEFAULT_SEO_KEYWORDS = (
    "kumquat, kumquat chain, digital cash, blockchain wallet, object-based money, "
    "digital denominations, wallet software, crypto wallet, parallel ledger"
)
CURRENCY_SYMBOL = "¤"
NODE_LAUNCHER_AUTH_SESSION_KEY = "node_launcher_auth"
EXPLORER_HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _currency_label(amount):
    return f"{CURRENCY_SYMBOL}{amount}"


def _launcher_auth_from_request(request):
    auth = request.session.get(NODE_LAUNCHER_AUTH_SESSION_KEY, {}) if hasattr(request, "session") else {}
    return {
        "NODE_LAUNCHER_KUBECONFIG": (auth.get("kubeconfig_path") or "").strip(),
        "NODE_LAUNCHER_KUBE_API_SERVER": (auth.get("api_server") or "").strip(),
        "NODE_LAUNCHER_KUBE_BEARER_TOKEN": (auth.get("bearer_token") or "").strip(),
        "NODE_LAUNCHER_KUBE_CA_CERT_B64": (auth.get("ca_cert_b64") or "").strip(),
        "namespace": (auth.get("namespace") or "").strip(),
    }


def _save_launcher_auth(request, payload):
    request.session[NODE_LAUNCHER_AUTH_SESSION_KEY] = {
        "api_server": (payload.get("api_server") or "").strip(),
        "bearer_token": (payload.get("bearer_token") or "").strip(),
        "ca_cert_b64": (payload.get("ca_cert_b64") or "").strip(),
        "namespace": (payload.get("namespace") or "").strip(),
        "kubeconfig_path": (payload.get("kubeconfig_path") or "").strip(),
    }
    request.session.modified = True


def _mask_launcher_token(token):
    token = (token or "").strip()
    if not token:
        return ""
    if len(token) <= 8:
        return "configured"
    return f"{token[:4]}...{token[-4:]}"

BILL_ITEMS = [
    {"label": _currency_label("100"), "kind": "bill", "id": "KMQ-00100000"},
    {"label": _currency_label("50"), "kind": "bill", "id": "KMQ-00050000"},
    {"label": _currency_label("20"), "kind": "bill", "id": "KMQ-00020000"},
    {"label": _currency_label("10"), "kind": "bill", "id": "KMQ-00010000"},
    {"label": _currency_label("5"), "kind": "bill", "id": "KMQ-00005000"},
    {"label": _currency_label("2"), "kind": "bill", "id": "KMQ-00002000"},
    {"label": _currency_label("1"), "kind": "bill", "id": "KMQ-00001000"},
    {"label": _currency_label("0.50"), "kind": "coin", "id": "KMQ-00000500"},
    {"label": _currency_label("0.25"), "kind": "coin", "id": "KMQ-00000250"},
    {"label": _currency_label("0.10"), "kind": "coin", "id": "KMQ-00000100"},
    {"label": _currency_label("0.05"), "kind": "coin", "id": "KMQ-00000050"},
    {"label": _currency_label("0.01"), "kind": "coin", "id": "KMQ-00000010"},
]

HOW_IT_WORKS_STEPS = [
    {
        "number": "01",
        "title": "Model units as objects",
        "body": "Kumquat represents each denomination as a discrete software unit with a visible identity, so the interface stays legible instead of collapsing into one balance row.",
    },
    {
        "number": "02",
        "title": "Track transfers clearly",
        "body": "Transfers read like moving distinct units between wallets. Motion reinforces the handoff instead of decorating it.",
    },
    {
        "number": "03",
        "title": "Read the wallet at a glance",
        "body": "The interface surfaces denomination mix, object count, and totals in the same view so the object model stays visible and legible.",
    },
]

DENOMINATION_GRID = [
    {"label": _currency_label("100"), "type": "bill"},
    {"label": _currency_label("50"), "type": "bill"},
    {"label": _currency_label("20"), "type": "bill"},
    {"label": _currency_label("10"), "type": "bill"},
    {"label": _currency_label("5"), "type": "bill"},
    {"label": _currency_label("2"), "type": "bill"},
    {"label": _currency_label("1"), "type": "bill"},
    {"label": _currency_label("0.50"), "type": "coin"},
    {"label": _currency_label("0.25"), "type": "coin"},
    {"label": _currency_label("0.10"), "type": "coin"},
    {"label": _currency_label("0.05"), "type": "coin"},
    {"label": _currency_label("0.01"), "type": "coin"},
]

WALLET_ROWS = [
    {"label": _currency_label("100.00"), "kind": "bill", "detail": "Large-format unit", "amount": 100.0},
    {"label": _currency_label("50.00"), "kind": "bill", "detail": "Transfer example", "amount": 50.0},
    {"label": _currency_label("20.00"), "kind": "bill", "detail": "Wallet row sample", "amount": 20.0},
    {"label": _currency_label("10.00"), "kind": "bill", "detail": "Interface unit", "amount": 10.0},
    {"label": _currency_label("5.00"), "kind": "bill", "detail": "Smaller-format unit", "amount": 5.0},
    {"label": _currency_label("2.00"), "kind": "bill", "detail": "Lower bill example", "amount": 2.0},
    {"label": _currency_label("1.00"), "kind": "bill", "detail": "Lowest bill example", "amount": 1.0},
    {"label": _currency_label("0.25"), "kind": "coin", "detail": "Coin example", "amount": 0.25},
    {"label": _currency_label("0.10"), "kind": "coin", "detail": "Coin example", "amount": 0.1},
    {"label": _currency_label("0.01"), "kind": "coin", "detail": "Coin example", "amount": 0.01},
]

SEO_FAQ_ITEMS = [
    {
        "question": "What is Kumquat?",
        "answer": "Kumquat is digital cash software that models value as visible units such as bills and coins instead of hiding everything behind one abstract balance.",
    },
    {
        "question": "How does the Kumquat wallet work?",
        "answer": "The website can generate an Ed25519 wallet, derive a Kumquat-compatible address from the public key, and store the encrypted private key for the signed-in user.",
    },
    {
        "question": "Why does Kumquat focus on denominations?",
        "answer": "Denominations make transfers and balances easier to inspect because the interface shows the composition of value, not just a final number after the transaction is complete.",
    },
]


def _database_state():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return "ok"
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return f"error: {exc}"


def _site_origin():
    return settings.SITE_URL.rstrip("/")


def _absolute_url(path_or_url):
    if path_or_url.startswith(("http://", "https://")):
        return path_or_url
    return f"{_site_origin()}{path_or_url}"


def _structured_data_json(structured_data):
    return [json.dumps(item, separators=(",", ":"), ensure_ascii=False) for item in structured_data]


def _seo_context(
    request,
    *,
    title,
    description,
    path=None,
    keywords=DEFAULT_SEO_KEYWORDS,
    index=True,
    og_type="website",
    image_path=None,
    structured_data=None,
):
    canonical_path = path or request.path
    image_url = _absolute_url(static(image_path or DEFAULT_SEO_IMAGE_PATH))
    structured_data = structured_data or []
    return {
        "seo": {
            "title": title,
            "description": description,
            "keywords": keywords,
            "canonical_url": _absolute_url(canonical_path),
            "robots": (
                "index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1"
                if index
                else "noindex,nofollow"
            ),
            "og_type": og_type,
            "site_name": settings.SITE_NAME,
            "site_url": _site_origin(),
            "image_url": image_url,
            "locale": "en_US",
            "theme_color": "#d85a30",
            "author": "Benjamin Levin",
            "structured_data_json": _structured_data_json(structured_data),
        }
    }


def _home_structured_data():
    homepage_url = _site_origin()
    image_url = _absolute_url(static(DEFAULT_SEO_IMAGE_PATH))
    return [
        {
            "@context": "https://schema.org",
            "@type": "Organization",
            "name": settings.SITE_NAME,
            "url": homepage_url,
            "logo": image_url,
            "founder": {
                "@type": "Person",
                "name": "Benjamin Levin",
            },
            "sameAs": [
                "https://github.com/kumquat-ben/kumquat",
                "https://x.com/kumquatben",
            ],
        },
        {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": settings.SITE_NAME,
            "url": homepage_url,
            "description": "A search engine homepage and index preview for Kumquat.",
        },
        {
            "@context": "https://schema.org",
            "@type": "SoftwareApplication",
            "name": "Kumquat Search",
            "applicationCategory": "SearchApplication",
            "operatingSystem": "Web",
            "url": homepage_url,
            "description": "A search engine interface focused on fast query entry and readable result previews.",
            "creator": {
                "@type": "Organization",
                "name": settings.SITE_NAME,
            },
        },
        {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": item["question"],
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": item["answer"],
                    },
                }
                for item in SEO_FAQ_ITEMS
            ],
        },
    ]


def _serialize_search_crawl_target(target):
    return {
        "id": target.id,
        "url": target.url,
        "normalized_url": target.normalized_url,
        "scope_netloc": target.scope_netloc,
        "status": target.status,
        "max_depth": target.max_depth,
        "max_pages": target.max_pages,
        "document_count": target.document_count,
        "last_error": target.last_error,
        "queued_at": target.queued_at.isoformat() if target.queued_at else None,
        "started_at": target.started_at.isoformat() if target.started_at else None,
        "finished_at": target.finished_at.isoformat() if target.finished_at else None,
    }


def _home_search_context(search_query):
    if not search_query:
        return {
            "search_reply": "Search the web with Kumquat.",
            "search_results": [],
            "search_status_label": "Ready",
            "search_status_class": "",
        }

    return {
        "search_reply": (
            f"Search functionality for “{search_query}” is not live yet. "
            "This page is currently a placeholder for the upcoming search engine."
        ),
        "search_results": [
            {
                "title": "Kumquat Search",
                "snippet": "A clean search homepage is now in place. Result ranking, indexing, and live retrieval will be added next.",
                "url": "https://kumquat.info/",
            },
            {
                "title": "Search Infrastructure",
                "snippet": "Query parsing, indexing, crawling, and result scoring are planned but not connected to this screen yet.",
                "url": "https://kumquat.info/jobs",
            },
            {
                "title": "Product Direction",
                "snippet": "Kumquat is pivoting toward a search experience with a minimal interface centered on query entry and readable results.",
                "url": "https://github.com/kumquat-ben/kumquat",
            },
        ],
        "search_status_label": "Preview",
        "search_status_class": "search-status-pill-pending",
    }


def _is_json_request(request):
    content_type = (request.content_type or "").lower()
    accept = (request.headers.get("Accept") or "").lower()
    return "application/json" in content_type or "application/json" in accept


def _redirect_back(request, fallback):
    return HttpResponseRedirect(request.POST.get("next") or request.GET.get("next") or fallback)


def _current_user_context(request):
    if not request.user.is_authenticated:
        return None
    return _serialize_user(request.user)


def _pagination_window(page_obj):
    return range(1, page_obj.paginator.num_pages + 1)


def _home_signup_context(request):
    signup_data = request.session.get(EARLY_ACCESS_SIGNUP_SESSION_KEY, {})
    signup_success = request.session.pop(EARLY_ACCESS_SIGNUP_SUCCESS_SESSION_KEY, False)
    signup_error = request.session.pop(EARLY_ACCESS_SIGNUP_ERROR_SESSION_KEY, "")

    if request.user.is_authenticated:
        signup_data = {
            "name": signup_data.get("name") or request.user.get_full_name() or request.user.username,
            "email": signup_data.get("email") or request.user.email,
        }

    return {
        "signup_data": {
            "name": signup_data.get("name", ""),
            "email": signup_data.get("email", ""),
        },
        "signup_success": signup_success,
        "signup_error": signup_error,
    }


def _wallet_fernet():
    material = hashlib.sha256(settings.SECRET_KEY.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(material))


def _encrypt_wallet_private_key(private_key_hex):
    return _wallet_fernet().encrypt(private_key_hex.encode("utf-8")).decode("utf-8")


def _decrypt_wallet_private_key(encrypted_private_key):
    return _wallet_fernet().decrypt(encrypted_private_key.encode("utf-8")).decode("utf-8")


def _generate_wallet_material():
    private_key = Ed25519PrivateKey.generate()
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_key_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    address_bytes = hashlib.sha256(public_key_bytes).digest()
    return {
        "private_key": private_key_bytes.hex(),
        "public_key": public_key_bytes.hex(),
        "address": encode_address(address_bytes),
    }


def _serialize_wallet(wallet):
    return {
        "address": wallet.address,
        "public_key": wallet.public_key,
        "created_at": wallet.created_at.isoformat() if wallet.created_at else None,
        "updated_at": wallet.updated_at.isoformat() if wallet.updated_at else None,
    }


def _home_wallet_context(request):
    wallet = None
    if request.user.is_authenticated:
        wallet = UserWallet.objects.filter(user=request.user).first()

    return {
        "wallet": _serialize_wallet(wallet) if wallet else None,
        "wallet_private_key": request.session.pop(WALLET_PRIVATE_KEY_SESSION_KEY, ""),
        "wallet_generation_error": request.session.pop(WALLET_GENERATION_ERROR_SESSION_KEY, ""),
        "wallet_generation_success": request.session.pop(WALLET_GENERATION_SUCCESS_SESSION_KEY, False),
        "wallet_generation_status": request.session.pop(WALLET_GENERATION_STATUS_SESSION_KEY, "created"),
    }


def _explorer_api_base_url():
    return (settings.EXPLORER_API_URL or "").rstrip("/")


def _explorer_available():
    return bool(_explorer_api_base_url())


def _explorer_json_request(path, *, query=None):
    base_url = _explorer_api_base_url()
    if not base_url:
        raise RuntimeError("Explorer backend is not configured.")

    url = f"{base_url}{path}"
    if query:
        encoded_query = urlencode({key: value for key, value in query.items() if value is not None})
        if encoded_query:
            url = f"{url}?{encoded_query}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "KumquatWebsiteExplorer/0.1",
        },
    )

    try:
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        message = None
        try:
            message = json.loads(error_body or "{}").get("error")
        except json.JSONDecodeError:
            message = None
        if exc.code == 404:
            raise LookupError(message or "Explorer record not found.") from exc
        raise RuntimeError(message or f"Explorer API HTTP {exc.code}.") from exc
    except URLError as exc:
        raise RuntimeError(f"Explorer network error: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("Explorer API returned invalid JSON.") from exc


def _company_payload(company):
    return {
        "id": company.id,
        "name": company.name,
        "slug": company.slug,
        "website": company.website,
        "info": company.info,
        "source": company.source,
        "source_url": company.source_url,
        "yc_url": company.yc_url,
        "batch": company.batch,
        "status": company.status,
        "employees": company.employees,
        "location": company.location,
        "tags": company.tags,
        "linkedin_url": company.linkedin_url,
        "twitter_url": company.twitter_url,
        "cb_url": company.cb_url,
        "careers_url": company.careers_url,
        "collected_at": company.collected_at.isoformat() if company.collected_at else None,
        "created_at": company.created_at.isoformat() if company.created_at else None,
        "updated_at": company.updated_at.isoformat() if company.updated_at else None,
    }


def _job_payload(job):
    return {
        "id": job.id,
        "title": job.title,
        "slug": job.slug,
        "company": _company_payload(job.company) if job.company else None,
        "location": job.location,
        "normalized_location": job.normalized_location,
        "employment_type": job.employment_type,
        "salary": job.salary,
        "excerpt": job.excerpt,
        "description": job.description,
        "apply_url": job.apply_url,
        "source": job.source,
        "metadata": job.metadata,
        "posted_at": job.posted_at.isoformat() if job.posted_at else None,
        "view_count": job.view_count,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


def _format_cents(amount_cents):
    dollars = int(amount_cents or 0) / 100
    return f"{CURRENCY_SYMBOL}{dollars:,.2f}"


def _explorer_transaction_ui(transaction):
    transaction = dict(transaction)
    transaction["value_label"] = _format_cents(transaction.get("value_cents"))
    transaction["coin_transfer_label"] = _format_cents(transaction.get("coin_transfer_cents"))
    transaction["coin_fee_label"] = _format_cents(transaction.get("coin_fee_cents"))
    return transaction


def _explorer_block_ui(block):
    block = dict(block)
    block["hash_short"] = f"{block['hash'][:12]}...{block['hash'][-8:]}"
    block["miner_short"] = f"{block['miner_address'][:10]}...{block['miner_address'][-6:]}"
    return block


def _explorer_guess_target(query):
    raw_query = str(query or "").strip()
    if not raw_query:
        return None
    if raw_query.isdigit():
        return reverse("explorer-block", args=[raw_query])
    try:
        normalized_address = normalize_address(raw_query)
        return reverse("explorer-address", args=[normalized_address])
    except AddressCodecError:
        pass
    if EXPLORER_HASH_RE.fullmatch(raw_query):
        normalized_hash = raw_query.lower()
        return reverse("explorer-transaction", args=[normalized_hash])
    return None


def _explorer_base_context(request, *, title, description, path, query=""):
    return {
        "auth_user": _current_user_context(request),
        "explorer_available": _explorer_available(),
        "explorer_query": query,
        **_seo_context(
            request,
            title=title,
            description=description,
            path=path,
        ),
    }


def _upsert_user_wallet(*, user, wallet_material, replace_existing):
    existing_wallet = UserWallet.objects.filter(user=user).first()
    encrypted_private_key = _encrypt_wallet_private_key(wallet_material["private_key"])

    if existing_wallet and not replace_existing:
        return existing_wallet, False, False

    if existing_wallet:
        existing_wallet.address = wallet_material["address"]
        existing_wallet.public_key = wallet_material["public_key"]
        existing_wallet.encrypted_private_key = encrypted_private_key
        existing_wallet.save(update_fields=["address", "public_key", "encrypted_private_key", "updated_at"])
        return existing_wallet, False, True

    wallet = UserWallet.objects.create(
        user=user,
        address=wallet_material["address"],
        public_key=wallet_material["public_key"],
        encrypted_private_key=encrypted_private_key,
    )
    return wallet, True, False


def _build_dashboard_context(request=None):
    User = get_user_model()
    users = [_serialize_user(user) for user in User.objects.order_by("-date_joined", "username")]
    signups = [
        {
            "email": signup.email,
            "name": signup.name,
            "created_at": signup.created_at,
            "updated_at": signup.updated_at,
        }
        for signup in EarlyAccessSignup.objects.order_by("-created_at")
    ]
    recent_sms = [
        _serialize_vonage_sms(message)
        for message in VonageInboundSms.objects.order_by("-received_at", "-created_at")[:10]
    ]
    auth_context = _launcher_auth_from_request(request) if request is not None else None
    managed_nodes = [_serialize_managed_node(node) for node in _load_managed_nodes(auth_context)]
    launcher_auth = auth_context or {}
    return {
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
        "recent_sms": recent_sms,
        "managed_nodes": managed_nodes,
        "launcher": {
            "enabled": launcher_enabled(),
            "default_image": settings.NODE_LAUNCHER_IMAGE,
            "default_network": settings.NODE_LAUNCHER_NETWORK,
            "default_chain_id": settings.NODE_LAUNCHER_CHAIN_ID,
            "default_namespace": settings.NODE_LAUNCHER_KUBERNETES_NAMESPACE,
            "auth_mode": (
                "bearer"
                if launcher_auth.get("NODE_LAUNCHER_KUBE_API_SERVER")
                and launcher_auth.get("NODE_LAUNCHER_KUBE_BEARER_TOKEN")
                else "kubeconfig"
                if launcher_auth.get("NODE_LAUNCHER_KUBECONFIG") or settings.NODE_LAUNCHER_KUBECONFIG
                else "environment"
            ),
            "auth_configured": bool(
                launcher_auth.get("NODE_LAUNCHER_KUBE_API_SERVER")
                and launcher_auth.get("NODE_LAUNCHER_KUBE_BEARER_TOKEN")
            ),
            "auth_api_server": launcher_auth.get("NODE_LAUNCHER_KUBE_API_SERVER", ""),
            "auth_namespace": launcher_auth.get("namespace") or settings.NODE_LAUNCHER_KUBERNETES_NAMESPACE,
            "auth_token_masked": _mask_launcher_token(
                launcher_auth.get("NODE_LAUNCHER_KUBE_BEARER_TOKEN", "")
            ),
        },
    }


def home_page_view(request):
    search_query = (request.GET.get("q") or "").strip()
    search_context = _home_search_context(search_query)
    context = {
        "auth_user": _current_user_context(request),
        "search_query": search_query,
        "search_submitted": bool(search_query),
        **search_context,
        **_seo_context(
            request,
            title="Kumquat | Search",
            description=(
                "Kumquat is building a search engine with a minimal homepage focused on query input and readable results."
            ),
            path=reverse("home"),
            structured_data=_home_structured_data(),
        ),
    }
    return render(request, "website/home.html", context)


def jobs_page_view(request):
    query = (request.GET.get("q") or "").strip()
    jobs = JobListing.objects.filter(is_active=True).select_related("company")
    if query:
        jobs = jobs.filter(
            Q(title__icontains=query)
            | Q(location__icontains=query)
            | Q(description__icontains=query)
            | Q(company__name__icontains=query)
        )

    jobs = jobs.order_by("-posted_at", "-created_at")[:50]
    context = {
        "jobs": jobs,
        "job_query": query,
        **_seo_context(
            request,
            title="Jobs | Kumquat",
            description="Browse Kumquat job listings imported from Athena-style backend features.",
            path=reverse("jobs"),
        ),
    }
    return render(request, "website/jobs.html", context)


def job_detail_page_view(request, slug):
    job = JobListing.objects.select_related("company").filter(slug=slug, is_active=True).first()
    if job is None:
        return render(
            request,
            "website/job_detail.html",
            {
                "job": None,
                "page_error": "Job not found.",
                **_seo_context(
                    request,
                    title="Job Not Found | Kumquat",
                    description="Requested job listing was not found.",
                    path=reverse("job-detail", args=[slug]),
                    index=False,
                ),
            },
            status=404,
        )

    JobListing.objects.filter(pk=job.pk).update(view_count=job.view_count + 1)
    job.view_count += 1
    context = {
        "job": job,
        **_seo_context(
            request,
            title=f"{job.title} | Kumquat Jobs",
            description=job.excerpt or job.description[:160] or "Job listing",
            path=reverse("job-detail", args=[slug]),
        ),
    }
    return render(request, "website/job_detail.html", context)


def companies_page_view(request):
    query = (request.GET.get("q") or "").strip()
    companies = CompanyProfile.objects.all()
    if query:
        companies = companies.filter(
            Q(name__icontains=query)
            | Q(location__icontains=query)
            | Q(info__icontains=query)
            | Q(tags__icontains=query)
        )

    companies = companies.order_by("name")[:100]
    context = {
        "companies": companies,
        "company_query": query,
        **_seo_context(
            request,
            title="Companies | Kumquat",
            description="Browse Kumquat company profiles imported from Athena-style backend features.",
            path=reverse("companies"),
        ),
    }
    return render(request, "website/companies.html", context)


def company_detail_page_view(request, slug):
    company = CompanyProfile.objects.filter(slug=slug).first()
    if company is None:
        return render(
            request,
            "website/company_detail.html",
            {
                "company": None,
                "page_error": "Company not found.",
                **_seo_context(
                    request,
                    title="Company Not Found | Kumquat",
                    description="Requested company profile was not found.",
                    path=reverse("company-detail", args=[slug]),
                    index=False,
                ),
            },
            status=404,
        )

    jobs = company.job_listings.filter(is_active=True).order_by("-posted_at", "-created_at")[:25]
    context = {
        "company": company,
        "jobs": jobs,
        **_seo_context(
            request,
            title=f"{company.name} | Kumquat Companies",
            description=company.info[:160] or f"Company profile for {company.name}.",
            path=reverse("company-detail", args=[slug]),
        ),
    }
    return render(request, "website/company_detail.html", context)


def jobs_api_view(request):
    query = (request.GET.get("q") or "").strip()
    jobs = JobListing.objects.filter(is_active=True).select_related("company")
    if query:
        jobs = jobs.filter(
            Q(title__icontains=query)
            | Q(location__icontains=query)
            | Q(description__icontains=query)
            | Q(company__name__icontains=query)
        )
    return JsonResponse({"results": [_job_payload(job) for job in jobs[:100]]})


def companies_api_view(request):
    query = (request.GET.get("q") or "").strip()
    companies = CompanyProfile.objects.all()
    if query:
        companies = companies.filter(
            Q(name__icontains=query)
            | Q(location__icontains=query)
            | Q(info__icontains=query)
            | Q(tags__icontains=query)
        )
    return JsonResponse({"results": [_company_payload(company) for company in companies[:100]]})


def explorer_home_page_view(request):
    search_query = (request.GET.get("q") or "").strip()
    if search_query:
        target_url = _explorer_guess_target(search_query)
        if target_url:
            return redirect(target_url)

    context = {
        **_explorer_base_context(
            request,
            title="Explorer | Kumquat",
            description="Public Kumquat block explorer for recent blocks, transactions, and wallet addresses.",
            path=reverse("explorer"),
            query=search_query,
        ),
        "summary": None,
        "search_error": "",
        "explorer_error": "",
    }

    if search_query:
        context["search_error"] = (
            "Enter a block height, a 64-character block or transaction hash, or a Kumquat wallet address."
        )

    if not _explorer_available():
        context["explorer_error"] = "Explorer backend is not configured yet."
        return render(request, "website/explorer_home.html", context)

    try:
        summary = _explorer_json_request("/api/explorer/summary", query={"blocks": 12, "transactions": 20})
    except (LookupError, RuntimeError) as exc:
        context["explorer_error"] = str(exc)
        return render(request, "website/explorer_home.html", context)

    summary["recent_blocks"] = [_explorer_block_ui(block) for block in summary.get("recent_blocks", [])]
    summary["latest_block"] = summary["recent_blocks"][0] if summary["recent_blocks"] else None
    summary["recent_transactions"] = [
        _explorer_transaction_ui(transaction)
        for transaction in summary.get("recent_transactions", [])
    ]
    context["summary"] = summary
    return render(request, "website/explorer_home.html", context)


def explorer_block_page_view(request, identifier):
    context = {
        **_explorer_base_context(
            request,
            title=f"Block {identifier} | Kumquat Explorer",
            description="Kumquat block detail and included transactions.",
            path=reverse("explorer-block", args=[identifier]),
        ),
        "block_detail": None,
        "explorer_error": "",
    }

    if not _explorer_available():
        context["explorer_error"] = "Explorer backend is not configured yet."
        return render(request, "website/explorer_block.html", context, status=503)

    try:
        block_detail = _explorer_json_request(f"/api/explorer/blocks/{quote(str(identifier), safe='')}")
    except LookupError:
        context["explorer_error"] = "Block not found."
        return render(request, "website/explorer_block.html", context, status=404)
    except RuntimeError as exc:
        context["explorer_error"] = str(exc)
        return render(request, "website/explorer_block.html", context, status=502)

    block_detail["block"] = _explorer_block_ui(block_detail["block"])
    block_detail["transactions"] = [
        _explorer_transaction_ui(transaction)
        for transaction in block_detail.get("transactions", [])
    ]
    context["block_detail"] = block_detail
    return render(request, "website/explorer_block.html", context)


def explorer_transaction_page_view(request, tx_hash):
    context = {
        **_explorer_base_context(
            request,
            title=f"Transaction {tx_hash[:12]} | Kumquat Explorer",
            description="Kumquat transaction detail.",
            path=reverse("explorer-transaction", args=[tx_hash]),
        ),
        "transaction_detail": None,
        "explorer_error": "",
    }

    if not _explorer_available():
        context["explorer_error"] = "Explorer backend is not configured yet."
        return render(request, "website/explorer_transaction.html", context, status=503)

    try:
        transaction_detail = _explorer_json_request(f"/api/explorer/transactions/{quote(str(tx_hash), safe='')}")
    except LookupError:
        context["explorer_error"] = "Transaction not found."
        return render(request, "website/explorer_transaction.html", context, status=404)
    except RuntimeError as exc:
        context["explorer_error"] = str(exc)
        return render(request, "website/explorer_transaction.html", context, status=502)

    transaction_detail["transaction"] = _explorer_transaction_ui(transaction_detail["transaction"])
    context["transaction_detail"] = transaction_detail
    return render(request, "website/explorer_transaction.html", context)


def explorer_address_page_view(request, address):
    context = {
        **_explorer_base_context(
            request,
            title=f"Address {address[:12]} | Kumquat Explorer",
            description="Kumquat address activity and wallet state.",
            path=reverse("explorer-address", args=[address]),
        ),
        "address_data": None,
        "explorer_error": "",
    }

    if not _explorer_available():
        context["explorer_error"] = "Explorer backend is not configured yet."
        return render(request, "website/explorer_address.html", context, status=503)

    try:
        address_data = _explorer_json_request(
            f"/api/explorer/addresses/{quote(str(address), safe='')}",
            query={"transactions": 50},
        )
    except RuntimeError as exc:
        message = str(exc)
        status = 404 if "not found" in message.lower() else 502
        context["explorer_error"] = message
        return render(request, "website/explorer_address.html", context, status=status)

    if address_data.get("account"):
        account = dict(address_data["account"])
        account["balance_label"] = _format_cents(account.get("balance_cents"))
        account["bill_value_label"] = _format_cents(account.get("bill_value_cents"))
        account["coin_value_label"] = _format_cents(account.get("coin_value_cents"))
        for item in account.get("bill_breakdown", []):
            item["value_label"] = _format_cents(item.get("value_cents"))
        for item in account.get("coin_breakdown", []):
            item["value_label"] = _format_cents(item.get("value_cents"))
        address_data["account"] = account
    address_data["transactions"] = [
        _explorer_transaction_ui(transaction)
        for transaction in address_data.get("transactions", [])
    ]
    context["address_data"] = address_data
    return render(request, "website/explorer_address.html", context)


def search_crawl_enqueue_view(request):
    if not request.user.is_authenticated:
        return JsonResponse({"error": "Authentication required."}, status=401)

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if "application/json" in (request.content_type or "").lower():
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON body."}, status=400)
    else:
        payload = request.POST

    raw_url = str(payload.get("url") or "").strip()
    if not raw_url:
        return JsonResponse({"error": "A crawl URL is required."}, status=400)

    try:
        normalized_url = normalize_crawl_url(raw_url)
    except SearchCrawlerError as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    try:
        max_depth = max(0, min(int(payload.get("max_depth", 1)), 3))
        max_pages = max(1, min(int(payload.get("max_pages", 25)), 100))
    except (TypeError, ValueError):
        return JsonResponse({"error": "max_depth and max_pages must be integers."}, status=400)

    parsed_url = urlparse(normalized_url)
    target, _ = SearchCrawlTarget.objects.update_or_create(
        normalized_url=normalized_url,
        defaults={
            "url": normalized_url,
            "scope_netloc": parsed_url.netloc.lower(),
            "status": SearchCrawlTarget.STATUS_QUEUED,
            "max_depth": max_depth,
            "max_pages": max_pages,
            "created_by": request.user,
            "last_error": "",
            "queued_at": datetime.now(timezone.utc),
            "started_at": None,
            "finished_at": None,
        },
    )
    dispatch_mode = schedule_crawl_search_target(target.id)
    target.refresh_from_db()

    return JsonResponse(
        {
            "status": "ok",
            "dispatch_mode": dispatch_mode,
            "target": _serialize_search_crawl_target(target),
        },
        status=201,
    )


def sign_in_page_view(request):
    return render(
        request,
        "website/sign_in.html",
        {
            "auth_user": _current_user_context(request),
            "oauth_configured": _oauth_is_configured(),
            **_seo_context(
                request,
                title="Sign In | Kumquat",
                description="Sign in to Kumquat to create a wallet, access early product releases, and manage your account.",
                path=reverse("sign-in"),
                index=False,
            ),
        },
    )


def admin_containers_page_view(request):
    if not request.user.is_authenticated:
        return HttpResponseRedirect("/auth/sign-in")

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    auth_context = _launcher_auth_from_request(request)
    dashboard = _build_dashboard_context(request)
    runtime_containers = _load_runtime_containers(auth_context)
    return render(
        request,
        "website/containers.html",
        {
            "auth_user": _current_user_context(request),
            "dashboard": dashboard,
            "runtime_containers": runtime_containers,
            **_seo_context(
                request,
                title="Containers | Kumquat",
                description="Internal Kumquat container and deployment management.",
                path=reverse("containers"),
                index=False,
            ),
        },
    )


def admin_vonage_sms_page_view(request):
    if not request.user.is_authenticated:
        return HttpResponseRedirect("/auth/sign-in")

    if not request.user.is_superuser:
        return JsonResponse({"error": "Superuser access required."}, status=403)

    messages_data = [
        {
            **_serialize_vonage_sms(message),
            "payload_pretty": json.dumps(message.payload, indent=2, sort_keys=True),
        }
        for message in VonageInboundSms.objects.order_by("-received_at", "-created_at")
    ]
    page_obj = Paginator(messages_data, 8).get_page(request.GET.get("page") or 1)
    selected_id = request.GET.get("selected")
    selected_message = None
    if selected_id:
        try:
            selected_id = int(selected_id)
        except ValueError:
            selected_id = None
    if selected_id is not None:
        selected_message = next((message for message in messages_data if message["id"] == selected_id), None)
    if selected_message is None:
        selected_message = page_obj.object_list[0] if page_obj.object_list else None

    return render(
        request,
        "website/sms.html",
        {
            "auth_user": _current_user_context(request),
            "messages": messages_data,
            "stats": {
                "messages": len(messages_data),
                "signed_messages": sum(1 for message in messages_data if message["signature_valid"] is True),
                "unsigned_messages": sum(1 for message in messages_data if not message["signature"]),
                "failed_signatures": sum(1 for message in messages_data if message["signature_valid"] is False),
            },
            "page_obj": page_obj,
            "page_numbers": _pagination_window(page_obj),
            "selected_message": selected_message,
            **_seo_context(
                request,
                title="SMS Inbox | Kumquat",
                description="Internal Kumquat SMS inbox.",
                path=reverse("sms"),
                index=False,
            ),
        },
    )


def _serialize_user(user):
    full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    try:
        wallet = user.wallet
    except UserWallet.DoesNotExist:
        wallet = None
    return {
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "full_name": full_name or user.username,
        "username": user.username,
        "wallet_address": wallet.address if wallet else "",
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


def _normalize_reward_address(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""

    try:
        return normalize_address(raw_value)
    except AddressCodecError as exc:
        raise ValueError(
            f"Reward address must be a valid Kumquat wallet address (kmq1...). {exc}"
        ) from exc


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
        "reward_address": node.reward_address,
        "enable_mining": node.enable_mining,
        "mining_threads": node.mining_threads,
        "api_port": node.api_port,
        "p2p_port": node.p2p_port,
        "metrics_port": node.metrics_port,
        "status": node.status,
        "last_error": node.last_error,
        "logs_tail": node.last_logs,
        "dashboard_url": dashboard_subdomain_url(node),
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


def _load_managed_nodes(auth_context=None):
    nodes = list(ManagedNode.objects.select_related("launched_by").order_by("-created_at"))
    hydrated = []
    for node in nodes:
        try:
            hydrated.append(refresh_node(node, auth_context))
        except NodeLauncherError:
            hydrated.append(node)
    return hydrated


def _load_runtime_containers(auth_context=None):
    try:
        runtime = list_runtime_containers(auth_context)
    except NodeLauncherError:
        return []
    managed_nodes = {node.id: node for node in _load_managed_nodes(auth_context)}
    for item in runtime:
        node_id = item.get("managed_node_id")
        managed_node = managed_nodes.get(node_id) if node_id else None
        if managed_node is not None:
            item["managed_node_name"] = managed_node.display_name
            item["dashboard_url"] = dashboard_subdomain_url(managed_node)
            item["dashboard_proxy_url"] = dashboard_proxy_path(managed_node)
    return sorted(runtime, key=lambda item: (item["managed_node_name"] or item["name"]).lower())


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
      .section {{
        margin-top: 28px;
      }}
      .section:first-child {{
        margin-top: 0;
      }}
      .section-heading {{
        margin: 0 0 14px;
        font-size: 22px;
        font-weight: 700;
      }}
      .form-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 14px;
      }}
      .field {{
        display: grid;
        gap: 8px;
      }}
      .field label {{
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .field input {{
        min-height: 46px;
        padding: 0.8rem 0.95rem;
        border: 1px solid var(--border);
        border-radius: 14px;
        background: rgba(255, 255, 255, 0.92);
        color: var(--ink);
        font: inherit;
      }}
      .checkbox-row {{
        display: flex;
        align-items: center;
        gap: 10px;
        margin-top: 10px;
        color: var(--muted);
      }}
      .checkbox-row input {{
        width: 18px;
        height: 18px;
      }}
      .actions {{
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 18px;
      }}
      .button-plain {{
        font: inherit;
        cursor: pointer;
      }}
      .meta {{
        margin: 0;
        color: var(--muted);
        font-size: 14px;
        line-height: 1.6;
      }}
      .node-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 16px;
      }}
      .node-card {{
        padding: 20px;
      }}
      .node-card h3 {{
        margin: 0 0 8px;
        font-size: 24px;
      }}
      .node-card pre {{
        margin-top: 12px;
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
        <a class="button" href="/dashboard">Dashboard</a>
        <a class="button" href="/containers">Containers</a>
        <a class="button" href="/sms">SMS Inbox</a>
        <a class="button button-primary" href="/auth/logout">Sign out</a>
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
      const ADMIN_NODE_LAUNCH_URL = "/nodes/launch";
      const ADMIN_NODE_LAUNCHER_AUTH_URL = "/nodes/launcher-auth";

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

      function renderManagedNodes(payload) {{
        const nodes = payload.managed_nodes || [];
        const launcher = payload.launcher || {{}};
        const nodeCards = nodes.map((node) => `
          <div class="card node-card">
            <p class="label">Managed Node</p>
            <h3>${{escapeHtml(node.display_name || node.name)}}</h3>
            <p class="meta">Status: <strong>${{escapeHtml(node.status || "unknown")}}</strong></p>
            <p class="meta">Launched: ${{escapeHtml(formatDate(node.launched_at))}}</p>
            <p class="meta">API port: <code>${{escapeHtml(node.api_port)}}</code> | P2P port: <code>${{escapeHtml(node.p2p_port)}}</code></p>
            <p class="meta">Image: <code>${{escapeHtml(node.image || "Unavailable")}}</code></p>
            <p class="meta">Mining: ${{escapeHtml(node.enable_mining ? "enabled" : "disabled")}}</p>
            <p class="meta">Reward address: <code>${{escapeHtml(node.reward_address || "node-derived default")}}</code></p>
            ${{node.last_error ? `<p class="error">${{escapeHtml(node.last_error)}}</p>` : ""}}
            <div class="actions">
              <a class="button" href="${{escapeHtml(node.dashboard_proxy_url || node.dashboard_url || "#")}}">Open GUI</a>
            </div>
            <pre>${{escapeHtml(node.logs_tail || "No logs available yet.")}}</pre>
          </div>
        `).join("");

        return `
          <div class="section panel">
            <p class="label">Node Launcher</p>
            <h2 class="section-heading">Launch managed node</h2>
            <p class="meta">Default image: <code>${{escapeHtml(launcher.default_image || "Unavailable")}}</code></p>
            <p class="meta">Kubernetes auth: <strong>${{escapeHtml(launcher.auth_mode || "environment")}}</strong>${{launcher.auth_api_server ? ` via <code>${{escapeHtml(launcher.auth_api_server)}}</code>` : ""}}</p>
            <p class="meta">Namespace: <code>${{escapeHtml(launcher.auth_namespace || launcher.default_namespace || "kumquat")}}</code></p>
            <form id="managed-node-auth-form">
              <div class="form-grid">
                <div class="field">
                  <label for="launcher-api-server">API server</label>
                  <input id="launcher-api-server" name="api_server" placeholder="https://cluster.example:6443" value="${{escapeHtml(launcher.auth_api_server || "")}}" />
                </div>
                <div class="field">
                  <label for="launcher-namespace">Namespace</label>
                  <input id="launcher-namespace" name="namespace" placeholder="kumquat" value="${{escapeHtml(launcher.auth_namespace || launcher.default_namespace || "kumquat")}}" />
                </div>
                <div class="field">
                  <label for="launcher-token">Bearer token</label>
                  <input id="launcher-token" name="bearer_token" type="password" placeholder="${{escapeHtml(launcher.auth_token_masked || "Paste service-account token")}}" />
                </div>
                <div class="field">
                  <label for="launcher-ca-cert">CA cert (base64)</label>
                  <input id="launcher-ca-cert" name="ca_cert_b64" placeholder="LS0tLS1CRUdJTi..." />
                </div>
              </div>
              <div class="actions">
                <button class="button button-secondary button-plain" type="submit">Save launcher auth</button>
                <button id="managed-node-auth-clear" class="button button-plain" type="button">Clear auth</button>
                <span id="managed-node-auth-status" class="status">${{launcher.auth_configured ? "Session bearer auth saved." : "Using environment or kubeconfig auth until overridden."}}</span>
              </div>
            </form>
            <form id="managed-node-launch-form">
              <div class="form-grid">
                <div class="field">
                  <label for="node-display-name">Display name</label>
                  <input id="node-display-name" name="display_name" placeholder="Managed Node ${{
                    escapeHtml((payload.stats?.managed_nodes || 0) + 1)
                  }}" />
                </div>
                <div class="field">
                  <label for="node-chain-id">Chain ID</label>
                  <input id="node-chain-id" name="chain_id" type="number" min="0" value="${{escapeHtml(launcher.default_chain_id || 1337)}}" />
                </div>
                <div class="field">
                  <label for="node-reward-address">Reward address</label>
                  <input id="node-reward-address" name="reward_address" placeholder="kmq1... wallet address" />
                </div>
                <div class="field">
                  <label for="node-mining-threads">Mining threads</label>
                  <input id="node-mining-threads" name="mining_threads" type="number" min="1" value="1" />
                </div>
              </div>
              <label class="checkbox-row">
                <input name="enable_mining" type="checkbox" />
                <span>Enable mining on launch</span>
              </label>
              <div class="actions">
                <button class="button button-primary button-plain" type="submit" ${{launcher.enabled ? "" : "disabled"}}>Launch Node</button>
                <span id="managed-node-launch-status" class="status">${{launcher.enabled ? "Launcher ready." : "Launcher is disabled in backend configuration."}}</span>
              </div>
            </form>
          </div>
          <div class="section">
            <h2 class="section-heading">Managed nodes</h2>
            <div class="node-grid">
              ${{
                nodeCards || '<div class="card node-card"><p class="meta">No managed nodes have been launched yet.</p></div>'
              }}
            </div>
          </div>
        `;
      }}

      function attachDashboardHandlers() {{
        const launchForm = document.getElementById("managed-node-launch-form");
        const launchStatus = document.getElementById("managed-node-launch-status");
        const authForm = document.getElementById("managed-node-auth-form");
        const authStatus = document.getElementById("managed-node-auth-status");
        const authClear = document.getElementById("managed-node-auth-clear");
        if (!launchForm || !launchStatus) {{
          return;
        }}

        if (authForm && authStatus) {{
          authForm.addEventListener("submit", async (event) => {{
            event.preventDefault();
            const formData = new FormData(authForm);
            const payload = {{
              api_server: (formData.get("api_server") || "").toString().trim(),
              namespace: (formData.get("namespace") || "").toString().trim(),
              bearer_token: (formData.get("bearer_token") || "").toString().trim(),
              ca_cert_b64: (formData.get("ca_cert_b64") || "").toString().trim(),
            }};
            authStatus.textContent = "Saving launcher auth...";
            try {{
              const response = await fetch(ADMIN_NODE_LAUNCHER_AUTH_URL, {{
                method: "POST",
                credentials: "same-origin",
                headers: {{
                  "Content-Type": "application/json",
                }},
                body: JSON.stringify(payload),
              }});
              const data = await response.json().catch(() => ({{}}));
              if (!response.ok) {{
                throw new Error(data.error || "Failed to save launcher auth.");
              }}
              authStatus.textContent = "Launcher auth saved. Refreshing dashboard...";
              await loadDashboard();
            }} catch (error) {{
              authStatus.textContent = error.message;
            }}
          }});
        }}

        if (authClear && authStatus) {{
          authClear.addEventListener("click", async () => {{
            authStatus.textContent = "Clearing launcher auth...";
            try {{
              const response = await fetch(ADMIN_NODE_LAUNCHER_AUTH_URL, {{
                method: "POST",
                credentials: "same-origin",
                headers: {{
                  "Content-Type": "application/json",
                }},
                body: JSON.stringify({{ action: "clear" }}),
              }});
              const data = await response.json().catch(() => ({{}}));
              if (!response.ok) {{
                throw new Error(data.error || "Failed to clear launcher auth.");
              }}
              authStatus.textContent = "Launcher auth cleared. Refreshing dashboard...";
              await loadDashboard();
            }} catch (error) {{
              authStatus.textContent = error.message;
            }}
          }});
        }}

        launchForm.addEventListener("submit", async (event) => {{
          event.preventDefault();
          const formData = new FormData(launchForm);
          const payload = {{
            display_name: (formData.get("display_name") || "").toString().trim(),
            chain_id: Number(formData.get("chain_id") || 0),
            reward_address: (formData.get("reward_address") || "").toString().trim(),
            mining_threads: Number(formData.get("mining_threads") || 1),
            enable_mining: formData.get("enable_mining") === "on",
          }};

          launchStatus.textContent = "Launching node...";
          try {{
            const response = await fetch(ADMIN_NODE_LAUNCH_URL, {{
              method: "POST",
              credentials: "same-origin",
              headers: {{
                "Content-Type": "application/json",
              }},
              body: JSON.stringify(payload),
            }});
            const data = await response.json().catch(() => ({{}}));
            if (!response.ok) {{
              throw new Error(data.error || "Failed to launch managed node.");
            }}
            launchStatus.textContent = "Node launched. Refreshing dashboard...";
            await loadDashboard();
          }} catch (error) {{
            launchStatus.textContent = error.message;
          }}
        }});
      }}

      async function loadDashboard() {{
        const response = await fetch(endpoint, {{ credentials: "same-origin" }});
        const payload = await response.json().catch(() => ({{}}));
        if (!response.ok) {{
          throw new Error(payload.error || "Failed to load admin data.");
        }}

        if (endpoint.includes("/sms/data")) {{
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
            <div class="card"><p class="label">Managed Nodes</p><p class="value">${{escapeHtml(payload.stats?.managed_nodes ?? 0)}}</p></div>
            <div class="card"><p class="label">Running Nodes</p><p class="value">${{escapeHtml(payload.stats?.running_nodes ?? 0)}}</p></div>
          </div>
          ${{
            renderManagedNodes(payload)
          }}
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
        attachDashboardHandlers();
      }}

      loadDashboard()
        .then(() => {{
          return;
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

    absolute_callback_url = request.build_absolute_uri("/auth/google/callback")
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

        return f"{parsed.scheme}://{parsed.netloc}/auth/google/callback"

    return "http://localhost:8000/auth/google/callback"


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


def _complete_google_oauth(request, *, code, returned_state):
    if not _oauth_is_configured():
        raise ValueError("Google OAuth is not configured.")

    saved_state = request.session.pop(GOOGLE_OAUTH_STATE_SESSION_KEY, "")
    redirect_uri = request.session.pop(
        GOOGLE_OAUTH_REDIRECT_URI_SESSION_KEY,
        settings.GOOGLE_OAUTH_REDIRECT_URI,
    )
    if not saved_state or returned_state != saved_state:
        raise ValueError("OAuth state mismatch.")

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
    login(request, user, backend="django.contrib.auth.backends.ModelBackend")
    return user, full_name


def google_oauth_start_view(request):
    if not _oauth_is_configured():
        if _is_json_request(request):
            return JsonResponse({"error": "Google OAuth is not configured."}, status=503)
        messages.error(request, "Google OAuth is not configured.")
        return HttpResponseRedirect("/auth/sign-in")

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
    error = (request.GET.get("error") or "").strip()
    code = (request.GET.get("code") or "").strip()
    state = (request.GET.get("state") or "").strip()
    status = "loading"
    message = "Finishing your Kumquat session..."
    resolved_user = None

    if error:
        status = "error"
        message = "Google sign-in was canceled or denied."
    elif not code or not state:
        status = "error"
        message = "Missing OAuth parameters from Google."
    else:
        try:
            user, full_name = _complete_google_oauth(request, code=code, returned_state=state)
            resolved_user = _serialize_user(user)
            resolved_user["full_name"] = full_name or user.username
            status = "success"
            message = f"Signed in as {resolved_user['full_name']}."
        except Exception:
            status = "error"
            message = "Google sign-in failed."

    return render(
        request,
        "website/callback.html",
        {
            "auth_user": _current_user_context(request),
            "status": status,
            "message": message,
            "resolved_user": resolved_user,
            **_seo_context(
                request,
                title="Authentication | Kumquat",
                description="Google authentication callback for Kumquat.",
                path=reverse("auth-google-callback"),
                index=False,
            ),
        },
    )


def robots_txt_view(_request):
    sitemap_url = _absolute_url(reverse("sitemap"))
    content = "\n".join(
        [
            "User-agent: *",
            "Allow: /",
            "Disallow: /auth/",
            "Disallow: /dashboard",
            "Disallow: /sms",
            "Disallow: /nodes/",
            "Disallow: /messages",
            "Disallow: /wallets/",
            "Disallow: /webhooks/",
            "Disallow: /admin/",
            f"Sitemap: {sitemap_url}",
            "",
        ]
    )
    return HttpResponse(content, content_type="text/plain")


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

    if payload.get("error"):
        return JsonResponse({"error": payload.get("error")}, status=400)

    code = (payload.get("code") or "").strip()
    returned_state = (payload.get("state") or "").strip()
    if not code:
        return JsonResponse({"error": "Missing Google authorization code."}, status=400)

    try:
        user, full_name = _complete_google_oauth(request, code=code, returned_state=returned_state)
    except Exception:
        return JsonResponse({"error": "Google OAuth exchange failed."}, status=502)

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
    if _is_json_request(request):
        return JsonResponse({"status": "ok"})
    return _redirect_back(request, "/")


@csrf_exempt
def wallet_generate_view(request):
    return _wallet_write_view(request, replace_existing=False)


@csrf_exempt
def wallet_regenerate_view(request):
    return _wallet_write_view(request, replace_existing=True)


def _wallet_write_view(request, *, replace_existing):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if not request.user.is_authenticated:
        if _is_json_request(request):
            return JsonResponse({"error": "Authentication required."}, status=401)
        return HttpResponseRedirect("/auth/sign-in")

    existing_wallet = UserWallet.objects.filter(user=request.user).first()
    if existing_wallet and not replace_existing:
        error_message = "You already have a wallet."
        if _is_json_request(request):
            return JsonResponse({"error": error_message, "wallet": _serialize_wallet(existing_wallet)}, status=409)
        request.session[WALLET_GENERATION_ERROR_SESSION_KEY] = error_message
        return HttpResponseRedirect("/#wallet")

    wallet_material = _generate_wallet_material()
    try:
        wallet, created, regenerated = _upsert_user_wallet(
            user=request.user,
            wallet_material=wallet_material,
            replace_existing=replace_existing,
        )
    except IntegrityError:
        wallet = UserWallet.objects.filter(user=request.user).first()
        error_message = "Could not regenerate wallet because the new address collided. Try again."
        if _is_json_request(request):
            return JsonResponse({"error": error_message, "wallet": _serialize_wallet(wallet)}, status=409)
        request.session[WALLET_GENERATION_ERROR_SESSION_KEY] = error_message
        return HttpResponseRedirect("/#wallet")

    if _is_json_request(request):
        return JsonResponse(
            {
                "status": "created" if created else "regenerated" if regenerated else "existing",
                "wallet": {
                    **_serialize_wallet(wallet),
                    "private_key": wallet_material["private_key"],
                },
            },
            status=201 if created else 200,
        )

    request.session[WALLET_PRIVATE_KEY_SESSION_KEY] = wallet_material["private_key"]
    request.session[WALLET_GENERATION_SUCCESS_SESSION_KEY] = True
    request.session[WALLET_GENERATION_STATUS_SESSION_KEY] = "created" if created else "regenerated"
    return HttpResponseRedirect("/#wallet")


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

    if _is_json_request(request):
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON body."}, status=400)
    else:
        payload = request.POST

    email = (payload.get("email") or "").strip().lower()
    name = (payload.get("name") or "").strip()

    if not email:
        if _is_json_request(request):
            return JsonResponse({"error": "Email is required."}, status=400)
        request.session[EARLY_ACCESS_SIGNUP_SESSION_KEY] = {"name": name, "email": email}
        request.session[EARLY_ACCESS_SIGNUP_ERROR_SESSION_KEY] = "Email is required."
        return HttpResponseRedirect("/#story")

    try:
        validate_email(email)
    except ValidationError:
        if _is_json_request(request):
            return JsonResponse({"error": "Enter a valid email address."}, status=400)
        request.session[EARLY_ACCESS_SIGNUP_SESSION_KEY] = {"name": name, "email": email}
        request.session[EARLY_ACCESS_SIGNUP_ERROR_SESSION_KEY] = "Enter a valid email address."
        return HttpResponseRedirect("/#story")

    signup, created = EarlyAccessSignup.objects.update_or_create(
        email=email,
        defaults={"name": name},
    )

    response_payload = {
        "status": "created" if created else "updated",
        "signup": {
            "email": signup.email,
            "name": signup.name,
            "created_at": signup.created_at.isoformat(),
        },
    }
    if _is_json_request(request):
        return JsonResponse(response_payload, status=201 if created else 200)

    request.session[EARLY_ACCESS_SIGNUP_SESSION_KEY] = {"name": signup.name, "email": signup.email}
    request.session[EARLY_ACCESS_SIGNUP_SUCCESS_SESSION_KEY] = True
    return HttpResponseRedirect("/#story")


def admin_dashboard_view(request):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    return JsonResponse(_build_dashboard_context(request))


@csrf_exempt
def admin_node_launcher_auth_view(request):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if _is_json_request(request):
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON body."}, status=400)
    else:
        payload = request.POST

    action = (payload.get("action") or "save").strip().lower()
    if action == "clear":
        request.session.pop(NODE_LAUNCHER_AUTH_SESSION_KEY, None)
        request.session.modified = True
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "cleared": True})
        messages.success(request, "Launcher Kubernetes auth cleared from this session.")
        return HttpResponseRedirect("/dashboard")

    _save_launcher_auth(request, payload)
    if _is_json_request(request):
        return JsonResponse({"status": "ok", "saved": True})
    messages.success(request, "Launcher Kubernetes auth saved for this session.")
    return HttpResponseRedirect("/dashboard")


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
        bootstrap_url="/dashboard/data",
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
        if _is_json_request(request):
            return JsonResponse({"error": "Node launcher is disabled."}, status=503)
        messages.error(request, "Node launcher is disabled.")
        return HttpResponseRedirect("/dashboard")

    if _is_json_request(request):
        try:
            payload = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON body."}, status=400)
    else:
        payload = request.POST

    try:
        reward_address = _normalize_reward_address(payload.get("reward_address"))
    except ValueError as exc:
        if _is_json_request(request):
            return JsonResponse({"error": str(exc)}, status=400)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/dashboard")

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
        reward_address=reward_address,
        enable_mining=bool(payload.get("enable_mining")),
        mining_threads=_parse_positive_int(payload.get("mining_threads")) or 1,
        api_port=ports["api_port"],
        p2p_port=ports["p2p_port"],
        metrics_port=ports["metrics_port"],
        launched_by=request.user,
    )

    try:
        node = launch_node(node, _launcher_auth_from_request(request))
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)}, status=201)
        messages.success(request, f"Managed node '{node.display_name}' launch requested successfully.")
        return HttpResponseRedirect("/dashboard")
    except NodeLauncherError as exc:
        node.last_error = str(exc)
        node.status = ManagedNode.STATUS_FAILED
        node.save(update_fields=["last_error", "status", "updated_at"])
        if _is_json_request(request):
            return JsonResponse({"error": str(exc), "node": _serialize_managed_node(node)}, status=502)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/dashboard")


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
        if _is_json_request(request):
            return JsonResponse({"error": "Managed node not found."}, status=404)
        messages.error(request, "Managed node not found.")
        return HttpResponseRedirect("/containers")

    try:
        node = stop_node(node, _launcher_auth_from_request(request))
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)})
        messages.success(request, f"Managed node '{node.display_name}' stop requested successfully.")
        return HttpResponseRedirect("/containers")
    except NodeLauncherError as exc:
        if _is_json_request(request):
            return JsonResponse({"error": str(exc)}, status=502)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/containers")


@csrf_exempt
def admin_node_restart_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        if _is_json_request(request):
            return JsonResponse({"error": "Managed node not found."}, status=404)
        messages.error(request, "Managed node not found.")
        return HttpResponseRedirect("/containers")

    try:
        node = restart_node(node, _launcher_auth_from_request(request))
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)})
        messages.success(request, f"Managed node '{node.display_name}' restarted successfully.")
        return HttpResponseRedirect("/containers")
    except NodeLauncherError as exc:
        if _is_json_request(request):
            return JsonResponse({"error": str(exc)}, status=502)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/containers")


@csrf_exempt
def admin_node_delete_container_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        if _is_json_request(request):
            return JsonResponse({"error": "Managed node not found."}, status=404)
        messages.error(request, "Managed node not found.")
        return HttpResponseRedirect("/containers")

    try:
        node = delete_container(node, _launcher_auth_from_request(request))
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "node": _serialize_managed_node(node)})
        messages.success(request, f"Pod for '{node.display_name}' cleared successfully.")
        return HttpResponseRedirect("/containers")
    except NodeLauncherError as exc:
        if _is_json_request(request):
            return JsonResponse({"error": str(exc)}, status=502)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/containers")


@csrf_exempt
def admin_node_delete_deployment_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        if _is_json_request(request):
            return JsonResponse({"error": "Managed node not found."}, status=404)
        messages.error(request, "Managed node not found.")
        return HttpResponseRedirect("/containers")

    display_name = node.display_name
    try:
        delete_deployment(node, _launcher_auth_from_request(request))
        if _is_json_request(request):
            return JsonResponse({"status": "ok", "deleted": True, "node_id": node_id})
        messages.success(request, f"Deployment '{display_name}' deleted successfully.")
        return HttpResponseRedirect("/containers")
    except (NodeLauncherError, OSError) as exc:
        if _is_json_request(request):
            return JsonResponse({"error": str(exc)}, status=502)
        messages.error(request, str(exc))
        return HttpResponseRedirect("/containers")


@csrf_exempt
def admin_runtime_container_restart_view(request, container_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    try:
        restart_runtime_container(container_id, _launcher_auth_from_request(request))
        messages.success(request, f"Pod '{container_id[:12]}' restarted successfully.")
    except NodeLauncherError as exc:
        messages.error(request, str(exc))
    return HttpResponseRedirect("/containers")


@csrf_exempt
def admin_runtime_container_delete_view(request, container_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    try:
        delete_runtime_container(container_id, _launcher_auth_from_request(request))
        messages.success(request, f"Pod '{container_id[:12]}' deleted successfully.")
    except NodeLauncherError as exc:
        messages.error(request, str(exc))
    return HttpResponseRedirect("/containers")


def admin_node_logs_view(request, node_id):
    admin_error = _admin_required_response(request)
    if admin_error:
        return admin_error

    try:
        node = ManagedNode.objects.get(pk=node_id)
    except ManagedNode.DoesNotExist:
        return JsonResponse({"error": "Managed node not found."}, status=404)

    try:
        auth_context = _launcher_auth_from_request(request)
        node = refresh_node(node, auth_context)
        logs = tail_logs(node, auth_context=auth_context)
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
        node = refresh_node(node, _launcher_auth_from_request(request))
    except NodeLauncherError as exc:
        return JsonResponse({"error": str(exc)}, status=502)

    if node.status != ManagedNode.STATUS_RUNNING:
        return JsonResponse({"error": "Managed node is not running."}, status=409)

    target_path = "/" + (subpath or "dashboard")
    if request.GET:
        target_path = f"{target_path}?{request.META.get('QUERY_STRING', '')}"

    upstream_url = upstream_rpc_url(node, target_path, _launcher_auth_from_request(request))

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
        bootstrap_url="/sms/data",
        back_href="/dashboard",
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
