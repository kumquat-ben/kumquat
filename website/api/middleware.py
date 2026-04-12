# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from django.conf import settings
from django.http import HttpResponse, HttpResponseNotAllowed, JsonResponse
from django.shortcuts import redirect


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


class NodeSubdomainProxyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        node_host_state = self._extract_node_id(request)
        if node_host_state is None:
            return self.get_response(request)
        if node_host_state is False:
            return JsonResponse({"error": "Invalid node subdomain."}, status=400)
        node_id = node_host_state

        if request.method.upper() not in settings.NODE_PROXY_ALLOWED_METHODS:
            return HttpResponseNotAllowed(sorted(settings.NODE_PROXY_ALLOWED_METHODS))

        if settings.NODE_PROXY_REQUIRE_AUTH and not request.user.is_authenticated:
            if self._prefers_html(request):
                return redirect(
                    f"{settings.NODE_PROXY_LOGIN_URL}?{urlencode({'next': request.build_absolute_uri()})}"
                )
            return JsonResponse(
                {"error": "Authentication required for node dashboard access."},
                status=401,
            )

        upstream_url = self._build_upstream_url(node_id, request)
        upstream_request = Request(
            upstream_url,
            data=request.body if request.body else None,
            method=request.method.upper(),
            headers=self._build_upstream_headers(request, node_id),
        )

        try:
            with urlopen(upstream_request, timeout=settings.NODE_PROXY_TIMEOUT_SECONDS) as upstream_response:
                return self._build_proxy_response(upstream_response, node_id)
        except HTTPError as exc:
            return self._build_error_response(exc, node_id)
        except URLError as exc:
            return JsonResponse(
                {
                    "error": "Node dashboard upstream is unavailable.",
                    "node_id": node_id,
                    "reason": str(exc.reason),
                },
                status=502,
            )

    def _extract_node_id(self, request):
        host = request.get_host().split(":", 1)[0].lower().strip(".")
        suffix = f".{settings.NODE_PROXY_BASE_DOMAIN}"
        if not host.endswith(suffix):
            return None

        node_id = host[: -len(suffix)]
        if not self._is_valid_node_id(node_id):
            return False
        return node_id

    def _is_valid_node_id(self, node_id):
        if not node_id or len(node_id) > 63:
            return False
        if node_id.startswith("-") or node_id.endswith("-"):
            return False
        return all(character.islower() or character.isdigit() or character == "-" for character in node_id)

    def _prefers_html(self, request):
        accept = (request.headers.get("Accept") or "").lower()
        return "text/html" in accept or "*/*" in accept

    def _build_upstream_url(self, node_id, request):
        path = request.get_full_path() or "/"
        return (
            f"http://{node_id}.{settings.NODE_PROXY_SERVICE_NAME}."
            f"{settings.NODE_PROXY_NAMESPACE}.svc.cluster.local:"
            f"{settings.NODE_PROXY_PORT}{path}"
        )

    def _build_upstream_headers(self, request, node_id):
        headers = {
            "X-Forwarded-Host": request.get_host(),
            "X-Forwarded-Proto": "https" if request.is_secure() else "http",
            "X-Kumquat-Node-Id": node_id,
        }

        for header_name in ("Accept", "Accept-Encoding", "User-Agent", "Content-Type"):
            value = request.headers.get(header_name)
            if value:
                headers[header_name] = value

        return headers

    def _build_proxy_response(self, upstream_response, node_id):
        body = upstream_response.read()
        response = HttpResponse(body, status=upstream_response.status)
        response["X-Kumquat-Node-Id"] = node_id

        for header_name, header_value in upstream_response.headers.items():
            if header_name.lower() in HOP_BY_HOP_HEADERS:
                continue
            response[header_name] = header_value

        return response

    def _build_error_response(self, exc, node_id):
        body = exc.read()
        response = HttpResponse(body, status=exc.code)
        response["X-Kumquat-Node-Id"] = node_id

        for header_name, header_value in exc.headers.items():
            if header_name.lower() in HOP_BY_HOP_HEADERS:
                continue
            response[header_name] = header_value

        return response
