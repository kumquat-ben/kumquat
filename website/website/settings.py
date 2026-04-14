# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_STATIC_DIR = BASE_DIR / "static"


def load_env_file(*paths):
    for path in paths:
        if not path.exists():
            continue

        for raw_line in path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            name, value = line.split("=", 1)
            name = name.strip()
            value = value.strip()

            if not name:
                continue

            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]

            os.environ.setdefault(name, value)


load_env_file(BASE_DIR / ".env", BASE_DIR.parent / ".env")


def env(name, default=None):
    value = os.getenv(name)
    if value is None:
        return default
    return value


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_list(name, default=""):
    value = os.getenv(name)
    if value is None:
        value = default
    return [item.strip() for item in value.split(",") if item.strip()]


SECRET_KEY = env("DJANGO_SECRET_KEY", "dev-only-secret-key")
DEBUG = env_bool("DJANGO_DEBUG", False)
ALLOWED_HOSTS = [host.strip() for host in env("DJANGO_ALLOWED_HOSTS", "*").split(",") if host.strip()]
CSRF_TRUSTED_ORIGINS = [
    origin.strip()
    for origin in env("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",")
    if origin.strip()
]
GOOGLE_OAUTH_CLIENT_ID = env("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_OAUTH_CLIENT_SECRET = env("GOOGLE_OAUTH_CLIENT_SECRET", "")
GOOGLE_OAUTH_REDIRECT_URI = env(
    "GOOGLE_OAUTH_REDIRECT_URI",
    "",
)
SESSION_COOKIE_DOMAIN = env("DJANGO_SESSION_COOKIE_DOMAIN")
CSRF_COOKIE_DOMAIN = env("DJANGO_CSRF_COOKIE_DOMAIN")
NODE_PROXY_BASE_DOMAIN = env("NODE_PROXY_BASE_DOMAIN", "node.kumquat.info").lower().strip(".")
NODE_PROXY_SERVICE_NAME = env("NODE_PROXY_SERVICE_NAME", "kumquat-blockchain-headless")
NODE_PROXY_NAMESPACE = env("NODE_PROXY_NAMESPACE", "kumquat")
NODE_PROXY_PORT = int(env("NODE_PROXY_PORT", "8545"))
NODE_PROXY_REQUIRE_AUTH = env_bool("NODE_PROXY_REQUIRE_AUTH", True)
NODE_PROXY_ALLOWED_METHODS = {
    method.upper() for method in env_list("NODE_PROXY_ALLOWED_METHODS", "GET,HEAD,OPTIONS")
}
NODE_PROXY_TIMEOUT_SECONDS = int(env("NODE_PROXY_TIMEOUT_SECONDS", "15"))
NODE_PROXY_LOGIN_URL = env("NODE_PROXY_LOGIN_URL", "https://kumquat.info/auth/google/start")
NODE_LAUNCHER_ENABLED = env_bool("NODE_LAUNCHER_ENABLED", False)
NODE_LAUNCHER_DOCKER_HOST = env("NODE_LAUNCHER_DOCKER_HOST", "")
NODE_LAUNCHER_ROOT = env("NODE_LAUNCHER_ROOT", "/var/lib/kumquat-node-launcher")
NODE_LAUNCHER_KUBECONFIG = env("NODE_LAUNCHER_KUBECONFIG", "")
NODE_LAUNCHER_KUBERNETES_NAMESPACE = env("NODE_LAUNCHER_KUBERNETES_NAMESPACE", "kumquat").strip()
NODE_LAUNCHER_IMAGE = env(
    "NODE_LAUNCHER_IMAGE",
    "351381968847.dkr.ecr.us-west-2.amazonaws.com/kumquat-blockchain:blockchain-20260414-033139-2cdb0fc",
)
NODE_LAUNCHER_IMAGE_PULL_POLICY = env("NODE_LAUNCHER_IMAGE_PULL_POLICY", "IfNotPresent").strip().lower()
NODE_LAUNCHER_REGISTRY_AUTH_FILE = env("NODE_LAUNCHER_REGISTRY_AUTH_FILE", "")
NODE_LAUNCHER_IMAGE_PULL_SECRETS = env_list("NODE_LAUNCHER_IMAGE_PULL_SECRETS", "ecr-pull-secret")
NODE_LAUNCHER_NETWORK = env("NODE_LAUNCHER_NETWORK", "dev")
NODE_LAUNCHER_CHAIN_ID = int(env("NODE_LAUNCHER_CHAIN_ID", "1337"))
NODE_LAUNCHER_BASE_API_PORT = int(env("NODE_LAUNCHER_BASE_API_PORT", "18545"))
NODE_LAUNCHER_BASE_P2P_PORT = int(env("NODE_LAUNCHER_BASE_P2P_PORT", "30380"))
NODE_LAUNCHER_BASE_METRICS_PORT = int(env("NODE_LAUNCHER_BASE_METRICS_PORT", "19100"))
NODE_LAUNCHER_STORAGE_CLASS_NAME = env("NODE_LAUNCHER_STORAGE_CLASS_NAME", "kumquat-mysql-gp3").strip()
NODE_LAUNCHER_STORAGE_SIZE = env("NODE_LAUNCHER_STORAGE_SIZE", "20Gi").strip()
NODE_LAUNCHER_NODE_SELECTOR = dict(
    item.split("=", 1)
    for item in env_list("NODE_LAUNCHER_NODE_SELECTOR", "workload=application")
    if "=" in item
)
VONAGE_ACCOUNT_SECRET = env("VONAGE_ACCOUNT_SECRET", "")
VONAGE_SMS_SIGNATURE_SECRET = env("VONAGE_SMS_SIGNATURE_SECRET", "")
VONAGE_SMS_SIGNATURE_ALGORITHM = env("VONAGE_SMS_SIGNATURE_ALGORITHM", "md5hash").lower()
SITE_URL = env("SITE_URL", "https://kumquat.info").rstrip("/")
SITE_NAME = env("SITE_NAME", "Kumquat")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.sitemaps",
    "django.contrib.staticfiles",
    "api",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "api.middleware.NodeSubdomainProxyMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "website.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "website.wsgi.application"
ASGI_APPLICATION = "website.asgi.application"

if env("MYSQL_HOST"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": env("MYSQL_DATABASE", "kumquat"),
            "USER": env("MYSQL_USER", "root"),
            "PASSWORD": env("MYSQL_PASSWORD", ""),
            "HOST": env("MYSQL_HOST", ""),
            "PORT": env("MYSQL_PORT", "3306"),
            "OPTIONS": {
                "charset": "utf8mb4",
            },
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = env("DJANGO_TIME_ZONE", "UTC")
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [PROJECT_STATIC_DIR] if PROJECT_STATIC_DIR.exists() else []
STATICFILES_STORAGE = "whitenoise.storage.CompressedStaticFilesStorage"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True

SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
CSRF_COOKIE_SAMESITE = "Lax"
