from pathlib import Path

from website.test_settings import *  # noqa: F403,F401


KUMQUAT_TEST_HOST = env("KUMQUAT_TEST_HOST", "kumquat.test")

ALLOWED_HOSTS = [  # noqa: F405
    "localhost",
    "127.0.0.1",
    KUMQUAT_TEST_HOST,
    ".localhost",
]

SITE_URL = f"http://{KUMQUAT_TEST_HOST}"  # noqa: F405
DEBUG = True  # noqa: F405

DATABASES = {  # noqa: F405
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(Path(BASE_DIR) / "test-browser.sqlite3"),  # noqa: F405
        "TEST": {
            "NAME": str(Path(BASE_DIR) / "test-browser-test.sqlite3"),  # noqa: F405
        },
    }
}
