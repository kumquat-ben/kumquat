#!/usr/bin/env python3
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
WEBSITE_DIR = ROOT_DIR / "website"

sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(WEBSITE_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.website_test_settings")

import django


django.setup()

from django.conf import settings
from django.test.utils import get_runner


def main():
    test_labels = sys.argv[1:] or ["tests.browser"]
    runner_class = get_runner(settings)
    test_runner = runner_class(verbosity=2)
    failures = test_runner.run_tests(test_labels)
    raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
