#!/usr/bin/env python
# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import os
import sys


def main():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
