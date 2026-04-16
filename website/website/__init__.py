# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import pymysql

pymysql.install_as_MySQLdb()

from .celery import app as celery_app

__all__ = ("celery_app",)
