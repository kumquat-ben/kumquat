# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.db import models
from django.utils import timezone


class EarlyAccessSignup(models.Model):
    email = models.EmailField(unique=True)
    name = models.CharField(max_length=120, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.email


class VonageInboundSms(models.Model):
    api_key = models.CharField(max_length=64, blank=True)
    message_id = models.CharField(max_length=120, blank=True, db_index=True)
    from_number = models.CharField(max_length=32, blank=True)
    to_number = models.CharField(max_length=32, blank=True)
    text = models.TextField(blank=True)
    message_type = models.CharField(max_length=32, blank=True)
    keyword = models.CharField(max_length=120, blank=True)
    message_timestamp = models.DateTimeField(null=True, blank=True)
    message_timestamp_raw = models.CharField(max_length=64, blank=True)
    event_timestamp = models.DateTimeField(null=True, blank=True)
    event_timestamp_raw = models.CharField(max_length=64, blank=True)
    nonce = models.CharField(max_length=120, blank=True)
    signature = models.CharField(max_length=255, blank=True)
    signature_valid = models.BooleanField(null=True, blank=True)
    signature_error = models.CharField(max_length=255, blank=True)
    is_concatenated = models.BooleanField(default=False)
    concat_ref = models.CharField(max_length=64, blank=True)
    concat_total = models.PositiveIntegerField(null=True, blank=True)
    concat_part = models.PositiveIntegerField(null=True, blank=True)
    data = models.TextField(blank=True)
    udh = models.TextField(blank=True)
    content_type = models.CharField(max_length=255, blank=True)
    request_method = models.CharField(max_length=16, blank=True)
    remote_addr = models.CharField(max_length=64, blank=True)
    user_agent = models.CharField(max_length=255, blank=True)
    payload = models.JSONField(default=dict, blank=True)
    raw_body = models.TextField(blank=True)
    received_at = models.DateTimeField(default=timezone.now, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-received_at", "-created_at"]

    def __str__(self):
        if self.message_id:
            return self.message_id
        if self.from_number and self.to_number:
            return f"{self.from_number} -> {self.to_number}"
        return f"Vonage inbound SMS {self.pk}"
