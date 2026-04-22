# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.db.models.functions import Lower
from django.utils.text import slugify


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


class ManagedNode(models.Model):
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"
    STATUS_EXITED = "exited"
    STATUS_STOPPED = "stopped"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_EXITED, "Exited"),
        (STATUS_STOPPED, "Stopped"),
        (STATUS_FAILED, "Failed"),
    ]

    name = models.SlugField(max_length=63, unique=True)
    display_name = models.CharField(max_length=120)
    container_name = models.CharField(max_length=120, blank=True)
    container_id = models.CharField(max_length=128, blank=True, db_index=True)
    image = models.CharField(max_length=255)
    network_name = models.CharField(max_length=32, default="dev")
    chain_id = models.PositiveBigIntegerField(default=1337)
    reward_address = models.CharField(max_length=96, blank=True)
    enable_mining = models.BooleanField(default=False)
    mining_threads = models.PositiveIntegerField(default=1)
    api_port = models.PositiveIntegerField(unique=True)
    p2p_port = models.PositiveIntegerField(unique=True)
    metrics_port = models.PositiveIntegerField(unique=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING)
    last_error = models.TextField(blank=True)
    last_logs = models.TextField(blank=True)
    launched_by = models.ForeignKey(
        get_user_model(),
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="managed_nodes",
    )
    launched_at = models.DateTimeField(default=timezone.now)
    last_status_at = models.DateTimeField(null=True, blank=True)
    stopped_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.display_name or self.name


class UserWallet(models.Model):
    user = models.OneToOneField(
        get_user_model(),
        on_delete=models.CASCADE,
        related_name="wallet",
    )
    address = models.CharField(max_length=96, unique=True, db_index=True)
    public_key = models.CharField(max_length=64, unique=True)
    encrypted_private_key = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.user_id}:{self.address}"


class SearchCrawlTarget(models.Model):
    BACKEND_BASIC = "basic"
    BACKEND_SCRAPY = "scrapy"

    BACKEND_CHOICES = [
        (BACKEND_BASIC, "Basic"),
        (BACKEND_SCRAPY, "Scrapy"),
    ]

    STATUS_QUEUED = "queued"
    STATUS_RUNNING = "running"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_QUEUED, "Queued"),
        (STATUS_RUNNING, "Running"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
    ]

    url = models.URLField(unique=True)
    normalized_url = models.URLField(unique=True)
    scope_netloc = models.CharField(max_length=255, db_index=True)
    crawl_backend = models.CharField(max_length=16, choices=BACKEND_CHOICES, default=BACKEND_BASIC)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_QUEUED)
    max_depth = models.PositiveIntegerField(default=1)
    max_pages = models.PositiveIntegerField(default=25)
    created_by = models.ForeignKey(
        get_user_model(),
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="search_crawl_targets",
    )
    last_error = models.TextField(blank=True)
    document_count = models.PositiveIntegerField(default=0)
    queued_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]

    def __str__(self):
        return self.normalized_url


class WebsiteDiscoveredDomain(models.Model):
    STATUS_NEW = "new"
    STATUS_QUEUED = "queued"
    STATUS_CRAWLED = "crawled"
    STATUS_FAILED = "failed"
    STATUS_IGNORED = "ignored"

    STATUS_CHOICES = [
        (STATUS_NEW, "New"),
        (STATUS_QUEUED, "Queued"),
        (STATUS_CRAWLED, "Crawled"),
        (STATUS_FAILED, "Failed"),
        (STATUS_IGNORED, "Ignored"),
    ]

    crawler_definition = models.ForeignKey(
        "WebsiteCrawlerDefinition",
        on_delete=models.CASCADE,
        related_name="discovered_domains",
    )
    crawl_target = models.ForeignKey(
        "SearchCrawlTarget",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="discovered_domains",
    )
    source_url = models.URLField(max_length=500, blank=True)
    domain = models.CharField(max_length=255, db_index=True)
    normalized_url = models.URLField(max_length=500)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_NEW)
    discovery_count = models.PositiveIntegerField(default=1)
    last_error = models.TextField(blank=True)
    discovered_at = models.DateTimeField(default=timezone.now)
    last_seen_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["domain", "-last_seen_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["crawler_definition", "domain"],
                name="unique_discovered_domain_per_crawler",
            ),
        ]

    def __str__(self):
        return self.domain


class SearchDocument(models.Model):
    crawl_target = models.ForeignKey(
        SearchCrawlTarget,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="documents",
    )
    url = models.URLField(unique=True)
    normalized_url = models.URLField(unique=True)
    title = models.CharField(max_length=255, blank=True)
    summary = models.TextField(blank=True)
    content = models.TextField(blank=True)
    content_hash = models.CharField(max_length=64, blank=True, db_index=True)
    depth = models.PositiveIntegerField(default=0)
    http_status = models.PositiveIntegerField(null=True, blank=True)
    link_count = models.PositiveIntegerField(default=0)
    crawled_at = models.DateTimeField(default=timezone.now, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-crawled_at", "-updated_at"]

    def __str__(self):
        return self.title or self.normalized_url


class SearchCommandAnalytics(models.Model):
    channel = models.CharField(max_length=32, unique=True)
    command_count = models.PositiveBigIntegerField(default=0)
    last_command_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["channel"]

    def __str__(self):
        return f"{self.channel}:{self.command_count}"


class DomainCrawlSuggestion(models.Model):
    STATUS_QUEUED = "queued"
    STATUS_DUPLICATE = "duplicate"
    STATUS_REJECTED = "rejected"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_QUEUED, "Queued"),
        (STATUS_DUPLICATE, "Duplicate"),
        (STATUS_REJECTED, "Rejected"),
        (STATUS_FAILED, "Failed"),
    ]

    submitted_url = models.URLField(max_length=500)
    normalized_url = models.URLField(max_length=500, db_index=True)
    scope_netloc = models.CharField(max_length=255, db_index=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_QUEUED)
    message = models.TextField(blank=True)
    crawl_target = models.ForeignKey(
        "SearchCrawlTarget",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="suggestions",
    )
    submitted_by = models.ForeignKey(
        get_user_model(),
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="domain_crawl_suggestions",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.normalized_url


class WebsiteCrawlerDefinition(models.Model):
    SOURCE_DESIGN_TIME = "design_time"
    SOURCE_RUNTIME = "runtime"

    SOURCE_CHOICES = [
        (SOURCE_DESIGN_TIME, "Design Time"),
        (SOURCE_RUNTIME, "Runtime"),
    ]

    VERTICAL_SMALL_BUSINESS_LAW = "small_business_law"
    VERTICAL_HOME_IMPROVEMENT = "home_improvement"
    VERTICAL_GENERAL_LOCAL = "general_local"

    VERTICAL_CHOICES = [
        (VERTICAL_SMALL_BUSINESS_LAW, "Small Business Law"),
        (VERTICAL_HOME_IMPROVEMENT, "Home Improvement"),
        (VERTICAL_GENERAL_LOCAL, "General Local Business"),
    ]

    name = models.CharField(max_length=160)
    slug = models.SlugField(max_length=180, unique=True)
    source_type = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_RUNTIME)
    vertical = models.CharField(max_length=40, choices=VERTICAL_CHOICES, default=VERTICAL_GENERAL_LOCAL)
    seed_url = models.URLField(max_length=500)
    scope_netloc = models.CharField(max_length=255, blank=True, db_index=True)
    prompt = models.TextField(blank=True)
    generated_code = models.TextField(blank=True)
    config = models.JSONField(default=dict, blank=True)
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(
        get_user_model(),
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="website_crawler_definitions",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name", "-updated_at"]

    def __str__(self):
        return self.name

    @staticmethod
    def build_unique_slug(name):
        base = slugify(name) or "website-crawler"
        base = base[:170]
        candidate = base
        suffix = 1

        while WebsiteCrawlerDefinition.objects.filter(slug=candidate).exists():
            suffix_str = str(suffix)
            truncated_base = base[: max(1, 170 - len(suffix_str) - 1)]
            candidate = f"{truncated_base}-{suffix_str}"
            suffix += 1
        return candidate


class CompanyProfile(models.Model):
    name = models.CharField(max_length=200)
    slug = models.SlugField(max_length=220, unique=True)
    website = models.URLField(max_length=300, blank=True)
    info = models.TextField(blank=True)
    source = models.CharField(max_length=120, blank=True)
    source_url = models.URLField(max_length=400, blank=True)
    yc_url = models.URLField(max_length=200, blank=True)
    batch = models.CharField(max_length=20, blank=True)
    status = models.CharField(max_length=60, blank=True)
    employees = models.CharField(max_length=50, blank=True)
    location = models.CharField(max_length=150, blank=True)
    tags = models.CharField(max_length=200, blank=True)
    linkedin_url = models.URLField(max_length=400, blank=True)
    twitter_url = models.URLField(max_length=400, blank=True)
    cb_url = models.URLField(max_length=400, blank=True)
    careers_url = models.URLField(max_length=400, blank=True)
    collected_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        constraints = [
            models.UniqueConstraint(Lower("name"), name="unique_company_profile_name_ci"),
        ]

    def __str__(self):
        return self.name

    @staticmethod
    def build_unique_slug(name):
        base = slugify(name) or "company"
        base = base[:200]
        candidate = base
        suffix = 1

        while CompanyProfile.objects.filter(slug=candidate).exists():
            suffix_str = str(suffix)
            truncated_base = base[: max(1, 200 - len(suffix_str) - 1)]
            candidate = f"{truncated_base}-{suffix_str}"
            suffix += 1

        return candidate


class JobListing(models.Model):
    company = models.ForeignKey(
        CompanyProfile,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="job_listings",
    )
    title = models.CharField(max_length=255)
    slug = models.SlugField(max_length=280, unique=True)
    location = models.CharField(max_length=255, blank=True)
    normalized_location = models.CharField(max_length=255, blank=True)
    employment_type = models.CharField(max_length=120, blank=True)
    salary = models.CharField(max_length=120, blank=True)
    excerpt = models.TextField(blank=True)
    description = models.TextField(blank=True)
    apply_url = models.URLField(max_length=500, blank=True)
    source = models.CharField(max_length=120, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    posted_at = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    view_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-posted_at", "-created_at", "title"]

    def __str__(self):
        return self.title

    @staticmethod
    def build_unique_slug(title):
        base = slugify(title) or "job"
        base = base[:240]
        candidate = base
        suffix = 1

        while JobListing.objects.filter(slug=candidate).exists():
            suffix_str = str(suffix)
            truncated_base = base[: max(1, 240 - len(suffix_str) - 1)]
            candidate = f"{truncated_base}-{suffix_str}"
            suffix += 1

        return candidate
