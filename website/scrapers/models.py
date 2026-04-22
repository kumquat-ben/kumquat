from django.conf import settings
from django.db import IntegrityError, models, transaction
from django.utils import timezone


class JobPostingQuerySet(models.QuerySet):
    @transaction.atomic
    def get_or_create(self, defaults=None, **kwargs):
        defaults = defaults or {}
        now = timezone.now()

        try:
            obj = self.select_for_update().get(**kwargs)
        except self.model.DoesNotExist:
            create_kwargs = {**kwargs, **defaults}
            create_kwargs.setdefault("last_crawled_at", now)
            try:
                return self.create(**create_kwargs), True
            except IntegrityError:
                obj = self.select_for_update().get(**kwargs)
        else:
            obj.duplicate_hit_count += 1
            obj.last_duplicate_seen_at = now
            obj.last_crawled_at = now
            obj.save(update_fields=["duplicate_hit_count", "last_duplicate_seen_at", "last_crawled_at"])
            return obj, False

        obj.duplicate_hit_count += 1
        obj.last_duplicate_seen_at = now
        obj.last_crawled_at = now
        obj.save(update_fields=["duplicate_hit_count", "last_duplicate_seen_at", "last_crawled_at"])
        return obj, False

    @transaction.atomic
    def update_or_create(self, defaults=None, create_defaults=None, **kwargs):
        defaults = defaults or {}
        create_defaults = create_defaults or defaults
        now = timezone.now()

        try:
            obj = self.select_for_update().get(**kwargs)
        except self.model.DoesNotExist:
            create_kwargs = {**kwargs, **create_defaults}
            create_kwargs.setdefault("last_crawled_at", now)
            try:
                return self.create(**create_kwargs), True
            except IntegrityError:
                obj = self.select_for_update().get(**kwargs)
        else:
            for field_name, value in defaults.items():
                setattr(obj, field_name, value)
            obj.duplicate_hit_count += 1
            obj.last_duplicate_seen_at = now
            obj.last_crawled_at = now
            update_fields = list(defaults.keys()) + [
                "duplicate_hit_count",
                "last_duplicate_seen_at",
                "last_crawled_at",
            ]
            obj.save(update_fields=list(dict.fromkeys(update_fields)))
            return obj, False

        for field_name, value in defaults.items():
            setattr(obj, field_name, value)
        obj.duplicate_hit_count += 1
        obj.last_duplicate_seen_at = now
        obj.last_crawled_at = now
        obj.save(
            update_fields=list(
                dict.fromkeys(
                    list(defaults.keys()) + [
                        "duplicate_hit_count",
                        "last_duplicate_seen_at",
                        "last_crawled_at",
                    ]
                )
            )
        )
        return obj, False


class Scraper(models.Model):
    company = models.CharField(max_length=255)
    url = models.URLField()
    code = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    last_run = models.DateTimeField(null=True, blank=True)
    active = models.BooleanField(default=True)
    interval_hours = models.PositiveIntegerField(default=24)
    timeout_seconds = models.PositiveIntegerField(default=180)

    def __str__(self) -> str:
        return f"{self.company} ({self.url})"


class JobPosting(models.Model):
    objects = models.Manager.from_queryset(JobPostingQuerySet)()

    scraper = models.ForeignKey(Scraper, on_delete=models.CASCADE, related_name="job_postings")
    title = models.CharField(max_length=255)
    location = models.CharField(max_length=255, null=True, blank=True)
    normalized_location = models.CharField(max_length=255, null=True, blank=True)
    location_latitude = models.FloatField(null=True, blank=True)
    location_longitude = models.FloatField(null=True, blank=True)
    location_place_id = models.CharField(max_length=255, null=True, blank=True)
    date = models.CharField(max_length=100, null=True, blank=True)
    link = models.URLField()
    description = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    last_crawled_at = models.DateTimeField(default=timezone.now, db_index=True)
    last_duplicate_seen_at = models.DateTimeField(null=True, blank=True)
    duplicate_hit_count = models.PositiveIntegerField(default=0)
    metadata = models.JSONField(null=True, blank=True)
    view_count = models.PositiveIntegerField(default=0)

    def __str__(self) -> str:
        return self.title

    class Meta:
        unique_together = ("scraper", "link")


class ManualScriptRun(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        ERROR = "error", "Error"
        CANCELLED = "cancelled", "Cancelled"

    script_name = models.CharField(max_length=255)
    queue = models.ForeignKey(
        "ManualScriptQueue",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="runs",
    )
    queue_position = models.PositiveIntegerField(null=True, blank=True)
    scheduled_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    output = models.TextField(blank=True)
    error = models.TextField(blank=True)

    class Meta:
        ordering = ["-scheduled_at"]

    def __str__(self) -> str:
        return f"{self.script_name} ({self.status})"


class ManualScriptSourceURL(models.Model):
    script_name = models.CharField(max_length=255)
    source_name = models.CharField(max_length=255, blank=True)
    url = models.URLField(max_length=1000)
    file_modified_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["script_name", "source_name", "url"]
        constraints = [
            models.UniqueConstraint(
                fields=["script_name", "url"],
                name="unique_manual_script_source_url",
            )
        ]

    def __str__(self) -> str:
        source = self.source_name or "literal"
        return f"{self.script_name}: {source} -> {self.url}"


class ManualScriptQueue(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        ERROR = "error", "Error"
        STOPPED = "stopped", "Stopped"

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    current_script_name = models.CharField(max_length=255, blank=True)
    total_scripts = models.PositiveIntegerField(default=0)
    completed_scripts = models.PositiveIntegerField(default=0)
    error = models.TextField(blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"Manual script queue {self.pk} ({self.status})"


class ManualScriptController(models.Model):
    is_enabled = models.BooleanField(default=False)
    loop_mode = models.BooleanField(default=True)
    queue_concurrency = models.PositiveIntegerField(default=2)
    desired_worker_replicas = models.PositiveIntegerField(default=1)
    last_started_at = models.DateTimeField(null=True, blank=True)
    last_stopped_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["id"]

    def __str__(self) -> str:
        state = "enabled" if self.is_enabled else "disabled"
        return f"Manual script controller ({state})"


class JobApplicationSubmissionManager(models.Model):
    class Status(models.TextChoices):
        CANDIDATE_SCRIPT_FOUND = "candidate_script_found", "Candidate Script Found"
        DESIGN_TIME_SCRIPT_NEEDED = "design_time_script_needed", "Design-Time Script Needed"

    class BrowserSessionStatus(models.TextChoices):
        PENDING = "pending", "Pending"
        COMPLETED = "completed", "Completed"

    job = models.OneToOneField(
        JobPosting,
        on_delete=models.CASCADE,
        related_name="submission_manager",
    )
    first_requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="job_submission_managers_first_requested",
    )
    last_requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="job_submission_managers_last_requested",
    )
    status = models.CharField(max_length=40, choices=Status.choices)
    browser_session_status = models.CharField(
        max_length=20,
        choices=BrowserSessionStatus.choices,
        default=BrowserSessionStatus.PENDING,
    )
    matched_script_name = models.CharField(max_length=255, blank=True)
    candidate_script_names = models.JSONField(default=list, blank=True)
    apply_url = models.URLField(max_length=1000, blank=True)
    apply_domain = models.CharField(max_length=255, blank=True)
    form_requirements = models.JSONField(default=dict, blank=True)
    requirements_summary = models.TextField(blank=True)
    notes = models.TextField(blank=True)
    request_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-id"]

    def __str__(self) -> str:
        return f"Submission manager for {self.job.title}"


class JobApplicationSubmissionRequest(models.Model):
    class Status(models.TextChoices):
        REQUESTED = "requested", "Requested"

    job = models.ForeignKey(
        JobPosting,
        on_delete=models.CASCADE,
        related_name="submission_requests",
    )
    manager = models.ForeignKey(
        JobApplicationSubmissionManager,
        on_delete=models.CASCADE,
        related_name="requests",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="job_submission_requests",
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.REQUESTED)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "user"],
                name="unique_job_submission_request_per_user",
            )
        ]

    def __str__(self) -> str:
        return f"{self.user} requested apply flow for {self.job.title}"


class ApplicantSiteCredential(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="applicant_site_credentials",
    )
    apply_domain = models.CharField(max_length=255)
    login_url = models.URLField(max_length=1000, blank=True)
    username = models.CharField(max_length=255)
    password = models.TextField()
    metadata = models.JSONField(default=dict, blank=True)
    last_used_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["apply_domain", "-updated_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["user", "apply_domain"],
                name="unique_applicant_site_credential_per_user_domain",
            )
        ]

    def __str__(self) -> str:
        return f"{self.user} credentials for {self.apply_domain}"


class JobApplicationRun(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        AWAITING_EMAIL_VERIFICATION = "awaiting_email_verification", "Awaiting Email Verification"
        ACTION_REQUIRED = "action_required", "Action Required"
        APPLIED = "applied", "Applied"
        NEEDS_REVIEW = "needs_review", "Needs Systems Review"
        FAILED = "failed", "Failed"
        INCOMPLETE = "incomplete", "Incomplete"
        ERROR = "error", "Error"

    class ApplyMethod(models.TextChoices):
        GENERIC_SCRIPT = "generic_script", "Generic Script"
        SITE_SPECIFIC_SCRIPT = "site_specific_script", "Site-Specific Script"
        UNKNOWN = "unknown", "Unknown"

    manager = models.ForeignKey(
        JobApplicationSubmissionManager,
        on_delete=models.CASCADE,
        related_name="application_runs",
    )
    job = models.ForeignKey(
        JobPosting,
        on_delete=models.CASCADE,
        related_name="application_runs",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="job_application_runs",
    )
    credential = models.ForeignKey(
        ApplicantSiteCredential,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="application_runs",
    )
    status = models.CharField(max_length=40, choices=Status.choices, default=Status.PENDING)
    selected_script_name = models.CharField(max_length=255, blank=True)
    apply_method = models.CharField(
        max_length=40,
        choices=ApplyMethod.choices,
        default=ApplyMethod.UNKNOWN,
    )
    apply_url = models.URLField(max_length=1000, blank=True)
    apply_domain = models.CharField(max_length=255, blank=True)
    form_classification = models.CharField(max_length=100, blank=True)
    current_step = models.CharField(max_length=100, blank=True)
    verification_prompt = models.TextField(blank=True)
    last_result = models.JSONField(default=dict, blank=True)
    runtime_state = models.JSONField(default=dict, blank=True)
    submitted_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    review_notes = models.TextField(blank=True)
    reviewed_at = models.DateTimeField(null=True, blank=True)
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reviewed_job_application_runs",
    )
    reprocess_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-id"]

    def __str__(self) -> str:
        return f"Application run {self.pk} for {self.user} on {self.job.title}"


class ScraperRun(models.Model):
    class Status(models.TextChoices):
        SUCCESS = "success", "Success"
        ERROR = "error", "Error"

    class Trigger(models.TextChoices):
        MANUAL = "manual", "Manual"
        SCHEDULER = "scheduler", "Scheduler"
        API = "api", "API"
        MANAGEMENT = "management", "Management Command"

    scraper = models.ForeignKey(Scraper, on_delete=models.CASCADE, related_name="runs")
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_ms = models.PositiveIntegerField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices)
    payload = models.JSONField(null=True, blank=True)
    error = models.TextField(blank=True)
    triggered_by = models.CharField(max_length=20, choices=Trigger.choices, default=Trigger.MANUAL)

    class Meta:
        ordering = ["-started_at"]

    def __str__(self) -> str:
        return f"Run {self.pk} for {self.scraper.company} ({self.status})"
