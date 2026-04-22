from django.urls import path

from .views import (
    JobApplicationSubmissionManagerView,
    JobApplicationSubmissionServiceView,
    JobPostingDeduplicationView,
    JobPostingDetailView,
    JobPostingListView,
    ManualScriptsView,
    ManualScriptSourceURLApiView,
    ManualScriptSourceURLListView,
    ScraperCodeView,
    ScraperManagementView,
    ScraperRunDetailView,
    create_scraper,
)

urlpatterns = [
    path("api/scrapers/", ScraperManagementView.as_view(), name="scrapers-manage"),
    path("api/scrapers/create/", create_scraper, name="scrapers-create"),
    path("api/scrapers/runs/<int:pk>/", ScraperRunDetailView.as_view(), name="scrapers-run-detail"),
    path("api/scrapers/<int:pk>/code/", ScraperCodeView.as_view(), name="scrapers-code"),
    path("api/manual-scripts/", ManualScriptsView.as_view(), name="scrapers-manual"),
    path("api/manual-scripts/urls/", ManualScriptSourceURLListView.as_view(), name="scrapers-manual-urls"),
    path("api/manual-scripts/urls.json", ManualScriptSourceURLApiView.as_view(), name="scrapers-manual-urls-api"),
    path("api/job-postings/", JobPostingListView.as_view(), name="job-posting-list"),
    path("api/job-postings/deduplicate/", JobPostingDeduplicationView.as_view(), name="job-deduplicate-tool"),
    path("api/job-postings/<int:pk>/apply/", JobApplicationSubmissionManagerView.as_view(), name="job-posting-apply-manager"),
    path("api/job-postings/<int:pk>/apply-service/", JobApplicationSubmissionServiceView.as_view(), name="job-posting-apply-service"),
    path("api/job-postings/<int:pk>/", JobPostingDetailView.as_view(), name="job-posting-detail"),
]
