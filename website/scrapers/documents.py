from django.conf import settings
from django_elasticsearch_dsl import Document, Index, fields
from django_elasticsearch_dsl.registries import registry

from .models import JobPosting, Scraper


jobs_index = Index(f"{settings.ELASTICSEARCH_INDEX_PREFIX}-job-postings")
jobs_index.settings(number_of_shards=1, number_of_replicas=0)


@registry.register_document
class JobPostingDocument(Document):
    company = fields.TextField()
    metadata_text = fields.TextField()

    class Index:
        name = jobs_index._name
        settings = jobs_index._settings

    class Django:
        model = JobPosting
        fields = [
            "id",
            "title",
            "location",
            "normalized_location",
            "date",
            "link",
            "description",
            "created_at",
            "last_crawled_at",
            "view_count",
        ]
        related_models = [Scraper]

    def prepare_company(self, instance):
        return (instance.scraper.company or "").strip() if instance.scraper_id else ""

    def prepare_metadata_text(self, instance):
        metadata = instance.metadata or {}
        if not isinstance(metadata, dict):
            return str(metadata)
        return " ".join(str(value) for value in metadata.values() if value not in (None, ""))

    def get_queryset(self):
        return super().get_queryset().select_related("scraper")

    def get_instances_from_related(self, related_instance):
        if isinstance(related_instance, Scraper):
            return related_instance.job_postings.all()
        return None
