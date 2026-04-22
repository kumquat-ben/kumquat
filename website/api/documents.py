from django.conf import settings
from django_elasticsearch_dsl import Document, Index
from django_elasticsearch_dsl.registries import registry

from .models import SearchDocument


search_documents_index = Index(f"{settings.ELASTICSEARCH_INDEX_PREFIX}-search-documents")
search_documents_index.settings(number_of_shards=1, number_of_replicas=0)


@registry.register_document
class SearchDocumentDocument(Document):
    class Index:
        name = search_documents_index._name
        settings = search_documents_index._settings

    class Django:
        model = SearchDocument
        fields = [
            "id",
            "url",
            "normalized_url",
            "title",
            "summary",
            "content",
            "content_hash",
            "depth",
            "http_status",
            "link_count",
            "crawled_at",
            "updated_at",
        ]
