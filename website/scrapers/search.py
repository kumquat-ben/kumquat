from django.db.models import Q

from elastic_transport import ConnectionError as ElasticsearchConnectionError
from elasticsearch import ApiError

from .documents import JobPostingDocument
from .models import JobPosting


def _build_job_result(job, *, snippet):
    company = job.scraper.company if getattr(job, "scraper_id", None) else ""
    title_parts = [job.title]
    if company:
        title_parts.append(company)
    return {
        "title": " | ".join(part for part in title_parts if part),
        "url": job.link,
        "summary": snippet or (job.description or "")[:220].strip(),
        "company": company,
        "location": job.location,
        "date": job.date,
    }


def _database_fallback_search(query, limit):
    jobs = (
        JobPosting.objects.select_related("scraper")
        .filter(
            Q(title__icontains=query)
            | Q(location__icontains=query)
            | Q(description__icontains=query)
            | Q(scraper__company__icontains=query)
        )
        .order_by("-last_crawled_at", "-created_at")[:limit]
    )
    results = [_build_job_result(job, snippet=(job.description or "")[:220].strip()) for job in jobs]
    return {
        "results": results,
        "match_count": len(results),
        "backend": "database",
    }


def search_jobs(query, *, limit=10):
    query = (query or "").strip()
    if not query:
        return {
            "results": [],
            "match_count": 0,
            "backend": "elasticsearch",
        }

    try:
        search = JobPostingDocument.search()
        search = search.query(
            "multi_match",
            query=query,
            fields=[
                "title^4",
                "company^3",
                "location^2",
                "normalized_location^2",
                "description",
                "metadata_text",
            ],
            type="best_fields",
            fuzziness="AUTO",
        )[:limit]
        response = search.execute()
    except (ElasticsearchConnectionError, ApiError):
        return _database_fallback_search(query, limit)

    results = []
    for hit in response:
        location = getattr(hit, "location", "")
        date = getattr(hit, "date", "")
        summary_parts = []
        if location:
            summary_parts.append(location)
        if date:
            summary_parts.append(date)
        description = getattr(hit, "description", "") or ""
        if description:
            summary_parts.append(description[:220].strip())
        results.append(
            {
                "title": " | ".join(part for part in [getattr(hit, "title", ""), getattr(hit, "company", "")] if part),
                "url": getattr(hit, "link", ""),
                "summary": " | ".join(part for part in summary_parts if part),
                "company": getattr(hit, "company", ""),
                "location": location,
                "date": date,
            }
        )

    return {
        "results": results,
        "match_count": len(results),
        "backend": "elasticsearch",
    }
