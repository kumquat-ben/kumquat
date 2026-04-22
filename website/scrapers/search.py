from django.core.paginator import EmptyPage, Paginator
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


def _database_fallback_search(query, page, page_size):
    jobs = (
        JobPosting.objects.select_related("scraper")
        .filter(
            Q(title__icontains=query)
            | Q(location__icontains=query)
            | Q(description__icontains=query)
            | Q(scraper__company__icontains=query)
        )
        .order_by("-last_crawled_at", "-created_at")
    )
    paginator = Paginator(jobs, page_size)
    page_obj = paginator.get_page(page)
    results = [_build_job_result(job, snippet=(job.description or "")[:220].strip()) for job in page_obj.object_list]
    return {
        "results": results,
        "match_count": paginator.count,
        "backend": "database",
        "page": page_obj.number,
        "page_size": page_size,
        "total_pages": paginator.num_pages,
        "has_next": page_obj.has_next(),
        "has_previous": page_obj.has_previous(),
        "next_page": page_obj.next_page_number() if page_obj.has_next() else None,
        "previous_page": page_obj.previous_page_number() if page_obj.has_previous() else None,
        "start_index": page_obj.start_index() if paginator.count else 0,
        "end_index": page_obj.end_index() if paginator.count else 0,
    }


def search_jobs(query, *, page=1, page_size=10):
    query = (query or "").strip()
    if not query:
        return {
            "results": [],
            "match_count": 0,
            "backend": "elasticsearch",
            "page": 1,
            "page_size": page_size,
            "total_pages": 0,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
            "start_index": 0,
            "end_index": 0,
        }

    page = max(int(page or 1), 1)
    start = (page - 1) * page_size

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
        )[start : start + page_size]
        response = search.execute()
    except (ElasticsearchConnectionError, ApiError):
        return _database_fallback_search(query, page, page_size)
    except (TypeError, ValueError):
        return _database_fallback_search(query, 1, page_size)

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
        "match_count": response.hits.total.value,
        "backend": "elasticsearch",
        "page": page,
        "page_size": page_size,
        "total_pages": ((response.hits.total.value - 1) // page_size + 1) if response.hits.total.value else 0,
        "has_next": start + page_size < response.hits.total.value,
        "has_previous": page > 1,
        "next_page": page + 1 if start + page_size < response.hits.total.value else None,
        "previous_page": page - 1 if page > 1 else None,
        "start_index": start + 1 if response.hits.total.value else 0,
        "end_index": min(start + len(results), response.hits.total.value),
    }
