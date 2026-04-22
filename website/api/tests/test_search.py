from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from api.models import (
    SearchCommandAnalytics,
    SearchCrawlTarget,
    SearchDocument,
    WebsiteCrawlerDefinition,
    WebsiteDiscoveredDomain,
)
from api.search import SearchCrawlerError, crawl_target, normalize_crawl_url


class SearchCrawlQueueViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(
            username="admin",
            email="admin@example.com",
            password="password",
            is_superuser=True,
            is_staff=True,
        )

    def test_superuser_can_queue_crawl_target(self):
        self.client.force_login(self.user)

        with patch("api.views.schedule_crawl_search_target", return_value="inline") as schedule_mock:
            response = self.client.post(
                "/search/crawl",
                data='{"url":"docs.example.com","max_depth":1,"max_pages":5}',
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        target = SearchCrawlTarget.objects.get()
        self.assertEqual(target.normalized_url, "https://docs.example.com/")
        self.assertEqual(target.scope_netloc, "docs.example.com")
        schedule_mock.assert_called_once_with(target.id)

    def test_queue_requires_superuser(self):
        regular_user = get_user_model().objects.create_user(
            username="user",
            email="user@example.com",
            password="password",
        )
        self.client.force_login(regular_user)

        response = self.client.post("/search/crawl", data={"url": "https://docs.example.com"})

        self.assertEqual(response.status_code, 403)

    def test_website_indexing_admin_page_lists_design_time_and_runtime_crawlers(self):
        self.client.force_login(self.user)
        WebsiteCrawlerDefinition.objects.create(
            name="Home Improvement Runtime",
            slug="home-improvement-runtime",
            vertical=WebsiteCrawlerDefinition.VERTICAL_HOME_IMPROVEMENT,
            seed_url="https://homes.example.com/",
            scope_netloc="homes.example.com",
            prompt="Generate crawler rules for local home improvement sites.",
            created_by=self.user,
        )

        response = self.client.get("/manage/website-indexing")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Website indexing")
        self.assertContains(response, "Home Improvement Runtime")
        self.assertContains(response, "example_small_business_law.py")

    def test_website_indexing_admin_can_save_runtime_crawler(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/manage/website-indexing",
            {
                "action": "save_runtime_crawler",
                "name": "Law Firm Runtime",
                "vertical": WebsiteCrawlerDefinition.VERTICAL_SMALL_BUSINESS_LAW,
                "seed_url": "law.example.com",
                "prompt": "Generate crawler rules for local law firms.",
                "generated_code": "def crawl():\n    return []",
                "is_active": "on",
            },
        )

        self.assertEqual(response.status_code, 302)
        crawler = WebsiteCrawlerDefinition.objects.get()
        self.assertEqual(crawler.slug, "law-firm-runtime")
        self.assertEqual(crawler.scope_netloc, "law.example.com")
        self.assertEqual(crawler.source_type, WebsiteCrawlerDefinition.SOURCE_RUNTIME)

    def test_website_indexing_admin_can_launch_scrapy_discovery(self):
        self.client.force_login(self.user)
        crawler = WebsiteCrawlerDefinition.objects.create(
            name="Discovery Runtime",
            slug="discovery-runtime",
            seed_url="https://seed.example.com/",
            scope_netloc="seed.example.com",
            created_by=self.user,
        )

        with patch("api.views.schedule_domain_discovery", return_value="inline") as schedule_mock:
            response = self.client.post(
                "/manage/website-indexing",
                {"action": "discover_domains_scrapy", "crawler_id": crawler.id},
            )

        self.assertEqual(response.status_code, 302)
        schedule_mock.assert_called_once_with(crawler.id)

    def test_website_indexing_admin_can_queue_scrapy_crawl_for_discovered_domain(self):
        self.client.force_login(self.user)
        crawler = WebsiteCrawlerDefinition.objects.create(
            name="Discovery Runtime",
            slug="discovery-runtime",
            seed_url="https://seed.example.com/",
            scope_netloc="seed.example.com",
            created_by=self.user,
        )
        discovered = WebsiteDiscoveredDomain.objects.create(
            crawler_definition=crawler,
            domain="docs.partner.example",
            normalized_url="https://docs.partner.example/",
            source_url="https://seed.example.com/resources",
        )

        with patch("api.views.schedule_crawl_search_target", return_value="inline") as schedule_mock:
            response = self.client.post(
                "/manage/website-indexing",
                {"action": "crawl_discovered_domain_scrapy", "discovered_domain_id": discovered.id},
            )

        self.assertEqual(response.status_code, 302)
        discovered.refresh_from_db()
        self.assertEqual(discovered.status, WebsiteDiscoveredDomain.STATUS_QUEUED)
        self.assertEqual(discovered.crawl_target.crawl_backend, SearchCrawlTarget.BACKEND_SCRAPY)
        schedule_mock.assert_called_once_with(discovered.crawl_target_id)


class SearchCrawlerTests(TestCase):
    def test_crawl_target_indexes_same_host_documents(self):
        target = SearchCrawlTarget.objects.create(
            url="https://docs.example.com/",
            normalized_url="https://docs.example.com/",
            scope_netloc="docs.example.com",
            max_depth=1,
            max_pages=5,
        )

        def fake_fetch(url):
            pages = {
                "https://docs.example.com/": (
                    200,
                    """
                    <html>
                      <head><title>Docs Home</title></head>
                      <body>
                        <p>Kumquat wallet docs live here.</p>
                        <a href="/wallet">Wallet</a>
                        <a href="https://outside.example.com/offsite">Offsite</a>
                      </body>
                    </html>
                    """,
                ),
                "https://docs.example.com/wallet": (
                    200,
                    """
                    <html>
                      <head><title>Wallet Setup</title></head>
                      <body>
                        <p>Set up your Kumquat wallet and search-ready docs.</p>
                      </body>
                    </html>
                    """,
                ),
            }
            return pages[url]

        crawl_target(target.id, fetch_html=fake_fetch)

        target.refresh_from_db()
        self.assertEqual(target.status, SearchCrawlTarget.STATUS_COMPLETED)
        self.assertEqual(target.document_count, 2)
        self.assertEqual(SearchDocument.objects.count(), 2)
        self.assertTrue(SearchDocument.objects.filter(normalized_url="https://docs.example.com/wallet").exists())
        self.assertFalse(
            SearchDocument.objects.filter(normalized_url="https://outside.example.com/offsite").exists()
        )

    def test_normalize_crawl_url_rejects_unsupported_scheme(self):
        with self.assertRaisesMessage(SearchCrawlerError, "Only http and https URLs can be crawled."):
            normalize_crawl_url("ftp://docs.example.com/file.txt")


class CliSearchViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    @patch("api.views.search_jobs")
    def test_cli_search_requires_cli_headers(self, search_mock):
        response = self.client.get("/api/search/cli", {"q": "agents"})

        self.assertEqual(response.status_code, 403)
        search_mock.assert_not_called()

    @patch("api.views.search_jobs")
    def test_cli_search_returns_results_and_tracks_command_count(self, search_mock):
        search_mock.return_value = {
            "results": [{"title": "Agent docs", "url": "https://docs.example.com", "summary": "Docs"}],
            "match_count": 1,
            "backend": "database",
            "page": 1,
            "page_size": 10,
            "total_pages": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
            "start_index": 1,
            "end_index": 1,
        }

        response = self.client.get(
            "/api/search/cli",
            {"q": "agents"},
            HTTP_X_KUMQUAT_CLIENT="cli",
            HTTP_USER_AGENT="kumquat-cli/0.1",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["channel"], "cli")
        self.assertEqual(payload["query"], "agents")
        self.assertEqual(payload["command_count"], 1)
        self.assertEqual(payload["match_count"], 1)
        self.assertEqual(SearchCommandAnalytics.objects.get(channel="cli").command_count, 1)


class SearchTypeaheadViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    @patch("api.views.search_jobs")
    def test_typeahead_ignores_short_queries(self, search_mock):
        response = self.client.get("/api/search/typeahead", {"q": "a"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"query": "a", "results": [], "backend": None, "match_count": 0},
        )
        search_mock.assert_not_called()

    @patch("api.views.search_jobs")
    def test_typeahead_returns_suggestions_from_search_service(self, search_mock):
        search_mock.return_value = {
            "results": [
                {
                    "title": "Platform Engineer | Kumquat",
                    "url": "https://jobs.example.com/platform-engineer",
                    "summary": "Remote | Build agent-friendly search systems for jobs and documents.",
                }
            ],
            "match_count": 1,
            "backend": "elasticsearch",
            "page": 1,
            "page_size": 5,
            "total_pages": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
            "start_index": 1,
            "end_index": 1,
        }

        response = self.client.get("/api/search/typeahead", {"q": "platform"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["query"], "platform")
        self.assertEqual(payload["backend"], "elasticsearch")
        self.assertEqual(payload["match_count"], 1)
        self.assertEqual(
            payload["results"],
            [
                {
                    "title": "Platform Engineer | Kumquat",
                    "url": "https://jobs.example.com/platform-engineer",
                    "summary": "Remote | Build agent-friendly search systems for jobs and documents.",
                }
            ],
        )
        search_mock.assert_called_once_with("platform", page=1, page_size=5)


class AdminDashboardSearchAnalyticsTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(
            username="admin-dashboard",
            email="admin-dashboard@example.com",
            password="password",
            is_superuser=True,
            is_staff=True,
        )

    def test_dashboard_data_includes_total_search_count(self):
        SearchCommandAnalytics.objects.create(channel="cli", command_count=3)
        SearchCommandAnalytics.objects.create(channel="web", command_count=7)
        self.client.force_login(self.user)

        response = self.client.get("/dashboard/data")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["stats"]["searches"], 10)
        self.assertEqual(
            payload["search_analytics"],
            [
                {
                    "channel": "cli",
                    "command_count": 3,
                    "last_command_at": None,
                },
                {
                    "channel": "web",
                    "command_count": 7,
                    "last_command_at": None,
                },
            ],
        )
