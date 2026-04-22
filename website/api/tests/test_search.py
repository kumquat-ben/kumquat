from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from api.models import SearchCommandAnalytics, SearchCrawlTarget, SearchDocument
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
