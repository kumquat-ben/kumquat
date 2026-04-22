import hashlib
import json
from unittest.mock import patch

from urllib.error import HTTPError
from django.test import Client, TestCase

from api.models import EarlyAccessSignup
from api.address_codec import encode_address
from scrapers.models import JobPosting, Scraper


class HomePageViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_home_page_renders_search_box(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'role="search"', html=False)
        self.assertContains(response, 'name="q"', html=False)
        self.assertContains(response, "Search all jobs.")
        self.assertContains(response, ">Search<", html=False)

    def test_home_page_echoes_submitted_query_in_reply_panel(self):
        with patch("api.views.search_jobs", return_value={"results": [], "match_count": 0, "backend": "elasticsearch"}):
            response = self.client.get("/", {"q": "python backend"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "python backend")
        self.assertContains(response, "No indexed jobs matched")

    def test_home_page_renders_indexed_search_results(self):
        scraper = Scraper.objects.create(company="Example Co", url="https://example.com/jobs", code="[]")
        JobPosting.objects.create(
            scraper=scraper,
            title="Senior Python Engineer",
            location="San Francisco, CA",
            link="https://example.com/jobs/python",
            description="Build search and indexing systems.",
        )

        with patch(
            "api.views.search_jobs",
            return_value={
                "results": [
                    {
                        "title": "Senior Python Engineer | Example Co",
                        "summary": "San Francisco, CA | Build search and indexing systems.",
                        "url": "https://example.com/jobs/python",
                    }
                ],
                "match_count": 1,
                "backend": "elasticsearch",
            },
        ):
            response = self.client.get("/", {"q": "python engineer"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Live")
        self.assertContains(response, "Senior Python Engineer")
        self.assertContains(response, "https://example.com/jobs/python")


class EarlyAccessSignupViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_json_signup_creates_record_and_normalizes_email(self):
        response = self.client.post(
            "/early-access",
            data='{"name":"Test User","email":"TEST@Example.COM"}',
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["status"], "created")
        signup = EarlyAccessSignup.objects.get()
        self.assertEqual(signup.name, "Test User")
        self.assertEqual(signup.email, "test@example.com")

    def test_json_signup_rejects_invalid_json(self):
        response = self.client.post(
            "/early-access",
            data="{",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "Invalid JSON body.")

    def test_form_signup_persists_and_redirects_home_story_anchor(self):
        response = self.client.post(
            "/early-access",
            data={"name": "Form User", "email": "form@example.com"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/#story")
        self.assertTrue(EarlyAccessSignup.objects.filter(email="form@example.com", name="Form User").exists())

    def test_form_signup_missing_email_sets_session_error(self):
        response = self.client.post(
            "/early-access",
            data={"name": "Missing Email", "email": ""},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/#story")
        session = self.client.session
        self.assertEqual(session["early_access_signup_error"], "Email is required.")
        self.assertEqual(
            session["early_access_signup"],
            {"name": "Missing Email", "email": ""},
        )


class HealthzViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_healthz_reports_ok(self):
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "database": "ok"})


class _MockUrlOpenResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ExplorerPageViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    @patch("api.views.urlopen")
    def test_explorer_home_renders_summary(self, mock_urlopen):
        mock_urlopen.return_value = _MockUrlOpenResponse(
            {
                "node": {
                    "latest_block_height": 259,
                    "peer_count": 1,
                    "mempool_size": 0,
                    "sync": {"status": "caught-up"},
                },
                "recent_blocks": [
                    {
                        "height": 259,
                        "hash": "a" * 64,
                        "prev_hash": "b" * 64,
                        "timestamp": 1710000000,
                        "miner_address": encode_address(hashlib.sha256(b"miner").digest()),
                        "transaction_count": 2,
                        "reward_token_count": 7,
                        "difficulty": 1,
                        "total_difficulty": "100",
                    }
                ],
                "recent_transactions": [
                    {
                        "hash": "c" * 64,
                        "block_height": 259,
                        "timestamp": 1710000001,
                        "sender_address": encode_address(hashlib.sha256(b"sender").digest()),
                        "recipient_address": encode_address(hashlib.sha256(b"recipient").digest()),
                        "value_cents": 1234,
                        "gas_price": 1,
                        "gas_limit": 21000,
                        "gas_used": 21000,
                        "nonce": 4,
                        "status": "confirmed",
                        "transfer_token_count": 1,
                        "coin_transfer_cents": 0,
                        "coin_fee_cents": 1,
                        "has_conversion_intent": False,
                    }
                ],
            }
        )

        with self.settings(EXPLORER_API_URL="http://explorer.test"):
            response = self.client.get("/explorer")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Public Chain Monitor")
        self.assertContains(response, "Latest Mined Block")
        self.assertContains(response, "259")
        self.assertContains(response, "/explorer/blocks/259")
        self.assertContains(response, "caught-up")
        self.assertContains(response, "12.34")

    @patch("api.views.urlopen")
    def test_explorer_home_handles_missing_upstream_summary(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            url="http://explorer.test/api/explorer/summary",
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

        with self.settings(EXPLORER_API_URL="http://explorer.test"):
            response = self.client.get("/explorer")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Explorer record not found.")

    def test_explorer_home_redirects_address_search_to_canonical_page(self):
        address = encode_address(hashlib.sha256(b"wallet").digest())

        response = self.client.get("/explorer", {"q": address})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"/explorer/addresses/{address}")

    @patch("api.views.urlopen")
    def test_explorer_block_page_renders_transactions(self, mock_urlopen):
        mock_urlopen.return_value = _MockUrlOpenResponse(
            {
                "block": {
                    "height": 42,
                    "hash": "d" * 64,
                    "prev_hash": "e" * 64,
                    "timestamp": 1710000020,
                    "miner_address": encode_address(hashlib.sha256(b"miner-42").digest()),
                    "transaction_count": 1,
                    "reward_token_count": 7,
                    "difficulty": 5,
                    "total_difficulty": "500",
                },
                "state_root": "f" * 64,
                "tx_root": "1" * 64,
                "pre_reward_state_root": "2" * 64,
                "result_commitment": "3" * 64,
                "poh_seq": 77,
                "poh_hash": "4" * 64,
                "nonce": 9,
                "conversion_fulfillment_order_count": 0,
                "transactions": [
                    {
                        "hash": "5" * 64,
                        "block_height": 42,
                        "timestamp": 1710000021,
                        "sender_address": encode_address(hashlib.sha256(b"s").digest()),
                        "recipient_address": encode_address(hashlib.sha256(b"r").digest()),
                        "value_cents": 500,
                        "gas_price": 1,
                        "gas_limit": 21000,
                        "gas_used": 21000,
                        "nonce": 1,
                        "status": "confirmed",
                        "transfer_token_count": 1,
                        "coin_transfer_cents": 0,
                        "coin_fee_cents": 1,
                        "has_conversion_intent": False,
                    }
                ],
            }
        )

        with self.settings(EXPLORER_API_URL="http://explorer.test"):
            response = self.client.get("/explorer/blocks/42")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Height 42")
        self.assertContains(response, "5.00")
        self.assertContains(response, "Block contents")

    @patch("api.views.urlopen")
    def test_explorer_address_page_renders_account_state(self, mock_urlopen):
        address = encode_address(hashlib.sha256(b"acct").digest())
        mock_urlopen.return_value = _MockUrlOpenResponse(
            {
                "address": address,
                "account": {
                    "address": address,
                    "account_type": "user",
                    "balance_cents": 5050,
                    "nonce": 2,
                    "last_updated": 99,
                    "bill_count": 2,
                    "bill_value_cents": 5000,
                    "bill_breakdown": [
                        {"denomination": "50", "count": 1, "value_cents": 5000}
                    ],
                    "coin_value_cents": 50,
                    "coin_breakdown": [
                        {"denomination": "0.5", "count": 1, "value_cents": 50}
                    ],
                    "compatibility_token_count": 2,
                    "compute_allocation_count": 0,
                    "has_code": False,
                    "conversion_order": None,
                },
                "transactions": [],
            }
        )

        with self.settings(EXPLORER_API_URL="http://explorer.test"):
            response = self.client.get(f"/explorer/addresses/{address}")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "50.50")
        self.assertContains(response, "Bill Breakdown")
        self.assertContains(response, "No indexed transactions for this address yet.")
