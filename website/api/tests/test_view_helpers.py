import hashlib
import hmac

from django.test import RequestFactory, SimpleTestCase, override_settings

from api.address_codec import encode_address
from api.views import (
    _absolute_url,
    _flatten_request_data,
    _normalize_reward_address,
    _sanitize_signature_value,
    _validate_vonage_signature,
)


class ViewHelperTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    @override_settings(SITE_URL="https://kumquat.example")
    def test_absolute_url_preserves_external_urls_and_expands_paths(self):
        self.assertEqual(_absolute_url("https://cdn.example/image.svg"), "https://cdn.example/image.svg")
        self.assertEqual(_absolute_url("/wallet"), "https://kumquat.example/wallet")

    def test_flatten_request_data_merges_json_and_query_string_values(self):
        request = self.factory.post(
            "/messages?channel=sms",
            data='{"name":"json-name","email":"json@example.com"}',
            content_type="application/json",
        )

        payload = _flatten_request_data(request)

        self.assertEqual(payload["name"], "json-name")
        self.assertEqual(payload["email"], "json@example.com")
        self.assertEqual(payload["channel"], "sms")

    def test_normalize_reward_address_accepts_kumquat_address(self):
        address = encode_address(hashlib.sha256(b"reward-address").digest())

        self.assertEqual(_normalize_reward_address(address), address)

    def test_normalize_reward_address_rejects_invalid_address(self):
        with self.assertRaisesMessage(ValueError, "Reward address must be a valid Kumquat wallet address"):
            _normalize_reward_address("not-a-wallet")

    @override_settings(
        VONAGE_SMS_SIGNATURE_SECRET="",
        VONAGE_ACCOUNT_SECRET="secret",
        VONAGE_SMS_SIGNATURE_ALGORITHM="md5hash",
    )
    def test_validate_vonage_signature_uses_account_secret_fallback(self):
        payload = {
            "api_key": "abc123",
            "msisdn": "+15555550123",
            "text": "hello&world=1",
        }
        signature_input = (
            f"&api_key={_sanitize_signature_value(payload['api_key'])}"
            f"&msisdn={_sanitize_signature_value(payload['msisdn'])}"
            f"&text={_sanitize_signature_value(payload['text'])}"
        )
        payload["sig"] = hashlib.md5(f"{signature_input}secret".encode("utf-8")).hexdigest()

        is_valid, error = _validate_vonage_signature(payload)

        self.assertTrue(is_valid)
        self.assertIn("VONAGE_ACCOUNT_SECRET fallback", error)

    @override_settings(
        VONAGE_SMS_SIGNATURE_SECRET="top-secret",
        VONAGE_ACCOUNT_SECRET="",
        VONAGE_SMS_SIGNATURE_ALGORITHM="sha256",
    )
    def test_validate_vonage_signature_supports_hmac_algorithms(self):
        payload = {
            "api_key": "abc123",
            "text": "hello",
        }
        signature_input = "&api_key=abc123&text=hello"
        payload["sig"] = hmac.new(
            b"top-secret",
            signature_input.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        is_valid, error = _validate_vonage_signature(payload)

        self.assertTrue(is_valid)
        self.assertEqual(error, "")
