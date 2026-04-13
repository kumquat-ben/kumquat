import hashlib

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from .address_codec import decode_address, encode_address, normalize_address
from .models import UserWallet
from .views import _decrypt_wallet_private_key


class WalletGenerationTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(
            username="wallet-user",
            email="wallet@example.com",
            password="test-password-123",
        )

    def test_wallet_generation_matches_blockchain_address_derivation(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        wallet = UserWallet.objects.get(user=self.user)
        private_key = payload["wallet"]["private_key"]

        expected_address = encode_address(hashlib.sha256(bytes.fromhex(wallet.public_key)).digest())
        self.assertEqual(payload["wallet"]["address"], expected_address)
        self.assertEqual(wallet.address, payload["wallet"]["address"])
        self.assertEqual(_decrypt_wallet_private_key(wallet.encrypted_private_key), private_key)
        self.assertNotEqual(wallet.encrypted_private_key, private_key)

    def test_address_codec_accepts_legacy_hex_and_normalizes_to_kmq(self):
        raw_address = hashlib.sha256(b"kumquat-test").digest()
        legacy_hex = raw_address.hex()

        self.assertEqual(decode_address(legacy_hex), raw_address)
        self.assertEqual(normalize_address(legacy_hex), encode_address(raw_address))

    def test_user_cannot_generate_second_wallet(self):
        self.client.force_login(self.user)

        first_response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )
        self.assertEqual(first_response.status_code, 201)

        second_response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(second_response.status_code, 409)
        self.assertEqual(UserWallet.objects.filter(user=self.user).count(), 1)

    def test_user_can_regenerate_existing_wallet(self):
        self.client.force_login(self.user)

        first_response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )
        self.assertEqual(first_response.status_code, 201)
        original_wallet = UserWallet.objects.get(user=self.user)
        original_address = original_wallet.address
        original_public_key = original_wallet.public_key

        second_response = self.client.post(
            "/wallets/regenerate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(second_response.status_code, 200)
        payload = second_response.json()
        updated_wallet = UserWallet.objects.get(user=self.user)

        self.assertEqual(payload["status"], "regenerated")
        self.assertEqual(UserWallet.objects.filter(user=self.user).count(), 1)
        self.assertNotEqual(updated_wallet.address, original_address)
        self.assertNotEqual(updated_wallet.public_key, original_public_key)
        self.assertEqual(updated_wallet.address, payload["wallet"]["address"])

    def test_wallet_generation_requires_authentication(self):
        response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 401)

        regenerate_response = self.client.post(
            "/wallets/regenerate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(regenerate_response.status_code, 401)
