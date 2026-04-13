import hashlib

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

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

        self.assertEqual(payload["wallet"]["address"], hashlib.sha256(bytes.fromhex(wallet.public_key)).hexdigest())
        self.assertEqual(wallet.address, payload["wallet"]["address"])
        self.assertEqual(_decrypt_wallet_private_key(wallet.encrypted_private_key), private_key)
        self.assertNotEqual(wallet.encrypted_private_key, private_key)

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

    def test_wallet_generation_requires_authentication(self):
        response = self.client.post(
            "/wallets/generate",
            data="{}",
            content_type="application/json",
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 401)
