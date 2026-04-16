from django.test import Client, TestCase

from api.models import EarlyAccessSignup


class HomePageViewTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_home_page_renders_search_box(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Beta")
        self.assertContains(response, "currently in beta")
        self.assertContains(response, 'role="search"', html=False)
        self.assertContains(response, 'name="q"', html=False)
        self.assertContains(response, "Request reply")

    def test_home_page_echoes_submitted_query_in_reply_panel(self):
        response = self.client.get("/", {"q": "find kumquat wallet docs"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "find kumquat wallet docs")
        self.assertContains(response, "Search capture is live")


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
