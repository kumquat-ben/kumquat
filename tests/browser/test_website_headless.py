import os
from pathlib import Path

from django.conf import settings
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class HostnameChromeTestCase(StaticLiveServerTestCase):
    host = "127.0.0.1"
    browser = None
    wait = None
    browser_hostname = os.environ.get("KUMQUAT_TEST_HOST", "kumquat.test")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.browser = webdriver.Chrome(options=cls._build_chrome_options())
        cls.browser.set_window_size(1440, 1200)
        cls.wait = WebDriverWait(cls.browser, 10)

    @classmethod
    def tearDownClass(cls):
        if cls.browser is not None:
            cls.browser.quit()
        super().tearDownClass()

    @classmethod
    def _build_chrome_options(cls):
        options = Options()
        if os.environ.get("KUMQUAT_TEST_HEADLESS", "1") != "0":
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1440,1200")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(
            f"--host-resolver-rules=MAP {cls.browser_hostname} 127.0.0.1"
        )

        chrome_binary = os.environ.get("KUMQUAT_TEST_CHROME_BINARY")
        if chrome_binary:
            options.binary_location = chrome_binary
        else:
            default_macos_binary = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
            if default_macos_binary.exists():
                options.binary_location = str(default_macos_binary)

        return options

    @property
    def hostname_live_server_url(self):
        _, port = self.live_server_url.rsplit(":", 1)
        return f"http://{self.browser_hostname}:{port}"

    def open_path(self, path="/"):
        self.browser.get(f"{self.hostname_live_server_url}{path}")

    def scroll_element_into_view(self, element):
        self.browser.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
            element,
        )


class WebsiteSmokeTests(HostnameChromeTestCase):
    def test_home_page_renders_through_custom_hostname(self):
        self.open_path("/")

        self.wait.until(EC.title_contains("Kumquat"))
        hero_title = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".hero-title"))
        )

        self.assertIn("It's Not an Apple. It's Not Bitcoin.", hero_title.text)
        self.assertEqual(self.browser.execute_script("return window.location.hostname;"), self.browser_hostname)

    def test_home_page_early_access_form_submits(self):
        self.open_path("/")

        email_input = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, 'form[action="/early-access"] input[name="email"]'))
        )
        form = self.browser.find_element(By.CSS_SELECTOR, 'form[action="/early-access"]')
        name_input = self.browser.find_element(By.CSS_SELECTOR, 'form[action="/early-access"] input[name="name"]')
        submit_button = self.browser.find_element(By.CSS_SELECTOR, 'form[action="/early-access"] button[type="submit"]')

        self.scroll_element_into_view(submit_button)
        self.assertTrue(submit_button.is_displayed())

        name_input.send_keys("Selenium User")
        email_input.send_keys("selenium@example.com")
        self.browser.execute_script("arguments[0].requestSubmit();", form)

        success_title = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".signup-success h3"))
        )
        self.assertEqual(success_title.text.strip(), "You’re on the list.")

    def test_sign_in_page_loads_on_hostname(self):
        self.open_path("/auth/sign-in")

        self.wait.until(EC.title_contains("Sign In"))
        card_title = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".signin-title"))
        )

        self.assertIn("Join before the", card_title.text)
        self.assertEqual(settings.SITE_URL, f"http://{self.browser_hostname}")
