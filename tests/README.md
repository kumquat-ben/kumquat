# Root Test Harness

This folder is for repository-level tests that should stay outside the Django app package.

## Website Selenium

The Selenium suite runs the Django website against an isolated test database and opens the site through a hostname instead of raw `localhost`.

Default hostname:

- `kumquat.test`

Install browser-test dependencies:

```bash
pip3 install -r tests/requirements.txt
```

Run the Selenium suite from the repository root:

```bash
python3 tests/run_website_selenium.py
```

Optional environment overrides:

- `KUMQUAT_TEST_HOST`: custom hostname to map to `127.0.0.1`
- `KUMQUAT_TEST_HEADLESS=0`: open a visible browser instead of headless mode
- `KUMQUAT_TEST_CHROME_BINARY`: explicit Chrome/Chromium binary path

Notes:

- The runner uses Django's test runner, so the database is isolated from local development data.
- Chrome host resolution is forced with `--host-resolver-rules`, which allows the browser to reach the local live server using the chosen hostname.
