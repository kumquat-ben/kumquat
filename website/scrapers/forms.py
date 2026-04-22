from django import forms


class ScraperCreateForm(forms.Form):
    company = forms.CharField(max_length=255, help_text="Name of the company to associate with this scraper.")
    url = forms.URLField(help_text="Starting URL that the scraper should target.")
    interval_hours = forms.IntegerField(
        min_value=1,
        initial=24,
        help_text="How frequently (in hours) the scraper should run.",
    )
    timeout_seconds = forms.IntegerField(
        min_value=30,
        initial=180,
        help_text="Maximum runtime (seconds) before aborting the scraper.",
    )
    active = forms.BooleanField(
        required=False,
        initial=True,
        help_text="Uncheck to create the scraper without scheduling it yet.",
    )
