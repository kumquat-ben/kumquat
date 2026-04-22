from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment
from django import template
from django.utils.safestring import mark_safe


register = template.Library()

ALLOWED_TAGS = {
    "a",
    "b",
    "blockquote",
    "br",
    "code",
    "em",
    "i",
    "li",
    "ol",
    "p",
    "strong",
    "ul",
}
ALLOWED_ATTRS = {"a": {"href", "title", "target", "rel"}}
ALLOWED_SCHEMES = {"", "http", "https", "mailto"}


def _is_safe_href(value):
    href = (value or "").strip()
    if not href:
        return False
    return urlsplit(href).scheme.lower() in ALLOWED_SCHEMES


@register.filter(name="render_basic_html")
def render_basic_html(value):
    if not value:
        return ""

    soup = BeautifulSoup(str(value), "html.parser")

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    for tag in soup.find_all(True):
        if tag.name in {"script", "style", "iframe", "object", "embed"}:
            tag.decompose()
            continue

        if tag.name not in ALLOWED_TAGS:
            tag.unwrap()
            continue

        allowed_attrs = ALLOWED_ATTRS.get(tag.name, set())
        for attr in list(tag.attrs):
            if attr not in allowed_attrs:
                del tag.attrs[attr]

        if tag.name == "a":
            href = tag.attrs.get("href")
            if not _is_safe_href(href):
                del tag.attrs["href"]
            else:
                tag.attrs["rel"] = "nofollow noreferrer"
                if tag.attrs.get("target") == "_blank":
                    tag.attrs["target"] = "_blank"
                elif "target" in tag.attrs:
                    del tag.attrs["target"]

    return mark_safe(str(soup))
