from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from django.conf import settings

try:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas
except Exception:  # pragma: no cover - dependency import is environment-specific
    LETTER = None
    inch = None
    canvas = None


def build_runtime_artifact_dir(*parts: str) -> Path:
    root = Path(settings.BASE_DIR) / ".application_runtime"
    path = root.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _iter_resume_lines(applicant: Dict[str, Any]) -> Iterable[str]:
    full_name = (applicant.get("full_name") or "").strip()
    headline = (applicant.get("headline") or "").strip()
    summary = (applicant.get("summary") or "").strip()
    email = (applicant.get("email") or "").strip()
    location = (applicant.get("location") or "").strip()
    website = (applicant.get("website") or "").strip()
    linkedin = (applicant.get("linkedin") or "").strip()
    skills = applicant.get("skills") or []
    experiences = applicant.get("experiences") or []
    educations = applicant.get("educations") or []

    if full_name:
        yield full_name
    if headline:
        yield headline
    contact_line = " | ".join(value for value in (email, location, website, linkedin) if value)
    if contact_line:
        yield contact_line
    if summary:
        yield ""
        yield "Summary"
        yield summary
    if skills:
        yield ""
        yield "Skills"
        yield ", ".join(str(skill).strip() for skill in skills if str(skill).strip())
    if experiences:
        yield ""
        yield "Experience"
        for experience in experiences:
            title = " - ".join(
                value
                for value in (
                    str(experience.get("title") or "").strip(),
                    str(experience.get("company") or "").strip(),
                )
                if value
            )
            if title:
                yield title
            detail_line = " | ".join(
                value
                for value in (
                    str(experience.get("location") or "").strip(),
                    str(experience.get("date_range") or "").strip(),
                )
                if value
            )
            if detail_line:
                yield detail_line
            description = str(experience.get("description") or "").strip()
            if description:
                yield description
            highlights = experience.get("highlights") or []
            for highlight in highlights:
                cleaned = str(highlight).strip()
                if cleaned:
                    yield f"* {cleaned}"
    if educations:
        yield ""
        yield "Education"
        for education in educations:
            degree_line = " - ".join(
                value
                for value in (
                    str(education.get("degree") or "").strip(),
                    str(education.get("institution") or "").strip(),
                )
                if value
            )
            if degree_line:
                yield degree_line
            detail_line = " | ".join(
                value
                for value in (
                    str(education.get("field_of_study") or "").strip(),
                    str(education.get("date_range") or "").strip(),
                )
                if value
            )
            if detail_line:
                yield detail_line


def _split_text_line(text: str, max_length: int = 95) -> List[str]:
    words = text.split()
    if not words:
        return [""]

    lines: List[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if len(candidate) <= max_length:
            current = candidate
            continue
        lines.append(current)
        current = word
    lines.append(current)
    return lines


def generate_resume_pdf(applicant: Dict[str, Any], *, user_id: int, run_id: int) -> Path:
    if canvas is None or LETTER is None or inch is None:
        raise RuntimeError("Resume PDF generation requires reportlab to be installed.")

    output_dir = build_runtime_artifact_dir("resumes", str(user_id))
    output_path = output_dir / f"job_application_resume_{run_id}.pdf"

    pdf = canvas.Canvas(str(output_path), pagesize=LETTER)
    page_width, page_height = LETTER
    x = 0.75 * inch
    y = page_height - 0.75 * inch
    line_height = 14

    for raw_line in _iter_resume_lines(applicant):
        lines = _split_text_line(raw_line) if raw_line else [""]
        for line in lines:
            if y <= 0.75 * inch:
                pdf.showPage()
                y = page_height - 0.75 * inch
            pdf.drawString(x, y, line)
            y -= line_height

    pdf.save()
    return output_path
