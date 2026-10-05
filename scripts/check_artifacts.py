#!/usr/bin/env python3
"""Checks on the portfolio files. Exit non-zero on any problem.

  images    exactly 1000x750, OCR finds no email, link, phone-like number or account id
  pdf       exactly one page, no link or contact detail in the text, clean metadata
  card      summary.md hashes to the sha8 it states and stays inside the length limits
  style     no em or en dashes, arrows, emoji or banned words in the text files

Needs `tesseract` and poppler (`pdfinfo`, `pdftotext`) on the PATH.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
PORTFOLIO = ROOT / "docs" / "portfolio"

# PDF text and everything else: any "@" or link is a failure.
CONTACT = re.compile(r"https?://|www\.|@|github|\b\d{3}[-. )]\d{3}[-. ]\d{4}\b|\b\d{9,}\b", re.I)
# OCR of a screenshot: Airflow shows "@task" as an operator label, so look for real emails.
IMAGE_CONTACT = re.compile(
    r"https?://|www\.|[\w.+-]+@[\w-]+\.[\w.]+|github|\b\d{3}[-. )]\d{3}[-. ]\d{4}\b|\b\d{9,}\b",
    re.I,
)
BANNED_CHARS = re.compile("[–—←-⇿\U0001f300-\U0001f9ff☀-➿]")
BANNED_WORDS = re.compile(r"\b(certified|compliant|guarantee)\b", re.I)
LEAKS = re.compile(r"/Users/|kunjarbhaduri|gmail|upwork|kunjar-hq|lincoln", re.I)

FOOTER = (
    "Kunjar Bhaduri. I lead and deliver the work myself, using frontier AI tooling to move fast."
)

problems: list[str] = []


def fail(msg: str) -> None:
    problems.append(msg)
    print("FAIL:", msg)


def run(*cmd: str) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def check_images() -> None:
    images = sorted(PORTFOLIO.glob("img-*.png"))
    if not images:
        fail("no images in docs/portfolio")
    for path in images:
        size = Image.open(path).size
        if size != (1000, 750):
            fail(f"{path.name} is {size}, expected (1000, 750)")
        text = run("tesseract", str(path), "-", "--psm", "11")
        hit = IMAGE_CONTACT.search(text)
        if hit:
            fail(f"{path.name}: OCR text contains {hit.group(0)!r}")
        if LEAKS.search(text):
            fail(f"{path.name}: OCR text contains a private or personal marker")
        print(f"ok  {path.name} {size} OCR clean ({len(text.split())} words read)")


def check_pdf() -> None:
    pdf = PORTFOLIO / "case-study.pdf"
    if not pdf.exists():
        fail("case-study.pdf is missing")
        return
    info = run("pdfinfo", str(pdf))
    pages = int(re.search(r"^Pages:\s+(\d+)", info, re.M).group(1))  # type: ignore[union-attr]
    if pages != 1:
        fail(f"case-study.pdf has {pages} pages")
    size = re.search(r"^Page size:\s+(.+)$", info, re.M)
    if not size or "612" not in size.group(1) or "792" not in size.group(1):
        fail(f"page size is not US Letter: {size.group(1) if size else 'unknown'}")
    text = run("pdftotext", str(pdf), "-")
    if CONTACT.search(text):
        fail(f"PDF text contains {CONTACT.search(text).group(0)!r}")  # type: ignore[union-attr]
    if FOOTER not in text.replace("\n", " "):
        fail("PDF footer line is missing or altered")
    for field in ("Author", "Creator", "Producer", "Title", "Subject", "Keywords"):
        m = re.search(rf"^{field}:\s*(.*)$", info, re.M)
        value = m.group(1).strip() if m else ""
        if LEAKS.search(value) or "@" in value:
            fail(f"PDF metadata {field} leaks: {value!r}")
    if b"/URI" in pdf.read_bytes():
        fail("PDF contains a link annotation")
    print(f"ok  case-study.pdf pages={pages} metadata clean")


def check_card() -> None:
    text = (PORTFOLIO / "summary.md").read_text()
    fields = {
        k: v.strip()
        for k, v in re.findall(r"^(Title|Role|Description|Skills|Text sha8): (.+)$", text, re.M)
    }
    for key, limit in (("Title", 70), ("Role", 100), ("Description", 600)):
        if len(fields[key]) > limit:
            fail(f"card {key} is {len(fields[key])} characters, limit {limit}")
    if not fields["Title"].startswith("Sample: "):
        fail("card title must start with 'Sample: '")
    if len([s for s in fields["Skills"].split(",") if s.strip()]) != 5:
        fail("card needs exactly 5 skills")
    digest = hashlib.sha256(
        f"{fields['Title']}\n{fields['Role']}\n{fields['Description']}".encode()
    ).hexdigest()[:8]
    if digest != fields["Text sha8"]:
        fail(f"card sha8 is {digest}, summary.md states {fields['Text sha8']}")
    print(f"ok  card text sha8={digest}")


def check_style() -> None:
    files = [
        ROOT / "README.md",
        ROOT / "docs" / "architecture.md",
        ROOT / "data" / "SOURCES.md",
        *PORTFOLIO.glob("*.md"),
    ]
    for path in files:
        text = path.read_text()
        if BANNED_CHARS.search(text):
            fail(f"{path.relative_to(ROOT)} has a dash, arrow or emoji character")
        if BANNED_WORDS.search(text):
            fail(f"{path.relative_to(ROOT)} has a banned word")
        if re.search(r"upwork|kunjar-hq|lincoln", text, re.I):
            fail(f"{path.relative_to(ROOT)} mentions a private or marketplace name")
    print("ok  style sweep")


if __name__ == "__main__":
    check_card()
    check_style()
    check_images()
    check_pdf()
    print("RESULT:", "FAILED" if problems else "all artifact checks passed")
    sys.exit(1 if problems else 0)
