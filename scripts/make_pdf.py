#!/usr/bin/env python3
"""Build docs/portfolio/case-study.pdf: one US Letter page from docs/portfolio/case-study.md.

Numbers in {{braces}} are filled from out/ci-facts.json, which scripts/collect_facts.py writes
from test and build output. A placeholder with no fact stops the build, so nothing is guessed.
"""

from __future__ import annotations

import base64
import html
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs" / "portfolio" / "case-study.md"
OUT = ROOT / "docs" / "portfolio" / "case-study.pdf"
FACTS = ROOT / "out" / "ci-facts.json"

CSS = """
@page { size: Letter; margin: 0.6in; }
* { box-sizing: border-box; }
body { margin: 0; font: 10pt/1.38 Helvetica, Arial, sans-serif; color: #1d2433; }
h1 { font-size: 17pt; line-height: 1.15; margin: 0 0 2pt; color: #14213d; }
.sub { color: #5b6577; font-size: 10pt; margin: 0 0 6pt; }
.rule { height: 3pt; background: #e8a317; margin: 0 0 8pt; }
h2 { font-size: 10.5pt; margin: 8pt 0 2pt; color: #14213d;
     text-transform: uppercase; letter-spacing: .04em; }
p { margin: 0 0 3pt; }
ul { margin: 0 0 2pt; padding-left: 14pt; }
li { margin: 0 0 1.5pt; }
.fig { margin: 6pt 0 2pt; }
.fig img { width: 100%; }
footer { margin-top: 8pt; padding-top: 4pt; border-top: 0.5pt solid #cfd5e0;
         color: #5b6577; font-size: 9pt; }
"""


def fill(text: str, facts: dict[str, object]) -> str:
    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in facts:
            sys.exit(f"missing fact {key!r}; run collect_facts.py first")
        value = facts[key]
        return f"{value:,}" if isinstance(value, int) else str(value)

    return re.sub(r"\{\{(\w+)\}\}", repl, text)


def render(markdown: str, diagram_uri: str) -> str:
    parts: list[str] = []
    title = ""
    footer = ""
    bullets: list[str] = []

    def flush() -> None:
        nonlocal bullets
        if bullets:
            parts.append("<ul>" + "".join(f"<li>{html.escape(b)}</li>" for b in bullets) + "</ul>")
            bullets = []

    for raw in markdown.splitlines():
        line = raw.rstrip()
        if not line:
            flush()
        elif line.startswith("# "):
            title = line[2:]
            parts.append(f"<h1>{html.escape(title)}</h1>")
        elif line.startswith("subtitle: "):
            parts.append(f'<div class="sub">{html.escape(line[10:])}</div><div class="rule"></div>')
        elif line.startswith("## "):
            flush()
            parts.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("- "):
            bullets.append(line[2:])
        elif line == "[[diagram]]":
            flush()
            parts.append(
                f'<div class="fig"><img src="{diagram_uri}" alt="Pipeline architecture"></div>'
            )
        elif line.startswith("footer: "):
            footer = line[8:]
        else:
            flush()
            parts.append(f"<p>{html.escape(line)}</p>")
    flush()
    body = "\n".join(parts)
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        f"<title>{html.escape(title)}</title><style>{CSS}</style></head>"
        f"<body>{body}<footer>{html.escape(footer)}</footer></body></html>"
    )


def main() -> int:
    facts = json.loads(FACTS.read_text()) if FACTS.exists() else {}
    diagram = ROOT / "docs" / "portfolio" / "diagram-wide.png"
    uri = "data:image/png;base64," + base64.b64encode(diagram.read_bytes()).decode()
    page_html = render(fill(SRC.read_text(), facts), uri)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content(page_html)
        page.pdf(
            path=str(OUT),
            format="Letter",
            print_background=True,
            margin={"top": "0.6in", "bottom": "0.6in", "left": "0.6in", "right": "0.6in"},
        )
        browser.close()
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
