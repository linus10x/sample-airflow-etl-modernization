#!/usr/bin/env python3
"""Render the three portfolio images, each exactly 1000x750 (4:3), without stretching.

  img-1  pipeline architecture, drawn from docs/portfolio/architecture-card.mmd with mermaid-cli
  img-2  Airflow grid view of the 30 day backfill; the day-5 vendor_c file is quarantined
  img-3  the pipeline health report

img-2 and img-3 are screenshots of this sample's own UI on synthetic data. They need a finished
`make demo`. Use --only 1 to render just the diagram.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageChops

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "portfolio"
SIZE = (1000, 750)
BG = (255, 255, 255)


def fit(src: Path, dest: Path, margin: int = 24) -> None:
    """Scale to fit inside the canvas, keep the aspect ratio, centre on a white background."""
    img = Image.open(src).convert("RGB")
    box = (SIZE[0] - 2 * margin, SIZE[1] - 2 * margin)
    scale = min(box[0] / img.width, box[1] / img.height)
    resized = img.resize((round(img.width * scale), round(img.height * scale)), Image.LANCZOS)
    canvas = Image.new("RGB", SIZE, BG)
    canvas.paste(resized, ((SIZE[0] - resized.width) // 2, (SIZE[1] - resized.height) // 2))
    canvas.save(dest, optimize=True)


def diagram_source() -> str:
    """The card diagram is a 4:3 friendly redraw of the first diagram in docs/architecture.md."""
    return (OUT / "architecture-card.mmd").read_text()


def render_mermaid(source: str, name: str, tmp: Path, width: int) -> Path:
    mmd = tmp / f"{name}.mmd"
    mmd.write_text(source)
    png = tmp / f"{name}.png"
    subprocess.run(
        [
            "npx",
            "-y",
            "@mermaid-js/mermaid-cli@11",
            "-i",
            str(mmd),
            "-o",
            str(png),
            "-c",
            str(ROOT / "docs" / "mermaid-config.json"),
            "-p",
            str(ROOT / "docs" / "puppeteer-config.json"),
            "-b",
            "white",
            "-w",
            str(width),
            "-s",
            "1",
        ],
        check=True,
        cwd=ROOT,
    )
    return png


def wide_diagram(tmp: Path) -> None:
    """The horizontal diagram from docs/architecture.md, cropped to its content, for the PDF."""
    text = (ROOT / "docs" / "architecture.md").read_text()
    match = re.search(r"```mermaid\n(.*?)```", text, re.S)
    assert match, "no mermaid block in docs/architecture.md"
    png = render_mermaid(match.group(1), "wide", tmp, 2000)
    img = Image.open(png).convert("RGB")
    box = ImageChops.difference(img, Image.new("RGB", img.size, BG)).getbbox()
    assert box
    pad = 20
    box = (
        max(0, box[0] - pad),
        max(0, box[1] - pad),
        min(img.width, box[2] + pad),
        min(img.height, box[3] + pad),
    )
    img.crop(box).save(OUT / "diagram-wide.png", optimize=True)


def img1(tmp: Path) -> None:
    mmd = tmp / "pipeline.mmd"
    mmd.write_text(diagram_source())
    png = tmp / "pipeline.png"
    subprocess.run(
        [
            "npx",
            "-y",
            "@mermaid-js/mermaid-cli@11",
            "-i",
            str(mmd),
            "-o",
            str(png),
            "-c",
            str(ROOT / "docs" / "mermaid-config.json"),
            "-p",
            str(ROOT / "docs" / "puppeteer-config.json"),
            "-b",
            "white",
            "-w",
            "2400",
            "-s",
            "1",
        ],
        check=True,
        cwd=ROOT,
    )
    fit(png, OUT / "img-1.png")


def login(page, base: str) -> None:  # type: ignore[no-untyped-def]
    page.goto(base + "/")
    page.wait_for_selector("input[name='username']", timeout=30000)
    page.fill("input[name='username']", "admin")
    page.fill("input[name='password']", "admin")
    page.click("button[type='submit']")
    page.wait_for_url(re.compile(r".*/(dags|$).*"), timeout=30000)


def screenshots(tmp: Path, base: str) -> None:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1500, "height": 1125}, device_scale_factor=2)
        login(page, base)

        # The vendor_c task of the day-5 run: red, with the readable reason in its log, next to
        # the green runs around it.
        run = "backfill__2026-03-05T00:00:00+00:00"
        page.goto(
            f"{base}/dags/ingest_vendor_files/runs/{run}/tasks/ingest/mapped/2"
            "?run_after_lte=2026-03-12T00%3A00%3A00Z"
        )
        page.wait_for_selector("text=quarantined", timeout=30000)
        page.wait_for_timeout(4000)
        toggle = page.locator("text=AirflowFailException").first
        if toggle.count():
            toggle.click()  # fold the stack trace, keep the readable reason
        page.evaluate("document.querySelectorAll('*').forEach(e => { e.scrollLeft = 0; })")
        page.wait_for_timeout(500)
        page.screenshot(path=str(tmp / "airflow.png"))
        fit(tmp / "airflow.png", OUT / "img-2.png", margin=0)

        page.goto((ROOT / "out" / "health_report.html").as_uri())
        page.set_viewport_size({"width": 1400, "height": 1050})
        page.add_style_tag(content="html { zoom: 0.93; }")
        page.wait_for_timeout(500)
        page.screenshot(path=str(tmp / "report.png"))
        fit(tmp / "report.png", OUT / "img-3.png", margin=0)
        browser.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--only", type=int, choices=(1, 2, 3))
    parser.add_argument("--base", default="http://localhost:8080")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        if args.only in (None, 1):
            img1(tmp)
            wide_diagram(tmp)
        if args.only in (None, 2, 3):
            screenshots(tmp, args.base)
    for name in ("img-1.png", "img-2.png", "img-3.png"):
        path = OUT / name
        if path.exists():
            print(name, Image.open(path).size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
