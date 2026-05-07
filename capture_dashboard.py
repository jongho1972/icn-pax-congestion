#!/usr/bin/env python3
"""인천공항 출국장 예상 승객수 대시보드 .container 영역 PNG 캡처 (Playwright sync_api).

GitHub Actions 등 헤드리스 환경에서 실행. 비번 게이트 통과 + body.capturing 클래스 +
1.5배 확대 후 .container 캡처. 메일링 스크립트 send_daily_email.py가 이 PNG를 첨부한다.

사용법:
    python capture_dashboard.py <output.png>
"""
import os
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "https://jhawk-pax-congestion.onrender.com"
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "0708")


def capture(out_path: Path) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(viewport={"width": 2000, "height": 900})
        page = context.new_page()
        # Render 콜드스타트 흡수
        page.goto(URL, wait_until="networkidle", timeout=180_000)
        # 비번 게이트 통과
        page.locator("#pw-input").fill(DASHBOARD_PASSWORD)
        page.locator("#pw-input").press("Enter")
        # Plotly 차트 SVG 등장 대기
        page.wait_for_selector(".js-plotly-plot svg", state="attached", timeout=30_000)
        page.wait_for_timeout(2_000)
        # 캡처용 클래스 + 1.5배 확대 + Plotly 리사이즈 트리거
        page.evaluate(
            """() => {
                document.body.classList.add('capturing');
                document.documentElement.style.zoom = '1.5';
                if (window.Plotly) {
                    document.querySelectorAll('.js-plotly-plot').forEach(el => window.Plotly.Plots.resize(el));
                }
            }"""
        )
        page.wait_for_timeout(1_500)
        page.locator(".container").screenshot(path=str(out_path), type="png")
        browser.close()


def main() -> int:
    if len(sys.argv) != 2:
        print("사용법: python capture_dashboard.py <output.png>", file=sys.stderr)
        return 1
    out = Path(sys.argv[1])
    capture(out)
    print(f"캡처 완료: {out} ({out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
