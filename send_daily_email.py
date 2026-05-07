#!/usr/bin/env python3
"""인천공항 출국장 예상 승객수 대시보드 일일 메일링 (SMTP).

캡처는 외부에서 (capture_dashboard.py) 수행하고, 본 스크립트는 PNG를 받아 SMTP로 발송한다.

사용법:
    python send_daily_email.py <image_path> [--test]
"""
import argparse
import os
import smtplib
import sys
from datetime import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env")

DASHBOARD_URL = "https://jhawk-pax-congestion.onrender.com"
MAILING_LIST_PATH = ROOT / "mailing_list.txt"


def load_recipients() -> list[str]:
    """mailing_list.txt 우선, 없으면 MAIL_RECIPIENTS env (콤마/세미콜론 구분)."""
    if MAILING_LIST_PATH.exists():
        text = MAILING_LIST_PATH.read_text(encoding="utf-8")
    else:
        text = os.environ.get("MAIL_RECIPIENTS", "")
    seen: set[str] = set()
    parts: list[str] = []
    for chunk in text.replace(";", "\n").replace(",", "\n").splitlines():
        addr = chunk.strip()
        if not addr or addr.startswith("#"):
            continue
        key = addr.lower()
        if key in seen:
            continue
        seen.add(key)
        parts.append(addr)
    return parts


def send(image_path: Path, recipients: list[str], date_str: str) -> None:
    user = os.environ["GMAIL_USER"].strip()
    password = "".join(os.environ["GMAIL_APP_PASSWORD"].split())

    msg = MIMEMultipart("related")
    msg["Subject"] = f"인천공항 출국장 예상 승객수 ({date_str})"
    msg["From"] = formataddr(("인천공항점(마케팅)", user))
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)

    sent_at = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    html = f"""<!doctype html>
<html><body style="font-family:'Noto Sans KR','Helvetica Neue',Arial,sans-serif;color:#222;background:#f7f7fa;padding:20px;margin:0;">
  <div style="max-width:960px;margin:0 auto;background:#fff;padding:28px;border-radius:8px;border:1px solid #e5e5ea;">
    <p style="margin:0 0 12px 0;font-size:14px;color:#444;">안녕하세요,</p>
    <p style="margin:0 0 20px 0;font-size:14px;color:#444;"><strong>인천공항 출국장 예상 승객수</strong>를 공유드립니다.</p>
    <p style="margin:0 0 20px 0;">
      <img src="cid:dashboard" alt="인천공항 출국장 예상 승객수 {date_str}" style="max-width:100%;height:auto;display:block;border:1px solid #ddd;border-radius:4px;">
    </p>
    <p style="margin:0 0 8px 0;font-size:13px;">
      대시보드 바로 가기: <a href="{DASHBOARD_URL}" style="color:#13407F;text-decoration:none;font-weight:600;">{DASHBOARD_URL}</a>
    </p>
    <p style="margin:24px 0 0 0;font-size:11px;color:#999;border-top:1px solid #eee;padding-top:12px;">
      자동 발송 메일입니다. 생성 시각: {sent_at}
    </p>
  </div>
</body></html>"""
    alt.attach(MIMEText(html, "html", "utf-8"))

    with open(image_path, "rb") as f:
        img = MIMEImage(f.read(), _subtype="png")
    img.add_header("Content-ID", "<dashboard>")
    img.add_header("Content-Disposition", "inline", filename=image_path.name)
    msg.attach(img)

    print(f"[SMTP] connect smtp.gmail.com:465", flush=True)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.set_debuglevel(1)
        server.login(user, password)
        refused = server.send_message(msg)
        print(f"[SMTP] send_message refused={refused!r}", flush=True)
        try:
            noop_code, noop_msg = server.noop()
            print(f"[SMTP] post-send NOOP={noop_code} {noop_msg!r}", flush=True)
        except Exception as e:
            print(f"[SMTP] post-send NOOP exception: {e!r}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="인천공항 출국장 예상 승객수 대시보드 일일 메일링")
    parser.add_argument("image", type=Path, help="발송할 PNG 이미지 경로")
    parser.add_argument("--test", action="store_true", help="GMAIL_USER 본인에게만 발송")
    args = parser.parse_args()

    if not args.image.exists():
        print(f"이미지 파일이 없습니다: {args.image}", file=sys.stderr)
        return 1

    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")

    if args.test:
        recipients = [os.environ["GMAIL_USER"]]
    else:
        recipients = load_recipients()
        if not recipients:
            print("수신자 목록이 비어있습니다 (mailing_list.txt / MAIL_RECIPIENTS)", file=sys.stderr)
            return 1

    print(f"[MAIL] recipients (count={len(recipients)}): {recipients}", flush=True)
    send(args.image, recipients, today)
    print(f"발송 완료: {today} → {recipients}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
