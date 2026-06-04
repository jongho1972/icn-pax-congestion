#!/usr/bin/env python3
"""인천공항 국제선 출국객수 대시보드 일일 메일링 (SMTP).

캡처는 외부에서 (capture_dashboard.py) 수행하고, 본 스크립트는 PNG를 받아 SMTP로 발송한다.

사용법:
    python send_daily_email.py <image_path> [--test]
"""
import argparse
import os
import re
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from icn_utils.aggregator import mtd_reserved, reserved_summary
from icn_utils.data_loader import load_day, load_range
from icn_utils.exchange_rate import load_rates

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env")

DASHBOARD_URL = "https://pax.jhawk.kr"
MAILING_LIST_PATH = ROOT / "mailing_list.txt"
DAILY_DIR = ROOT / "Daily_Data"
WEEKDAY_HANJA = ["月", "火", "水", "木", "金", "土", "日"]
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def to_sms_safe(text: str) -> str:
    """문자발송 시스템(EUC-KR/SMS 문자셋) 호환 텍스트로 변환.

    Gmail 본문은 UTF-8이라 ₩(U+20A9)·한자 요일·em-dash 등 EUC-KR 밖 문자가
    섞이면 일부 문자발송 시스템에서 변환 에러가 난다. 해당 문자만 ASCII/한글로 치환.
    """
    # 환율 미가용 폴백: "$1=₩—" → "$1=-"
    text = text.replace("₩—", "-").replace("₩–", "-").replace("₩-", "-")
    # 원화 기호: "$1=₩1,511.3" → "$1=1,511.3원"
    text = re.sub(r"₩([\d,.]+)", r"\1원", text)
    # 한자 요일 → 한글 요일
    for han, kr in zip(WEEKDAY_HANJA, WEEKDAY_KR):
        text = text.replace(han, kr)
    # 잔여 특수문자 정리
    text = text.replace("₩", "원").replace("—", "-").replace("–", "-")
    return text


def build_kpi_block() -> tuple[str, str]:
    """대시보드와 동일한 SMS 기준(예약합계 출국)으로 KPI 텍스트를 만들어 반환.

    focus 일자 = tomorrow 발표(reserved.tomorrow.total>0) 시 내일, 아니면 오늘.
    Returns: (kpi_text_with_newlines, focus_yyyy_mm_dd)
    """
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    tomorrow = today + timedelta(days=1)
    today_ymd = today.strftime("%Y%m%d")
    tomorrow_ymd = tomorrow.strftime("%Y%m%d")

    today_data, _ = load_day(str(DAILY_DIR), today_ymd)
    tomorrow_data, _ = load_day(str(DAILY_DIR), tomorrow_ymd)

    first = today.replace(day=1)
    daily_map = load_range(str(DAILY_DIR), first, tomorrow)

    # 전월(1일~말일) 데이터 — D-1 누적 일수 부족 시 baseline 폴백 (대시보드와 동일)
    prev_last = first - timedelta(days=1)
    prev_first = prev_last.replace(day=1)
    prev_month_map = load_range(str(DAILY_DIR), prev_first, prev_last)

    reserved = reserved_summary(today_data, tomorrow_data)
    rates = load_rates(DAILY_DIR)

    focus_is_tomorrow = bool(reserved["tomorrow"]["total"] > 0)
    focus_date = tomorrow if focus_is_tomorrow else today
    # baseline 윈도우 끝점 = focus 일자 직전 (대시보드와 동일 기준)
    anchor_end = focus_date - timedelta(days=1)
    mtd = mtd_reserved(daily_map, today, prev_month_map, anchor_end)
    focus_ymd = tomorrow_ymd if focus_is_tomorrow else today_ymd
    focus_kpi = reserved["tomorrow"] if focus_is_tomorrow else reserved["today"]

    weekday_kr = WEEKDAY_HANJA[focus_date.weekday()]
    rate_value = rates.get(focus_ymd) or rates.get(today_ymd)
    rate_str = f"{rate_value:,.1f}" if rate_value else "—"

    def fmt_n(n: int) -> str:
        return f"{n:,}명" if n else "명"

    label = mtd.get("label") or "—"
    available = bool(mtd.get("available"))
    lines = [
        f"{focus_date.month}/{focus_date.day}일({weekday_kr})",
        f"전체 출국객수: {fmt_n(focus_kpi['total'])}",
        f"T1 출국객수: {fmt_n(focus_kpi['T1'])}",
    ]
    if available:
        lines.append(f"({label} {fmt_n(mtd['T1'])})")
    lines.append(f"T2 출국객수: {fmt_n(focus_kpi['T2'])}")
    if available:
        lines.append(f"({label} {fmt_n(mtd['T2'])})")
    lines.append(f"환율 $1=₩{rate_str}")
    text = "\n".join(lines)
    return text, focus_date.strftime("%Y-%m-%d")


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


def send(image_path: Path, recipients: list[str], date_str: str, kpi_text: str) -> None:
    kpi_text_sms = to_sms_safe(kpi_text)
    user = os.environ["GMAIL_USER"].strip()
    password = "".join(os.environ["GMAIL_APP_PASSWORD"].split())

    msg = MIMEMultipart("related")
    msg["Subject"] = f"인천공항 국제선 출국객수 ({date_str})"
    msg["From"] = formataddr(("인천공항점(마케팅)", user))
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)

    sent_at = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    html = f"""<!doctype html>
<html><body style="font-family:'Noto Sans KR','Helvetica Neue',Arial,sans-serif;color:#222;background:#f7f7fa;padding:20px;margin:0;">
  <div style="max-width:960px;margin:0 auto;background:#fff;padding:28px;border-radius:8px;border:1px solid #e5e5ea;">
    <p style="margin:0 0 12px 0;font-size:14px;color:#444;">안녕하세요,</p>
    <p style="margin:0 0 20px 0;font-size:14px;color:#444;"><strong>인천공항 국제선 출국객수</strong>를 공유드립니다.</p>
    <div style="margin:0 0 20px 0;padding:14px 16px;background:#f4f6f9;border:1px dashed #b9c2cf;border-radius:6px;">
      <p style="margin:0 0 8px 0;font-size:12px;color:#13407F;font-weight:700;">문자 발송용 (아래 텍스트를 복사해 문자 시스템에 붙여넣으세요)</p>
      <div style="margin:0;font-size:13px;color:#222;line-height:1.7;white-space:pre-line;font-family:'Courier New',monospace;">{kpi_text_sms}</div>
    </div>
    <p style="margin:0 0 20px 0;">
      <img src="cid:dashboard" alt="인천공항 국제선 출국객수 {date_str}" style="max-width:100%;height:auto;display:block;border:1px solid #ddd;border-radius:4px;">
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
    parser = argparse.ArgumentParser(description="인천공항 국제선 출국객수 대시보드 일일 메일링")
    parser.add_argument("image", type=Path, help="발송할 PNG 이미지 경로")
    parser.add_argument("--test", action="store_true", help="GMAIL_USER 본인에게만 발송")
    args = parser.parse_args()

    if not args.image.exists():
        print(f"이미지 파일이 없습니다: {args.image}", file=sys.stderr)
        return 1

    kpi_text, target_date = build_kpi_block()
    print(f"[KPI] focus={target_date}\n{kpi_text}", flush=True)
    print(f"[KPI-SMS] 문자발송용\n{to_sms_safe(kpi_text)}", flush=True)

    if args.test:
        recipients = [os.environ["GMAIL_USER"]]
    else:
        recipients = load_recipients()
        if not recipients:
            print("수신자 목록이 비어있습니다 (mailing_list.txt / MAIL_RECIPIENTS)", file=sys.stderr)
            return 1

    print(f"[MAIL] recipients (count={len(recipients)}): {recipients}", flush=True)
    send(args.image, recipients, target_date, kpi_text)
    print(f"발송 완료: {target_date} → {recipients}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
