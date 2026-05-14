"""인천공항 국제선 출국객수 대시보드 (FastAPI + Jinja2).

데이터 소스: airport.kr 공항 예상 혼잡도 엑셀 (자세한 스키마는
icn_utils/excel_parser.py 참조). 매일 17:05 + 23:30 KST cron으로 받아
Daily_Data/passgr_YYYYMMDD.pkl에 통합 dict 저장.
"""
from __future__ import annotations

import calendar
import csv
import hmac
import io
import json
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import holidays as _kr_holidays_pkg
except ImportError:  # holidays is in requirements; this is just a safety net
    _kr_holidays_pkg = None

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("icn_pax_congestion")

from icn_utils.aggregator import (
    ALL_ZONE_KEYS, REGIONS, T1_GATES, T2_GATES, WEEKDAY_KR,
    _reserved_out, daily_series_by_month, daily_totals, fmt_peak_hour,
    gate_compare, hourly_mtd_avg, hourly_per_gate, kpi_summary,
    monthly_compare, mtd_per_gate, mtd_reserved, mtd_route,
    prev_dow_reserved_avg,
    reserved_summary, route_compare, route_matrix, route_summary,
)
from icn_utils.data_loader import list_available_dates, load_day, load_range
from icn_utils.exchange_rate import load_rates

load_dotenv()

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"

DATA_START_DATE = date(2026, 1, 1)  # 엑셀 데이터 첫 일자 (2026-01-01부터 backfill 보유)

app = FastAPI(title="인천공항 국제선 출국객수")
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE / "templates"))

# ---------- TTL 캐시 ----------
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()
_TTL_SECONDS = 60 * 60 * 48


def _cache_get(key: str):
    with _CACHE_LOCK:
        if key not in _CACHE:
            return None
        ts, val = _CACHE[key]
        if time.time() - ts > _TTL_SECONDS:
            return None
        return val


def _cache_set(key: str, val) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), val)


def _cache_clear() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


# ---------- IP 레이트 리밋 ----------
_RATE_BUCKET: dict[str, list[float]] = {}
_RATE_LOCK = threading.Lock()


def _rate_check(ip: str, max_per_window: int = 5, window_seconds: int = 300) -> bool:
    now = time.time()
    with _RATE_LOCK:
        bucket = _RATE_BUCKET.setdefault(ip, [])
        bucket[:] = [t for t in bucket if now - t < window_seconds]
        if len(bucket) >= max_per_window:
            return False
        bucket.append(now)
        return True


def _kst_today() -> date:
    """KST 오늘. ICN_TODAY_OVERRIDE 환경변수(YYYYMMDD)로 시뮬레이션 가능."""
    override = os.environ.get("ICN_TODAY_OVERRIDE", "").strip()
    if override:
        try:
            return datetime.strptime(override, "%Y%m%d").date()
        except ValueError:
            logger.warning("invalid ICN_TODAY_OVERRIDE=%r, using real KST", override)
    return datetime.now(KST).date()


def _resolved_today(ym: str | None) -> tuple[date, bool]:
    """ym 쿼리(YYYYMM)이 유효한 과거 월을 가리키면 그 달 말일과 archive=True 반환.

    이번 달이거나 미래 월·잘못된 입력은 KST 오늘로 폴백 (archive=False).
    """
    today_kst = _kst_today()
    if not ym:
        return today_kst, False
    try:
        y, m = int(ym[:4]), int(ym[4:6])
        first = date(y, m, 1)
    except (ValueError, IndexError):
        return today_kst, False
    if first < DATA_START_DATE.replace(day=1):
        return today_kst, False
    if first.year == today_kst.year and first.month == today_kst.month:
        return today_kst, False
    if first > today_kst.replace(day=1):
        return today_kst, False
    next_month_first = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day = next_month_first - timedelta(days=1)
    return last_day, True


def _available_months() -> list[str]:
    """Daily_Data 보유 월(YYYYMM) — DATA_START_DATE 이후 + 이번 달 포함, 정렬."""
    avail = list_available_dates(str(DAILY_DIR))
    cutoff = DATA_START_DATE.strftime("%Y%m%d")
    months = sorted({d[:6] for d in avail if d >= cutoff})
    cur = _kst_today().strftime("%Y%m")
    if cur not in months:
        months.append(cur)
        months.sort()
    return months


# ---------- 페이로드 빌드 ----------
def build_payload(today: date | None = None, archive: bool = False) -> dict:
    if today is None:
        today = _kst_today()
    cache_key = today.strftime("%Y%m%d") + ("_a" if archive else "")
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _BUILD_LOCK:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        return _build_payload_locked(today, archive)


def _build_payload_locked(today: date, archive: bool = False) -> dict:
    cache_key = today.strftime("%Y%m%d") + ("_a" if archive else "")
    tomorrow = today + timedelta(days=1)
    # 이번달 1일~말일 전체 (차트·월누적용)
    curr_first = today.replace(day=1)
    last_dom_curr = calendar.monthrange(today.year, today.month)[1]
    curr_last = date(today.year, today.month, last_dom_curr)
    curr_range_start = max(curr_first, DATA_START_DATE)
    # live 모드: D+1까지는 별도로 보장. 이번달 안에서는 curr_last를 상한으로
    # archive 모드: 이번달(=과거 월) 말일까지
    curr_range_end = min(curr_last, tomorrow if not archive else curr_last)
    daily_map = load_range(str(DAILY_DIR), curr_range_start, curr_range_end)

    # 전월(1일~말일) 데이터 — 비교·차트 baseline용
    prev_last = curr_first - timedelta(days=1)
    prev_first = prev_last.replace(day=1)
    if prev_last < DATA_START_DATE:
        prev_month_map: dict = {}
    else:
        prev_first_clamped = max(prev_first, DATA_START_DATE)
        prev_month_map = load_range(str(DAILY_DIR), prev_first_clamped, prev_last)

    today_ymd = today.strftime("%Y%m%d")
    tomorrow_ymd = tomorrow.strftime("%Y%m%d")

    today_data, today_src = daily_map.get(today_ymd, (None, "none"))
    if archive:
        # archive 모드: tomorrow는 다음달 1일 → 카드 focus를 today로 강제
        tomorrow_data, tomorrow_src = None, "none"
    else:
        tomorrow_data, tomorrow_src = daily_map.get(tomorrow_ymd, (None, "none"))

    # KPI
    kpi = kpi_summary(today_data, tomorrow_data)

    # 핵심 요약 (SMS 동일 기준 — 예약합계 출국)
    reserved = reserved_summary(today_data, tomorrow_data)
    focus_is_tomorrow = bool(reserved["tomorrow"]["total"] > 0)
    focus_date = tomorrow if focus_is_tomorrow else today
    anchor_end = focus_date - timedelta(days=1)
    focus_wd = focus_date.weekday()
    # 전월 동요일 평균 (예약합계 출국 = 환승객 포함) — 항공편수 대시보드와 동일 패턴
    prev_year_kpi = prev_last.year if prev_last >= DATA_START_DATE else today.year
    prev_month_kpi = prev_last.month if prev_last >= DATA_START_DATE else today.month
    dow_reserved = prev_dow_reserved_avg(prev_month_map, prev_year_kpi, prev_month_kpi)
    dow_t1 = dow_reserved["T1"].get(focus_wd, 0)
    dow_t2 = dow_reserved["T2"].get(focus_wd, 0)
    dow_total = dow_reserved["total"].get(focus_wd, 0)
    dow_available = (dow_t1 > 0 or dow_t2 > 0)
    reserved_mtd = {
        "T1": dow_t1,
        "T2": dow_t2,
        "total": dow_total,
        "available": dow_available,
        "label": f"{prev_month_kpi}월 {WEEKDAY_KR[focus_wd]}요일 평균" if dow_available else "—",
        "kind": "prev_dow" if dow_available else "none",
        "anchor": f"{prev_month_kpi}월 {WEEKDAY_KR[focus_wd]}요일",
        "days": 0,
    }

    def _delta_pct(focus_v: int, base_v: int):
        if not dow_available:
            return None
        if not focus_v or not base_v or base_v <= 0:
            return None
        return round((focus_v - base_v) / base_v * 100, 1)

    reserved_delta = {
        "today": {
            "T1": _delta_pct(reserved["today"]["T1"], reserved_mtd["T1"]),
            "T2": _delta_pct(reserved["today"]["T2"], reserved_mtd["T2"]),
            "total": _delta_pct(reserved["today"]["total"], reserved_mtd["total"]),
        },
        "tomorrow": {
            "T1": _delta_pct(reserved["tomorrow"]["T1"], reserved_mtd["T1"]),
            "T2": _delta_pct(reserved["tomorrow"]["T2"], reserved_mtd["T2"]),
            "total": _delta_pct(reserved["tomorrow"]["total"], reserved_mtd["total"]),
        },
    }

    # 면세점 고시환율 (USD/KRW) — 서울외국환중개 전일 고시 (모든 면세점 공통)
    rates = load_rates(DAILY_DIR)
    exchange = {
        "today": rates.get(today_ymd),
        "tomorrow": rates.get(tomorrow_ymd),
        "available": len(rates),
    }

    # MTD — 일자별 추이 가로선·표 비교용 (KPI 카드와 동일 베이스 = 예약합계 출국, 환승객 포함)
    mtd = mtd_reserved(daily_map, today, prev_month_map, anchor_end)
    # 출국장별 평면도(▲▼)는 출국장 통과 raw 데이터 기반 → 그대로 출국장 통과 기준 유지
    gate_mtd = mtd_per_gate(daily_map, today, prev_month_map, anchor_end)
    # delta_pct는 mtd와 동일 베이스(예약합계 출국)로 산출 — focus(오늘/내일) 예약 vs MTD 평균
    focus_reserved = reserved["tomorrow"] if focus_is_tomorrow else reserved["today"]
    delta_pct_T1 = None
    delta_pct_T2 = None
    if mtd.get("available"):
        if mtd["T1"] > 0 and focus_reserved["T1"] > 0:
            delta_pct_T1 = round((focus_reserved["T1"] - mtd["T1"]) / mtd["T1"] * 100, 1)
        if mtd["T2"] > 0 and focus_reserved["T2"] > 0:
            delta_pct_T2 = round((focus_reserved["T2"] - mtd["T2"]) / mtd["T2"] * 100, 1)

    # 출국장별 시간대 (7개 zone)
    today_per_gate = hourly_per_gate(today_data)
    tomorrow_per_gate = hourly_per_gate(tomorrow_data)

    # 노선별
    today_route_T1 = route_matrix(today_data, "T1")
    today_route_T2 = route_matrix(today_data, "T2")
    tomorrow_route_T1 = route_matrix(tomorrow_data, "T1")
    tomorrow_route_T2 = route_matrix(tomorrow_data, "T2")
    today_route_summary_T1 = route_summary(today_data, "T1")
    today_route_summary_T2 = route_summary(today_data, "T2")
    tomorrow_route_summary_T1 = route_summary(tomorrow_data, "T1")
    tomorrow_route_summary_T2 = route_summary(tomorrow_data, "T2")
    mtd_route_T1 = mtd_route(daily_map, today, "T1", prev_month_map, anchor_end)
    mtd_route_T2 = mtd_route(daily_map, today, "T2", prev_month_map, anchor_end)

    # === 일자별 차트(항공편수 동일 패턴) + 월누적 + 동일일수 비교 ===
    prev_year = prev_last.year if prev_last >= DATA_START_DATE else today.year
    prev_month = prev_last.month if prev_last >= DATA_START_DATE else today.month
    last_dom_prev = calendar.monthrange(prev_year, prev_month)[1] if prev_last >= DATA_START_DATE else 0
    chart_last_day = max(last_dom_curr, last_dom_prev)

    # 일자별 추이 차트 — KPI 카드와 동일 베이스(예약합계 출국, 환승객 포함)
    chart_curr = daily_series_by_month(daily_map, today.year, today.month, chart_last_day, basis="reserved")
    chart_prev = daily_series_by_month(prev_month_map, prev_year, prev_month, chart_last_day, basis="reserved")

    # 주말·공휴일 (이번달 기준)
    if _kr_holidays_pkg is not None:
        try:
            kr_hol = _kr_holidays_pkg.KR(years=today.year)
        except Exception:
            kr_hol = {}
    else:
        kr_hol = {}
    red_days = []
    for d in range(1, chart_last_day + 1):
        if d > last_dom_curr:
            continue
        dt = date(today.year, today.month, d)
        if dt.weekday() >= 5 or dt in kr_hol:
            red_days.append(d)

    # 동일일수 비교용 cutoff — 이번달은 데이터 보유 최대 일자(D+1 포함, 1~13일까지 활용)
    # 전월은 동일 일수(말일을 넘을 수 없음)
    cutoff_curr = chart_curr["max_day"] if not archive else last_dom_curr
    cutoff_curr = max(0, min(cutoff_curr, last_dom_curr))
    cutoff_prev = min(cutoff_curr, last_dom_prev) if last_dom_prev > 0 else 0

    # 시간대별 차트 — 이번달 MTD 평균 vs 전월 동일일수 MTD 평균 (출국장 통과 기준)
    curr_hourly_mtd = hourly_mtd_avg(daily_map, today.year, today.month, cutoff_curr)
    prev_hourly_mtd = hourly_mtd_avg(prev_month_map, prev_year, prev_month, cutoff_prev) if last_dom_prev > 0 else {
        "hours": curr_hourly_mtd["hours"], "T1": [0]*24, "T2": [0]*24, "days": 0, "available": False,
    }
    mtd_hourly = {
        **curr_hourly_mtd,
        "label": f"{today.month}월 (1~{cutoff_curr}일)" if curr_hourly_mtd["available"] else "—",
    }
    prev_mtd_hourly = {
        **prev_hourly_mtd,
        "label": f"{prev_month}월 (1~{cutoff_prev}일)" if prev_hourly_mtd["available"] else "—",
    }

    # 월누적 표·요약 — KPI 카드와 동일 베이스(예약합계 출국)
    monthly = monthly_compare(daily_map, prev_month_map,
                              today.year, today.month, prev_year, prev_month,
                              cutoff_curr, cutoff_prev, basis="reserved")
    gate_cmp = gate_compare(daily_map, prev_month_map,
                            today.year, today.month, prev_year, prev_month,
                            cutoff_curr, cutoff_prev)
    route_cmp_T1 = route_compare(daily_map, prev_month_map, "T1",
                                 today.year, today.month, prev_year, prev_month,
                                 cutoff_curr, cutoff_prev)
    route_cmp_T2 = route_compare(daily_map, prev_month_map, "T2",
                                 today.year, today.month, prev_year, prev_month,
                                 cutoff_curr, cutoff_prev)

    prev_label_text = f"{prev_month}월" if last_dom_prev > 0 else None
    curr_label_text = f"{today.month}월"
    period_label = f"1~{cutoff_curr}일" if cutoff_curr > 0 else "—"

    chart_data = {
        "chart_last_day": chart_last_day,
        "today_day": today.day if not archive else None,
        "max_day_curr": chart_curr["max_day"],
        "max_day_prev": chart_prev["max_day"],
        "red_days": red_days,
        "curr_label": curr_label_text,
        "prev_label": prev_label_text or "전월",
        "series": {
            "T1_curr": chart_curr["T1"], "T2_curr": chart_curr["T2"],
            "T1_prev": chart_prev["T1"], "T2_prev": chart_prev["T2"],
        },
    }

    # 일자별 추이 표 — KPI 카드와 동일 베이스(예약합계 출국)
    daily_df = daily_totals(daily_map, basis="reserved")

    # 전월 동요일비 산출용 평균 — 일자별 표 ▲▼ 비교용, KPI와 동일 베이스
    dow_avg = prev_dow_reserved_avg(prev_month_map, prev_year, prev_month) if last_dom_prev > 0 else {"T1": {}, "T2": {}, "total": {}}

    def _ratio(c: int, avg) -> float | None:
        if not avg or avg <= 0:
            return None
        return round((c - avg) / avg * 100, 1)

    table_rows = []
    for _, row in daily_df.iterrows():
        ymd = row["YYYYMMDD"]
        try:
            dt = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        is_today = (dt == today)
        is_tomorrow = (dt == tomorrow)
        is_future = (dt > today)
        wd = dt.weekday()
        is_red = wd >= 5
        t1_v = int(row["T1"])
        t2_v = int(row["T2"])
        total_v = t1_v + t2_v
        table_rows.append({
            "ymd": ymd,
            "label": f"{dt.month}/{dt.day}",
            "weekday": WEEKDAY_KR[wd],
            "is_red": is_red,
            "is_today": is_today,
            "is_tomorrow": is_tomorrow,
            "is_future": is_future,
            "T1": t1_v,
            "T2": t2_v,
            "total": total_v,
            "T1_ratio": _ratio(t1_v, dow_avg["T1"].get(wd)) if t1_v > 0 else None,
            "T2_ratio": _ratio(t2_v, dow_avg["T2"].get(wd)) if t2_v > 0 else None,
            "total_ratio": _ratio(total_v, dow_avg["total"].get(wd)) if total_v > 0 else None,
            "source": row["source"],
        })

    # 데이터 기준 시각 = pkl backfill 시 기록된 실제 airport.kr 수집 시각.
    # focus(내일 우선) 데이터의 fetched_at을 우선 사용하고, 없으면 today 폴백.
    # pkl 부재 시에만 현재 시각으로 폴백.
    def _pkl_fetched_at(d):
        if not isinstance(d, dict):
            return None
        v = d.get("fetched_at")
        if not v:
            return None
        s = str(v).strip()
        # template에서 " KST" 접미를 다시 붙이므로 중복 방지
        return s[:-4].strip() if s.endswith("KST") else s
    fetched_at_str = (
        _pkl_fetched_at(tomorrow_data if focus_is_tomorrow else today_data)
        or _pkl_fetched_at(today_data)
        or _pkl_fetched_at(tomorrow_data)
        or datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    )

    # 기간 표기 = 항공편수 대시보드와 동일 패턴: "{prev}/{curr} 1~Nㅇ일 동일기간"
    if prev_label_text and cutoff_curr > 0:
        data_period = f"{prev_label_text}/{curr_label_text} {period_label} 동일기간"
    else:
        data_period = f"{curr_range_start.month}/{curr_range_start.day} ~ {focus_date.month}/{focus_date.day}"

    payload = {
        # SMS 알림 동일 기준 핵심 요약 (예약합계 출국 = 환승객 포함)
        "reserved": {
            "today": reserved["today"],
            "tomorrow": reserved["tomorrow"],
            "mtd": reserved_mtd,
            "delta": reserved_delta,
        },
        # 화면 전체 기준 일자 — 핵심 요약 카드와 다른 모든 통계가 같은 일자를 보도록
        "focus_is_tomorrow": focus_is_tomorrow,
        "focus_label": ("내일 예상" if focus_is_tomorrow
                        else ("월말 기준" if archive else "오늘 예상")),
        # 면세점 고시환율 (USD/KRW)
        "exchange": exchange,
        "today": {
            "date": today.strftime("%Y-%m-%d"),
            "weekday": WEEKDAY_KR[today.weekday()],
            "kpi": kpi["today"],
            "peak_hour_T1_label": fmt_peak_hour(kpi["today"]["peak_hour_T1"]),
            "peak_hour_T2_label": fmt_peak_hour(kpi["today"]["peak_hour_T2"]),
            "source": today_src,
        },
        "tomorrow": {
            "date": tomorrow.strftime("%Y-%m-%d"),
            "weekday": WEEKDAY_KR[tomorrow.weekday()],
            "kpi": kpi["tomorrow"],
            "peak_hour_T1_label": fmt_peak_hour(kpi["tomorrow"]["peak_hour_T1"]),
            "peak_hour_T2_label": fmt_peak_hour(kpi["tomorrow"]["peak_hour_T2"]),
            "source": tomorrow_src,
        },
        "mtd": mtd,
        "gate_mtd": gate_mtd,
        "delta_pct_T1": delta_pct_T1,
        "delta_pct_T2": delta_pct_T2,
        "mtd_hourly": mtd_hourly,
        "prev_mtd_hourly": prev_mtd_hourly,
        "today_per_gate": today_per_gate,
        "tomorrow_per_gate": tomorrow_per_gate,
        # 노선별 (신규)
        "today_route_T1": today_route_T1,
        "today_route_T2": today_route_T2,
        "tomorrow_route_T1": tomorrow_route_T1,
        "tomorrow_route_T2": tomorrow_route_T2,
        "today_route_summary_T1": today_route_summary_T1,
        "today_route_summary_T2": today_route_summary_T2,
        "tomorrow_route_summary_T1": tomorrow_route_summary_T1,
        "tomorrow_route_summary_T2": tomorrow_route_summary_T2,
        "mtd_route_T1": mtd_route_T1,
        "mtd_route_T2": mtd_route_T2,
        # 일자별 차트 (항공편수 대시보드와 동일 패턴)
        "chart_data": chart_data,
        # 월누적 (동일일수 비교)
        "monthly": {
            **monthly,
            "prev_label": prev_label_text or "전월",
            "curr_label": curr_label_text,
            "period_label": period_label,
            "available": monthly["days_curr"] > 0 and monthly["days_prev"] > 0,
        },
        # 출국장별·도착지별 동일일수 비교
        "gate_compare": {
            **gate_cmp,
            "prev_label": prev_label_text or "전월",
            "curr_label": curr_label_text,
            "period_label": period_label,
            "available": gate_cmp["days_curr"] > 0,
        },
        "route_compare_T1": {
            **route_cmp_T1,
            "prev_label": prev_label_text or "전월",
            "curr_label": curr_label_text,
            "period_label": period_label,
            "available": route_cmp_T1["days_curr"] > 0,
        },
        "route_compare_T2": {
            **route_cmp_T2,
            "prev_label": prev_label_text or "전월",
            "curr_label": curr_label_text,
            "period_label": period_label,
            "available": route_cmp_T2["days_curr"] > 0,
        },
        # 표·메타
        "table_rows": table_rows,
        "fetched_at": fetched_at_str,
        "data_period": data_period,
        # 월 선택 메타
        "is_archive": archive,
        "selected_ym": today.strftime("%Y%m"),
        "current_ym": _kst_today().strftime("%Y%m"),
        "available_months": _available_months(),
    }

    _cache_set(cache_key, payload)
    return payload


@app.on_event("startup")
def warm_cache_on_startup() -> None:
    _cache_clear()
    try:
        build_payload()
        logger.info("startup cache warmed")
    except Exception as exc:
        logger.warning("startup cache warm skipped: %r", exc)


# ---------- 라우트 ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, ym: str | None = None):
    target_today, archive = _resolved_today(ym)
    payload = await run_in_threadpool(build_payload, target_today, archive)

    today = _kst_today()
    tomorrow = today + timedelta(days=1)
    avail = [d for d in list_available_dates(str(DAILY_DIR))
             if d >= DATA_START_DATE.strftime("%Y%m%d")]
    if avail:
        try:
            min_dt = datetime.strptime(avail[0], "%Y%m%d").date()
        except ValueError:
            min_dt = DATA_START_DATE
        try:
            max_dt = datetime.strptime(avail[-1], "%Y%m%d").date()
        except ValueError:
            max_dt = tomorrow
        if max_dt < tomorrow:
            max_dt = tomorrow
    else:
        min_dt = DATA_START_DATE
        max_dt = tomorrow

    response = templates.TemplateResponse(
        request,
        "index.html",
        {
            "p": payload,
            "payload_json": json.dumps(payload, ensure_ascii=False),
            "export_min_date": min_dt.strftime("%Y-%m-%d"),
            "export_max_date": max_dt.strftime("%Y-%m-%d"),
            "export_default_start": min_dt.strftime("%Y-%m-%d"),
            "export_default_end": max_dt.strftime("%Y-%m-%d"),
        },
    )
    response.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=600"
    return response


@app.post("/api/refresh")
async def refresh_cache(x_refresh_token: str | None = Header(None)):
    expected = os.environ.get("REFRESH_TOKEN", "")
    if not expected:
        raise HTTPException(503, "refresh disabled (REFRESH_TOKEN not set)")
    if not x_refresh_token or not hmac.compare_digest(x_refresh_token, expected):
        raise HTTPException(401, "invalid token")
    _cache_clear()
    try:
        payload = await run_in_threadpool(build_payload)
    except Exception as exc:
        logger.error("refresh build failed: %r", exc)
        raise HTTPException(502, f"build failed: {exc!r}")
    today_kpi = payload["today"]["kpi"]
    tomorrow_kpi = payload["tomorrow"]["kpi"]
    logger.info("refresh ok: today=%s tomorrow=%s",
                today_kpi["T1"] + today_kpi["T2"],
                tomorrow_kpi["T1"] + tomorrow_kpi["T2"])
    return {
        "ok": True,
        "fetched_at": payload["fetched_at"],
        "today_total": today_kpi["T1"] + today_kpi["T2"],
        "tomorrow_total": tomorrow_kpi["T1"] + tomorrow_kpi["T2"],
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(KST).isoformat()}


# ---------- raw CSV export ----------
def _build_export_rows(daily_map: dict[str, tuple[dict, str]]):
    """일자별 시간대별 wide CSV (T1 출국장 5개 + T2 2개 + 권역 7개 × 2터미널 + KPI 카드 일자 합계)."""
    header = (
        ["날짜", "시간대"]
        + ["전체출국객수", "T1출국객수", "T2출국객수"]
        + [f"T1출국{g}" for g in T1_GATES]
        + ["T1출국합계"]
        + [f"T2출국{g}" for g in T2_GATES]
        + ["T2출국합계"]
        + [f"T1권역_{r}" for r in REGIONS]
        + [f"T2권역_{r}" for r in REGIONS]
    )
    yield header
    for ymd in sorted(daily_map.keys()):
        data, _src = daily_map[ymd]
        if not data:
            continue
        # KPI 카드 동일 기준 (예약합계 출국 = 환승객 포함) — 일자별 단일 값, 시간대 행에 반복
        t1_kpi = _reserved_out(data, "T1")
        t2_kpi = _reserved_out(data, "T2")
        total_kpi = t1_kpi + t2_kpi
        # 24시간 매트릭스 빌드
        t1_dep = {row["hour"]: row for row in (data["T1"]["depart"]["시간대별"] or [])}
        t2_dep = {row["hour"]: row for row in (data["T2"]["depart"]["시간대별"] or [])}
        t1_rt = {row["hour"]: row for row in (data["T1"]["depart_route"]["시간대별"] or [])}
        t2_rt = {row["hour"]: row for row in (data["T2"]["depart_route"]["시간대별"] or [])}
        for h in range(24):
            hour_key = f"{h:02d}_{(h+1)%24:02d}"
            t1d = t1_dep.get(hour_key, {})
            t2d = t2_dep.get(hour_key, {})
            t1r = t1_rt.get(hour_key, {})
            t2r = t2_rt.get(hour_key, {})
            row = [ymd, hour_key, total_kpi, t1_kpi, t2_kpi]
            for g in T1_GATES:
                row.append(int(t1d.get(g) or 0))
            row.append(int(t1d.get("total") or 0))
            for g in T2_GATES:
                row.append(int(t2d.get(g) or 0))
            row.append(int(t2d.get("total") or 0))
            for r in REGIONS:
                row.append(int(t1r.get(r) or 0))
            for r in REGIONS:
                row.append(int(t2r.get(r) or 0))
            yield row


@app.get("/api/export-raw")
async def export_raw(request: Request, start: str, end: str):
    """start/end (YYYYMMDD) 범위 raw CSV 다운로드 — 90일 상한 + 5회/5분 IP 레이트 리밋."""
    client_ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                 or (request.client.host if request.client else "unknown"))
    if not _rate_check(client_ip):
        raise HTTPException(429, "요청이 너무 잦습니다. 잠시 후 다시 시도해주세요.")

    try:
        start_dt = datetime.strptime(start, "%Y%m%d").date()
        end_dt = datetime.strptime(end, "%Y%m%d").date()
    except ValueError:
        raise HTTPException(400, "start/end는 YYYYMMDD 형식이어야 합니다.")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    if start_dt < DATA_START_DATE:
        start_dt = DATA_START_DATE
    if end_dt < DATA_START_DATE:
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")
    if (end_dt - start_dt).days > 90:
        raise HTTPException(400, "최대 90일 범위까지 내보낼 수 있습니다.")

    daily_map = load_range(str(DAILY_DIR), start_dt, end_dt)
    if not any(data is not None for data, _ in daily_map.values()):
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")

    def csv_chunks():
        yield "﻿"  # UTF-8 BOM (Excel 한글 호환)
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in _build_export_rows(daily_map):
            writer.writerow(row)
            text = buf.getvalue()
            buf.seek(0)
            buf.truncate()
            yield text

    fname = f"icn_pax_congestion_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        csv_chunks(),
        media_type="text/csv; charset=utf-8-sig",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )
