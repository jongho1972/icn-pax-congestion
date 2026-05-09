"""인천공항 출국장 혼잡도 대시보드 (FastAPI + Jinja2).

데이터 소스: airport.kr 공항 예상 혼잡도 엑셀 (자세한 스키마는
icn_utils/excel_parser.py 참조). 매일 17:05 + 23:30 KST cron으로 받아
Daily_Data/passgr_YYYYMMDD.pkl에 통합 dict 저장.
"""
from __future__ import annotations

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
    daily_totals, fmt_peak_hour, hourly_per_gate, hourly_t1_t2,
    kpi_summary, mtd_hourly_t1_t2, mtd_per_gate, mtd_reserved, mtd_route,
    mtd_summary, reserved_summary, route_matrix, route_summary,
)
from icn_utils.data_loader import list_available_dates, load_day, load_range
from icn_utils.exchange_rate import load_rates

load_dotenv()

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"

DAILY_TREND_DAYS = 30  # 일자별 차트 표시 일수 (D-29 ~ D+1)
DATA_START_DATE = date(2026, 5, 1)  # 엑셀 데이터 첫 일자 (5/1부터 백필)

app = FastAPI(title="인천공항 출국장 혼잡도")
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


# ---------- 페이로드 빌드 ----------
def build_payload() -> dict:
    today = _kst_today()
    cache_key = today.strftime("%Y%m%d")
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    with _BUILD_LOCK:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        return _build_payload_locked(today)


def _build_payload_locked(today: date) -> dict:
    cache_key = today.strftime("%Y%m%d")
    tomorrow = today + timedelta(days=1)
    range_start = today - timedelta(days=DAILY_TREND_DAYS - 2)
    if range_start < DATA_START_DATE:
        range_start = DATA_START_DATE
    daily_map = load_range(str(DAILY_DIR), range_start, tomorrow)

    today_ymd = today.strftime("%Y%m%d")
    tomorrow_ymd = tomorrow.strftime("%Y%m%d")

    today_data, today_src = daily_map.get(today_ymd, (None, "none"))
    tomorrow_data, tomorrow_src = daily_map.get(tomorrow_ymd, (None, "none"))

    # KPI
    kpi = kpi_summary(today_data, tomorrow_data)

    # 핵심 요약 (SMS 동일 기준 — 예약합계 출국)
    reserved = reserved_summary(today_data, tomorrow_data)
    reserved_mtd = mtd_reserved(daily_map, today)

    def _delta_pct(focus_v: int, mtd_v: int):
        if not focus_v or not mtd_v or mtd_v <= 0:
            return None
        return round((focus_v - mtd_v) / mtd_v * 100, 1)

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

    # MTD
    mtd = mtd_summary(daily_map, today)
    gate_mtd = mtd_per_gate(daily_map, today)
    delta_pct_T1 = None
    delta_pct_T2 = None
    if mtd["T1"] > 0 and kpi["tomorrow"]["T1"] > 0:
        delta_pct_T1 = round((kpi["tomorrow"]["T1"] - mtd["T1"]) / mtd["T1"] * 100, 1)
    if mtd["T2"] > 0 and kpi["tomorrow"]["T2"] > 0:
        delta_pct_T2 = round((kpi["tomorrow"]["T2"] - mtd["T2"]) / mtd["T2"] * 100, 1)

    # 시간대별 차트
    today_hourly = hourly_t1_t2(today_data)
    tomorrow_hourly = hourly_t1_t2(tomorrow_data)
    mtd_hourly = mtd_hourly_t1_t2(daily_map, today)

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
    mtd_route_T1 = mtd_route(daily_map, today, "T1")
    mtd_route_T2 = mtd_route(daily_map, today, "T2")

    # 일자별 추이
    daily_df = daily_totals(daily_map)

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
        table_rows.append({
            "ymd": ymd,
            "label": f"{dt.month}/{dt.day}",
            "weekday": WEEKDAY_KR[wd],
            "is_red": is_red,
            "is_today": is_today,
            "is_tomorrow": is_tomorrow,
            "is_future": is_future,
            "T1": int(row["T1"]),
            "T2": int(row["T2"]),
            "peak_hour_T1": fmt_peak_hour(row["peak_hour_T1"]),
            "peak_total_T1": int(row["peak_total_T1"]),
            "peak_hour_T2": fmt_peak_hour(row["peak_hour_T2"]),
            "peak_total_T2": int(row["peak_total_T2"]),
            "source": row["source"],
        })

    fetched_at = datetime.now(KST)

    avail = [d for d in list_available_dates(str(DAILY_DIR))
             if d >= DATA_START_DATE.strftime("%Y%m%d")]
    if avail:
        try:
            d_min = datetime.strptime(avail[0], "%Y%m%d").date()
            d_max = datetime.strptime(avail[-1], "%Y%m%d").date()
            data_period = f"{d_min.month}/{d_min.day} ~ {d_max.month}/{d_max.day}"
        except ValueError:
            data_period = "—"
    else:
        data_period = "—"

    payload = {
        # SMS 알림 동일 기준 핵심 요약 (예약합계 출국 = 환승객 포함)
        "reserved": {
            "today": reserved["today"],
            "tomorrow": reserved["tomorrow"],
            "mtd": reserved_mtd,
            "delta": reserved_delta,
        },
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
        "today_hourly": today_hourly,
        "tomorrow_hourly": tomorrow_hourly,
        "mtd_hourly": mtd_hourly,
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
        # 표·메타
        "table_rows": table_rows,
        "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M"),
        "data_period": data_period,
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
async def index(request: Request):
    payload = await run_in_threadpool(build_payload)

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
    """일자별 시간대별 wide CSV (T1 출국장 5개 + T2 2개 + 권역 7개 × 2터미널)."""
    header = (
        ["날짜", "시간대"]
        + [f"T1출국{g}" for g in T1_GATES]
        + ["T1출국합계"]
        + [f"T2출국{g}" for g in T2_GATES]
        + ["T2출국합계"]
        + [f"T1권역_{r}" for r in REGIONS]
        + [f"T2권역_{r}" for r in REGIONS]
        + ["출처"]
    )
    yield header
    for ymd in sorted(daily_map.keys()):
        data, src = daily_map[ymd]
        if not data:
            continue
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
            row = [ymd, hour_key]
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
            row.append(src)
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
