"""인천공항 출국장 혼잡도 대시보드 (FastAPI + Jinja2)."""
from __future__ import annotations

import hmac
import json
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("icn_pax_congestion")

from icn_utils.aggregator import (
    WEEKDAY_KR, daily_totals, fmt_peak_hour, hourly_per_gate, hourly_t1_t2,
    kpi_summary, mtd_hourly_t1_t2, mtd_per_gate, mtd_summary,
)
from icn_utils.data_loader import (
    fetch_live, list_available_dates, load_day, load_range,
)

load_dotenv()

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"

DAILY_TREND_DAYS = 30  # 일자별 차트 표시 일수 (D-29 ~ D+1)
DATA_START_DATE = date(2026, 5, 1)  # 이 날짜 이전 데이터는 무시 (사전 테스트분 제외)

app = FastAPI(title="인천공항 출국장 혼잡도")
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE / "templates"))

# ---------- TTL 캐시 (메모리만; Render 컨테이너 휘발성 + 멀티워커 race 회피) ----------
_CACHE: dict[str, tuple[float, object]] = {}
_CACHE_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()  # dogpile 방지 (동시 빌드 1회로 합침)
_TTL_SECONDS = 60 * 60 * 48  # 48시간 (cron 누락 안전 마진)


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


# ---------- 간이 IP 레이트 리밋 (export-raw DoS 방어) ----------
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


# ---------- 데이터 빌드 ----------
def build_payload(service_key: str | None) -> dict:
    """전체 페이지 페이로드 생성. 캐시 키는 오늘 날짜.

    dogpile 락으로 동시 빌드를 1회로 합쳐 외부 API 중복 호출과 메모리 폭증 방지.
    """
    today = datetime.now(KST).date()
    cache_key = today.strftime("%Y%m%d")
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with _BUILD_LOCK:
        # 락 획득 후 재확인 — 대기 중에 다른 스레드가 빌드를 끝냈을 수 있음
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        return _build_payload_locked(service_key, today)


def _build_payload_locked(service_key: str | None, today: date) -> dict:
    cache_key = today.strftime("%Y%m%d")
    tomorrow = today + timedelta(days=1)
    future_end = today + timedelta(days=2)  # D+2 — airport.kr 스크래핑이 D+2까지 제공
    range_start = today - timedelta(days=DAILY_TREND_DAYS - 3)  # D+2까지 포함하기 위해 보정
    if range_start < DATA_START_DATE:
        range_start = DATA_START_DATE
    daily_map = load_range(str(DAILY_DIR), range_start, future_end)

    # 오늘·내일 데이터: 디스크 우선 → 비어있으면 라이브 API fallback
    today_ymd = today.strftime("%Y%m%d")
    tomorrow_ymd = tomorrow.strftime("%Y%m%d")

    today_df, today_src = daily_map.get(today_ymd, (pd.DataFrame(), "none"))
    tomorrow_df, tomorrow_src = daily_map.get(tomorrow_ymd, (pd.DataFrame(), "none"))

    if (today_df.empty or tomorrow_df.empty) and service_key:
        try:
            live = fetch_live(service_key)
        except Exception as exc:
            logger.warning("fetch_live failed, falling back to empty: %r", exc)
            live = {}
        if today_df.empty and today_ymd in live:
            today_df = live[today_ymd]
            today_src = "live"
            daily_map[today_ymd] = (today_df, today_src)
        if tomorrow_df.empty and tomorrow_ymd in live:
            tomorrow_df = live[tomorrow_ymd]
            tomorrow_src = "live"
            daily_map[tomorrow_ymd] = (tomorrow_df, tomorrow_src)

    # KPI
    kpi = kpi_summary(today_df, tomorrow_df)

    # MTD 평균 (이번달 1일 ~ 어제, d0 실측)
    mtd = mtd_summary(daily_map, today)
    gate_mtd = mtd_per_gate(daily_map, today)
    delta_pct_T1 = None
    delta_pct_T2 = None
    if mtd["T1"] > 0 and kpi["tomorrow"]["T1"] > 0:
        delta_pct_T1 = round(
            (kpi["tomorrow"]["T1"] - mtd["T1"]) / mtd["T1"] * 100, 1
        )
    if mtd["T2"] > 0 and kpi["tomorrow"]["T2"] > 0:
        delta_pct_T2 = round(
            (kpi["tomorrow"]["T2"] - mtd["T2"]) / mtd["T2"] * 100, 1
        )

    # 시간대별 차트 (T1/T2 별 패널, 내일 vs MTD 평균)
    today_hourly = hourly_t1_t2(today_df)
    tomorrow_hourly = hourly_t1_t2(tomorrow_df)
    mtd_hourly = mtd_hourly_t1_t2(daily_map, today)

    # 출국장별 시간대 분포 (오늘 기준)
    today_per_gate = hourly_per_gate(today_df)
    tomorrow_per_gate = hourly_per_gate(tomorrow_df)

    # 일자별 추이 (D-29 ~ D+1)
    daily_df = daily_totals(daily_map)

    # 표용: 일자별 전체 + 요일·미래 표시
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

    # 활용 데이터 기간 (M/D ~ M/D) — 실제 pkl 있는 날짜 기준
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
        "table_rows": table_rows,
        "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M"),
        "data_period": data_period,
    }

    _cache_set(cache_key, payload)
    return payload


@app.on_event("startup")
def warm_cache_on_startup() -> None:
    _cache_clear()
    service_key = os.environ.get("INCHEON_API_KEY", "")
    try:
        build_payload(service_key)
        logger.info("startup cache warmed")
    except Exception as exc:
        logger.warning("startup cache warm skipped: %r", exc)


# ---------- 라우트 ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    service_key = os.environ.get("INCHEON_API_KEY", "")
    payload = await run_in_threadpool(build_payload, service_key)

    today = datetime.now(KST).date()
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
    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        raise HTTPException(500, "INCHEON_API_KEY not set")
    _cache_clear()
    try:
        payload = await run_in_threadpool(build_payload, service_key)
    except Exception as exc:
        logger.error("refresh build failed: %r", exc)
        raise HTTPException(502, f"build failed: {exc!r}")
    logger.info("refresh ok: today=%s tomorrow=%s",
                payload["today"]["kpi"]["T1"] + payload["today"]["kpi"]["T2"],
                payload["tomorrow"]["kpi"]["T1"] + payload["tomorrow"]["kpi"]["T2"])
    today_kpi = payload["today"]["kpi"]
    tomorrow_kpi = payload["tomorrow"]["kpi"]
    return {
        "ok": True,
        "fetched_at": payload["fetched_at"],
        "today_total": today_kpi["T1"] + today_kpi["T2"],
        "tomorrow_total": tomorrow_kpi["T1"] + tomorrow_kpi["T2"],
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(KST).isoformat()}


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
    if not any((df is not None and not df.empty) for df, _ in daily_map.values()):
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")

    rename_map = {
        "adate": "날짜",
        "atime": "시간대",
        "t1dg1": "T1출국1", "t1dg2": "T1출국2",
        "t1dg3": "T1출국3", "t1dg4": "T1출국4",
        "t1dg5": "T1출국5", "t1dg6": "T1출국6(교통약자)",
        "t1dgsum1": "T1출국합계",
        "t2dg1": "T2출국1", "t2dg2": "T2출국2",
        "t2dgsum2": "T2출국합계",
        "source": "출처",
    }
    drop_cols = [
        "tmp1", "tmp2",
        "t1eg1", "t1eg2", "t1eg3", "t1eg4", "t1egsum1",
        "t2eg1", "t2eg2", "t2egsum1",
    ]

    def csv_chunks():
        # 일자별 chunk 단위 yield → 메모리에 전체 concat 적재 회피 (OOM 방어)
        yield "﻿"  # UTF-8 BOM (Excel 한글 호환)
        first = True
        for ymd in sorted(daily_map.keys()):
            df, src = daily_map[ymd]
            if df is None or df.empty:
                continue
            d = df.copy()
            d["source"] = src
            d = d.drop(columns=drop_cols, errors="ignore")
            d = d.rename(columns=rename_map)
            yield d.to_csv(index=False, header=first, encoding="utf-8")
            first = False

    fname = f"icn_pax_congestion_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        csv_chunks(),
        media_type="text/csv; charset=utf-8-sig",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )
