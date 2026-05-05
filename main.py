"""인천공항 출국장 혼잡도 대시보드 (FastAPI + Jinja2)."""
from __future__ import annotations

import json
import os
import pickle
import time
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from icn_utils.aggregator import (
    WEEKDAY_KR, daily_totals, fmt_peak_hour, hourly_per_gate, hourly_t1_t2,
    kpi_summary, mtd_summary,
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

# ---------- TTL 캐시 (메모리 + 디스크) ----------
_CACHE: dict[str, tuple[float, object]] = {}
_TTL_SECONDS = 60 * 60 * 48  # 48시간 (cron 누락 안전 마진)
CACHE_FILE = Path("/tmp") / "icn_pax_congestion_cache.pkl"


def _cache_get(key: str):
    if key not in _CACHE:
        return None
    ts, val = _CACHE[key]
    if time.time() - ts > _TTL_SECONDS:
        return None
    return val


def _cache_set(key: str, val) -> None:
    _CACHE[key] = (time.time(), val)
    _save_disk_cache()


def _load_disk_cache() -> None:
    if not CACHE_FILE.exists():
        return
    try:
        with CACHE_FILE.open("rb") as f:
            data = pickle.load(f)
        if isinstance(data, dict):
            _CACHE.update(data)
    except Exception as exc:
        print(f"[disk cache load] skipped: {exc!r}")


def _save_disk_cache() -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE_FILE.with_suffix(".pkl.tmp")
        with tmp.open("wb") as f:
            pickle.dump(_CACHE, f)
        tmp.replace(CACHE_FILE)
    except Exception as exc:
        print(f"[disk cache save] skipped: {exc!r}")


# ---------- 데이터 빌드 ----------
def build_payload(service_key: str | None) -> dict:
    """전체 페이지 페이로드 생성. 캐시 키는 오늘 날짜."""
    today = datetime.now(KST).date()
    cache_key = today.strftime("%Y%m%d")
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    tomorrow = today + timedelta(days=1)
    range_start = today - timedelta(days=DAILY_TREND_DAYS - 2)  # D-29부터
    if range_start < DATA_START_DATE:
        range_start = DATA_START_DATE
    daily_map = load_range(str(DAILY_DIR), range_start, tomorrow)

    # 오늘·내일 데이터: 디스크 우선 → 비어있으면 라이브 API fallback
    today_ymd = today.strftime("%Y%m%d")
    tomorrow_ymd = tomorrow.strftime("%Y%m%d")

    today_df, today_src = daily_map.get(today_ymd, (pd.DataFrame(), "none"))
    tomorrow_df, tomorrow_src = daily_map.get(tomorrow_ymd, (pd.DataFrame(), "none"))

    if (today_df.empty or tomorrow_df.empty) and service_key:
        try:
            live = fetch_live(service_key)
        except Exception:
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

    # 시간대별 차트 (T1/T2 별 패널, 오늘 vs 내일)
    today_hourly = hourly_t1_t2(today_df)
    tomorrow_hourly = hourly_t1_t2(tomorrow_df)

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
        is_future = (dt > today)
        wd = dt.weekday()
        is_red = wd >= 5
        table_rows.append({
            "ymd": ymd,
            "label": f"{dt.month}/{dt.day}",
            "weekday": WEEKDAY_KR[wd],
            "is_red": is_red,
            "is_today": is_today,
            "is_future": is_future,
            "T1": int(row["T1"]),
            "T2": int(row["T2"]),
            "peak_hour_T1": fmt_peak_hour(row["peak_hour_T1"]),
            "peak_total_T1": int(row["peak_total_T1"]),
            "peak_hour_T2": fmt_peak_hour(row["peak_hour_T2"]),
            "peak_total_T2": int(row["peak_total_T2"]),
            "source": row["source"],
        })

    # 차트용 시리즈 (T1·T2만, 합계 라인 제거)
    daily_chart = {
        "dates": [r["label"] for r in table_rows],
        "ymds": [r["ymd"] for r in table_rows],
        "T1": [r["T1"] for r in table_rows],
        "T2": [r["T2"] for r in table_rows],
        "is_future": [r["is_future"] for r in table_rows],
        "is_red": [r["is_red"] for r in table_rows],
        "today_idx": next((i for i, r in enumerate(table_rows) if r["is_today"]), -1),
    }

    fetched_at = datetime.now(KST)
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
        "delta_pct_T1": delta_pct_T1,
        "delta_pct_T2": delta_pct_T2,
        "today_hourly": today_hourly,
        "tomorrow_hourly": tomorrow_hourly,
        "today_per_gate": today_per_gate,
        "tomorrow_per_gate": tomorrow_per_gate,
        "daily_chart": daily_chart,
        "table_rows": table_rows,
        "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M"),
        "data_dates_count": sum(
            1 for d in list_available_dates(str(DAILY_DIR))
            if d >= DATA_START_DATE.strftime("%Y%m%d")
        ),
    }

    _cache_set(cache_key, payload)
    return payload


@app.on_event("startup")
def warm_cache_on_startup() -> None:
    _load_disk_cache()
    service_key = os.environ.get("INCHEON_API_KEY", "")
    try:
        build_payload(service_key)
    except Exception as exc:
        print(f"[warm_cache_on_startup] skipped: {exc!r}")


# ---------- 라우트 ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    service_key = os.environ.get("INCHEON_API_KEY", "")
    payload = build_payload(service_key)
    response = templates.TemplateResponse(
        request,
        "index.html",
        {
            "p": payload,
            "payload_json": json.dumps(payload, ensure_ascii=False),
        },
    )
    response.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=600"
    return response


@app.post("/api/refresh")
async def refresh_cache(x_refresh_token: str | None = Header(None)):
    expected = os.environ.get("REFRESH_TOKEN", "")
    if expected and x_refresh_token != expected:
        raise HTTPException(401, "invalid token")
    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        raise HTTPException(500, "INCHEON_API_KEY not set")
    _CACHE.clear()
    try:
        payload = build_payload(service_key)
    except Exception as exc:
        raise HTTPException(502, f"build failed: {exc!r}")
    return {
        "ok": True,
        "fetched_at": payload["fetched_at"],
        "today_total": payload["today"]["kpi"]["total"],
        "tomorrow_total": payload["tomorrow"]["kpi"]["total"],
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(KST).isoformat()}


@app.get("/api/export-raw")
async def export_raw(start: str, end: str):
    """start/end (YYYYMMDD) 범위 raw CSV 다운로드."""
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
    if (end_dt - start_dt).days > 366:
        raise HTTPException(400, "최대 366일 범위까지 내보낼 수 있습니다.")

    daily_map = load_range(str(DAILY_DIR), start_dt, end_dt)
    parts = []
    for ymd, (df, src) in sorted(daily_map.items()):
        if df is None or df.empty:
            continue
        d = df.copy()
        d["source"] = src
        parts.append(d)
    if not parts:
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")
    out = pd.concat(parts, ignore_index=True)

    buf = BytesIO()
    out.to_csv(buf, index=False, encoding="utf-8-sig")
    size = buf.tell()
    buf.seek(0)
    fname = f"icn_pax_congestion_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        buf,
        media_type="text/csv; charset=utf-8-sig",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Content-Length": str(size),
            "Cache-Control": "no-store",
        },
    )
