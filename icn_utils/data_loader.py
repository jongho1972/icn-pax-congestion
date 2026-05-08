"""인천공항 출국장 혼잡도 데이터 로더.

저장 구조: Daily_Data/passgr_YYYYMMDD_{d0,d1,web}.pkl
- d0  = 그날 23:30 cron 호출분 (그날 마감값에 가장 가까움)
- web = airport.kr 공식 페이지 17:05 cron 스크래핑 (D-2~D+2 범위, 7일치)
- d1  = 그 전날에 미리 받은 D+1 예측분 (검증/백업용)

조회 우선순위: d0 > web > d1.
- 과거·당일은 d0 (그날 마감값)이 가장 정확
- 미래(D+2)는 web 만 가능 — OpenAPI는 D+1까지만 제공
- d1은 web/d0와 거의 동일하지만 fallback으로 보존
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("icn_pax_congestion.data_loader")

KST = ZoneInfo("Asia/Seoul")
# https로 통일 — 인천공항 OpenAPI는 TLS 지원함
API_URL = "https://apis.data.go.kr/B551177/passgrAnncmt/getPassgrAnncmt"


def _make_session() -> requests.Session:
    """5xx·504에 자동 재시도 + 지수 백오프."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,  # 1s, 2s, 4s
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_SESSION = _make_session()

NUMERIC_COLS = [
    "t1eg1", "t1eg2", "t1eg3", "t1eg4", "t1egsum1",
    "t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg5", "t1dg6", "t1dgsum1",
    "t2eg1", "t2eg2", "t2egsum1",
    "t2dg1", "t2dg2", "t2dgsum2",
]

# 출국장(Departure Gate) 컬럼만 — 화면에서 사용
DG_COLS_T1 = ["t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg5", "t1dg6"]
DG_COLS_T2 = ["t2dg1", "t2dg2"]


def _drop_total_row(df: pd.DataFrame) -> pd.DataFrame:
    """API 응답 마지막 행은 atime='합계' (24시간 누적) — 중복 합산 방지를 위해 제거."""
    if df.empty or "atime" not in df.columns:
        return df
    return df[df["atime"].astype(str).str.contains("_", na=False)].reset_index(drop=True)


def _fetch_api(service_key: str, selectdate: int) -> pd.DataFrame:
    """API 단일 호출. 빈 응답이면 빈 DF 반환."""
    params = {
        "serviceKey": service_key,
        "type": "json",
        "selectdate": selectdate,
        "numOfRows": 100,
    }
    r = _SESSION.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    try:
        body = r.json()["response"]["body"]
    except (KeyError, ValueError):
        return pd.DataFrame()
    items = body.get("items") or []
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    if "adate" not in df.columns or "atime" not in df.columns:
        return pd.DataFrame()
    for c in NUMERIC_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return _drop_total_row(df)


def _load_pkl(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_pickle(path)
    except Exception:
        return pd.DataFrame()
    # 누락 컬럼 보정 (구버전 pkl 호환)
    for c in NUMERIC_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return _drop_total_row(df)


def load_day(daily_dir: str, ymd: str) -> tuple[pd.DataFrame, str]:
    """주어진 날짜의 가장 신뢰도 높은 데이터를 반환.

    Returns: (df, source) — source ∈ {"d0", "web", "d1", "none"}
    우선순위: d0(그날 마감) > web(17:00 발표) > d1(전일 D+1).
    """
    p_d0 = os.path.join(daily_dir, f"passgr_{ymd}_d0.pkl")
    p_web = os.path.join(daily_dir, f"passgr_{ymd}_web.pkl")
    p_d1 = os.path.join(daily_dir, f"passgr_{ymd}_d1.pkl")
    df = _load_pkl(p_d0)
    if not df.empty:
        return df, "d0"
    df = _load_pkl(p_web)
    if not df.empty:
        return df, "web"
    df = _load_pkl(p_d1)
    if not df.empty:
        return df, "d1"
    return pd.DataFrame(), "none"


def load_range(
    daily_dir: str, start: date, end: date
) -> dict[str, tuple[pd.DataFrame, str]]:
    """start ~ end (양끝 포함) 범위의 일별 데이터를 dict로 반환.

    key = YYYYMMDD, value = (df, source)
    """
    out: dict[str, tuple[pd.DataFrame, str]] = {}
    cur = start
    while cur <= end:
        ymd = cur.strftime("%Y%m%d")
        out[ymd] = load_day(daily_dir, ymd)
        cur += timedelta(days=1)
    return out


def fetch_live(service_key: str) -> dict[str, pd.DataFrame]:
    """API 실시간 호출 (캐시·디스크 무관). 메모리 fallback 용도.

    Returns: {YYYYMMDD: df} for D-0 + D+1 (응답 가능한 날짜만)
    """
    out: dict[str, pd.DataFrame] = {}
    for sel in (0, 1):
        try:
            df = _fetch_api(service_key, sel)
        except Exception as exc:
            logger.warning("fetch_live selectdate=%d failed: %r", sel, exc)
            continue
        if df.empty:
            logger.info("fetch_live selectdate=%d returned empty (likely D+1 not yet published)", sel)
            continue
        ymd = str(df["adate"].iloc[0])
        out[ymd] = df
    return out


def list_available_dates(daily_dir: str) -> list[str]:
    """Daily_Data 에 데이터가 있는 날짜(YYYYMMDD) 정렬 리스트."""
    if not os.path.isdir(daily_dir):
        return []
    seen: set[str] = set()
    for name in os.listdir(daily_dir):
        if not name.startswith("passgr_") or not name.endswith(".pkl"):
            continue
        # passgr_YYYYMMDD_dN.pkl
        try:
            ymd = name[len("passgr_") : len("passgr_") + 8]
            datetime.strptime(ymd, "%Y%m%d")
            seen.add(ymd)
        except ValueError:
            continue
    return sorted(seen)
