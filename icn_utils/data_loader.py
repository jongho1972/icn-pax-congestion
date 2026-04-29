"""인천공항 출국장 혼잡도 데이터 로더.

저장 구조: Daily_Data/passgr_YYYYMMDD_{d0,d1}.pkl
- d0 = 그날 D-0(실측 근사) 호출분
- d1 = 그 전날에 미리 받은 D+1 예측분 (검증/백업용)

조회 시 우선순위: 같은 날짜에 d0가 있으면 d0, 없으면 d1.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
import requests

KST = ZoneInfo("Asia/Seoul")
API_URL = "http://apis.data.go.kr/B551177/passgrAnncmt/getPassgrAnncmt"

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
    r = requests.get(API_URL, params=params, timeout=30)
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

    Returns: (df, source) — source ∈ {"d0", "d1", "none"}
    """
    p_d0 = os.path.join(daily_dir, f"passgr_{ymd}_d0.pkl")
    p_d1 = os.path.join(daily_dir, f"passgr_{ymd}_d1.pkl")
    df = _load_pkl(p_d0)
    if not df.empty:
        return df, "d0"
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
        except Exception:
            continue
        if df.empty:
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
