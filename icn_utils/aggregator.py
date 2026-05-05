"""출국장 혼잡도 집계.

API 한 행 = 한 시간대(00_01 ~ 23_24). 출국장 합계는 t1dgsum1 / t2dgsum2.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd

DG_COLS_T1 = ["t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg5", "t1dg6"]
DG_COLS_T2 = ["t2dg1", "t2dg2"]
T1_TOTAL = "t1dgsum1"
T2_TOTAL = "t2dgsum2"

HOUR_LABELS = [f"{h:02d}" for h in range(24)]  # "00".."23"


def _hour_from_atime(atime: str) -> int:
    """'00_01' → 0, '23_24' → 23 (시작시각 기준)."""
    try:
        return int(str(atime).split("_")[0])
    except Exception:
        return -1


def hourly_t1_t2(df: pd.DataFrame) -> dict[str, list[int]]:
    """24시간 × T1·T2 출국장 합계 시리즈.

    Returns: {"hours": [...], "T1": [...], "T2": [...], "total": [...]}
    """
    if df is None or df.empty:
        zeros = [0] * 24
        return {"hours": HOUR_LABELS, "T1": zeros[:], "T2": zeros[:], "total": zeros[:]}
    work = df.copy()
    work["__h"] = work["atime"].map(_hour_from_atime)
    work = work[work["__h"].between(0, 23)]
    g = work.groupby("__h").agg({T1_TOTAL: "sum", T2_TOTAL: "sum"}).reindex(range(24)).fillna(0)
    t1 = g[T1_TOTAL].astype(int).tolist()
    t2 = g[T2_TOTAL].astype(int).tolist()
    return {
        "hours": HOUR_LABELS,
        "T1": t1,
        "T2": t2,
        "total": [a + b for a, b in zip(t1, t2)],
    }


def hourly_per_gate(df: pd.DataFrame) -> dict[str, list[int]]:
    """24시간 × 각 출국장 시리즈 (T1 6개 + T2 2개)."""
    cols = DG_COLS_T1 + DG_COLS_T2
    if df is None or df.empty:
        return {c: [0] * 24 for c in cols}
    work = df.copy()
    work["__h"] = work["atime"].map(_hour_from_atime)
    work = work[work["__h"].between(0, 23)]
    g = work.groupby("__h")[cols].sum().reindex(range(24)).fillna(0)
    return {c: g[c].astype(int).tolist() for c in cols}


def daily_totals(daily_map: dict[str, tuple[pd.DataFrame, str]]) -> pd.DataFrame:
    """일별 T1/T2/합계/피크시간/소스(d0/d1).

    Returns: DataFrame[YYYYMMDD, T1, T2, total, peak_hour, peak_total, source]
    """
    rows = []
    for ymd, (df, src) in sorted(daily_map.items()):
        if df is None or df.empty:
            rows.append(
                {"YYYYMMDD": ymd, "T1": 0, "T2": 0, "total": 0,
                 "peak_hour": None, "peak_total": 0, "source": src}
            )
            continue
        t1 = int(df[T1_TOTAL].sum())
        t2 = int(df[T2_TOTAL].sum())
        # 피크: 시간대별 T1+T2 합 최대
        work = df.copy()
        work["__h"] = work["atime"].map(_hour_from_atime)
        work = work[work["__h"].between(0, 23)]
        work["__total"] = work[T1_TOTAL] + work[T2_TOTAL]
        g = work.groupby("__h")["__total"].sum()
        peak_hour = int(g.idxmax()) if len(g) else None
        peak_total = int(g.max()) if len(g) else 0
        rows.append({
            "YYYYMMDD": ymd, "T1": t1, "T2": t2, "total": t1 + t2,
            "peak_hour": peak_hour, "peak_total": peak_total, "source": src,
        })
    return pd.DataFrame(rows)


def kpi_summary(today_df: pd.DataFrame, tomorrow_df: pd.DataFrame) -> dict:
    """헤더 KPI: 오늘 총객수·내일 예상·오늘 피크·내일 피크."""
    def one(df):
        if df is None or df.empty:
            return {"total": 0, "T1": 0, "T2": 0, "peak_hour": None, "peak_total": 0}
        t1 = int(df[T1_TOTAL].sum())
        t2 = int(df[T2_TOTAL].sum())
        work = df.copy()
        work["__h"] = work["atime"].map(_hour_from_atime)
        work = work[work["__h"].between(0, 23)]
        work["__total"] = work[T1_TOTAL] + work[T2_TOTAL]
        g = work.groupby("__h")["__total"].sum()
        peak_hour = int(g.idxmax()) if len(g) else None
        peak_total = int(g.max()) if len(g) else 0
        return {"total": t1 + t2, "T1": t1, "T2": t2,
                "peak_hour": peak_hour, "peak_total": peak_total}
    return {"today": one(today_df), "tomorrow": one(tomorrow_df)}


def mtd_summary(daily_map: dict[str, tuple[pd.DataFrame, str]], today: date) -> dict:
    """이번 달 1일 ~ 어제까지의 d0(실측) 일평균.

    Returns: {"days", "T1", "T2", "total", "period_label"}.
    데이터가 없으면 days=0.
    """
    first = today.replace(day=1)
    yesterday = today - timedelta(days=1)
    t1s, t2s = [], []
    for ymd, (df, src) in daily_map.items():
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        if d < first or d > yesterday:
            continue
        if src not in ("d0", "live"):
            continue
        if df is None or df.empty:
            continue
        t1s.append(int(df[T1_TOTAL].sum()))
        t2s.append(int(df[T2_TOTAL].sum()))
    n = len(t1s)
    if n == 0:
        return {"days": 0, "T1": 0, "T2": 0, "total": 0,
                "period_label": "—"}
    t1_avg = round(sum(t1s) / n)
    t2_avg = round(sum(t2s) / n)
    return {
        "days": n,
        "T1": t1_avg,
        "T2": t2_avg,
        "total": t1_avg + t2_avg,
        "period_label": f"{first.month}/{first.day} ~ {yesterday.month}/{yesterday.day}",
    }


WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def fmt_peak_hour(h) -> str:
    if h is None:
        return "—"
    try:
        if pd.isna(h):
            return "—"
    except Exception:
        pass
    try:
        h = int(h)
    except (TypeError, ValueError):
        return "—"
    if h < 0 or h > 23:
        return "—"
    return f"{h:02d}~{(h+1)%24:02d}시"
