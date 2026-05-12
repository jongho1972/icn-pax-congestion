"""출국장 혼잡도 집계 (airport.kr 엑셀 스키마 기반).

입력: data_loader.load_day가 반환하는 통합 dict.

T1 출국장 키: "1","2","3","4","5_6"  (출국장 5·6번이 합산되어 엑셀 원본 그대로)
T2 출국장 키: "1","2"
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, Optional

import pandas as pd

# 출국장 zone 키
T1_GATES = ["1", "2", "3", "4", "5_6"]
T2_GATES = ["1", "2"]

# 화면 zone ID (templates에서 SVG 매핑용)
T1_ZONE_KEYS = ["t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg56"]
T2_ZONE_KEYS = ["t2dg1", "t2dg2"]
ALL_ZONE_KEYS = T1_ZONE_KEYS + T2_ZONE_KEYS

REGIONS = ["일본", "중국", "동남아", "미주", "유럽", "오세아니아", "기타"]
HOUR_LABELS = [f"{h:02d}" for h in range(24)]
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _hour_from(hour_label: str) -> int:
    """'00_01' → 0, '23_00' → 23 (시작시각 기준)."""
    try:
        return int(str(hour_label).split("_")[0])
    except Exception:
        return -1


def _terminal_total(data: Optional[dict], terminal: str) -> int:
    """터미널별 일합계 (출국장별 합산 = 환승객 보정 후 실제 출국장 통과 인원)."""
    if not data:
        return 0
    gates = (data.get(terminal) or {}).get("depart", {}).get("출국장별") or {}
    return sum(int(v or 0) for v in gates.values())


def _reserved_out(data: Optional[dict], terminal: str) -> int:
    """예약승객 출국 합계 (환승객 포함, SMS 알림과 동일 기준)."""
    if not data:
        return 0
    return int(((data.get(terminal) or {}).get("depart", {}).get("예약합계") or {}).get("출국") or 0)


def _hourly_terminal(data: Optional[dict], terminal: str) -> list[int]:
    """24시간 × 해당 터미널 출국장 합계 시리즈."""
    out = [0] * 24
    if not data:
        return out
    rows = (data.get(terminal) or {}).get("depart", {}).get("시간대별") or []
    for row in rows:
        h = _hour_from(row.get("hour"))
        if 0 <= h < 24:
            out[h] = int(row.get("total") or 0)
    return out


def _hourly_per_gate_terminal(data: Optional[dict], terminal: str) -> dict[str, list[int]]:
    """24시간 × 각 출국장 시리즈 (해당 터미널)."""
    gates = T1_GATES if terminal == "T1" else T2_GATES
    zone_keys = T1_ZONE_KEYS if terminal == "T1" else T2_ZONE_KEYS
    out = {z: [0] * 24 for z in zone_keys}
    if not data:
        return out
    rows = (data.get(terminal) or {}).get("depart", {}).get("시간대별") or []
    for row in rows:
        h = _hour_from(row.get("hour"))
        if not (0 <= h < 24):
            continue
        for gate, zk in zip(gates, zone_keys):
            out[zk][h] = int(row.get(gate) or 0)
    return out


# ---------- 차트용 ----------
def hourly_t1_t2(data: Optional[dict]) -> dict[str, list]:
    """24시간 × T1·T2 출국장 합계 시리즈."""
    t1 = _hourly_terminal(data, "T1")
    t2 = _hourly_terminal(data, "T2")
    return {
        "hours": HOUR_LABELS,
        "T1": t1,
        "T2": t2,
        "total": [a + b for a, b in zip(t1, t2)],
    }


def hourly_per_gate(data: Optional[dict]) -> dict[str, list[int]]:
    """24시간 × 7개 zone (T1 5개 + T2 2개)."""
    out: dict[str, list[int]] = {}
    out.update(_hourly_per_gate_terminal(data, "T1"))
    out.update(_hourly_per_gate_terminal(data, "T2"))
    return out


def kpi_summary(today_data: Optional[dict], tomorrow_data: Optional[dict]) -> dict:
    """헤더 KPI — T1·T2 각각 일합·피크 시간대·피크 객수."""
    def one(data):
        t1_hourly = _hourly_terminal(data, "T1")
        t2_hourly = _hourly_terminal(data, "T2")
        t1 = sum(t1_hourly)
        t2 = sum(t2_hourly)
        peak_h_t1 = max(range(24), key=lambda i: t1_hourly[i]) if any(t1_hourly) else None
        peak_h_t2 = max(range(24), key=lambda i: t2_hourly[i]) if any(t2_hourly) else None
        return {
            "T1": t1, "T2": t2,
            "peak_hour_T1": peak_h_t1,
            "peak_total_T1": int(t1_hourly[peak_h_t1]) if peak_h_t1 is not None else 0,
            "peak_hour_T2": peak_h_t2,
            "peak_total_T2": int(t2_hourly[peak_h_t2]) if peak_h_t2 is not None else 0,
        }
    return {"today": one(today_data), "tomorrow": one(tomorrow_data)}


def daily_totals(daily_map: dict[str, tuple[Optional[dict], str]]) -> pd.DataFrame:
    """일별 T1/T2/터미널별 피크/소스.

    Returns: DataFrame[YYYYMMDD, T1, T2,
                        peak_hour_T1, peak_total_T1,
                        peak_hour_T2, peak_total_T2, source]
    """
    rows = []
    for ymd, (data, src) in sorted(daily_map.items()):
        if not data:
            rows.append({
                "YYYYMMDD": ymd, "T1": 0, "T2": 0,
                "peak_hour_T1": None, "peak_total_T1": 0,
                "peak_hour_T2": None, "peak_total_T2": 0,
                "source": src,
            })
            continue
        t1_hourly = _hourly_terminal(data, "T1")
        t2_hourly = _hourly_terminal(data, "T2")
        t1 = sum(t1_hourly)
        t2 = sum(t2_hourly)
        peak_h_t1 = max(range(24), key=lambda i: t1_hourly[i]) if any(t1_hourly) else None
        peak_h_t2 = max(range(24), key=lambda i: t2_hourly[i]) if any(t2_hourly) else None
        rows.append({
            "YYYYMMDD": ymd, "T1": t1, "T2": t2,
            "peak_hour_T1": peak_h_t1,
            "peak_total_T1": int(t1_hourly[peak_h_t1]) if peak_h_t1 is not None else 0,
            "peak_hour_T2": peak_h_t2,
            "peak_total_T2": int(t2_hourly[peak_h_t2]) if peak_h_t2 is not None else 0,
            "source": src,
        })
    return pd.DataFrame(rows)


# ---------- Baseline (D-1 기준 MTD + 전월 폴백) ----------
MIN_MTD_DAYS = 3  # 이번 달 D-1까지 누적 일수가 이 값 미만이면 전월 평균으로 폴백


def _resolve_baseline(daily_map, today: date, prev_month_map=None, anchor_end: date | None = None):
    """비교 기준선(baseline)을 반환.

    1순위: 이번 달 1일 ~ anchor_end 누적 일수가 MIN_MTD_DAYS 이상 → "MTD 평균(~M/D)"
    2순위: 전월 1일 ~ 말일 데이터가 1개 이상 → "전월 평균"
    그 외: 비교 기준선 없음 ("—")

    anchor_end: 평균 윈도우 끝점(포함). 보통 focus_date - 1day.
                None이면 today - 1day (focus=오늘 가정).

    Returns: (entries, kind, label, anchor)
        entries: list[tuple[date, dict]]
        kind:    "mtd_d_minus_1" | "prev_month" | "none"
        label:   사용자 노출용 라벨 (값과 함께 표기)
        anchor:  보조 라벨
    """
    first = today.replace(day=1)
    if anchor_end is None:
        anchor_end = today - timedelta(days=1)

    in_window: list[tuple[date, dict]] = []
    for ymd, (data, src) in (daily_map or {}).items():
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        if d < first or d > anchor_end:
            continue
        if not data or src == "none":
            continue
        in_window.append((d, data))

    if len(in_window) >= MIN_MTD_DAYS:
        return (
            in_window,
            "mtd_d_minus_1",
            f"{anchor_end.month}/{anchor_end.day} MTD 평균",
            f"{anchor_end.month}/{anchor_end.day}",
        )

    prev: list[tuple[date, dict]] = []
    for ymd, (data, src) in (prev_month_map or {}).items():
        if not data or src == "none":
            continue
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        prev.append((d, data))

    if prev:
        prev_first = min(d for d, _ in prev)
        return (
            prev,
            "prev_month",
            f"{prev_first.month}월 평균",
            f"{prev_first.month}월",
        )

    return ([], "none", "—", "—")


def _baseline_meta(kind: str, label: str, anchor: str, days: int) -> dict:
    return {
        "kind": kind,
        "label": label,
        "anchor": anchor,
        "days": days,
        "available": kind != "none" and days > 0,
    }


# ---------- MTD ----------
def mtd_summary(daily_map, today: date, prev_month_map=None, anchor_end: date | None = None) -> dict:
    """비교 기준선의 일평균 T1·T2 (출국장 통과 기준)."""
    entries, kind, label, anchor = _resolve_baseline(daily_map, today, prev_month_map, anchor_end)
    n = len(entries)
    meta = _baseline_meta(kind, label, anchor, n)
    if n == 0:
        return {"T1": 0, "T2": 0, "total": 0, **meta}
    t1s = [_terminal_total(d, "T1") for _, d in entries]
    t2s = [_terminal_total(d, "T2") for _, d in entries]
    t1_avg = round(sum(t1s) / n)
    t2_avg = round(sum(t2s) / n)
    return {"T1": t1_avg, "T2": t2_avg, "total": t1_avg + t2_avg, **meta}


def reserved_summary(today_data: Optional[dict], tomorrow_data: Optional[dict]) -> dict:
    """예약승객 출국 기준 (SMS 알림 동일 기준) — 환승객 포함."""
    def one(data):
        t1 = _reserved_out(data, "T1")
        t2 = _reserved_out(data, "T2")
        return {"T1": t1, "T2": t2, "total": t1 + t2}
    return {"today": one(today_data), "tomorrow": one(tomorrow_data)}


def mtd_reserved(daily_map, today: date, prev_month_map=None, anchor_end: date | None = None) -> dict:
    """비교 기준선의 예약승객 출국 일평균 (SMS 동일 기준)."""
    entries, kind, label, anchor = _resolve_baseline(daily_map, today, prev_month_map, anchor_end)
    n = len(entries)
    meta = _baseline_meta(kind, label, anchor, n)
    if n == 0:
        return {"T1": 0, "T2": 0, "total": 0, **meta}
    t1s = [_reserved_out(d, "T1") for _, d in entries]
    t2s = [_reserved_out(d, "T2") for _, d in entries]
    t1_avg = round(sum(t1s) / n)
    t2_avg = round(sum(t2s) / n)
    return {"T1": t1_avg, "T2": t2_avg, "total": t1_avg + t2_avg, **meta}


def mtd_per_gate(daily_map, today: date, prev_month_map=None, anchor_end: date | None = None) -> dict:
    """비교 기준선의 7개 zone 일평균. baseline 메타 포함."""
    entries, kind, label, anchor = _resolve_baseline(daily_map, today, prev_month_map, anchor_end)
    n = len(entries)
    meta = _baseline_meta(kind, label, anchor, n)
    if n == 0:
        return {"values": {z: 0 for z in ALL_ZONE_KEYS}, **meta}
    sums = {z: 0 for z in ALL_ZONE_KEYS}
    for _, data in entries:
        per = hourly_per_gate(data)
        for z in ALL_ZONE_KEYS:
            sums[z] += sum(per[z])
    values = {z: round(sums[z] / n) for z in ALL_ZONE_KEYS}
    return {"values": values, **meta}


def mtd_hourly_t1_t2(daily_map, today: date, prev_month_map=None, anchor_end: date | None = None) -> dict:
    """비교 기준선의 시간대별 평균 (T1·T2)."""
    entries, kind, label, anchor = _resolve_baseline(daily_map, today, prev_month_map, anchor_end)
    n = len(entries)
    meta = _baseline_meta(kind, label, anchor, n)
    if n == 0:
        return {"hours": HOUR_LABELS, "T1": [0] * 24, "T2": [0] * 24, **meta}
    sums_t1 = [0] * 24
    sums_t2 = [0] * 24
    for _, data in entries:
        t1 = _hourly_terminal(data, "T1")
        t2 = _hourly_terminal(data, "T2")
        for i in range(24):
            sums_t1[i] += t1[i]
            sums_t2[i] += t2[i]
    return {
        "hours": HOUR_LABELS,
        "T1": [round(sums_t1[i] / n) for i in range(24)],
        "T2": [round(sums_t2[i] / n) for i in range(24)],
        **meta,
    }


# ---------- 노선별 (신규) ----------
def route_matrix(data: Optional[dict], terminal: str) -> dict:
    """24시간 × 7권역 매트릭스 (해당 터미널 출국 노선별)."""
    matrix = [[0] * 24 for _ in REGIONS]
    if not data:
        return {"hours": HOUR_LABELS, "regions": REGIONS, "matrix": matrix}
    rows = (data.get(terminal) or {}).get("depart_route", {}).get("시간대별") or []
    for row in rows:
        h = _hour_from(row.get("hour"))
        if not (0 <= h < 24):
            continue
        for r_idx, region in enumerate(REGIONS):
            matrix[r_idx][h] = int(row.get(region) or 0)
    return {"hours": HOUR_LABELS, "regions": REGIONS, "matrix": matrix}


def route_summary(data: Optional[dict], terminal: str) -> dict:
    """권역별 일합계와 비율 (해당 터미널 출국)."""
    totals = {r: 0 for r in REGIONS}
    if data:
        src = (data.get(terminal) or {}).get("depart_route", {}).get("권역합계") or {}
        for r in REGIONS:
            totals[r] = int(src.get(r) or 0)
    grand = sum(totals.values())
    ratios = {r: (totals[r] / grand if grand > 0 else 0.0) for r in REGIONS}
    return {
        "regions": REGIONS,
        "values": [totals[r] for r in REGIONS],
        "ratios": [round(ratios[r] * 100, 1) for r in REGIONS],
        "total": grand,
    }


def mtd_route(daily_map, today: date, terminal: str, prev_month_map=None, anchor_end: date | None = None) -> dict:
    """비교 기준선의 권역별 일평균 + 비율."""
    entries, kind, label, anchor = _resolve_baseline(daily_map, today, prev_month_map, anchor_end)
    n = len(entries)
    meta = _baseline_meta(kind, label, anchor, n)
    if n == 0:
        return {
            "regions": REGIONS,
            "values": [0] * len(REGIONS),
            "ratios": [0.0] * len(REGIONS),
            "total": 0,
            **meta,
        }
    sums = {r: 0 for r in REGIONS}
    for _, data in entries:
        src = (data.get(terminal) or {}).get("depart_route", {}).get("권역합계") or {}
        for r in REGIONS:
            sums[r] += int(src.get(r) or 0)
    avgs = {r: round(sums[r] / n) for r in REGIONS}
    grand = sum(avgs.values())
    return {
        "regions": REGIONS,
        "values": [avgs[r] for r in REGIONS],
        "ratios": [round((avgs[r] / grand * 100) if grand > 0 else 0.0, 1) for r in REGIONS],
        "total": grand,
        **meta,
    }


# ---------- 일자별 차트 / 월누적 / 동일일수 비교 (항공편수 대시보드와 동일 패턴) ----------
def _iter_month(daily_map, year: int, month: int, day_max: int):
    """주어진 daily_map에서 (year, month) 1~day_max 범위의 (date, data) 만 반환."""
    for ymd, (data, src) in (daily_map or {}).items():
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        if d.year != year or d.month != month or d.day < 1 or d.day > day_max:
            continue
        if not data or src == "none":
            continue
        yield d, data


def daily_series_by_month(daily_map, year: int, month: int, chart_last_day: int) -> dict:
    """월간 일자별 T1·T2 시리즈 (1~chart_last_day, 데이터 없는 날=null).

    이번달 차트는 데이터가 있는 일자까지만 그리고, 그 이후는 null로 둔다.
    전월 차트는 말일까지 데이터가 채워진 상태.
    """
    by_day: dict[int, tuple[int, int]] = {}
    for d, data in _iter_month(daily_map, year, month, day_max=31):
        by_day[d.day] = (_terminal_total(data, "T1"), _terminal_total(data, "T2"))
    t1 = [None] * chart_last_day
    t2 = [None] * chart_last_day
    for day, (a, b) in by_day.items():
        if 1 <= day <= chart_last_day:
            t1[day - 1] = a
            t2[day - 1] = b
    max_day = max(by_day.keys()) if by_day else 0
    return {"T1": t1, "T2": t2, "max_day": max_day}


def monthly_compare(daily_map_curr, daily_map_prev,
                    year_c: int, month_c: int, year_p: int, month_p: int,
                    cutoff_curr: int, cutoff_prev: int) -> dict:
    """이번달/전월 동일일수 월누적 비교 (출국장 통과 기준)."""
    def sum_term(daily_map, year, month, cutoff):
        t1, t2, n = 0, 0, 0
        for _, data in _iter_month(daily_map, year, month, cutoff):
            t1 += _terminal_total(data, "T1")
            t2 += _terminal_total(data, "T2")
            n += 1
        return t1, t2, n
    t1c, t2c, dc = sum_term(daily_map_curr, year_c, month_c, cutoff_curr)
    t1p, t2p, dp = sum_term(daily_map_prev, year_p, month_p, cutoff_prev)
    return {
        "T1_curr": t1c, "T2_curr": t2c, "days_curr": dc,
        "T1_prev": t1p, "T2_prev": t2p, "days_prev": dp,
    }


def gate_compare(daily_map_curr, daily_map_prev,
                 year_c: int, month_c: int, year_p: int, month_p: int,
                 cutoff_curr: int, cutoff_prev: int) -> dict:
    """출국장별 동일일수 합계 비교 (24시간 × zone 매트릭스 누계 포함).

    UI에서 시간대 셀렉터로 오전/오후/저녁/심야를 골라 합산할 수 있도록
    'curr_hourly' / 'prev_hourly' 도 함께 노출한다 (sum across days, per hour).
    """
    def sum_per_gate(daily_map, year, month, cutoff):
        sums = {z: 0 for z in ALL_ZONE_KEYS}
        hourly = {z: [0] * 24 for z in ALL_ZONE_KEYS}
        n = 0
        for _, data in _iter_month(daily_map, year, month, cutoff):
            per = hourly_per_gate(data)
            for z in ALL_ZONE_KEYS:
                arr = per[z]
                for h in range(24):
                    hourly[z][h] += arr[h]
                sums[z] += sum(arr)
            n += 1
        return sums, hourly, n
    curr_sums, curr_hourly, n_c = sum_per_gate(daily_map_curr, year_c, month_c, cutoff_curr)
    prev_sums, prev_hourly, n_p = sum_per_gate(daily_map_prev, year_p, month_p, cutoff_prev)
    return {
        "curr": curr_sums, "prev": prev_sums,
        "curr_hourly": curr_hourly, "prev_hourly": prev_hourly,
        "days_curr": n_c, "days_prev": n_p,
    }


def prev_dow_reserved_avg(daily_map_prev, year_p: int, month_p: int) -> dict:
    """전월 요일별 예약합계 출국 평균 (환승객 포함, SMS·KPI 카드 동일 기준).

    Returns: {"T1": {wd: avg}, "T2": {wd: avg}, "total": {wd: avg}}
    """
    bk_t1: dict[int, list[int]] = {}
    bk_t2: dict[int, list[int]] = {}
    for d, data in _iter_month(daily_map_prev, year_p, month_p, day_max=31):
        wd = d.weekday()
        bk_t1.setdefault(wd, []).append(_reserved_out(data, "T1"))
        bk_t2.setdefault(wd, []).append(_reserved_out(data, "T2"))

    def _avg(buckets):
        return {wd: round(sum(v) / len(v)) for wd, v in buckets.items() if v}

    avg_t1 = _avg(bk_t1)
    avg_t2 = _avg(bk_t2)
    avg_total = {}
    for wd in set(avg_t1) | set(avg_t2):
        avg_total[wd] = avg_t1.get(wd, 0) + avg_t2.get(wd, 0)
    return {"T1": avg_t1, "T2": avg_t2, "total": avg_total}


def prev_dow_hourly_avg(daily_map_prev, year_p: int, month_p: int, weekday: int) -> dict:
    """전월 특정 요일의 시간대별 T1·T2 평균 (출국장 통과 기준).

    weekday: 0(월)~6(일)
    Returns: {"hours": HOUR_LABELS, "T1": [24], "T2": [24], "available": bool, "n": int}
    """
    sums_t1 = [0] * 24
    sums_t2 = [0] * 24
    n = 0
    for d, data in _iter_month(daily_map_prev, year_p, month_p, day_max=31):
        if d.weekday() != weekday:
            continue
        t1 = _hourly_terminal(data, "T1")
        t2 = _hourly_terminal(data, "T2")
        for i in range(24):
            sums_t1[i] += t1[i]
            sums_t2[i] += t2[i]
        n += 1
    if n == 0:
        return {"hours": HOUR_LABELS, "T1": [0] * 24, "T2": [0] * 24, "available": False, "n": 0}
    return {
        "hours": HOUR_LABELS,
        "T1": [round(sums_t1[i] / n) for i in range(24)],
        "T2": [round(sums_t2[i] / n) for i in range(24)],
        "available": True,
        "n": n,
    }


def prev_dow_avg(daily_map_prev, year_p: int, month_p: int) -> dict:
    """전월 요일별 T1/T2/합계 평균 (출국장 통과 기준).

    Returns: {"T1": {wd: avg}, "T2": {wd: avg}, "total": {wd: avg}} — wd ∈ 0..6 (월~일)
    """
    buckets_t1: dict[int, list[int]] = {}
    buckets_t2: dict[int, list[int]] = {}
    for d, data in _iter_month(daily_map_prev, year_p, month_p, day_max=31):
        wd = d.weekday()
        buckets_t1.setdefault(wd, []).append(_terminal_total(data, "T1"))
        buckets_t2.setdefault(wd, []).append(_terminal_total(data, "T2"))

    def _avg(buckets):
        return {wd: (sum(v) / len(v)) for wd, v in buckets.items() if v}

    avg_t1 = _avg(buckets_t1)
    avg_t2 = _avg(buckets_t2)
    avg_total = {}
    for wd in set(avg_t1) | set(avg_t2):
        avg_total[wd] = avg_t1.get(wd, 0) + avg_t2.get(wd, 0)
    return {"T1": avg_t1, "T2": avg_t2, "total": avg_total}


def route_compare(daily_map_curr, daily_map_prev, terminal: str,
                  year_c: int, month_c: int, year_p: int, month_p: int,
                  cutoff_curr: int, cutoff_prev: int) -> dict:
    """권역별 동일일수 합계 비교 (해당 터미널)."""
    def sum_route(daily_map, year, month, cutoff):
        sums = {r: 0 for r in REGIONS}
        n = 0
        for _, data in _iter_month(daily_map, year, month, cutoff):
            src = (data.get(terminal) or {}).get("depart_route", {}).get("권역합계") or {}
            for r in REGIONS:
                sums[r] += int(src.get(r) or 0)
            n += 1
        return sums, n
    curr_sums, n_c = sum_route(daily_map_curr, year_c, month_c, cutoff_curr)
    prev_sums, n_p = sum_route(daily_map_prev, year_p, month_p, cutoff_prev)
    return {
        "regions": REGIONS,
        "curr": [curr_sums[r] for r in REGIONS],
        "prev": [prev_sums[r] for r in REGIONS],
        "days_curr": n_c, "days_prev": n_p,
    }


# ---------- 라벨 포맷 ----------
def fmt_peak_hour(h) -> str:
    """피크 시간대 라벨. 형식 통일: '08~09' (시작~종료)."""
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
    return f"{h:02d}~{(h+1)%24:02d}"
