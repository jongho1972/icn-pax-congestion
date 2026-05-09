"""출국장 혼잡도 집계 (airport.kr 엑셀 스키마 기반).

입력: data_loader.load_day가 반환하는 통합 dict.

T1 출국장 키: "1","2","3","4","5_6"  (출국장 5·6번이 합산되어 엑셀 원본 그대로)
T2 출국장 키: "1","2"
"""
from __future__ import annotations

from datetime import date, datetime
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


# ---------- MTD ----------
def _iter_month_to_date(daily_map, today: date):
    """이번 달 1일 ~ 오늘까지 유효 data만 yield."""
    first = today.replace(day=1)
    for ymd, (data, src) in daily_map.items():
        try:
            d = datetime.strptime(ymd, "%Y%m%d").date()
        except ValueError:
            continue
        if d < first or d > today:
            continue
        if not data or src == "none":
            continue
        yield d, data


def mtd_summary(daily_map, today: date) -> dict:
    """이번 달 1일 ~ 오늘까지의 일평균 T1·T2 (출국장 통과 기준)."""
    first = today.replace(day=1)
    t1s, t2s = [], []
    for _, data in _iter_month_to_date(daily_map, today):
        t1s.append(_terminal_total(data, "T1"))
        t2s.append(_terminal_total(data, "T2"))
    n = len(t1s)
    if n == 0:
        return {"days": 0, "T1": 0, "T2": 0, "total": 0, "period_label": "—"}
    t1_avg = round(sum(t1s) / n)
    t2_avg = round(sum(t2s) / n)
    return {
        "days": n, "T1": t1_avg, "T2": t2_avg,
        "total": t1_avg + t2_avg,
        "period_label": f"{first.month}/{first.day} ~ {today.month}/{today.day}",
    }


def reserved_summary(today_data: Optional[dict], tomorrow_data: Optional[dict]) -> dict:
    """예약승객 출국 기준 (SMS 알림 동일 기준) — 환승객 포함."""
    def one(data):
        t1 = _reserved_out(data, "T1")
        t2 = _reserved_out(data, "T2")
        return {"T1": t1, "T2": t2, "total": t1 + t2}
    return {"today": one(today_data), "tomorrow": one(tomorrow_data)}


def mtd_reserved(daily_map, today: date) -> dict:
    """이번 달 1일 ~ 오늘까지 예약승객 출국 일평균 (SMS 동일 기준)."""
    first = today.replace(day=1)
    t1s, t2s = [], []
    for _, data in _iter_month_to_date(daily_map, today):
        t1s.append(_reserved_out(data, "T1"))
        t2s.append(_reserved_out(data, "T2"))
    n = len(t1s)
    if n == 0:
        return {"days": 0, "T1": 0, "T2": 0, "total": 0,
                "period_label": "—", "anchor_label": "—"}
    t1_avg = round(sum(t1s) / n)
    t2_avg = round(sum(t2s) / n)
    # SMS 패턴: "(5/X MTD 평균 XX명)" — 마지막 누적 일자(=오늘)를 anchor로 표기
    return {
        "days": n, "T1": t1_avg, "T2": t2_avg,
        "total": t1_avg + t2_avg,
        "period_label": f"{first.month}/{first.day} ~ {today.month}/{today.day}",
        "anchor_label": f"{today.month}/{today.day}",
    }


def mtd_per_gate(daily_map, today: date) -> dict[str, int]:
    """이번 달 1일 ~ 오늘까지 7개 zone 일평균."""
    sums = {z: 0 for z in ALL_ZONE_KEYS}
    n = 0
    for _, data in _iter_month_to_date(daily_map, today):
        n += 1
        per = hourly_per_gate(data)
        for z in ALL_ZONE_KEYS:
            sums[z] += sum(per[z])
    if n == 0:
        return {z: 0 for z in ALL_ZONE_KEYS}
    return {z: round(sums[z] / n) for z in ALL_ZONE_KEYS}


def mtd_hourly_t1_t2(daily_map, today: date) -> dict:
    """이번 달 1일 ~ 오늘까지 시간대별 평균 (T1·T2)."""
    sums_t1 = [0] * 24
    sums_t2 = [0] * 24
    n = 0
    for _, data in _iter_month_to_date(daily_map, today):
        t1 = _hourly_terminal(data, "T1")
        t2 = _hourly_terminal(data, "T2")
        for i in range(24):
            sums_t1[i] += t1[i]
            sums_t2[i] += t2[i]
        n += 1
    if n == 0:
        return {"hours": HOUR_LABELS, "T1": [0] * 24, "T2": [0] * 24, "days": 0}
    return {
        "hours": HOUR_LABELS,
        "T1": [round(sums_t1[i] / n) for i in range(24)],
        "T2": [round(sums_t2[i] / n) for i in range(24)],
        "days": n,
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


def mtd_route(daily_map, today: date, terminal: str) -> dict:
    """이번 달 1일 ~ 오늘까지 권역별 일평균 + 비율."""
    sums = {r: 0 for r in REGIONS}
    n = 0
    for _, data in _iter_month_to_date(daily_map, today):
        src = (data.get(terminal) or {}).get("depart_route", {}).get("권역합계") or {}
        for r in REGIONS:
            sums[r] += int(src.get(r) or 0)
        n += 1
    if n == 0:
        zeros = {r: 0 for r in REGIONS}
        return {
            "regions": REGIONS,
            "values": [0] * len(REGIONS),
            "ratios": [0.0] * len(REGIONS),
            "total": 0, "days": 0,
        }
    avgs = {r: round(sums[r] / n) for r in REGIONS}
    grand = sum(avgs.values())
    return {
        "regions": REGIONS,
        "values": [avgs[r] for r in REGIONS],
        "ratios": [round((avgs[r] / grand * 100) if grand > 0 else 0.0, 1) for r in REGIONS],
        "total": grand,
        "days": n,
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
