"""airport.kr 공항 예상 혼잡도 엑셀 파서.

다운로드 URL:
    https://www.airport.kr/pni/ap_ko/statisticPredictCrowdedOfInoutExcel.do
파라미터: selTm={T1|T2}, pday=YYYYMMDD

엑셀(.xls) 안에는 9개 시트가 있다:
    출국승객예고, 입국승객예고, 환승객예고,
    출국노선별승객예고, 입국노선별승객예고1, 입국노선별승객예고,
    출국셔틀트레인승강장예상인원, 입국셔틀트레인승강장예상인원, basedata

본 모듈은 시트별 파서를 제공하고, 한 터미널의 9개 시트를 단일 dict로 변환한다.
"""
from __future__ import annotations

import io
import re
from typing import Optional

import pandas as pd

REGIONS = ["일본", "중국", "동남아", "미주", "유럽", "오세아니아", "기타"]
HOUR_LABELS = [f"{h:02d}_{(h+1)%24:02d}" for h in range(24)]  # "00_01" ~ "23_00"

_HOUR_RE = re.compile(r"(\d{1,2})\s*[~-]\s*(\d{1,2})\s*시?")


def parse_hour(label) -> Optional[str]:
    """'0~1시' / '00~01' → '00_01'. 매치 안되면 None."""
    if label is None:
        return None
    m = _HOUR_RE.search(str(label))
    if not m:
        return None
    h0, h1 = int(m.group(1)), int(m.group(2))
    if not (0 <= h0 <= 24 and 0 <= h1 <= 24):
        return None
    return f"{h0:02d}_{h1 % 24:02d}"


def _to_int(v) -> int:
    if v is None:
        return 0
    try:
        if pd.isna(v):
            return 0
    except (TypeError, ValueError):
        pass
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).replace(",", "").strip()
    if not s or s in ("-", "—"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _to_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _find_row(df: pd.DataFrame, col: int, anchor: str, start: int = 0) -> int:
    """col 컬럼에서 anchor 문자열을 포함하는 첫 행 인덱스 (없으면 -1)."""
    for i in range(start, len(df)):
        v = df.iat[i, col] if df.shape[1] > col else None
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        if anchor in str(v):
            return i
    return -1


# ---------- 출국승객예고 ----------
def parse_depart(df: pd.DataFrame, terminal: str) -> dict:
    """출국승객예고 시트 → 예약합계 + 출국장별 + 동서비율 + 시간대별.

    T1 출국장 키: "1","2","3","4","5_6" / T2: "1","2"
    """
    gate_keys = ["1", "2", "3", "4", "5_6"] if terminal == "T1" else ["1", "2"]

    # 1. 예약승객 (출국/입국/계)
    row_reserved = _find_row(df, 0, "실적(명)")
    reserved = {"출국": 0, "입국": 0, "계": 0}
    if row_reserved >= 0 and df.shape[1] >= 4:
        reserved["출국"] = _to_int(df.iat[row_reserved, 1])
        reserved["입국"] = _to_int(df.iat[row_reserved, 2])
        reserved["계"] = _to_int(df.iat[row_reserved, 3])

    # 2. 출국장별 예상여객 (다음 "실적(명)" 행)
    row_gate_total = _find_row(df, 0, "실적(명)", start=row_reserved + 1) if row_reserved >= 0 else -1
    gate_totals = {k: 0 for k in gate_keys}
    if row_gate_total >= 0:
        for i, k in enumerate(gate_keys):
            col = 1 + i
            if col < df.shape[1]:
                gate_totals[k] = _to_int(df.iat[row_gate_total, col])

    # 동/서 비율
    row_dongseo = _find_row(df, 0, "동/서")
    east_west = {"동": 0.0, "서": 0.0}
    if row_dongseo >= 0:
        # T1: col 1 = 동, col 4 = 서
        # T2: 동/서 비율은 의미 없음 (출국장 2개)
        if terminal == "T1" and df.shape[1] >= 5:
            east_west["동"] = _to_float(df.iat[row_dongseo, 1])
            east_west["서"] = _to_float(df.iat[row_dongseo, 4])

    # 3. 시간대별 출국장별 예상여객
    # T2 시트는 시간대별 영역에서 컬럼 사이에 빈 컬럼(NaN)이 끼어있는 sparse 구조이므로
    # "항목" 헤더 행에서 "출국장X" 라벨 위치로 컬럼 인덱스를 동적 매핑한다.
    row_hour_anchor = _find_row(df, 0, "시간대별 출국장별")
    hourly = []
    if row_hour_anchor >= 0:
        header_row = _find_row(df, 0, "항목", start=row_hour_anchor + 1)
        gate_to_col: dict[str, int] = {}
        if header_row >= 0:
            for col in range(1, df.shape[1]):
                v = df.iat[header_row, col]
                try:
                    if pd.isna(v):
                        continue
                except (TypeError, ValueError):
                    pass
                if v is None:
                    continue
                s = str(v).strip()
                # "출국장1" → "1", "출국장5,6" → "5_6"
                if not s.startswith("출국장"):
                    continue
                key = s.replace("출국장", "").strip()
                key = key.replace(",", "_").replace(" ", "")
                if key in gate_keys and key not in gate_to_col:
                    gate_to_col[key] = col
        # 매핑 실패 시 fallback: 연속 인덱싱
        for j, k in enumerate(gate_keys):
            gate_to_col.setdefault(k, 1 + j)

        scan_start = (header_row + 1) if header_row >= 0 else (row_hour_anchor + 1)
        for i in range(scan_start, len(df)):
            label = df.iat[i, 0] if df.shape[1] > 0 else None
            h = parse_hour(label)
            if h is None:
                continue
            row = {"hour": h}
            total = 0
            for k in gate_keys:
                col = gate_to_col[k]
                v = _to_int(df.iat[i, col]) if 0 <= col < df.shape[1] else 0
                row[k] = v
                total += v
            row["total"] = total
            hourly.append(row)
            if len(hourly) >= 24:
                break

    return {
        "예약합계": reserved,
        "출국장별": gate_totals,
        "동서비율": east_west,
        "시간대별": hourly,
    }


# ---------- 입국승객예고 ----------
def parse_arrive(df: pd.DataFrame, terminal: str) -> dict:
    """입국승객예고 → 심사대별 + 시간대별.

    T1 심사대 키: "AB","C","D","EF" / T2: "A","B"
    """
    gate_keys = ["AB", "C", "D", "EF"] if terminal == "T1" else ["A", "B"]
    n = len(gate_keys)

    row_anchor = _find_row(df, 0, "시간대별 입국심사대별")
    if row_anchor < 0:
        return {"심사대별": {k: 0 for k in gate_keys}, "시간대별": []}

    # T2 시트는 컬럼이 sparse(NaN으로 분리)하므로 헤더 라벨 위치로 동적 매핑
    header_row = _find_row(df, 0, "항목", start=row_anchor + 1)
    gate_to_col: dict[str, int] = {}
    if header_row >= 0:
        for col in range(1, df.shape[1]):
            v = df.iat[header_row, col]
            try:
                if pd.isna(v):
                    continue
            except (TypeError, ValueError):
                pass
            if v is None:
                continue
            label = str(v).strip().replace(",", "").replace(" ", "")
            if label in gate_keys and label not in gate_to_col:
                gate_to_col[label] = col
    for j, k in enumerate(gate_keys):
        gate_to_col.setdefault(k, 1 + j)

    scan_start = (header_row + 1) if header_row >= 0 else (row_anchor + 1)
    hourly = []
    totals = {k: 0 for k in gate_keys}
    for i in range(scan_start, len(df)):
        label = df.iat[i, 0] if df.shape[1] > 0 else None
        h = parse_hour(label)
        if h is None:
            continue
        row = {"hour": h}
        ttl = 0
        for k in gate_keys:
            col = gate_to_col[k]
            v = _to_int(df.iat[i, col]) if 0 <= col < df.shape[1] else 0
            row[k] = v
            ttl += v
            totals[k] += v
        row["total"] = ttl
        hourly.append(row)
        if len(hourly) >= 24:
            break

    return {"심사대별": totals, "시간대별": hourly}


# ---------- 환승객예고 ----------
def parse_transit(df: pd.DataFrame) -> dict:
    """환승객예고 → KE/OZ + 비율 + 보안검색대별.

    구조: row 3=헤더(대한항공/아시아나/계), row 4=실적, row 5=비율
    보안검색대별은 시트 하단에 있음 (보안검색대 라벨 기준).
    """
    out = {"KE": 0, "OZ": 0, "계": 0, "비율_KE": 0.0, "보안검색대별": {}}

    row_reserved = _find_row(df, 0, "실적(명)")
    if row_reserved >= 0:
        # 실제 데이터 위치: KE는 col 1, OZ는 col 3, 계는 col 5 (NaN으로 sparse)
        if df.shape[1] >= 6:
            out["KE"] = _to_int(df.iat[row_reserved, 1])
            out["OZ"] = _to_int(df.iat[row_reserved, 3])
            out["계"] = _to_int(df.iat[row_reserved, 5])

    row_ratio = _find_row(df, 0, "비율", start=row_reserved + 1) if row_reserved >= 0 else -1
    if row_ratio >= 0 and df.shape[1] >= 2:
        out["비율_KE"] = _to_float(df.iat[row_ratio, 1])

    return out


# ---------- 출국/입국 노선별 ----------
def parse_route(df: pd.DataFrame, direction: str) -> dict:
    """노선별 승객예고 → 권역합계 + 시간대별.

    direction: "depart" 또는 "arrive". 헤더 컬럼은 7개(REGIONS) 동일.
    엑셀에서 입국노선별은 컬럼이 sparse하므로 헤더 텍스트로 컬럼 인덱스 매핑한다.
    """
    # 1. 권역 헤더 위치 탐색 (col 0 = "항목")
    header_row = _find_row(df, 0, "항목")
    if header_row < 0:
        return {"권역합계": {r: 0 for r in REGIONS}, "시간대별": []}

    # 헤더 행에서 각 권역의 컬럼 인덱스 매핑
    region_cols: dict[str, int] = {}
    for col in range(1, df.shape[1]):
        v = df.iat[header_row, col]
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if s in REGIONS and s not in region_cols:
            region_cols[s] = col
    # 모든 권역이 매핑되지 않았으면 빈 결과
    if len(region_cols) < len(REGIONS):
        # 누락 권역은 0
        for r in REGIONS:
            region_cols.setdefault(r, -1)

    # 2. 권역합계 (실적(명) 행)
    row_total = _find_row(df, 0, "실적(명)", start=header_row + 1)
    region_totals = {r: 0 for r in REGIONS}
    if row_total >= 0:
        for r in REGIONS:
            col = region_cols[r]
            if col >= 0 and col < df.shape[1]:
                region_totals[r] = _to_int(df.iat[row_total, col])

    # 3. 시간대별 헤더 (두 번째 "항목" 행)
    header2 = _find_row(df, 0, "항목", start=header_row + 1)
    if header2 < 0:
        return {"권역합계": region_totals, "시간대별": []}

    # 시간대별 권역 컬럼은 보통 첫 번째 헤더와 동일하지만 다시 매핑
    region_cols2: dict[str, int] = {}
    for col in range(1, df.shape[1]):
        v = df.iat[header2, col]
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if s in REGIONS and s not in region_cols2:
            region_cols2[s] = col
    for r in REGIONS:
        region_cols2.setdefault(r, region_cols[r])

    hourly = []
    for i in range(header2 + 1, len(df)):
        label = df.iat[i, 0] if df.shape[1] > 0 else None
        h = parse_hour(label)
        if h is None:
            continue
        row = {"hour": h}
        for r in REGIONS:
            col = region_cols2[r]
            row[r] = _to_int(df.iat[i, col]) if (col >= 0 and col < df.shape[1]) else 0
        hourly.append(row)
        if len(hourly) >= 24:
            break

    return {"권역합계": region_totals, "시간대별": hourly}


# ---------- 출국 셔틀트레인 ----------
def parse_shuttle_depart(df: pd.DataFrame) -> list:
    """출국 셔틀트레인 승강장 예상 인원 → [{"hour": "00_01", "value": N}, ...]."""
    out = []
    for i in range(len(df)):
        label = df.iat[i, 0] if df.shape[1] > 0 else None
        h = parse_hour(label)
        if h is None:
            continue
        # 값은 col 2에 위치 (col 1은 NaN)
        v = _to_int(df.iat[i, 2]) if df.shape[1] >= 3 else 0
        out.append({"hour": h, "value": v})
        if len(out) >= 24:
            break
    return out


# ---------- 입국 셔틀트레인 ----------
def parse_shuttle_arrive(df: pd.DataFrame) -> list:
    """입국 셔틀트레인 → [{"hour": "00_01", "동": N, "서": N, "계": N}, ...]."""
    out = []
    for i in range(len(df)):
        label = df.iat[i, 0] if df.shape[1] > 0 else None
        h = parse_hour(label)
        if h is None:
            continue
        east = _to_int(df.iat[i, 1]) if df.shape[1] >= 2 else 0
        west = _to_int(df.iat[i, 2]) if df.shape[1] >= 3 else 0
        total = _to_int(df.iat[i, 3]) if df.shape[1] >= 4 else east + west
        out.append({"hour": h, "동": east, "서": west, "계": total})
        if len(out) >= 24:
            break
    return out


# ---------- 통합 파서 ----------
SHEET_NAMES = {
    "depart": "출국승객예고",
    "arrive": "입국승객예고",
    "transit": "환승객예고",
    "depart_route": "출국노선별승객예고",
    "arrive_route": "입국노선별승객예고",
    "shuttle_depart": "출국셔틀트레인승강장예상인원",
    "shuttle_arrive": "입국셔틀트레인승강장예상인원",
}


def parse_terminal(xls_bytes: bytes, terminal: str) -> dict:
    """T1 또는 T2 엑셀 한 파일 → 통합 dict.

    실패한 시트는 빈 구조로 채워 다른 시트는 살린다 (인천공항 측 시트명 변경 대비).
    """
    xls = pd.ExcelFile(io.BytesIO(xls_bytes), engine="xlrd")

    def read(name: str) -> pd.DataFrame:
        if name not in xls.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(xls, sheet_name=name, header=None)

    return {
        "depart": parse_depart(read(SHEET_NAMES["depart"]), terminal),
        "arrive": parse_arrive(read(SHEET_NAMES["arrive"]), terminal),
        "transit": parse_transit(read(SHEET_NAMES["transit"])),
        "depart_route": parse_route(read(SHEET_NAMES["depart_route"]), "depart"),
        "arrive_route": parse_route(read(SHEET_NAMES["arrive_route"]), "arrive"),
        "shuttle_depart": parse_shuttle_depart(read(SHEET_NAMES["shuttle_depart"])),
        "shuttle_arrive": parse_shuttle_arrive(read(SHEET_NAMES["shuttle_arrive"])),
    }
