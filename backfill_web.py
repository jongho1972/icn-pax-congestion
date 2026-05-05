"""airport.kr 공항 예상 혼잡도 페이지 스크래퍼.

OpenAPI는 D-0/D+1만 제공하지만 airport.kr 공식 페이지는 D-?~D+2까지 7일치를
제공한다. 매일 17:00 KST에 갱신되며 17:05 cron에서 함께 호출.

저장 형식: Daily_Data/passgr_YYYYMMDD_web.pkl — OpenAPI 결과와 동일한 25행 컬럼
스키마(adate, atime, t1eg1~4, t1egsum1, t1dg1~6, t1dgsum1, t2eg1~2, t2egsum1,
t2dg1~2, t2dgsum2). 합계 행도 포함.

조회 우선순위 (data_loader): d0(그날 마감) > web(17:00 발표) > d1(전일 D+1)
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

KST = ZoneInfo("Asia/Seoul")
BASE_URL = "https://www.airport.kr/pni/ap_ko/statisticPredictCrowdedOfInout.do"
DAILY_DIR = Path(__file__).resolve().parent / "Daily_Data"

# 스크래퍼가 시도할 일자 오프셋 (오늘 기준). 실제 데이터는 보통 D-?~D+2.
DATE_OFFSETS = list(range(-2, 8))  # D-2 ~ D+7 — 비어있는 날은 자동 skip

ALL_COLS = [
    "adate", "atime",
    "t1eg1", "t1eg2", "t1eg3", "t1eg4", "t1egsum1",
    "t1dg1", "t1dg2", "t1dg3", "t1dg4", "t1dg5", "t1dg6", "t1dgsum1",
    "t2eg1", "t2eg2", "t2egsum1",
    "t2dg1", "t2dg2", "t2dgsum2",
    "tmp1", "tmp2",
]

UA = "Mozilla/5.0 (compatible; icn-pax-congestion-bot/1.0)"


def _to_int(s: str) -> int:
    s = (s or "").replace(",", "").strip()
    return int(s) if s.isdigit() else 0


def _fetch(terminal: str, target: date) -> list[list[str]]:
    """단일 (T1|T2, 날짜) 페이지의 tbody 행 추출."""
    pday = target.strftime("%Y%m%d")
    resp = requests.get(
        BASE_URL,
        params={"selTm": terminal, "pday": pday},
        timeout=30,
        headers={"User-Agent": UA},
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []
    rows = []
    for tr in tbody.find_all("tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    return rows


def _parse_atime(label: str) -> str | None:
    """'00~01시' → '00_01', '합계'/공란 → None."""
    m = re.match(r"(\d{2})\s*[~-]\s*(\d{2})", label)
    if not m:
        return None
    return f"{m.group(1)}_{m.group(2)}"


def _parse_t1(rows: list[list[str]]) -> dict[str, dict]:
    """T1 페이지 12개 컬럼: 시간 | 입국장 A,B | C | D | E,F | 입국합계 | 출국 1 | 2 | 3 | 4 | 5,6 | 출국합계.

    OpenAPI 매핑: t1eg1=A,B / t1eg2=E,F / t1eg3=C / t1eg4=D
                 t1dg1~5는 1~5번, t1dg6=0(교통약자, 페이지에선 5,6 합산이지만 6번은 항상 0).
    """
    out: dict[str, dict] = {}
    for r in rows:
        if len(r) < 12:
            continue
        atime = _parse_atime(r[0])
        if atime is None:
            continue
        out[atime] = {
            "atime": atime,
            "t1eg1": _to_int(r[1]),  # A,B
            "t1eg3": _to_int(r[2]),  # C
            "t1eg4": _to_int(r[3]),  # D
            "t1eg2": _to_int(r[4]),  # E,F
            "t1egsum1": _to_int(r[5]),
            "t1dg1": _to_int(r[6]),
            "t1dg2": _to_int(r[7]),
            "t1dg3": _to_int(r[8]),
            "t1dg4": _to_int(r[9]),
            "t1dg5": _to_int(r[10]),  # 5,6 합산. 6번=교통약자=항상 0이므로 5번 단독값 근사
            "t1dg6": 0,
            "t1dgsum1": _to_int(r[11]),
        }
    return out


def _parse_t2(rows: list[list[str]]) -> dict[str, dict]:
    """T2 페이지 7개 컬럼: 시간 | 입국 A | B | 입국합계 | 출국 1 | 2 | 출국합계."""
    out: dict[str, dict] = {}
    for r in rows:
        if len(r) < 7:
            continue
        atime = _parse_atime(r[0])
        if atime is None:
            continue
        out[atime] = {
            "atime": atime,
            "t2eg1": _to_int(r[1]),
            "t2eg2": _to_int(r[2]),
            "t2egsum1": _to_int(r[3]),
            "t2dg1": _to_int(r[4]),
            "t2dg2": _to_int(r[5]),
            "t2dgsum2": _to_int(r[6]),
        }
    return out


def fetch_day(target: date) -> pd.DataFrame:
    """T1·T2 두 페이지 호출 후 OpenAPI 스키마와 동일한 DataFrame으로 병합.

    데이터가 비어있으면(미래·미지원 날짜) 빈 DataFrame 반환.
    """
    t1_rows = _fetch("T1", target)
    t2_rows = _fetch("T2", target)
    if not t1_rows and not t2_rows:
        return pd.DataFrame()
    t1 = _parse_t1(t1_rows)
    t2 = _parse_t2(t2_rows)
    atimes = sorted(set(t1.keys()) | set(t2.keys()))
    if not atimes:
        return pd.DataFrame()

    # 모든 값이 0이면 데이터 미생성으로 간주
    has_value = False
    rows = []
    adate = target.strftime("%Y%m%d")
    for atime in atimes:
        merged = {"adate": adate, "atime": atime}
        merged.update(t1.get(atime, {}))
        merged.update(t2.get(atime, {}))
        rows.append(merged)
        if any(merged.get(k, 0) > 0 for k in (
            "t1dgsum1", "t2dgsum2", "t1egsum1", "t2egsum1",
        )):
            has_value = True
    if not has_value:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 누락 컬럼 0으로 채우고 컬럼 순서 통일
    for c in ALL_COLS:
        if c not in df.columns:
            df[c] = 0 if c not in ("adate", "atime", "tmp1", "tmp2") else ""
    df = df[ALL_COLS]
    return df


def save_pickle(target: date, df: pd.DataFrame) -> Path:
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"passgr_{target.strftime('%Y%m%d')}_web.pkl"
    # pandas to_pickle 사용 — pandas 버전·환경 간 read_pickle 호환성 보장
    # (plain pickle.dump 사용 시 Render 환경에서 read_pickle 실패하던 이슈 수정)
    df.to_pickle(path)
    return path


def run() -> None:
    today = datetime.now(KST).date()
    saved = []
    skipped = []
    for off in DATE_OFFSETS:
        target = today + timedelta(days=off)
        try:
            df = fetch_day(target)
        except Exception as exc:
            print(f"[D{off:+d} {target}] error: {exc!r}")
            continue
        if df.empty:
            skipped.append(target.strftime("%Y%m%d"))
            continue
        path = save_pickle(target, df)
        saved.append(path.name)
        print(f"[D{off:+d} {target}] saved {path.name} ({len(df)} rows)")
    print(f"\nDONE saved={len(saved)} skipped={len(skipped)} ({skipped})")


if __name__ == "__main__":
    run()
